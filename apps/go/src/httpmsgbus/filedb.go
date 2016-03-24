/***************************************************************************
 *   Copyright (C) by GFZ Potsdam                                          *
 *                                                                         *
 *   Author:  Andres Heinloo                                               *
 *   Email:   andres@gfz-potsdam.de                                        *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2, or (at your option)   *
 *   any later version. For more information, see http://www.gnu.org/      *
 ***************************************************************************/

package main

import (
	"errors"
	"fmt"
	"github.com/golang/groupcache/lru"
	"github.com/golang/protobuf/proto"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
)

// fdbInsertDescriptor represents an insert request.
type fdbInsertDescriptor struct {
	m    *Message       // Message to be inserted
	coll *fdbCollection // Collection to insert the message into
}

// fdbQueryDescriptor implements a QueryDescriptor, which is used as a handle
// for receiving query results and to cancel a running query.
type fdbQueryDescriptor struct {
	seq        int64          // Starting sequence number
	endseq     int64          // Ending sequence number (-1 if none)
	starttime  Time           // Starting time (zero time if none)
	endtime    Time           // Ending time (zero time if none)
	topicRx    *regexp.Regexp // Topic must match this
	topicNrx   *regexp.Regexp // Topic must NOT match this
	filter     *Filter        // Pointer to a Filter or nil
	cid        string         // Client ID (sender must NOT equal to this)
	limit      int            // Query limit
	first      bool           // First query or continuation (for optimization)
	coll       *fdbCollection // Collection to query
	replyChan  chan *Message  // Out: channel where query results are sent to
	errorChan  chan error     // Out: channel where possible error is sent to
	cancelChan chan bool      // In: cancellation request
	notifyChan chan bool      // Out: query result available
}

// Read returns one query result, a nil if no result is available, or an error.
func (self *fdbQueryDescriptor) Read() (m *Message, err error) {
	select {
	case m = <-self.replyChan:
		return
	default:
	}

	select {
	case err = <-self.errorChan:
		return
	default:
	}

	return
}

// Cancel cancels a running query.
func (self *fdbQueryDescriptor) Cancel() {
	select {
	case self.cancelChan <- true:
	default:
	}
}

// ReadNotify returns a buffered bool channel of size 1; a value (true) is
// sent to this channel every time after a new result is available.
func (self *fdbQueryDescriptor) ReadNotify() <-chan bool {
	return self.notifyChan
}

// fdbCache is used to manage open file handles.
type fdbCache struct {
	c     *lru.Cache // Cache (not thread-safe)
	mutex sync.Mutex // Mutex to protect cache from concurrent access
}

// fdbNewCache creates a new fdbCache object.
func fdbNewCache(size int) *fdbCache {
	self := &fdbCache{c: lru.New(size)}

	self.c.OnEvicted = func(k lru.Key, v interface{}) {
		go k.(*fdbFile).evict()
	}

	return self
}

// Add adds a new fdbFile under cache control. If the cache is full, least
// recently used file handle will be closed.
func (self *fdbCache) Add(f *fdbFile) {
	self.mutex.Lock()
	self.c.Add(f, nil)
	self.mutex.Unlock()
}

// Remove removes a file from cache. It is called when the file is deleted.
func (self *fdbCache) Remove(f *fdbFile) {
	self.mutex.Lock()
	self.c.Remove(f)
	self.mutex.Unlock()
}

// Refresh moves an existing entry to the front.
func (self *fdbCache) Refresh(f *fdbFile) {
	self.mutex.Lock()
	self.c.Get(f)
	self.mutex.Unlock()
}

// fdbFile represents one file in collection.
type fdbFile struct {
	path         string         // Path to the file on disk
	baseseq      int64          // Sequence number of the first block in the file
	blocksize    int            // Size of one block
	startseq     int64          // Sequence number of the first message stored in the file
	endseq       int64          // Sequence number of the last message stored in the file + 1
	startendtime Time           // End time of the first message stored in the file
	endtime      Time           // End time of the last message stored in the file
	fd           *os.File       // File handle (nil if not open)
	fdc          *fdbCache      // Cache to manage open file handles
	bufferPool   *sync.Pool     // Pool of reusable buffers of block size
	wg           sync.WaitGroup // WaitGroup for cache
	mutex        sync.Mutex     // Mutex for cache
}

// fdbNewFile creates a new fdbFile object. If the corresponding file exists
// on disk, it is used to initialize the object.
func fdbNewFile(path string, baseseq int64, blocksize int, fdc *fdbCache, bufferPool *sync.Pool) (*fdbFile, error) {
	self := &fdbFile{
		path:       path,
		baseseq:    baseseq,
		blocksize:  blocksize,
		startseq:   -1,
		fdc:        fdc,
		bufferPool: bufferPool,
	}

	if err := self.ensureOpen(); err != nil {
		return nil, err

	} else {
		defer self.done()
	}

	if fi, err := self.fd.Stat(); err != nil {
		return nil, err

	} else if size := fi.Size(); size == 0 {
		return self, nil

	} else if size%int64(blocksize) != 0 {
		return nil, errors.New(fmt.Sprintf("%s has invalid size %d", path, size))

	} else {
		var pos int64 = 0

		buf := self.bufferPool.Get().([]byte)
		defer self.bufferPool.Put(buf)

		pb := proto.NewBuffer(nil)

		for {
			if _, err := self.fd.ReadAt(buf, pos); err != nil {
				return nil, errors.New(fmt.Sprintf("error reading %s: %s", path, err.Error()))

			} else if m, err := DecodeNative(buf, pb); err != nil {
				return nil, err

			} else if m != nil {
				self.startseq = m.Seq.Value
				self.startendtime = m.Endtime
				break
			}

			pos += int64(blocksize)
		}

		if _, err := self.fd.ReadAt(buf, size-int64(blocksize)); err != nil {
			return nil, errors.New(fmt.Sprintf("error reading %s: %s", path, err.Error()))

		} else if m, err := DecodeNative(buf, pb); err != nil {
			return nil, err

		} else if m != nil {
			self.endseq = m.Seq.Value + 1
			self.endtime = m.Endtime
		}
	}

	return self, nil
}

// ensureOpen makes sure that the file is open and is not going to be closed
// until done() is called.
func (self *fdbFile) ensureOpen() error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.fd == nil {
		var err error
		self.fd, err = os.OpenFile(self.path, os.O_RDWR|os.O_CREATE, os.FileMode(0666))

		if err != nil {
			return err
		}

		self.fdc.Add(self)

	} else {
		self.fdc.Refresh(self)
	}

	self.wg.Add(1)
	return nil
}

// done reverts the effect of ensureOpen(). The file can be closed whenever
// a file handle is needed.
func (self *fdbFile) done() {
	self.wg.Done()
}

// evict is a callback from cache to close the file handle.
func (self *fdbFile) evict() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.wg.Wait()

	if self.fd == nil {
		panic("self.fd == nil")

	} else if err := self.fd.Close(); err != nil {
		panic(err)
	}

	self.fd = nil
}

// Delete deletes the file physically from disk. Note: file handle is not
// closed here, because that is done by evict(), which is triggered by
// self.fdc.Remove(self).
func (self *fdbFile) Delete() error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.fd != nil {
		self.fdc.Remove(self)
	}

	return os.Remove(self.path)
}

// Put writes a message into the file at a position determined by the sequence
// number.
func (self *fdbFile) Put(m *Message) error {
	if err := self.ensureOpen(); err != nil {
		return err

	} else {
		defer self.done()
	}

	// Get a temporary buffer from pool. Reusing buffers is supposed to be
	// more efficient than allocating new buffers all the time. The size
	// of buffer equals self.blocksize.
	buf := self.bufferPool.Get().([]byte)
	defer self.bufferPool.Put(buf)

	pb := proto.NewBuffer(nil)

	if n, err := EncodeNative(m, buf, pb); err != nil {
		return err

	} else {
		if m.Seq.Value >= self.endseq {
			// "Truncate" might be misleading. We are actually making
			// the file larger. Note that this may create a sparse file.
			if err := self.fd.Truncate((m.Seq.Value - self.baseseq + 1) * int64(self.blocksize)); err != nil {
				return err
			}
		}

		if _, err := self.fd.WriteAt(buf[:n], (m.Seq.Value-self.baseseq)*int64(self.blocksize)); err != nil {
			return err
		}

		if self.startseq == -1 {
			self.startseq = m.Seq.Value
			self.endseq = m.Seq.Value + 1
			self.startendtime = m.Endtime
			self.endtime = m.Endtime

		} else if m.Seq.Value < self.startseq {
			self.startseq = m.Seq.Value
			self.startendtime = m.Endtime

		} else if m.Seq.Value+1 > self.endseq {
			self.endseq = m.Seq.Value + 1
			self.endtime = m.Endtime
		}
	}

	return nil
}

// Read reads the file at position determined by startseq.
func (self *fdbFile) Read(buf []byte, startseq int64) (int, error) {
	if err := self.ensureOpen(); err != nil {
		return 0, err

	} else {
		defer self.done()
	}

	n := int(self.endseq-startseq) * self.blocksize

	if n > len(buf) {
		n = len(buf)
	}

	if n, err := self.fd.ReadAt(buf[:n], (startseq-self.baseseq)*int64(self.blocksize)); err != nil {
		return 0, errors.New(fmt.Sprintf("error reading %s: %s", self.path, err.Error()))

	} else {
		return n, nil
	}
}

// FindTime tries to find a sequence number of a message containing time t,
// assuming that the messages are sorted by endtime. There is some heuristics
// involved.
func (self *fdbFile) FindTime(t Time) (int64, error) {
	buf := self.bufferPool.Get().([]byte)
	defer self.bufferPool.Put(buf)

	pb := proto.NewBuffer(nil)

	endseq := self.endseq
	endtime := self.endtime

	for {
		if endtime.Sub(self.startendtime.Time) <= 0 ||
			t.Sub(self.startendtime.Time) <= 0 {
			return self.startseq, nil
		}

		seq := self.startseq +
			int64(float64((endseq-self.startseq))*
				float64(t.Sub(self.startendtime.Time))/
				float64(endtime.Sub(self.startendtime.Time))) - 100

		if seq < 0 {
			seq = 0
		}

		if seq <= self.startseq {
			return self.startseq, nil

		} else if seq >= endseq {
			return endseq - 1, nil
		}

		var m *Message
		var err error

		for {
			_, err = self.Read(buf, seq)

			if err != nil {
				return 0, err
			}

			m, err = DecodeNative(buf, pb)

			if err != nil {
				return 0, err
			}

			if m != nil {
				break
			}

			seq++
		}

		if m.Endtime.IsZero() {
			return self.startseq, nil

		} else if m.Endtime.Sub(t.Time) <= 0 {
			return m.Seq.Value, nil

		} else if m.Seq.Value == endseq {
			return m.Seq.Value, nil

		} else {
			endseq = m.Seq.Value
			endtime = m.Starttime
		}
	}
}

// fdbRing implements a simple ringbuffer to store files with sequence numbers
// baseseq..baseseq+len(buf)-1. Sequence number of a file is baseseq divided by
// blocks per file.
type fdbRing struct {
	buf     []*fdbFile
	baseidx int
	baseseq int64
	seq     int64
}

// fdbNewRing creates a new fdbRing object.
func fdbNewRing(size int) *fdbRing {
	return &fdbRing{buf: make([]*fdbFile, size)}
}

// Put adds a file to the ring, given that seq >= baseseq.
// If seq > baseseq+len(buf)-1, then older messages are removed as needed
// and baseseq is updated accordingly.
func (self *fdbRing) Put(seq int64, file *fdbFile) {
	if seq < self.baseseq {
		return

	} else if seq >= self.seq+int64(len(self.buf)) {
		for i, s := self.baseidx, self.baseseq; s < self.seq; i, s = (i+1)%len(self.buf), s+1 {
			if self.buf[i] != nil {
				self.buf[i].Delete()
				self.buf[i] = nil
			}
		}

		self.baseidx = 0
		self.baseseq = seq
		self.buf[0] = file

	} else {
		for seq >= self.baseseq+int64(len(self.buf)) {
			if self.buf[self.baseidx] != nil {
				self.buf[self.baseidx].Delete()
				self.buf[self.baseidx] = nil
			}

			self.baseidx = (self.baseidx + 1) % len(self.buf)
			self.baseseq++
		}

		i := (int64(self.baseidx) + seq - self.baseseq) % int64(len(self.buf))
		self.buf[i] = file
	}

	if seq+1 > self.seq {
		self.seq = seq + 1
	}
}

// Get returns a file with given sequence number. If no such file exists,
// the next file with closest sequence number is returned. If that also fails,
// a nil file is returned.
func (self *fdbRing) Get(seq int64) (int64, *fdbFile) {
	if seq < self.baseseq {
		seq = self.baseseq
	}

	for s := seq; s < self.seq; s++ {
		i := (int64(self.baseidx) + s - self.baseseq) % int64(len(self.buf))

		if self.buf[i] != nil {
			return s, self.buf[i]
		}
	}

	return 0, nil
}

// fdbCollection implements a Collection, which is a persistent storage for
// one queue.
type fdbCollection struct {
	path          string                    // Path to directory containing the files of this collection
	blocksPerFile int                       // Number of blocks in one file
	blocksize     int                       // Size of one block (maximum message size)
	endseq        int64                     // Sequence number of the last message in collection + 1
	insertChan    chan *fdbInsertDescriptor // Channel for insert requests (owned by the repository)
	queryChan     chan *fdbQueryDescriptor  // Channel for query requests (owned by the repository)
	replyChanSize int                       // Size of the reply channel to create
	fdc           *fdbCache                 // Cache passed on to files created (owned by the repository)
	ring          *fdbRing                  // Our file ring
	bufferPool    *sync.Pool                // Buffer pool (owned by the repository)
	rwmutex       sync.RWMutex              // Protects the collection from concurrent reads and writes
}

// fdbNewCollection creates a new fdbCollection object. If the corresponding
// collection exists on disk, it is used to initialize the object.
func fdbNewCollection(path string, numberOfFiles int, blocksPerFile int, blocksize int, insertChan chan *fdbInsertDescriptor, queryChan chan *fdbQueryDescriptor, replyChanSize int, fdc *fdbCache, bufferPool *sync.Pool) (*fdbCollection, error) {
	self := &fdbCollection{
		path:          path,
		blocksPerFile: blocksPerFile,
		blocksize:     blocksize,
		insertChan:    insertChan,
		queryChan:     queryChan,
		replyChanSize: replyChanSize,
		fdc:           fdc,
		ring:          fdbNewRing(numberOfFiles),
		bufferPool:    bufferPool,
	}

	if err := os.MkdirAll(path, os.FileMode(0777)); err != nil {
		return nil, err
	}

	if entries, err := ioutil.ReadDir(path); err != nil {
		return nil, err

	} else {
		names := make([]string, 0, numberOfFiles)

		for _, fi := range entries {
			names = append(names, fi.Name())
		}

		sort.Strings(names)

		for _, name := range names {
			if baseseq, err := strconv.ParseInt(name, 16, 64); err != nil {
				return nil, err

			} else if file, err := fdbNewFile(self.path+"/"+name, baseseq, blocksize, fdc, bufferPool); err != nil {
				return nil, err

			} else {
				self.ring.Put(baseseq/int64(blocksPerFile), file)

				if file.endseq > self.endseq {
					self.endseq = file.endseq
				}
			}
		}
	}

	return self, nil
}

// Insert queues a message for insertion. The message must contain a valid
// sequence number.
func (self *fdbCollection) Insert(m *Message) {
	d := &fdbInsertDescriptor{m, self}
	self.insertChan <- d
}

// _insert performs the actual insert operation.
func (self *fdbCollection) _insert(d *fdbInsertDescriptor) error {
	self.rwmutex.Lock()
	defer self.rwmutex.Unlock()

	baseseq := d.m.Seq.Value - (d.m.Seq.Value % int64(self.blocksPerFile))
	fseq, file := self.ring.Get(baseseq / int64(self.blocksPerFile))

	if file != nil && fseq*int64(self.blocksPerFile) != file.baseseq {
		panic("file ring is corrupt")
	}

	if file == nil {
		var err error
		file, err = fdbNewFile(fmt.Sprintf("%s/%016x", self.path, baseseq), baseseq, self.blocksize, self.fdc, self.bufferPool)

		if err != nil {
			return err
		}

		self.ring.Put(baseseq/int64(self.blocksPerFile), file)

	} else if file.baseseq > baseseq {
		return nil
	}

	if err := file.Put(d.m); err != nil {
		return err
	}

	if d.m.Seq.Value+1 > self.endseq {
		self.endseq = d.m.Seq.Value + 1
	}

	return nil
}

// Query adds a query to the queue. See fdbQueryDescriptor for description
// of arguments.
func (self *fdbCollection) Query(seq int64, endseq int64, starttime Time, endtime Time, topicRx *regexp.Regexp, topicNrx *regexp.Regexp, filterExpr map[string]interface{}, cid string, limit int, first bool) (QueryDescriptor, error) {
	d := &fdbQueryDescriptor{
		seq:        seq,
		endseq:     endseq,
		starttime:  starttime,
		endtime:    endtime,
		topicRx:    topicRx,
		topicNrx:   topicNrx,
		cid:        cid,
		limit:      limit,
		first:      first,
		coll:       self,
		replyChan:  make(chan *Message, self.replyChanSize),
		errorChan:  make(chan error, 1),
		cancelChan: make(chan bool, 1),
		notifyChan: make(chan bool, 1),
	}

	if filterExpr != nil {
		var err error
		d.filter, err = NewFilter(filterExpr)

		if err != nil {
			return nil, err
		}
	}

	select {
	case self.queryChan <- d:
	default:
		return nil, errors.New("too many pending queries")
	}

	return d, nil
}

// _query1 is a helper method that finds the file based on seq and d.starttime
// and reads up to len(buf) bytes from that file. The actual starting sequence
// number of the buffer and number of bytes read, or possibly an error is
// returned.
func (self *fdbCollection) _query1(seq int64, d *fdbQueryDescriptor, buf []byte) (int64, int, error) {
	self.rwmutex.RLock()
	defer self.rwmutex.RUnlock()

	if seq < 0 {
		seq += self.endseq

		if seq < 0 {
			seq = 0
		}
	}

	baseseq := seq - (seq % int64(self.blocksPerFile))

	var file *fdbFile
	var fseq int64

	for {
		fseq, file = self.ring.Get(baseseq / int64(self.blocksPerFile))

		if file != nil && fseq*int64(self.blocksPerFile) != file.baseseq {
			panic("file ring is corrupt")
		}

		if file == nil {
			return seq, 0, io.EOF
		}

		if seq >= file.endseq {
			baseseq = file.baseseq + int64(self.blocksPerFile)
			continue
		}

		if d.first && !d.starttime.IsZero() {
			if file.endtime.Sub(d.starttime.Time) < 0 {
				baseseq = file.baseseq + int64(self.blocksPerFile)
				continue
			}

			var err error
			seq, err = file.FindTime(d.starttime)

			if err != nil {
				return seq, 0, err
			}

		} else if seq < file.startseq {
			seq = file.startseq
		}

		break
	}

	if n, err := file.Read(buf, seq); err != nil {
		return seq, 0, err

	} else {
		return seq, n, nil
	}
}

// query performs the actual query operation and sends resulting messages to
// self.queryChan. The return value ends up in self.errorChan.
func (self *fdbCollection) _query(d *fdbQueryDescriptor, buf []byte, pb *proto.Buffer) error {
	seq := d.seq
	count := 0

	for {
		var n int
		var err error

		seq, n, err = self._query1(seq, d, buf)

		if err != nil {
			return err
		}

		d.first = false

		for i := 0; i < n; i += self.blocksize {
			if m, err := DecodeNative(buf[i:i+self.blocksize], pb); err != nil {
				return err

			} else if m == nil {
				seq++
				continue

			} else if (d.endseq >= 0 && m.Seq.Value > d.endseq) ||
				(!m.Starttime.IsZero() && !d.endtime.IsZero() && m.Endtime.Sub(d.endtime.Time) > 0) {
				return io.EOF

			} else if (m.Sender == "" || m.Sender != d.cid) &&
				(m.Endtime.IsZero() || d.starttime.IsZero() || m.Endtime.Sub(d.starttime.Time) > 0) &&
				(d.topicRx == nil || d.topicRx.MatchString(m.Topic)) &&
				(d.topicNrx == nil || !d.topicNrx.MatchString(m.Topic)) &&
				(d.filter == nil || d.filter.Match(m)) {

				// If something has been written to cancelChan, quit.
				select {
				case <-d.cancelChan:
					return io.EOF

				default:
				}

				// Try to send the result to replyChan. Return ECONTINUE
				// if the channel is full.
				select {
				case d.replyChan <- m:

				default:
					return ECONTINUE
				}

				// Send notification to the reader. The length of
				// notifyChan is 1; if the channel is already full, then
				// the new notification is simply dropped.
				select {
				case d.notifyChan <- true:
				default:
				}

				count++

				if d.limit > 0 && count >= d.limit {
					return ECONTINUE
				}
			}

			seq++
		}
	}

	return io.EOF
}

// NumQueriesQueued returns the number of queries that are queued and not yet
// executing.
func (self *fdbCollection) NumQueriesQueued() int {
	return len(self.queryChan)
}

// OldestData returns the sequence number and end time of the first message
// in collection.
func (self *fdbCollection) OldestData() (int64, Time) {
	self.rwmutex.RLock()
	defer self.rwmutex.RUnlock()

	if _, file := self.ring.Get(0); file != nil {
		return file.startseq, file.startendtime

	} else {
		return -1, Time{}
	}
}

// fdbRepository implements Repository, which is persistent storage for one
// bus.
type fdbRepository struct {
	path          string                    // Path to directory containing collections of this repository
	numberOfFiles int                       // Number of files in collection
	blocksPerFile int                       // Number of blocks in one file
	blocksize     int                       // Size of one block (maximum message size)
	bufsize       int                       // Size of read buffer to allocate
	collections   map[string]*fdbCollection // Collections managed by the repository
	insertChan    chan *fdbInsertDescriptor // Channel for insert requests (passed on to collections)
	queryChan     chan *fdbQueryDescriptor  // Channel for query requests (passed on to collections)
	replyChanSize int                       // Size of the reply channel to create (passed on to collections)
	fdc           *fdbCache                 // Cache passed on to collections
	bufferPool    *sync.Pool                // Buffer pool passed on to collections
	waitGroup     *sync.WaitGroup           // WaitGroup for shutdown
}

// fdbNewRepository creates a new fdbRepository object. If the corresponding
// repository exists on disk, it is used to initialize the object.
func fdbNewRepository(path string, numberOfFiles int, blocksPerFile int, blocksize int, bufsize int, insertsParallel int, insertsQueued int, queriesParallel int, queriesQueued int, replyChanSize int, fdc *fdbCache, bufferPool *sync.Pool, waitGroup *sync.WaitGroup) (*fdbRepository, error) {
	self := &fdbRepository{
		path:          path,
		numberOfFiles: numberOfFiles,
		blocksPerFile: blocksPerFile,
		blocksize:     blocksize,
		bufsize:       bufsize,
		collections:   map[string]*fdbCollection{},
		insertChan:    make(chan *fdbInsertDescriptor, insertsQueued),
		queryChan:     make(chan *fdbQueryDescriptor, queriesQueued),
		replyChanSize: replyChanSize,
		fdc:           fdc,
		bufferPool:    bufferPool,
		waitGroup:     waitGroup,
	}

	if err := os.MkdirAll(path, os.FileMode(0777)); err != nil {
		return nil, err
	}

	if entries, err := ioutil.ReadDir(path); err != nil {
		return nil, err

	} else {
		for _, fi := range entries {
			if _, err := self.InitCollection(fi.Name()); err != nil {
				return nil, err
			}
		}
	}

	if rem := bufsize % blocksize; rem > 0 {
		bufsize += blocksize - rem
	}

	for i := 0; i < insertsParallel; i++ {
		self.waitGroup.Add(1)
		go self.insertTask()
	}

	for i := 0; i < queriesParallel; i++ {
		go self.queryTask(make([]byte, bufsize), proto.NewBuffer(nil))
	}

	return self, nil
}

// insertTask is a goroutine that waits for insert requests on the insertChan
// and executes those. If the insert channel reaches EOF (eg., it is empty and
// has been closed), insertTask decrements the WaitGroup counter and quits.
// The latter is needed by the repository to ensure that pending inserts have
// finished when shutting down.
func (self *fdbRepository) insertTask() {
	for {
		d, ok := <-self.insertChan

		if !ok {
			self.waitGroup.Done()
			break
		}

		err := d.coll._insert(d)

		if err != nil {
			log.Println(err)
		}
	}
}

// queryTask is a goroutine that waits for query requests on the queryChan
// and executes those.
func (self *fdbRepository) queryTask(buf []byte, pb *proto.Buffer) {
	for {
		d := <-self.queryChan
		err := d.coll._query(d, buf, pb)

		if err != nil {
			select {
			case d.errorChan <- err:
			default:
			}

			select {
			case d.notifyChan <- true:
			default:
			}
		}
	}
}

// InitCollection opens a new collection. Use Collection() to get a reference
// to a collection that is already known to the bus.
func (self *fdbRepository) InitCollection(name string) (Collection, error) {
	log.Println("opening", self.path+"/"+name)

	if coll, err := fdbNewCollection(self.path+"/"+name, self.numberOfFiles, self.blocksPerFile, self.blocksize, self.insertChan, self.queryChan, self.replyChanSize, self.fdc, self.bufferPool); err != nil {
		return nil, err

	} else {
		self.collections[name] = coll
		return coll, nil
	}
}

// CollectionNames returns a list of collection names.
func (self *fdbRepository) CollectionNames() ([]string, error) {
	result := make([]string, 0, len(self.collections))

	for name := range self.collections {
		result = append(result, name)
	}

	return result, nil
}

// Collection returns a reference to an existing collection.
func (self *fdbRepository) Collection(name string) Collection {
	if coll, ok := self.collections[name]; !ok {
		panic("collection '" + name + "' not found")

	} else {
		return coll
	}
}

// Shutdown tells the repository to shut down after all pending inserts are
// finished.
func (self *fdbRepository) Shutdown() {
	close(self.insertChan)
}

// fdbRepositoryFactory implements RepositoryFactory, whose job is to create
// repositories.
type fdbRepositoryFactory struct {
	path            string          // Path to base directory containing repositories
	numberOfFiles   int             // Number of files in collection
	blocksPerFile   int             // Number of blocks in one file
	blocksize       int             // Size of one block (maximum message size)
	bufsize         int             // Size of read buffer to allocate
	insertsParallel int             // Number of insert goroutines
	insertsQueued   int             // Size of insert channel
	queriesParallel int             // Number of query goroutines
	queriesQueued   int             // Size of query channel
	replyChanSize   int             // Size of reply channel
	fdc             *fdbCache       // Cache to manage open file handles
	bufferPool      *sync.Pool      // Pool of reusable buffers of block size
	waitGroup       *sync.WaitGroup // WaitGroup for shutdown
}

// NewFileRepositoryFactory creates a new fdbRepositoryFactory object.
func NewFileRepositoryFactory(path string, numberOfFiles int, blocksPerFile int, blocksize int, bufsize int, maxOpenFiles int, insertsParallel int, insertsQueued int, queriesParallel int, queriesQueued int, replyChanSize int) (RepositoryFactory, error) {
	self := &fdbRepositoryFactory{
		path:            path,
		numberOfFiles:   numberOfFiles,
		blocksPerFile:   blocksPerFile,
		blocksize:       blocksize,
		bufsize:         bufsize,
		insertsParallel: insertsParallel,
		insertsQueued:   insertsQueued,
		queriesParallel: queriesParallel,
		queriesQueued:   queriesQueued,
		replyChanSize:   replyChanSize,
		fdc:             fdbNewCache(maxOpenFiles),
		bufferPool:      &sync.Pool{New: func() interface{} { return make([]byte, blocksize) }},
		waitGroup:       &sync.WaitGroup{},
	}

	if err := os.MkdirAll(path, os.FileMode(0777)); err != nil {
		return nil, err
	}

	return self, nil
}

// Repository creates a new fdbRepository object.
func (self *fdbRepositoryFactory) Repository(name string) (Repository, error) {
	return fdbNewRepository(self.path+"/"+name, self.numberOfFiles, self.blocksPerFile, self.blocksize, self.bufsize, self.insertsParallel, self.insertsQueued, self.queriesParallel, self.queriesQueued, self.replyChanSize, self.fdc, self.bufferPool, self.waitGroup)
}

// Shutdown waits for pending inserts to finish. The Shutdown() method of each
// repository must have been called beforehand.
func (self *fdbRepositoryFactory) Shutdown() {
	self.waitGroup.Wait()
}
