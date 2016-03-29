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
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"regexp"
	"strings"
	"sync"
)

// mdbInsertDescriptor represents an insert request.
type mdbInsertDescriptor struct {
	m     *Message // Message to be inserted
	cname string   // Name of collection to insert the message into
}

// mdbQueryDescriptor implements a QueryDescriptor, which is used as a handle
// for receiving query results and to cancel a running query.
type mdbQueryDescriptor struct {
	q          bson.M        // MongoDB query
	sort       []string      // MongoDB sort descriptor
	limit      int           // Query limit
	cname      string        // Collection name
	replyChan  chan *Message // Out: channel where query results are sent to
	errorChan  chan error    // Out: channel where possible error is sent to
	cancelChan chan bool     // In: cancellation request
	readyChan  chan bool     // Out: query result available
}

// Read returns one query result, a nil if no result is available, or an error.
func (self *mdbQueryDescriptor) Read() (m *Message, err error) {
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
func (self *mdbQueryDescriptor) Cancel() {
	select {
	case self.cancelChan <- true:
	default:
	}
}

// ReadNotify returns a buffered bool channel of size 1; a value (true) is
// sent to this channel every time after a new result is available.
func (self *mdbQueryDescriptor) ReadNotify() <-chan bool {
	return self.readyChan
}

// mdbCollection implements a Collection, which is a persistent storage for
// one queue.
type mdbCollection struct {
	name          string                    // Collection name
	insertChan    chan *mdbInsertDescriptor // Channel for insert requests (owned by the repository)
	queryChan     chan *mdbQueryDescriptor  // Channel for query requests (owned by the repository)
	replyChanSize int                       // Size of the reply channel to create
}

// Insert queues a message for insertion. The message must contain a valid
// sequence number.
func (self *mdbCollection) Insert(m *Message) {
	d := &mdbInsertDescriptor{m, self.name}
	self.insertChan <- d
}

// Query adds a query to the queue. See fdbQueryDescriptor for description
// of arguments.
func (self *mdbCollection) Query(seq int64, endseq int64, starttime Time, endtime Time, topicRx *regexp.Regexp, topicNrx *regexp.Regexp, filterExpr map[string]interface{}, cid string, limit int, first bool) (QueryDescriptor, error) {
	d := &mdbQueryDescriptor{
		cname:      self.name,
		replyChan:  make(chan *Message, self.replyChanSize),
		errorChan:  make(chan error, 1),
		cancelChan: make(chan bool, 1),
		readyChan:  make(chan bool, 1),
	}

	if seq >= 0 {
		d.q = bson.M{"seq": bson.M{"$gte": seq}}
		d.sort = []string{"seq"}
		d.limit = limit

	} else {
		d.q = bson.M{}
		d.sort = []string{"-seq"}
		d.limit = int(-seq)
	}

	if cid != "" {
		d.q["sender"] = bson.M{"$ne": cid}
	}

	if topicRx != nil {
		d.q["topic"] = bson.RegEx{topicRx.String(), ""}
	}

	if topicNrx != nil {
		d.q = bson.M{"$and": []bson.M{d.q, {"topic": bson.M{"$not": bson.RegEx{topicNrx.String(), ""}}}}}
	}

	if endseq >= 0 {
		d.q = bson.M{"$and": []bson.M{d.q, {"seq": bson.M{"$lte": endseq}}}}
	}

	if !starttime.IsZero() {
		d.q["endtime"] = bson.M{"$gt": starttime.String()}
	}

	if !endtime.IsZero() {
		d.q = bson.M{"$and": []bson.M{d.q, {"endtime": bson.M{"$lte": endtime.String()}}}}
	}

	if filterExpr != nil {
		d.q = bson.M{"$and": []bson.M{d.q, filterExpr}}
	}

	select {
	case self.queryChan <- d:
	default:
		return nil, errors.New("too many pending queries")
	}

	return d, nil
}

// NumQueriesQueued returns the number of queries that are queued and not yet
// executing.
func (self *mdbCollection) NumQueriesQueued() int {
	return len(self.queryChan)
}

// OldestData returns the sequence number and end time of the first message
// in collection.
func (self *mdbCollection) OldestData() (int64, Time) {
	if d, err := self.Query(0, 0, Time{}, Time{}, nil, nil, nil, "", 1, true); err != nil {
		log.Println(err)

	} else {
		<-d.ReadNotify()

		if m, err := d.Read(); err == nil {
			return m.Seq.Value, m.Endtime

		} else if err != io.EOF {
			log.Println(err)
		}
	}

	return -1, Time{}
}

// mdbRepository implements Repository, which is persistent storage for one
// bus.
type mdbRepository struct {
	db             *mgo.Database             // Associated Mongo database
	collectionSize int                       // Size of capped collection
	insertChan     chan *mdbInsertDescriptor // Channel for insert requests (passed on to collections)
	queryChan      chan *mdbQueryDescriptor  // Channel for query requests (passed on to collections)
	replyChanSize  int                       // Size of the reply channel to create (passed on to collections)
	waitGroup      *sync.WaitGroup           // WaitGroup for shutdown
}

// mdbNewRepository creates a new mdbRepository object.
func mdbNewRepository(mgoSession *mgo.Session, name string, collectionSize int, insertsParallel int, insertsQueued int, queriesParallel int, queriesQueued int, replyChanSize int, waitGroup *sync.WaitGroup) (*mdbRepository, error) {
	self := &mdbRepository{
		db:             mgoSession.DB(name),
		collectionSize: collectionSize,
		insertChan:     make(chan *mdbInsertDescriptor, insertsQueued),
		queryChan:      make(chan *mdbQueryDescriptor, queriesQueued),
		replyChanSize:  replyChanSize,
		waitGroup:      waitGroup,
	}

	for i := 0; i < insertsParallel; i++ {
		self.waitGroup.Add(1)
		go self.insertTask(mgoSession.Copy().DB(name))
	}

	for i := 0; i < queriesParallel; i++ {
		go self.queryTask(mgoSession.Copy().DB(name))
	}

	return self, nil
}

// insertTask is a goroutine that waits for insert requests on the insertChan
// and executes those. If the insert channel reaches EOF (eg., it is empty and
// has been closed), insertTask decrements the WaitGroup counter and quits.
// The latter is needed by the repository to ensure that pending inserts have
// finished when shutting down.
func (self *mdbRepository) insertTask(db *mgo.Database) {
	for {
		d, ok := <-self.insertChan

		if !ok {
			self.waitGroup.Done()
			break
		}

		err := db.C(d.cname).Insert(d.m)

		if err != nil {
			log.Println(err)
		}
	}
}

// queryTask is a goroutine that waits for query requests on the queryChan
// and executes those.
func (self *mdbRepository) queryTask(db *mgo.Database) {
	for {
		d := <-self.queryChan
		q := db.C(d.cname).Find(d.q)

		if len(d.sort) > 0 {
			q = q.Sort(d.sort...)
		}

		if d.limit > 0 {
			q = q.Limit(d.limit)
		}

		iter := q.Iter()
		count := 0
		finished := false

	loop:
		for {
			m := &Message{}

			if !iter.Next(m) {
				finished = true
				break
			}

			select {
			case <-d.cancelChan:
				finished = true
				break loop

			default:
			}

			select {
			case <-d.cancelChan:
				finished = true
				break loop

			case d.replyChan <- m:

			default:
				break loop
			}

			select {
			case d.readyChan <- true:
			default:
			}

			count++
		}

		err := iter.Close()

		if err == nil {
			if !finished || (d.limit > 0 && count >= d.limit) {
				err = ECONTINUE

			} else {
				err = io.EOF
			}
		}

		select {
		case d.errorChan <- err:
		default:
		}

		select {
		case d.readyChan <- true:
		default:
		}
	}
}

// InitCollection opens a new collection. Use Collection() to get a reference
// to a collection that is already known to the bus.
func (self *mdbRepository) InitCollection(name string) (Collection, error) {
	C := self.db.C(name)

	if err := C.Create(&mgo.CollectionInfo{Capped: true, MaxBytes: self.collectionSize * 1024 * 1024}); err != nil {
		return nil, err
	}

	if err := C.EnsureIndex(mgo.Index{Key: []string{"seq"}, Unique: true}); err != nil {
		return nil, err
	}

	if err := C.EnsureIndex(mgo.Index{Key: []string{"endtime"}}); err != nil {
		return nil, err
	}

	return &mdbCollection{name, self.insertChan, self.queryChan, self.replyChanSize}, nil
}

// CollectionNames returns a list of collection names.
func (self *mdbRepository) CollectionNames() ([]string, error) {
	if names, err := self.db.CollectionNames(); err != nil {
		return nil, err

	} else {
		result := make([]string, 0, len(names))

		for _, name := range names {
			if !strings.HasPrefix(name, "system.") {
				result = append(result, name)
			}
		}

		return result, nil
	}
}

// Collection returns a reference to an existing collection.
func (self *mdbRepository) Collection(name string) Collection {
	return &mdbCollection{name, self.insertChan, self.queryChan, self.replyChanSize}
}

// Shutdown tells the repository to shut down after all pending inserts are
// finished.
func (self *mdbRepository) Shutdown() {
	close(self.insertChan)
}

// mdbRepositoryFactory implements RepositoryFactory, whose job is to create
// repositories.
type mdbRepositoryFactory struct {
	mgoSession      *mgo.Session    // Associated Mongo session
	collectionSize  int             // Size of capped collections
	insertsParallel int             // Number of insert goroutines
	insertsQueued   int             // Size of insert channel
	queriesParallel int             // Number of query goroutines
	queriesQueued   int             // Size of query channel
	replyChanSize   int             // Size of reply channel
	waitGroup       *sync.WaitGroup // WaitGroup for shutdown
}

// NewMongoRepositoryFactory creates a new mdbRepositoryFactory mdbRepositoryFactory object.
func NewMongoRepositoryFactory(mgoSession *mgo.Session, collectionSize int, insertsParallel int, insertsQueued int, queriesParallel int, queriesQueued int, replyChanSize int) (RepositoryFactory, error) {
	self := &mdbRepositoryFactory{
		mgoSession:      mgoSession,
		collectionSize:  collectionSize,
		insertsParallel: insertsParallel,
		insertsQueued:   insertsQueued,
		queriesParallel: queriesParallel,
		queriesQueued:   queriesQueued,
		replyChanSize:   replyChanSize,
		waitGroup:       &sync.WaitGroup{},
	}

	return self, nil
}

// Repository creates a new mdbRepository object.
func (self *mdbRepositoryFactory) Repository(name string) (Repository, error) {
	return mdbNewRepository(self.mgoSession, name, self.collectionSize, self.insertsParallel, self.insertsQueued, self.queriesParallel, self.queriesQueued, self.replyChanSize, self.waitGroup)
}

// Shutdown waits for pending inserts to finish. The Shutdown() method of each
// repository must have been called beforehand.
func (self *mdbRepositoryFactory) Shutdown() {
	self.waitGroup.Wait()
}
