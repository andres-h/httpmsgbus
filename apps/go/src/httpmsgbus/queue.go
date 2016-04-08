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
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// Message.Topic must match this.
	regexTopicName = regexp.MustCompile("^[\\w\\d_]*$")

	// Each element of OpenParamQueue.Topics must match this
	// (metacharacters '!', '?' and '*' allowed).
	regexTopicPattern = regexp.MustCompile("^!?[\\w\\d\\?\\*_]*$")
)

// QueueSubscription holds the data that is shared between a Session and a
// Queue (A Session can use multiple Queues and a Queue can be used by multiple
// Sessions). See Session.notify() for notifier explanation.
type QueueSubscription struct {
	seq       int64              // Current sequence number, 64-bit aligned for atomic
	queue     *Queue             // The queue
	notifier  func(string, bool) // Function to be called when a new message is added
	cid       string             // Client ID (sender must NOT equal to this)
	topics    []string           // Queue parameter from /open: topics
	topicRx   *regexp.Regexp     // Topic must match this
	topicNrx  *regexp.Regexp     // Topic must NOT match this
	endseq    int64              // Queue parameter from /open: endseq (-1 if unset)
	starttime Time               // Queue parameter from /open: starttime
	endtime   Time               // Queue parameter from /open: endtime
	filter    *Filter            // Queue parameter from /open: filter (parsed)
	qlen      int                // Queue parameter from /open: qlen
	oowait    int                // Queue parameter from /open: oowait
	keep      bool               // Queue parameter from /open: keep
	seedlink  bool               // Queue parameter from /open: seedlink
	startwait time.Time          // Used in connection with oowait
	query     QueryDescriptor    // Current query descriptor or nil
	gotData   bool               // Helps to optimize filedb query
	mutex     sync.Mutex         // Protects Pull() from concurrent access
}

// QueueName returns the name of the associated queue.
func (self *QueueSubscription) QueueName() string {
	return self.queue.Name()
}

// Pull gets the next message (pointed by self.seq) from the queue. If a
// message with wanted seq is not available, it may return a message with
// larger seq, depending on qlen and oowait. If no eligible message is found,
// a nil is returned.
//
// Pull is protected by a mutex, in case multiple HTTP connections try to
// use the same session.
func (self *QueueSubscription) Pull() (*Message, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.queue.pull(self)
}

// Finish separates QueueSubscription from its Queue and must be called when
// the associated session is purged. Failing to do so causes a memory leak,
// because the Queue still has a reference to QueueSubscription.notifier,
// which in turn refers to Session, ultimately preventing Session from getting
// garbage collected.
func (self *QueueSubscription) Finish() {
	self.queue.unsubscribe(self)
	self.queue = nil

	if self.query != nil {
		self.query.Cancel()
		self.query = nil
	}
}

// Seq returns the current sequence number.
func (self *QueueSubscription) Seq() int64 {
	// Use atomic, so race detector won't complain when this is called
	// from Session.DumpStatus().
	return atomic.LoadInt64(&self.seq)
}

// mRing implements a simple ringbuffer to store messages with sequence
// numbers baseseq..baseseq+len(buf)-1.
type mRing struct {
	buf     []*Message // Array of messages
	baseidx int        // Index of the first message
	baseseq int64      // Seq of the first message
	seq     int64      // Seq of the last message + 1
}

// mNewRing creates a new mRing object.
func mNewRing(size int) *mRing {
	return &mRing{buf: make([]*Message, size)}
}

// Put adds a message to the ring. Messages with seq < baseseq are ignored.
// If seq > baseseq+len(buf)-1, then older messages are removed as needed
// and baseseq is updated accordingly.
func (self *mRing) Put(m *Message) {
	if m.Seq.Value < self.baseseq {
		return

	} else if m.Seq.Value >= self.seq+int64(len(self.buf)) {
		for i, s := self.baseidx, self.baseseq; s < self.seq; i, s = (i+1)%len(self.buf), s+1 {
			self.buf[i] = nil
		}

		self.baseidx = 0
		self.baseseq = m.Seq.Value
		self.buf[0] = m

	} else {
		for m.Seq.Value >= self.baseseq+int64(len(self.buf)) {
			self.buf[self.baseidx] = nil
			self.baseidx = (self.baseidx + 1) % len(self.buf)
			self.baseseq++
		}

		i := (int64(self.baseidx) + m.Seq.Value - self.baseseq) % int64(len(self.buf))
		self.buf[i] = m
	}

	if m.Seq.Value+1 > self.seq {
		self.seq = m.Seq.Value + 1
	}
}

// Get returns a message with given sequence number. If no such message exists,
// the next message with closest sequence number is returned. If that also fails,
// a nil is returned.
func (self *mRing) Get(seq int64) *Message {
	if seq < self.baseseq {
		seq = self.baseseq
	}

	for s := seq; s < self.seq; s++ {
		i := (int64(self.baseidx) + s - self.baseseq) % int64(len(self.buf))

		if self.buf[i] != nil {
			if s != self.buf[i].Seq.Value {
				panic("message ring is corrupt")
			}

			return self.buf[i]
		}
	}

	return nil
}

// topicInfo is a helper struct to store endseq and endtime of a topic.
type topicInfo struct {
	endseq  int64
	endtime Time
}

// Queue represents an HMB queue. See Session.notify() for notifier explanation.
type Queue struct {
	name              string                       // Queue name
	coll              Collection                   // Associated collection, or nil
	queryLimit        int                          // Max number of messages to request at once
	delta             int                          // Accept sequence numbers up to seq+delta
	ring              *mRing                       // The ringbuffer
	seq               int64                        // The next sequence number
	notifiersAttached map[*func(string, bool)]bool // List of functions to be called when a message is added
	topics            map[string]*topicInfo        // Keeps track of endseq and endtime of each topic
	rwmutex           sync.RWMutex                 // Protects the queue from concurrent reads and writes
	mutex             sync.Mutex                   // Protects notifiersAttached from concurrent access
}

// NewQueue creates a new Queue object. If a collection already exists, then
// last bufferSize messages are preloaded.
func NewQueue(name string, coll Collection, bufferSize int, queryLimit int, delta int) (*Queue, error) {
	self := &Queue{
		name:              name,
		coll:              coll,
		queryLimit:        queryLimit,
		delta:             delta,
		ring:              mNewRing(bufferSize),
		notifiersAttached: map[*func(string, bool)]bool{},
		topics:            map[string]*topicInfo{},
	}

	if coll != nil {
		if q, err := coll.Query(int64(-bufferSize), -1, Time{}, Time{}, nil, nil, nil, "", bufferSize, true); err != nil {
			return nil, err

		} else {
			for {
				if m, err := q.Read(); err == ECONTINUE || err == io.EOF {
					break

				} else if err != nil {
					return nil, err

				} else if m == nil {
					<-q.ReadNotify()

				} else {
					if m.Seq.Value+1 > self.seq {
						self.seq = m.Seq.Value + 1
					}

					self.ring.Put(m)

					ti, ok := self.topics[m.Topic]

					if !ok {
						ti = &topicInfo{}
						self.topics[m.Topic] = ti
					}

					if m.Seq.Value+1 > ti.endseq {
						if !m.Endtime.IsZero() {
							ti.endtime = m.Endtime
							ti.endseq = m.Seq.Value + 1
						}
					}
				}
			}
		}
	}

	return self, nil
}

// Name returns the queue name.
func (self *Queue) Name() string {
	return self.name
}

// Push adds a new message to the queue.
func (self *Queue) Push(m *Message) {
	self.rwmutex.Lock()

	if !m.Seq.Set {
		seq := self.seq
		self.seq += 1
		m.Seq.Value = seq
		m.Seq.Set = true

	} else if m.Seq.Value+1 > self.seq {
		self.seq = m.Seq.Value + 1
	}

	self.ring.Put(m)

	ti, ok := self.topics[m.Topic]

	if !ok {
		ti = &topicInfo{}
		self.topics[m.Topic] = ti
	}

	if m.Seq.Value+1 > ti.endseq {
		if !m.Endtime.IsZero() {
			ti.endtime = m.Endtime
			ti.endseq = m.Seq.Value + 1
		}
	}

	self.rwmutex.Unlock()

	if self.coll != nil {
		self.coll.Insert(m)
	}

	self.mutex.Lock()

	for notifier := range self.notifiersAttached {
		(*notifier)(self.name, true)
	}

	self.mutex.Unlock()
}

// query is a helper method to call self.coll.Query.
func (self *Queue) query(sub *QueueSubscription) error {
	var filterExpr map[string]interface{}

	if sub.filter != nil {
		filterExpr = sub.filter.Source()

	} else {
		filterExpr = nil
	}

	d, err := self.coll.Query(sub.seq, sub.endseq, sub.starttime, sub.endtime, sub.topicRx, sub.topicNrx, filterExpr, sub.cid, self.queryLimit, !sub.gotData)

	if err != nil {
		sub.query = nil

	} else {
		sub.query = d
	}

	return err
}

// pull pulls a message from the queue. Don't call it directly, but use
// QueueSubscription.Pull() instead.
func (self *Queue) pull(sub *QueueSubscription) (*Message, error) {
	self.rwmutex.RLock()
	defer self.rwmutex.RUnlock()

	// If the sequence difference exceeds qlen, drop some messages.
	if sub.qlen >= 0 && sub.seq < self.seq-int64(sub.qlen)-1 {
		sub.seq = self.seq - int64(sub.qlen) - 1
	}

	queryDone := false

	// If we have a QueryDescriptor available, read it.
	if sub.query != nil {
		if m, err := sub.query.Read(); err == io.EOF {
			// Query has reached the end.
			queryDone = true
			sub.query = nil

		} else if err == ECONTINUE {
			// Query has stopped before reaching the end, either
			// because of queryLimit or reply channel getting full.
			// Will check the ringbuffer and start a new query if
			// needed.
			sub.query = nil

		} else if err != nil {
			// Some serious error occurred. Returning an error
			// causes client connection to be dropped.
			return nil, err

		} else if m == nil {
			if self.coll.NumQueriesQueued() <= 1 {
				// Allow running queries in parallel, since the system is idle enough.
				go func() { <-sub.query.ReadNotify(); sub.notifier(self.name, false) }()
				return nil, nil

			} else {
				// Temporarily unlock the mutex and wait for notfication.
				// Note that a panic in ReadNotify() would cause a double
				// unlock error and mask the original panic due to defer.
				self.rwmutex.RUnlock()
				<-sub.query.ReadNotify()
				self.rwmutex.RLock()

				// A query result may be available. Call the notifier
				// and return just to be called again.
				sub.notifier(self.name, false)
				return nil, nil
			}

		} else {
			// At this point we really got a message.
			sub.gotData = true
			atomic.StoreInt64(&sub.seq, m.Seq.Value+1)
			return m, nil
		}
	}

	nogap := false

	// If the oowait option is used, we wait for missing messages (do not
	// allow gaps) until oowait seconds have passed.
	if sub.oowait >= 0 &&
		(sub.startwait.IsZero() ||
			time.Now().Sub(sub.startwait) < time.Duration(sub.oowait)*time.Second) {
		nogap = true
	}

	// Try to find an eligible message until self.seq has not been reached.
	for sub.seq < self.seq {
		m := self.ring.Get(sub.seq)

		if m == nil {
			// There must be at least one message with seq >= sub.seq,
			// so this cannot happen unless something is corrupt.
			panic("m == nil")
		}

		if m.Seq.Value > sub.seq {
			// The seq we got is larger than what we wanted, so either a
			// message is missing or it is no longer in the ringbuffer.
			if self.ring.baseseq > sub.seq {
				// It's worth to look into the repository, but
				// only if we do have repository and haven't
				// checked it before (!queryDone).
				if self.coll != nil && !queryDone {
					sub.startwait = time.Time{}
					if err := self.query(sub); err != nil {
						return nil, err

					} else {
						// Call the notifier and return
						// just to be called again.
						sub.notifier(self.name, false)
						return nil, nil
					}
				}

			} else if nogap {
				// The message with wanted seq is missing. If
				// oowait is used, wait for it.
				if sub.startwait.IsZero() {
					sub.startwait = time.Now()
				}

				return nil, nil
			}

		}

		// At this point we have a message. Reset startwait.
		sub.startwait = time.Time{}
		sub.gotData = true

		// Check for starttime, endtime, endseq and filters.
		if sub.endseq >= 0 && m.Seq.Value > sub.endseq ||
			(!m.Starttime.IsZero() && !sub.endtime.IsZero() && m.Starttime.Sub(sub.endtime.Time) > 0) {
			self.unsubscribe(sub)
			return nil, io.EOF
		}

		if !m.Endtime.IsZero() && !sub.starttime.IsZero() && m.Endtime.Sub(sub.starttime.Time) <= 0 {
			atomic.StoreInt64(&sub.seq, m.Seq.Value+1)
			continue
		}

		if m.Sender != sub.cid &&
			(sub.topicRx == nil || sub.topicRx.MatchString(m.Topic)) &&
			(sub.topicNrx == nil || !sub.topicNrx.MatchString(m.Topic)) &&
			(sub.filter == nil || sub.filter.Match(m)) {
			atomic.StoreInt64(&sub.seq, m.Seq.Value+1)
			return m, nil
		}

		atomic.StoreInt64(&sub.seq, m.Seq.Value+1)
	}

	// We have reached the end of queue without finding an eligible
	// message. Unsubscribe from the queue unless sub.keep is set.
	if !sub.keep {
		self.unsubscribe(sub)
		return nil, io.EOF
	}

	return nil, nil
}

// Subscribe creates a QueueSubscription and adds the given notifier to the list.
func (self *Queue) Subscribe(cid string, notifier func(string, bool), topics []string, seq int64, endseq int64,
	starttime Time, endtime Time, filter *Filter, qlen int, oowait int, keep bool, seedlink bool) (*QueueSubscription, error) {

	pt := []string{}
	nt := []string{}

	for _, t := range topics {
		if !regexTopicPattern.MatchString(t) {
			return nil, errors.New("topic contains invalid characters")
		}

		t = strings.Replace(strings.Replace(t, "?", ".", -1), "*", ".*", -1)

		if t[:1] != "!" {
			pt = append(pt, t)

		} else {
			nt = append(nt, t[1:])
		}
	}

	sub := &QueueSubscription{
		queue:     self,
		cid:       cid,
		notifier:  notifier,
		topics:    topics,
		seq:       seq,
		endseq:    endseq,
		starttime: starttime,
		endtime:   endtime,
		filter:    filter,
		qlen:      qlen,
		oowait:    oowait,
		keep:      keep,
		seedlink:  seedlink,
	}

	if len(pt) > 0 {
		sub.topicRx = regexp.MustCompile("^" + strings.Join(pt, "|") + "$")
	}

	if len(nt) > 0 {
		sub.topicNrx = regexp.MustCompile("^" + strings.Join(nt, "|") + "$")
	}

	// Lock the mutex, making sure that no new messages are pushed to
	// queue while we are checking self.seq.
	self.rwmutex.RLock()
	defer self.rwmutex.RUnlock()

	if sub.seq < 0 {
		// If sub.seq < 0, count from the end of the queue.
		sub.seq = self.seq + sub.seq + 1

		if sub.seq < 0 {
			sub.seq = 0
		}

	} else if sub.seedlink {
		// sub.seq is a seedlink-compatible 24-bit sequence number
		// with wrap.
		sub.seq = (self.seq &^ 0xffffff) | (sub.seq & 0xffffff)

		if sub.seq-self.seq > int64(self.delta) {
			sub.seq -= 0x1000000

		} else if sub.seq-self.seq < int64(self.delta)-0xffffff {
			sub.seq += 0x1000000
		}

	} else if sub.seq-self.seq > int64(self.delta) {
		sub.seq = self.seq
	}

	// If the packet with requested starttime is in the ring, we can avoid
	// a query.
	if !sub.starttime.IsZero() {
		for {
			m := self.ring.Get(sub.seq + 1)

			if m == nil || m.Endtime.IsZero() || m.Endtime.Sub(sub.starttime.Time) > 0 {
				break
			}

			sub.seq = m.Seq.Value
		}
	}

	self.mutex.Lock()
	self.notifiersAttached[&sub.notifier] = true
	self.mutex.Unlock()

	return sub, nil
}

// unsubscribe removes sub.notifier from notifiersAttached, making it
// ultimately possible to to garbage collect a Session. Don't call it
// from outside directly, but use QueueSubscription.Finish() instead.
func (self *Queue) unsubscribe(sub *QueueSubscription) {
	self.mutex.Lock()
	delete(self.notifiersAttached, &sub.notifier)
	self.mutex.Unlock()
}

// DumpInfo generates the JSON data for /info.
func (self *Queue) DumpInfo(w io.Writer) error {
	d := QueueInfo{}

	self.rwmutex.RLock()

	if self.seq > 0 {
		var m *Message

		m = self.ring.Get(0)
		d.Startseq = m.Seq
		d.Starttime = m.Starttime

		m = self.ring.Get(self.seq - 1)
		d.Endseq = Sequence{m.Seq.Value + 1, true}
		d.Endtime = m.Endtime

		self.rwmutex.RUnlock()

		if self.coll != nil {
			if seq, t := self.coll.OldestData(); seq != -1 {
				d.Startseq = Sequence{seq, true}
				d.Starttime = t

			} else {
				d.Startseq = Sequence{}
				d.Starttime = Time{}
			}
		}

		topics := map[string]QueueInfoTopic{}

		self.rwmutex.RLock()

		for k, v := range self.topics {
			topics[k] = QueueInfoTopic{
				Endtime: v.endtime,
			}
		}

		d.Topic = topics
	}

	self.rwmutex.RUnlock()

	if buf, err := json.Marshal(d); err != nil {
		panic(err)

	} else if _, err := w.Write(buf); err != nil {
		return err
	}

	return nil
}
