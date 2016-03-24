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
	"sync"
	"sync/atomic"
	"time"
)

// sessionStats stores statistics data that is needed by /status only.
// Note: 'sent' and 'received' must be 64-bit aligned, otherwise atomic
// operations will not work on 32-bit systems and we don't want to use
// a mutex here. https://golang.org/pkg/sync/atomic/#pkg-note-BUG
type sessionStats struct {
	sent     int64
	received int64
	peer     string
	ctime    Time
}

// Session represents an HMB session.
type Session struct {
	stats             sessionStats                  // Statistics info
	format            string                        // "JSON" or "BSON" (used by Producer)
	cid               string                        // Client ID
	heartbeatInterval int                           // Parameter from /open: heartbeat (used by Producer)
	recvLimit         int                           // Parameter from /open: recv_limit (used by Producer)
	notifier          func()                        // Func to notify Producer
	oidQueue          string                        // Queue of last message, for Validate()
	oidSeq            int64                         // Seq of last message, for Validate()
	subs              map[string]*QueueSubscription // Queue subscriptions
	rtset             map[*QueueSubscription]bool   // Real-time data available
	dbset             map[*QueueSubscription]bool   // Query results available
	eof               map[*QueueSubscription]bool   // End of queue reached (!keep)
	mutex             sync.Mutex                    // Mutex to protect data from concurrent access
}

// NewSession creates a new Session object.
func NewSession(peer string, format string, cid string, heartbeatInterval int, recvLimit int) *Session {
	return &Session{
		stats: sessionStats{
			peer:  peer,
			ctime: Time{time.Now()},
		},
		format:            format,
		cid:               cid,
		heartbeatInterval: heartbeatInterval,
		recvLimit:         recvLimit,
		subs:              map[string]*QueueSubscription{},
		rtset:             map[*QueueSubscription]bool{},
		dbset:             map[*QueueSubscription]bool{},
		eof:               map[*QueueSubscription]bool{},
	}
}

// Format returns the message format used by the session ("JSON" or "BSON").
func (self *Session) Format() string {
	return self.format
}

// Cid returns the client ID.
func (self *Session) Cid() string {
	return self.cid
}

// HeartbeatInterval returns the heartbeat interval requested by the client.
func (self *Session) HeartbeatInterval() int {
	return self.heartbeatInterval
}

// RecvLimit returns the receive limit requested by the client.
func (self *Session) RecvLimit() int {
	return self.recvLimit
}

// JoinQueue connects a Session to Queue via QueueSubscription.
func (self *Session) JoinQueue(queue *Queue, topics []string, seq Sequence, endseq Sequence,
	starttime Time, endtime Time, filter *Filter, qlen int, oowait int, keep bool, seedlink bool) (Sequence, error) {

	var _seq int64 = -1
	var _endseq int64 = -1

	if seq.Set {
		_seq = seq.Value
	}

	if endseq.Set {
		_endseq = endseq.Value
	}

	if sub, err := queue.Subscribe(self.cid, self.notify, topics, _seq, _endseq, starttime, endtime, filter, qlen, oowait, keep, seedlink); err != nil {
		return Sequence{}, err

	} else {
		self.mutex.Lock()
		self.subs[queue.Name()] = sub
		self.rtset[sub] = true
		self.mutex.Unlock()
		return Sequence{sub.Seq(), true}, nil
	}
}

// notify is called by a subscribed Queue when a new message is added to it
// (rt == true) or a query result may be available (rt == false).
// Notification is passed to Producer, which calls Pull() below.
func (self *Session) notify(qname string, rt bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if sub, ok := self.subs[qname]; ok {
		if rt {
			self.rtset[sub] = true

		} else {
			self.dbset[sub] = true
		}

		if self.notifier != nil {
			// Notify Producer.
			self.notifier()
		}
	}
}

// Pull returns the next message from one of the subscribed queues. First the
// queues in dbset are considered, then the queues in rtset, but ignoring
// any queues with pending query. In case of multiple candidates, the queue is
// chosen quasi-randomly thanks to Go map randomization.
// nil is returned if no message is available. Error is EOF if all queues have
// returned EOF.
func (self *Session) Pull() (*Message, error) {
	var sub *QueueSubscription
	var rt bool

	for {
		self.mutex.Lock()

		if len(self.rtset)+len(self.dbset) == 0 {
			self.mutex.Unlock()
			break
		}

		if len(self.dbset) > 0 {
			rt = false

			for sub = range self.dbset {
				break
			}

			delete(self.dbset, sub)

		} else {
			rt = true

			for sub = range self.rtset {
				break
			}

			delete(self.rtset, sub)

			if sub.query != nil {
				self.mutex.Unlock()
				continue
			}
		}

		self.mutex.Unlock()

		if m, err := sub.Pull(); err == io.EOF {
			self.mutex.Lock()
			self.eof[sub] = true
			self.mutex.Unlock()

		} else if err != nil {
			return nil, errors.New(sub.QueueName() + ": " + err.Error())

		} else if m != nil {
			self.mutex.Lock()
			self.oidQueue = m.Queue
			self.oidSeq = m.Seq.Value

			if rt {
				self.rtset[sub] = true

			} else {
				self.dbset[sub] = true
			}

			self.mutex.Unlock()
			return m, nil
		}
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if len(self.eof) == len(self.subs) {
		return nil, io.EOF
	}

	return nil, nil
}

// AttachNotifier attaches a notifier. It is called by Producer.
func (self *Session) AttachNotifier(notifier func()) {
	self.mutex.Lock()
	self.notifier = notifier
	self.mutex.Unlock()
}

// DetachNotifier detaches a notifier. It is called by Producer.
func (self *Session) DetachNotifier() {
	self.mutex.Lock()
	self.notifier = nil
	self.mutex.Unlock()
}

// Validate checks for sequence continuity. It is called from main.
func (self *Session) Validate(oidQueue string, oidSeq int64) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return (oidQueue == self.oidQueue && oidSeq == self.oidSeq)
}

// Kill terminates a session and frees its resources. It is called by Bus.
func (self *Session) Kill() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for _, sub := range self.subs {
		sub.Finish()
	}
}

// DumpStatus generates the JSON data for /status.
func (self *Session) DumpStatus(w io.Writer) error {
	d := SessionStatus{
		Cid:               self.cid,
		Address:           self.stats.peer,
		Ctime:             self.stats.ctime,
		Sent:              atomic.LoadInt64(&self.stats.sent),
		Received:          atomic.LoadInt64(&self.stats.received),
		Format:            self.format,
		HeartbeatInterval: self.heartbeatInterval,
		RecvLimit:         self.recvLimit,
	}

	if buf, err := json.Marshal(d); err != nil {
		panic(err)

	} else if _, err := w.Write(append(buf[:len(buf)-5], '{')); err != nil {
		return err
	}

	first := true

	for qname, sub := range self.subs {
		self.mutex.Lock()
		eof := self.eof[sub]
		self.mutex.Unlock()

		d := SessionStatusQueue{
			Topics:    sub.topics,
			Starttime: sub.starttime,
			Endtime:   sub.endtime,
			Keep:      sub.keep,
			Seedlink:  sub.seedlink,
			Eof:       eof,
		}

		if seq := sub.Seq(); seq != -1 {
			d.Seq = Sequence{seq, true}
		}

		if sub.endseq != -1 {
			d.Endseq = Sequence{sub.endseq, true}
		}

		if sub.qlen != -1 {
			qlen := sub.qlen
			d.Qlen = &qlen
		}

		if sub.oowait != -1 {
			oowait := sub.oowait
			d.Oowait = &oowait
		}

		if first {
			first = false
		} else {
			w.Write([]byte(","))
		}

		if buf, err := json.Marshal(qname); err != nil {
			panic(err)

		} else if _, err := w.Write(append(buf, ':')); err != nil {
			return err
		}

		if buf, err := json.Marshal(d); err != nil {
			panic(err)

		} else if _, err := w.Write(buf); err != nil {
			return err
		}
	}

	if _, err := w.Write([]byte("}}")); err != nil {
		return err
	}

	return nil
}

type readerCounter struct {
	r         BytesReader
	bytecount *int64
}

func (self *readerCounter) Read(b []byte) (int, error) {
	n, err := self.r.Read(b)
	atomic.AddInt64(self.bytecount, int64(n))
	return n, err
}

func (self *readerCounter) Close() error {
	return self.r.Close()
}

// ReaderCounter returns a new BytesReader that updates self.stats.sent.
func (self *Session) ReaderCounter(r BytesReader) BytesReader {
	return &readerCounter{r, &self.stats.sent}
}

type writerCounter struct {
	w         BytesWriter
	bytecount *int64
}

func (self *writerCounter) Write(b []byte) (int, error) {
	n, err := self.w.Write(b)
	atomic.AddInt64(self.bytecount, int64(n))
	return n, err
}

func (self *writerCounter) Flush() {
	self.w.Flush()
}

func (self *writerCounter) CloseNotify() <-chan bool {
	return self.w.CloseNotify()
}

// WriterCounter returns a new BytesWriter that updates self.stats.received.
func (self *Session) WriterCounter(w BytesWriter) BytesWriter {
	return &writerCounter{w, &self.stats.received}
}
