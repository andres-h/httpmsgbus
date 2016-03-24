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
	"container/list"
	"encoding/json"
	"io"
	"sync"
	"time"
)

// EBadRequest is an error that indicates a problem on the client side.
type EBadRequest struct {
	msg string
}

func (self *EBadRequest) Error() string {
	return self.msg
}

func (self *EBadRequest) BadRequestTag() {}

func IsBadRequest(err error) bool {
	_, ok := err.(interface{BadRequestTag()})
	return ok
}

// EServiceUnavailable is an error that indicates a problem on the server side
// or refusal of service for some other reason, even though the request was
// technically correct.
type EServiceUnavailable struct {
	msg string
}

func (self *EServiceUnavailable) Error() string {
	return self.msg
}

func (self *EServiceUnavailable) ServiceUnavailableTag() {}

func IsServiceUnavailable(err error) bool {
	_, ok := err.(interface{ServiceUnavailableTag()})
	return ok
}

// extendedSession extends Session with some members needed by the Bus.
type extendedSession struct {
	*Session                   // Pointer to Session (we inherit its methods)
	usecount     int           // Use counter, normally 0..2 (used by /send and /recv)
	timeInactive time.Time     // Time since the session is no longer used
	ip           string        // IP address of user
	e            *list.Element // A pointer to an element of Bus.ipInactive[ip], or nil
}

// Bus represent an HMB bus.
type Bus struct {
	repo           Repository                  // Repository used by the bus, or nil
	bufferSize     int                         // Commandline parameter, passed to Queue objects
	queryLimit     int                         // Commandline parameter, passed to Queue objects
	delta          int                         // Commandline parameter, passed to Queue objects
	sessionTimeout int                         // Commandline parameter
	sessionsPerIP  int                         // Commandline parameter
	queues         map[string]*Queue           // Queues managed by the bus
	sessions       map[string]*extendedSession // Sessions managed by the bus
	ipInactive     map[string]*list.List       // List of inactive sessions per IP
	ipCount        map[string]int              // Total number of sessions per IP
	shutdownFlag   bool                        // Set when shutting down
	mutex          sync.Mutex                  // Mutex to protect data from concurrent access
}

// NewBus creates a new Bus object. If there is an associated repository, then
// any existing queues are initialized.
func NewBus(repo Repository, bufferSize int, queryLimit int, delta int, sessionTimeout int, sessionsPerIP int) (*Bus, error) {
	self := &Bus{
		repo:           repo,
		bufferSize:     bufferSize,
		queryLimit:     queryLimit,
		sessionTimeout: sessionTimeout,
		sessionsPerIP:  sessionsPerIP,
		queues:         map[string]*Queue{},
		sessions:       map[string]*extendedSession{},
		ipInactive:     map[string]*list.List{},
		ipCount:        map[string]int{},
	}

	// If we have a repository, create a Queue object for each of its
	// collections.
	if repo != nil {
		if names, err := repo.CollectionNames(); err != nil {
			return nil, err

		} else {
			for _, name := range names {
				if queue, err := NewQueue(name, repo.Collection(name), bufferSize, queryLimit, delta); err != nil {
					return nil, &EServiceUnavailable{err.Error()}

				} else {
					self.queues[name] = queue
				}
			}
		}
	}

	// If there is no queue "ANNOUNCEMENT", create it.
	name := "ANNOUNCEMENT"

	if _, ok := self.queues[name]; !ok {
		var coll Collection

		if repo != nil {
			var err error
			coll, err = repo.InitCollection(name)

			if err != nil {
				return nil, err
			}

		} else {
			coll = nil
		}

		if queue, err := NewQueue(name, coll, bufferSize, queryLimit, delta); err != nil {
			return nil, err

		} else {
			self.queues[name] = queue
		}
	}

	// Start the periodic clienup task.
	go self.cleanupTask()

	return self, nil
}

// cleanupTask is a goroutine that periodically purges expired sessions.
func (self *Bus) cleanupTask() {
	for now := range time.Tick(time.Minute) {
		self.mutex.Lock()

		for ip, l := range self.ipInactive {
			for e := l.Front(); e != nil; _, e = l.Remove(e), l.Front() {
				sid := e.Value.(string)

				if int(now.Sub(self.sessions[sid].timeInactive).Seconds()) < self.sessionTimeout {
					break
				}

				self.sessions[sid].Kill()
				delete(self.sessions, sid)
				self.ipCount[ip]--
			}

			if l.Len() == 0 {
				delete(self.ipInactive, ip)
			}

			if self.ipCount[ip] == 0 {
				delete(self.ipCount, ip)
			}
		}

		self.mutex.Unlock()
	}
}

// Open creates a new session.
func (self *Bus) Open(ip string, port string, format string, param *OpenParam) (*OpenAck, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// Refuse to create a session for an IP if the number of active sessions
	// has reached self.sessionsPerIP; otherwise purge the oldest inactive
	// session if needed.
	if self.sessionsPerIP > 0 && self.ipCount[ip] >= self.sessionsPerIP {
		if l, ok := self.ipInactive[ip]; !ok || l.Len() == 0 {
			return nil, &EServiceUnavailable{"maximum number of sessions per IP exceeded"}

		} else {
			e := l.Front()
			l.Remove(e)
			sid := e.Value.(string)
			self.sessions[sid].Kill()
			delete(self.sessions, sid)
			self.ipCount[ip]--
		}
	}

	var cid string

	// If no client ID is specified by the user, create a new one.
	if param.Cid != "" {
		cid = param.Cid
	} else {
		cid = NewCid()
	}

	// Create a new Session object.
	sess := NewSession(ip+":"+port, format, cid, param.HeartbeatInterval, param.RecvLimit)
	sid := NewSid()
	ack := OpenAck{Sid: sid, Cid: cid, Queue: map[string]OpenAckQueue{}}

	// Iterate over all queues requested by the user and try to join them.
	for qname, q := range param.Queue {
		var filter *Filter = nil
		var qlen int = -1
		var oowait int = -1
		var keep bool = true

		if q.FilterExpr != nil {
			var err error
			filter, err = NewFilter(q.FilterExpr)

			if err != nil {
				errstr := err.Error()
				ack.Queue[qname] = OpenAckQueue{Error: &errstr}
				continue
			}
		}

		if q.Qlen != nil {
			qlen = *q.Qlen
		}

		if q.Oowait != nil {
			oowait = *q.Oowait
		}

		if q.Keep != nil {
			keep = *q.Keep
		}

		if queue, ok := self.queues[qname]; ok {
			if seq, err := sess.JoinQueue(queue, q.Topics, q.Seq, q.Endseq, q.Starttime, q.Endtime, filter, qlen, oowait, keep, q.Seedlink); err != nil {
				errstr := err.Error()
				ack.Queue[qname] = OpenAckQueue{Error: &errstr}
			} else {
				ack.Queue[qname] = OpenAckQueue{Seq: seq}
			}
		} else {
			errstr := "queue not found"
			ack.Queue[qname] = OpenAckQueue{Error: &errstr}
		}
	}

	if _, ok := self.ipInactive[ip]; !ok {
		self.ipInactive[ip] = list.New()
	}

	// Add the new session in inactive state.
	self.sessions[sid] = &extendedSession{sess, 0, time.Now(), ip, self.ipInactive[ip].PushBack(sid)}
	self.ipCount[ip]++
	return &ack, nil
}

// Push pushes a message to m.Queue, creating the queue (and the associated
// collection) if it does not exist. A new queue is announced by sending
// a message to ANNOUNCEMENT; clients receiving the announcement may want to
// subscribe to this new queue.
func (self *Bus) Push(m *Message) error {
	// HEARBEAT message is a no-op used to keep the session alive.
	if m.Type == "HEARTBEAT" {
		return nil
	}

	// EOF and NEW_QUEUE are reserved for HMB iself.
	if m.Type == "EOF" || m.Type == "NEW_QUEUE" || m.Type == "" {
		return &EBadRequest{"invalid 'type'"}
	}

	if m.Queue == "" {
		return &EBadRequest{"invalid 'queue'"}
	}

	if !regexTopicName.MatchString(m.Topic) {
		return &EBadRequest{"invalid 'topic'"}
	}

	if m.Seq.Value < 0 {
		return &EBadRequest{"invalid 'seq'"}
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.shutdownFlag {
		return &EServiceUnavailable{"server is shutting down"}
	}

	queue, ok := self.queues[m.Queue]

	if !ok {
		var coll Collection
		var err error

		if self.repo == nil {
			coll = nil

		} else {
			coll, err = self.repo.InitCollection(m.Queue)

			if err != nil {
				return &EServiceUnavailable{err.Error()}
			}
		}

		queue, err = NewQueue(m.Queue, coll, self.bufferSize, self.queryLimit, self.delta)

		if err != nil {
			return &EServiceUnavailable{err.Error()}
		}

		self.queues[m.Queue] = queue

		a := Message{
			Type:  "NEW_QUEUE",
			Queue: "ANNOUNCEMENT",
			Data:  Payload{map[string]interface{}{"name": m.Queue}},
		}

		self.queues["ANNOUNCEMENT"].Push(&a)
	}

	queue.Push(m)
	return nil
}

// AcquireSession returns an existing Session object and increases its
// usecount (so it won't get purged). ReleaseSession() must be called when the
// session is no longer used.
func (self *Bus) AcquireSession(sid string) (*Session, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if s, ok := self.sessions[sid]; !ok {
		return nil, &EBadRequest{"invalid session"}

	} else {
		s.usecount++

		if s.e != nil {
			self.ipInactive[s.ip].Remove(s.e)
			s.e = nil
		}

		return s.Session, nil
	}
}

// ReleaseSession decreases the usecount of a session.
func (self *Bus) ReleaseSession(sid string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if s, ok := self.sessions[sid]; !ok {
		panic("session not found")

	} else if s.usecount <= 0 {
		panic("usecount <= 0")

	} else {
		s.usecount--

		if s.usecount == 0 {
			if _, ok := self.ipInactive[s.ip]; !ok {
				self.ipInactive[s.ip] = list.New()
			}

			s.timeInactive = time.Now()
			s.e = self.ipInactive[s.ip].PushBack(sid)
		}
	}
}

// Info generates the JSON data for /info.
func (self *Bus) Info(w io.Writer) error {
	self.mutex.Lock()

	queueNames := make([]string, 0, len(self.queues))

	for qname := range self.queues {
		queueNames = append(queueNames, qname)
	}

	self.mutex.Unlock()

	if _, err := w.Write([]byte("{\"queue\":{")); err != nil {
		return err
	}

	first := true

	for _, qname := range queueNames {
		var q *Queue
		var ok bool

		self.mutex.Lock()
		q, ok = self.queues[qname]
		self.mutex.Unlock()

		if ok {
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

			if err := q.DumpInfo(w); err != nil {
				return err
			}
		}
	}

	if _, err := w.Write([]byte("}}")); err != nil {
		return err
	}

	return nil
}

// Status generates the JSON data for /status.
func (self *Bus) Status(w io.Writer) error {
	self.mutex.Lock()

	sessionIds := make([]string, 0, len(self.sessions))

	for sid := range self.sessions {
		sessionIds = append(sessionIds, sid)
	}

	self.mutex.Unlock()

	if _, err := w.Write([]byte("{\"session\":{")); err != nil {
		return err
	}

	first := true

	for _, sid := range sessionIds {
		var s *extendedSession
		var ok bool

		self.mutex.Lock()
		s, ok = self.sessions[sid]
		self.mutex.Unlock()

		if ok {
			if first {
				first = false
			} else {
				w.Write([]byte(","))
			}

			if buf, err := json.Marshal(sid); err != nil {
				panic(err)

			} else if _, err := w.Write(append(buf, ':')); err != nil {
				return err
			}

			if err := s.DumpStatus(w); err != nil {
				return err
			}
		}
	}

	if _, err := w.Write([]byte("}}")); err != nil {
		return err
	}

	return nil
}

// Shutdown tells the bus to shut down.
func (self *Bus) Shutdown() {
	self.mutex.Lock()
	self.shutdownFlag = true
	self.mutex.Unlock()

	if self.repo != nil {
		self.repo.Shutdown()
	}
}
