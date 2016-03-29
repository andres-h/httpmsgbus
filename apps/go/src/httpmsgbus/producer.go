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
	"io"
	"sync/atomic"
	"time"
)

// Producer pulls data from the queues and sends to client.
type Producer struct {
	session    *Session        // Session
	streaming  bool            // /stream (true) or /recv (false)
	w          MessageWriter   // Writer for sending messages to the client
	stopChan   <-chan struct{} // Closed when SIGINT or SIGTERM received
	notifyChan chan bool       // Written when new messages arrive
	hbFlag     int32           // Set to 1 when a heatbeat must be sent
}

// NewProducer creates a new Producer object.
func NewProducer(session *Session, streaming bool, w MessageWriter, stopChan <-chan struct{}) *Producer {
	return &Producer{session, streaming, w, stopChan, make(chan bool, 1), 0}
}

// notify is registered with Session and is called by Session when new messages
// arrive.
func (self *Producer) notify() {
	select {
	case self.notifyChan <- true:
	default:
	}
}

// Start starts the Producer. The function returns when
// a) session.Pull() returns EOF;
// b) /recv is used and pending data has been sent or recvLimit is reached;
// c) connection is broken or closed by client;
// d) closeChan is closed (SIGINT or SIGTERM received).
func (self *Producer) Start() {
	self.session.AttachNotifier(self.notify)

	if self.session.HeartbeatInterval() > 0 {
		// Create a goroutine that periodically sets hbFlag to 1.
		heartbeatTicker := time.NewTicker(time.Duration(self.session.HeartbeatInterval()) * time.Second)

		// Stopping the ticker makes a read from heartbeatTicker.C hang
		// forever, preventing the Producer, Session and related data
		// structures from getting garbage collected. A second channel
		// must be created to avoid this.
		exitTickerLoop := make(chan struct{})

		defer func() { heartbeatTicker.Stop(); close(exitTickerLoop) }()

		go func() {
			for {
				select {
				case <-heartbeatTicker.C:
					atomic.StoreInt32(&self.hbFlag, int32(1))
					self.notify()

				case <-exitTickerLoop:
					return
				}
			}
		}()
	}

	bytesSent := 0
	recvLimit := 1024

	if self.session.RecvLimit() > 0 {
		recvLimit = self.session.RecvLimit()
	}

	// The HTTP server sends true to this channel when the connection is closed.
	closeChan := self.w.CloseNotify()

loop:
	for {
		if m, err := self.session.Pull(); err == io.EOF {
			if _, err := self.w.Write(&Message{Type: "EOF"}); err != nil {
				log.Println(err)
				break

			}

			self.w.Flush(true)
			break

		} else if err != nil {
			log.Println(err)
			break

		} else if m != nil {
			if n, err := self.w.Write(m); err != nil {
				log.Println(err)
				break

			} else {
				bytesSent += n

				if !self.streaming && bytesSent/1024 >= recvLimit {
					self.w.Flush(true)
					break
				}

				select {
				case <-self.stopChan:
					self.w.Flush(true)
					break loop

				case <-closeChan:
					break loop

				default:
					continue
				}
			}

		} else {
			if atomic.SwapInt32(&self.hbFlag, int32(0)) != 0 {
				if n, err := self.w.Write(&Message{Type: "HEARTBEAT"}); err != nil {
					log.Println(err)
					break

				} else {
					bytesSent += n
				}
			}

			if bytesSent > 0 {
				if !self.streaming {
					self.w.Flush(true)
					break

				} else {
					self.w.Flush(false)
					bytesSent = 0
				}
			}
		}

		select {
		case <-self.notifyChan:

		case <-self.stopChan:
			break loop

		case <-closeChan:
			break loop
		}
	}

	// It is tempting to use defer, but we don't want DetachNotifier() to
	// be called if a panic occurs, because that may cause deadlock due to
	// session mutex and suppress the panic message.
	self.session.DetachNotifier()
}
