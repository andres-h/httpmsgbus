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
	"bufio"
	"encoding/binary"
	"fmt"
	"hmb"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	RECLEN    = 512
	DATASTART = 64
	TIMEFMT   = "2006/01/02 15:04:05.0000"
)

type InfoCache struct {
	hmb    *hmb.Client
	expire int
	t      time.Time
	info   *map[string]*hmb.QueueInfo
	err    *error
	ready  chan struct{}
	mutex  sync.Mutex
}

func NewInfoCache(hmb *hmb.Client, expire int) *InfoCache {
	return &InfoCache{
		hmb:    hmb,
		expire: expire,
	}
}

func (self *InfoCache) refresh() {
	info, err := self.hmb.Info()

	self.mutex.Lock()

	if err == nil {
		self.t = time.Now()
		*self.info = info
		*self.err = err
	}

	close(self.ready)
	self.ready = nil
	self.mutex.Unlock()
}

func (self *InfoCache) Request(cancel <-chan struct{}) (map[string]*hmb.QueueInfo, error) {
	self.mutex.Lock()

	if self.info != nil && time.Now().Sub(self.t) < time.Duration(self.expire)*time.Second {
		defer self.mutex.Unlock()
		return *self.info, nil
	}

	if self.ready == nil {
		self.info = new(map[string]*hmb.QueueInfo)
		self.err = new(error)
		self.ready = make(chan struct{})
		go self.refresh()
	}

	info := self.info
	err := self.err
	ready := self.ready
	self.mutex.Unlock()

	select {
	case <-ready:
		return *info, *err

	case <-cancel:
		return nil, hmb.ECANCELED
	}
}

type InfoGenerator struct {
	level    int
	seedname string
	ip       net.IP
	w        *bufio.Writer
	mutex    *sync.Mutex
	master   MasterInterface
	cache    *InfoCache
	cancel   chan struct{}
	rec      [RECLEN]byte
	recno    int
	i        int
}

func NewInfoGenerator(level int, seedname string, ip net.IP, w *bufio.Writer, mutex *sync.Mutex, master MasterInterface, cache *InfoCache) *InfoGenerator {
	self := &InfoGenerator{
		level:    level,
		seedname: seedname,
		ip:       ip,
		w:        w,
		mutex:    mutex,
		master:   master,
		cache:    cache,
		cancel:   make(chan struct{}),
		i:        DATASTART,
	}

	for i := 0; i < 20; i++ {
		self.rec[i] = ' '
	}

	self.rec[6] = 'D'
	copy(self.rec[15:18], seedname)
	self.rec[39] = byte(1)
	self.rec[45] = byte(DATASTART)
	self.rec[47] = byte(48)
	self.rec[48] = byte(3)
	self.rec[49] = byte(232)
	self.rec[53] = byte(1)
	self.rec[54] = byte(9)

	return self
}

func (self *InfoGenerator) flush(final bool) error {
	t := time.Now().UTC()
	copy(self.rec[0:6], []byte(fmt.Sprintf("%06d", self.recno)))
	binary.BigEndian.PutUint16(self.rec[20:22], uint16(t.Year()))
	binary.BigEndian.PutUint16(self.rec[22:24], uint16(t.YearDay()))
	self.rec[24] = byte(t.Hour())
	self.rec[25] = byte(t.Minute())
	self.rec[26] = byte(t.Second())
	binary.BigEndian.PutUint16(self.rec[28:30], uint16(t.Nanosecond()/100000))
	binary.BigEndian.PutUint16(self.rec[30:32], uint16(self.i-DATASTART))

	for i := self.i; i < RECLEN; i++ {
		self.rec[i] = 0
	}

	var head string

	if final {
		head = "SLINFO  "

	} else {
		head = "SLINFO *"
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if _, err := self.w.Write([]byte(head)); err != nil {
		return err
	}

	if _, err := self.w.Write(self.rec[:]); err != nil {
		return err
	}

	if err := self.w.Flush(); err != nil {
		return err
	}

	self.i = DATASTART
	self.recno++
	return nil
}

func (self *InfoGenerator) write(b []byte) (int, error) {
	n := 0

	for n < len(b) {
		select {
		case <-self.cancel:
			return 0, hmb.ECANCELED
		default:
		}

		if self.i == RECLEN {
			if err := self.flush(false); err != nil {
				return n, err
			}
		}

		size := len(b) - n

		if size > RECLEN-self.i {
			size = RECLEN - self.i
		}

		copy(self.rec[self.i:self.i+size], b[n:n+size])

		n += size
		self.i += size
	}

	return n, nil
}

func (self *InfoGenerator) infoLevel2(q *hmb.QueueInfo) error {
	for k, t := range q.Topic {
		var loc, cha, ext string

		if s := strings.Split(k, "_"); len(s) != 3 {
			continue

		} else {
			loc = s[0]
			cha = s[1]
			ext = s[2]
		}

		var stime, etime string

		if !t.Starttime.IsZero() {
			stime = t.Starttime.Format(TIMEFMT)

		} else {
			stime = q.Starttime.Format(TIMEFMT)
		}

		if !t.Endtime.IsZero() {
			etime = t.Endtime.Format(TIMEFMT)

		} else {
			etime = q.Endtime.Format(TIMEFMT)
		}

		if _, err := self.write([]byte(fmt.Sprintf("<stream location=\"%s\" seedname=\"%s\" type=\"%s\" begin_time=\"%s\" end_time=\"%s\" begin_recno=\"0\" end_recno=\"0\" gap_check=\"disabled\" gap_treshold=\"0\"/>",
			loc, cha, ext, stime, etime))); err != nil {
			return err
		}
	}

	return nil
}

func (self *InfoGenerator) infoLevel1() error {
	queues, err := self.cache.Request(self.cancel)

	if err != nil {
		return err
	}

	for _, k := range self.master.StationList(self.ip) {
		s := self.master.StationConfig(k)

		if q, ok := queues["WAVE_"+k.NetworkCode+"_"+k.StationCode]; !ok {
			if _, err := self.write([]byte(fmt.Sprintf("<station name=\"%s\" network=\"%s\" description=\"%s\" begin_seq=\"0\" end_seq=\"0\" stream_check=\"enabled\"/>",
				k.StationCode, k.NetworkCode, s.Description))); err != nil {
				return err
			}

		} else if _, err := self.write([]byte(fmt.Sprintf("<station name=\"%s\" network=\"%s\" description=\"%s\" begin_seq=\"%d\" end_seq=\"%d\" stream_check=\"enabled\"",
			k.StationCode, k.NetworkCode, s.Description, q.Startseq.Value&0xffffff, q.Endseq.Value&0xffffff))); err != nil {
			return err

		} else if self.level == 1 {
			if _, err := self.write([]byte("/>")); err != nil {
				return err
			}

		} else {
			if _, err := self.write([]byte(">")); err != nil {
				return err
			}

			if err := self.infoLevel2(q); err != nil {
				return err
			}

			if _, err := self.write([]byte("</station>")); err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *InfoGenerator) infoLevel0() error {
	if _, err := self.write([]byte("<?xml version=\"1.0\"?>")); err != nil {
		return err

	} else if _, err := self.write([]byte(fmt.Sprintf("<seedlink software=\"%s\" organization=\"%s\" started=\"%s\">",
		self.master.SoftwareId(),
		self.master.Organization(),
		self.master.Started().Format(TIMEFMT)))); err != nil {
		return err

	} else if self.level == 0 {
		if _, err := self.write([]byte("<capability name=\"dialup\"/><capability name=\"multistation\"/><capability name=\"window-extraction\"/><capability name=\"info:id\"/><capability name=\"info:capabilities\"/><capability name=\"info:stations\"/><capability name=\"info:streams\"/>")); err != nil {
			return err
		}

	} else if err := self.infoLevel1(); err != nil {
		return err
	}

	if _, err := self.write([]byte("</seedlink>")); err != nil {
		return err
	}

	return nil
}

func (self *InfoGenerator) Do() error {
	if err := self.infoLevel0(); err != nil {
		return err
	}

	if err := self.flush(true); err != nil {
		return err
	}

	return nil
}

func (self *InfoGenerator) CancelRequest() {
	close(self.cancel)
}
