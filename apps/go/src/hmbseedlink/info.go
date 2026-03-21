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
	"bitbucket.org/andresh/httpmsgbus/apps/go/src/hmb"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	TIMEFMT           = "2006/01/02 15:04:05.0000"
	MS2_RECLEN        = 512
	MS2_DATASTART     = 64
	INFO_ERROR        = -1
	INFO_ID           = 0
	INFO_FORMATS      = 1
	INFO_CAPABILITIES = 2
	INFO_STATIONS     = 3
	INFO_STREAMS      = 4
	INFO_CONNECTIONS  = 5
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

type MSEEDInfoGenerator struct {
	level    int
	seedname string
	ip       net.IP
	w        *bufio.Writer
	mutex    *sync.Mutex
	master   MasterInterface
	cache    *InfoCache
	cancel   chan struct{}
	ready    chan struct{}
	rec      [MS2_RECLEN]byte
	recno    int
	i        int
}

func NewMSEEDInfoGenerator(level int, ip net.IP, w *bufio.Writer, mutex *sync.Mutex, master MasterInterface, cache *InfoCache) InfoGenerator {
	self := &MSEEDInfoGenerator{
		level:  level,
		ip:     ip,
		w:      w,
		mutex:  mutex,
		master: master,
		cache:  cache,
		cancel: make(chan struct{}),
		ready:  make(chan struct{}),
		i:      MS2_DATASTART,
	}

	seedname := "INF"

	if level == INFO_ERROR {
		seedname = "ERR"
	}

	for i := range 20 {
		self.rec[i] = ' '
	}

	self.rec[6] = 'D'
	copy(self.rec[15:18], seedname)
	self.rec[39] = byte(1)
	self.rec[45] = byte(MS2_DATASTART)
	self.rec[47] = byte(48)
	self.rec[48] = byte(3)
	self.rec[49] = byte(232)
	self.rec[53] = byte(1)
	self.rec[54] = byte(9)

	return self
}

func (self *MSEEDInfoGenerator) flush(final bool) error {
	t := time.Now().UTC()
	copy(self.rec[0:6], fmt.Appendf(nil, "%06d", self.recno))
	binary.BigEndian.PutUint16(self.rec[20:22], uint16(t.Year()))
	binary.BigEndian.PutUint16(self.rec[22:24], uint16(t.YearDay()))
	self.rec[24] = byte(t.Hour())
	self.rec[25] = byte(t.Minute())
	self.rec[26] = byte(t.Second())
	binary.BigEndian.PutUint16(self.rec[28:30], uint16(t.Nanosecond()/100000))
	binary.BigEndian.PutUint16(self.rec[30:32], uint16(self.i-MS2_DATASTART))

	for i := self.i; i < MS2_RECLEN; i++ {
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

	self.i = MS2_DATASTART
	self.recno++
	return nil
}

func (self *MSEEDInfoGenerator) write(b []byte) (int, error) {
	n := 0

	for n < len(b) {
		select {
		case <-self.cancel:
			return 0, hmb.ECANCELED
		default:
		}

		if self.i == MS2_RECLEN {
			if err := self.flush(false); err != nil {
				return n, err
			}
		}

		size := min(len(b)-n, MS2_RECLEN-self.i)

		copy(self.rec[self.i:self.i+size], b[n:n+size])

		n += size
		self.i += size
	}

	return n, nil
}

func (self *MSEEDInfoGenerator) streams(q *hmb.QueueInfo) error {
	for k, t := range q.Topic {
		var loc, cha, ext string

		if s := strings.Split(k, "_"); len(s) != 5 || s[4][0] != '2' {
			continue

		} else {
			loc = s[0]
			cha = s[1] + s[2] + s[3]
			ext = s[4][1:2]
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

		if _, err := self.write(fmt.Appendf(nil, "<stream location=\"%s\" seedname=\"%s\" type=\"%s\" begin_time=\"%s\" end_time=\"%s\" begin_recno=\"0\" end_recno=\"0\" gap_check=\"disabled\" gap_treshold=\"0\"/>",
			loc, cha, ext, stime, etime)); err != nil {
			return err
		}
	}

	return nil
}

func (self *MSEEDInfoGenerator) stations() error {
	queues, err := self.cache.Request(self.cancel)

	if err != nil {
		return err
	}

	for _, k := range self.master.StationList(self.ip) {
		s := self.master.StationConfig(k)

		if q, ok := queues["FDSN_"+k.NetworkCode+"_"+k.StationCode]; !ok {
			if _, err := self.write(fmt.Appendf(nil, "<station name=\"%s\" network=\"%s\" description=\"%s\" begin_seq=\"0\" end_seq=\"0\" stream_check=\"enabled\"/>",
				k.StationCode, k.NetworkCode, s.Description)); err != nil {
				return err
			}

		} else if _, err := self.write(fmt.Appendf(nil, "<station name=\"%s\" network=\"%s\" description=\"%s\" begin_seq=\"%d\" end_seq=\"%d\" stream_check=\"enabled\"",
			k.StationCode, k.NetworkCode, s.Description, q.Startseq.Value&0xffffff, q.Endseq.Value&0xffffff)); err != nil {
			return err

		} else if self.level == INFO_STATIONS {
			if _, err := self.write([]byte("/>")); err != nil {
				return err
			}

		} else {
			if _, err := self.write([]byte(">")); err != nil {
				return err
			}

			if err := self.streams(q); err != nil {
				return err
			}

			if _, err := self.write([]byte("</station>")); err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *MSEEDInfoGenerator) generate() error {
	if _, err := self.write([]byte("<?xml version=\"1.0\"?>")); err != nil {
		return err

	} else if _, err := self.write(fmt.Appendf(nil, "<seedlink software=\"%s\" organization=\"%s\" started=\"%s\">",
		self.master.SoftwareId(),
		self.master.Organization(),
		self.master.Started().Format(TIMEFMT))); err != nil {
		return err

	} else if self.level == INFO_CAPABILITIES {
		if _, err := self.write([]byte("<capability name=\"dialup\"/><capability name=\"multistation\"/><capability name=\"window-extraction\"/><capability name=\"info:id\"/><capability name=\"info:capabilities\"/><capability name=\"info:stations\"/><capability name=\"info:streams\"/>")); err != nil {
			return err
		}

	} else if self.level >= INFO_STATIONS {
		if err := self.stations(); err != nil {
			return err
		}
	}

	if _, err := self.write([]byte("</seedlink>")); err != nil {
		return err
	}

	return nil
}

func (self *MSEEDInfoGenerator) Do() error {
	defer close(self.ready)

	if err := self.generate(); err != nil {
		return err
	}

	if err := self.flush(true); err != nil {
		return err
	}

	return nil
}

func (self *MSEEDInfoGenerator) CancelRequest() {
	close(self.cancel)
}

func (self *MSEEDInfoGenerator) ReadyWait() {
	<-self.ready
}

type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type FormatInfo struct {
	Mimetype  string            `json:"mimetype"`
	Subformat map[string]string `json:"subformat"`
}

type StreamInfo struct {
	Id        string `json:"id"`
	Format    string `json:"format"`
	Subformat string `json:"subformat"`
	Starttime string `json:"start_time"`
	Endtime   string `json:"end_time"`
}

type StationInfo struct {
	Id          string         `json:"id"`
	Description string         `json:"description"`
	Startseq    int64          `json:"start_seq"`
	Endseq      int64          `json:"end_seq"`
	Backfill    int            `json:"backfill"`
	Stream      *[]*StreamInfo `json:"stream,omitempty"`
}

type Info struct {
	Software     string                  `json:"software"`
	Organization string                  `json:"organization"`
	Started      string                  `json:"started"`
	Error        *ErrorInfo              `json:"error,omitempty"`
	Format       *map[string]*FormatInfo `json:"format,omitempty"`
	Capability   *[]string               `json:"capability,omitempty"`
	Station      *[]*StationInfo         `json:"station,omitempty"`
}

type JSONInfoGenerator struct {
	level     int
	stationRx *regexp.Regexp
	streamRx  *regexp.Regexp
	formatRx  *regexp.Regexp
	ip        net.IP
	w         *bufio.Writer
	mutex     *sync.Mutex
	master    MasterInterface
	cache     *InfoCache
	cancel    chan struct{}
	ready     chan struct{}
	info      Info
}

func NewJSONInfoGenerator(level int, stationRx *regexp.Regexp, streamRx *regexp.Regexp, formatRx *regexp.Regexp, ip net.IP, w *bufio.Writer, mutex *sync.Mutex, master MasterInterface, cache *InfoCache) *JSONInfoGenerator {
	self := &JSONInfoGenerator{
		level:     level,
		stationRx: stationRx,
		streamRx:  streamRx,
		formatRx:  formatRx,
		ip:        ip,
		w:         w,
		mutex:     mutex,
		master:    master,
		cache:     cache,
		cancel:    make(chan struct{}),
		ready:     make(chan struct{}),
	}

	return self
}

func (self *JSONInfoGenerator) stations(addStreams bool) error {
	queues, err := self.cache.Request(self.cancel)

	if err != nil {
		return err
	}

	self.info.Station = &[]*StationInfo{}

	for _, k := range self.master.StationList(self.ip) {
		if !self.stationRx.MatchString(k.NetworkCode + "_" + k.StationCode) {
			continue
		}

		s := self.master.StationConfig(k)
		station := &StationInfo{
			Id:          k.NetworkCode + "_" + k.StationCode,
			Description: s.Description,
			Startseq:    0,
			Endseq:      0,
			Backfill:    -1,
		}

		if q, ok := queues["FDSN_"+k.NetworkCode+"_"+k.StationCode]; ok {
			station.Startseq = q.Startseq.Value
			station.Endseq = q.Endseq.Value

			if addStreams {
				station.Stream = &[]*StreamInfo{}

				for k, t := range q.Topic {
					if len(k) < 3 || k[len(k)-1:] != "D" {
						continue
					}

					streamId := k[:len(k)-3]

					if !self.streamRx.MatchString(streamId) {
						continue
					}

					if !self.formatRx.MatchString("3D") {
						continue
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

					*station.Stream = append(*station.Stream, &StreamInfo{
						Id:        streamId,
						Format:    "3",
						Subformat: "D",
						Starttime: stime,
						Endtime:   etime,
					})
				}
			}
		}

		*self.info.Station = append(*self.info.Station, station)
	}

	return nil
}

func (self *JSONInfoGenerator) capabilities() error {
	self.info.Capability = &[]string{"SLPROTO:4.0", "TIME"}
	return nil
}

func (self *JSONInfoGenerator) formats() error {
	self.info.Format = &map[string]*FormatInfo{
		"3": &FormatInfo{
			Mimetype: "application/vnd.fdsn.mseed3",
			Subformat: map[string]string{
				"D": "data/generic",
			},
		},
	}

	return nil
}

func (self *JSONInfoGenerator) collect() error {
	self.info.Software = self.master.SoftwareId()
	self.info.Organization = self.master.Organization()
	self.info.Started = self.master.Started().Format(TIMEFMT)

	addStreams := false

	switch self.level {
	case INFO_STREAMS:
		addStreams = true
		fallthrough

	case INFO_STATIONS:
		if err := self.stations(addStreams); err != nil {
			return err
		}
		fallthrough

	case INFO_CAPABILITIES:
		if err := self.capabilities(); err != nil {
			return err
		}
		fallthrough

	case INFO_FORMATS:
		if err := self.formats(); err != nil {
			return err
		}
		fallthrough

	case INFO_ID:
		return nil

	case INFO_CONNECTIONS:
		fallthrough

	default:
		self.info.Error = &ErrorInfo{"ARGUMENTS", "requested item is not available"}
	}

	return nil
}

func (self *JSONInfoGenerator) flush() error {
	header := [17]byte{'S', 'E', 'J', 'I'}

	if self.info.Error != nil {
		header[3] = 'E'
	}

	if payload, err := json.Marshal(self.info); err != nil {
		return err

	} else {
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(payload)))

		self.mutex.Lock()
		defer self.mutex.Unlock()

		if _, err := self.w.Write(header[:]); err != nil {
			return err
		}

		if _, err := self.w.Write(payload); err != nil {
			return err
		}

		if err := self.w.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (self *JSONInfoGenerator) Do() error {
	defer close(self.ready)

	if err := self.collect(); err != nil {
		return err
	}

	if err := self.flush(); err != nil {
		return err
	}

	return nil
}

func (self *JSONInfoGenerator) CancelRequest() {
	close(self.cancel)
}

func (self *JSONInfoGenerator) ReadyWait() {
	<-self.ready
}
