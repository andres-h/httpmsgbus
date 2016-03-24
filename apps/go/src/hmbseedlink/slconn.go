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
	"bytes"
	"errors"
	"fmt"
	"hmb"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const NSELECTORS = 100

var commands = []*regexp.Regexp{
	regexp.MustCompile("^([Hh][Ee][Ll][Ll][Oo])\\s*$"),
	regexp.MustCompile("^([Cc][Aa][Tt])\\s*$"),
	regexp.MustCompile("^([Bb][Aa][Tt][Cc][Hh])\\s*$"),
	regexp.MustCompile("^([Ss][Tt][Aa][Tt][Ii][Oo][Nn])\\s*([A-Z0-9]{1,5})\\s+([A-Z0-9]{1,2})\\s*$"),
	regexp.MustCompile("^([Ss][Ee][Ll][Ee][Cc][Tt])\\s*(?:(!)?(?:([A-Z0-9\\?]{2})?([A-Z0-9\\?]{3})(?:\\.([DETCLO\\?]))?|([DETCLO])))?\\s*$"),
	regexp.MustCompile("^([Dd][Aa][Tt][Aa])\\s*(?:([0-9A-Fa-f]{1,6})(?:\\s+(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}))?)?\\s*$"),
	regexp.MustCompile("^([Ff][Ee][Tt][Cc][Hh])\\s*(?:([0-9A-Fa-f]{1,6})(?:\\s+(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}))?)?\\s*$"),
	regexp.MustCompile("^([Tt][Ii][Mm][Ee])\\s*(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2})(?:\\s+(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}))?\\s*$"),
	regexp.MustCompile("^([Ee][Nn][Dd])\\s*$"),
	regexp.MustCompile("^([Ii][Nn][Ff][Oo])\\s*([A-Za-z]+)\\s*$"),
	regexp.MustCompile("^([Bb][Yy][Ee])\\s*$"),
}

type SeedlinkConnection struct {
	master    MasterInterface
	conn      net.Conn
	ip        net.IP
	source    string
	timeout   int
	retryWait int
	qlen      *int
	oowait    *int
	w         *bufio.Writer
	param     *hmb.OpenParam
	queue     *hmb.OpenParamQueue
	hmb       *hmb.Client
	infoGen   *InfoGenerator
	batchmode bool
	mutex     sync.Mutex
}

func NewSeedlinkConnection(master MasterInterface, conn net.Conn, ip net.IP, source string, timeout int, retryWait int, qlen int, oowait int) *SeedlinkConnection {
	keep := false
	self := &SeedlinkConnection{
		master:    master,
		conn:      conn,
		ip:        ip,
		source:    source,
		timeout:   timeout,
		retryWait: retryWait,
		w:         bufio.NewWriter(conn),
		param: &hmb.OpenParam{
			HeartbeatInterval: timeout / 2,
			Queue: map[string]*hmb.OpenParamQueue{
				"ANNOUNCEMENT": {
					Keep: &keep,
				},
			},
		},
	}

	if qlen > 0 {
		self.qlen = &qlen
	}

	if oowait > 0 {
		self.oowait = &oowait
	}

	go self.start()
	return self
}

func (self *SeedlinkConnection) Println(v ...interface{}) {
	args := make([]interface{}, 1, len(v)+1)
	args[0] = "[" + self.conn.RemoteAddr().String() + "]"
	log.Println(append(args, v...)...)
}

func (self *SeedlinkConnection) Printf(format string, v ...interface{}) {
	self.Println(fmt.Sprintf(format, v...))
}

func (self *SeedlinkConnection) _OK() {
	if !self.batchmode {
		self.mutex.Lock()
		self.w.Write([]byte("OK\r\n"))
		self.w.Flush()
		self.mutex.Unlock()
	}
}

func (self *SeedlinkConnection) _ERROR() {
	if !self.batchmode {
		self.mutex.Lock()
		self.w.Write([]byte("ERROR\r\n"))
		self.w.Flush()
		self.mutex.Unlock()
	}
}

func (self *SeedlinkConnection) _HELLO() {
	self.mutex.Lock()
	self.w.Write([]byte("SeedLink v3.0 [" + self.master.SoftwareId() + "]\r\n"))
	self.w.Write([]byte(self.master.Organization() + "\r\n"))
	self.w.Flush()
	self.mutex.Unlock()
}

func (self *SeedlinkConnection) _CAT() {
	self.mutex.Lock()

	for _, k := range self.master.StationList(self.ip) {
		s := self.master.StationConfig(k)
		self.w.Write([]byte(fmt.Sprintf("%2s %-5s %s\r\n", k.NetworkCode, k.StationCode, s.Description)))
	}

	self.w.Write([]byte("END"))
	self.w.Flush()
	self.mutex.Unlock()
}

func (self *SeedlinkConnection) _BATCH() {
	self.batchmode = true
	self.mutex.Lock()
	self.w.Write([]byte("OK\r\n"))
	self.w.Flush()
	self.mutex.Unlock()
}

func (self *SeedlinkConnection) _STATION(stationCode, networkCode string) {
	if s := self.master.StationConfig(StationKey{networkCode, stationCode}); s == nil {
		self.Println("station not found")
		self.queue = nil
		self._ERROR()

	} else if len(s.ACL) != 0 && !s.ACL.Contains(self.ip) {
		self.Println("access denied")
		self.queue = nil
		self._ERROR()

	} else {
		var ok bool
		self.queue, ok = self.param.Queue["WAVE_"+networkCode+"_"+stationCode]

		if !ok {
			self.queue = &hmb.OpenParamQueue{Seedlink: true, Qlen: self.qlen, Oowait: self.oowait}
			self.param.Queue["WAVE_"+networkCode+"_"+stationCode] = self.queue
		}

		self._OK()
	}
}

func (self *SeedlinkConnection) _SELECT(neg, loc, cha, ext string) {
	if self.queue == nil {
		self.Println("no station selected")
		self._ERROR()
		return
	}

	if self.queue.Topics == nil {
		self.queue.Topics = make([]string, 0, NSELECTORS)
	}

	if neg == "" && loc == "" && cha == "" && ext == "" {
		self.queue.Topics = self.queue.Topics[:0]
		self._OK()

	} else if len(self.queue.Topics) >= NSELECTORS {
		self.Println("maximum number of selectors exceeded")
		self._ERROR()

	} else {
		if loc == "" {
			loc = "*"
		}

		if cha == "" {
			cha = "*"
		}

		if ext == "" {
			ext = "*"
		}

		self.queue.Topics = append(self.queue.Topics, neg+loc+"_"+cha+"_"+ext)
		self._OK()
	}
}

func makeTime(t []string) (hmb.Time, error) {
	if t == nil {
		return hmb.Time{}, nil

	} else if year, err := strconv.Atoi(t[0]); err != nil || year < 1970 || year > 2100 {
		return hmb.Time{}, errors.New("invalid year")

	} else if month, err := strconv.Atoi(t[1]); err != nil || month < 1 || month > 12 {
		return hmb.Time{}, errors.New("invalid month")

	} else if day, err := strconv.Atoi(t[2]); err != nil || day < 1 || day > 31 {
		return hmb.Time{}, errors.New("invalid day")

	} else if hour, err := strconv.Atoi(t[3]); err != nil || hour < 0 || hour > 23 {
		return hmb.Time{}, errors.New("invalid hour")

	} else if min, err := strconv.Atoi(t[4]); err != nil || min < 0 || min > 59 {
		return hmb.Time{}, errors.New("invalid minute")

	} else if sec, err := strconv.Atoi(t[5]); err != nil || sec < 0 || sec > 60 {
		return hmb.Time{}, errors.New("invalid second")

	} else {
		return hmb.Time{time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC)}, nil
	}
}

func (self *SeedlinkConnection) dataFetchTime(keep bool, seq string, starttime []string, endtime []string) {
	if self.queue == nil {
		self.Println("no station selected")
		self._ERROR()
		return
	}

	var _seq hmb.Sequence

	if seq != "" {
		if seq, err := strconv.ParseInt(seq, 16, 32); err != nil {
			self.Println("invalid sequence number")
			self._ERROR()
			return

		} else {
			_seq = hmb.Sequence{seq, true}
		}

	} else if keep {
		_seq = hmb.Sequence{-1, true}

	} else {
		_seq = hmb.Sequence{-2, true}
	}

	if keep {
		self.param.Queue["ANNOUNCEMENT"].Keep = &keep
	}

	if starttime, err := makeTime(starttime); err != nil {
		self.Println(err)
		self._ERROR()

	} else if endtime, err := makeTime(endtime); err != nil {
		self.Println(err)
		self._ERROR()

	} else {
		self.queue.Seq = _seq
		self.queue.Starttime = starttime
		self.queue.Endtime = endtime
		self.queue.Keep = &keep
		self._OK()
	}
}

func (self *SeedlinkConnection) _DATA(seq, year, month, day, hour, min, sec string) {
	var starttime []string

	if year != "" {
		starttime = []string{year, month, day, hour, min, sec}
	}

	self.dataFetchTime(true, seq, starttime, nil)
}

func (self *SeedlinkConnection) _FETCH(seq, year, month, day, hour, min, sec string) {
	var starttime []string

	if year != "" {
		starttime = []string{year, month, day, hour, min, sec}
	}

	self.dataFetchTime(false, seq, starttime, nil)
}

func (self *SeedlinkConnection) _TIME(year1, month1, day1, hour1, min1, sec1, year2, month2, day2, hour2, min2, sec2 string) {
	var starttime, endtime []string

	if year1 != "" {
		starttime = []string{year1, month1, day1, hour1, min1, sec1}
	}

	if year2 != "" {
		endtime = []string{year2, month2, day2, hour2, min2, sec2}
	}

	self.dataFetchTime(true, "0", starttime, endtime)
}

func (self *SeedlinkConnection) _END() {
	self.hmb = hmb.NewClient(self.source, self.ip, self.param, self.timeout, self.retryWait, self)
	go self.dataServe(self.hmb)
}

func (self *SeedlinkConnection) _INFO(arg string) {
	var level int
	var seedname string

	switch strings.ToUpper(arg) {
	case "ID":
		level = 0
		seedname = "INF"

	case "STATIONS":
		level = 1
		seedname = "INF"

	case "STREAMS":
		level = 2
		seedname = "INF"

	case "ALL":
		level = 2
		seedname = "INF"

	default:
		self.Println("unsupported info level")
		level = 0
		seedname = "ERR"
	}

	if self.infoGen != nil {
		self.infoGen.CancelRequest()
	}

	self.infoGen = self.master.InfoRequest(level, seedname, self.ip, self.w, &self.mutex)
	go self.infoServe(self.infoGen)
}

func (self *SeedlinkConnection) dataServe(h *hmb.Client) {
	defer h.Close()

	var m *hmb.Message
	var err error

	for err == nil {
		if m, err = h.Recv(); err != nil {
			self.Println(err)

			if err == io.EOF {
				self.mutex.Lock()

				if _, err := self.w.Write([]byte("END")); err != nil {
					self.Println(err)

				} else if err := self.w.Flush(); err != nil {
					self.Println(err)
				}

				self.mutex.Unlock()
			}

		} else if m != nil && m.Type == "MSEED" {
			if data, ok := m.Data.Data.([]byte); !ok {
				self.Println("invalid MSEED message")

			} else {
				self.mutex.Lock()

				if _, err = self.w.Write([]byte(fmt.Sprintf("SL%06X", m.Seq.Value%0xffffff))); err != nil {
					self.Println(err)

				} else if _, err = self.w.Write(data); err != nil {
					self.Println(err)

				} else if err = self.w.Flush(); err != nil {
					self.Println(err)
				}

				self.mutex.Unlock()
			}
		}
	}
}

func (self *SeedlinkConnection) infoServe(infoGen *InfoGenerator) {
	if err := infoGen.Do(); err != nil {
		self.Println(err)
	}
}

func scanCommands(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF {
		return 0, nil, nil
	}

	if i := bytes.IndexByte(data, '\r'); i >= 0 {
		return i + 1, bytes.TrimSpace(data[0:i]), nil
	}

	return 0, nil, nil
}

func (self *SeedlinkConnection) start() {
	defer self.conn.Close()

	scanner := bufio.NewScanner(self.conn)
	scanner.Split(scanCommands)

loop:
	for scanner.Scan() {
		cmd := scanner.Text()

		for _, rx := range commands {
			if a := rx.FindStringSubmatch(cmd); a != nil {
				self.Println(cmd)

				kw := strings.ToUpper(a[1])

				if self.hmb != nil && kw != "INFO" && kw != "BYE" {
					self.Println("exiting transfer state")
					self.hmb.CancelRequest()
					self.hmb = nil
				}

				switch kw {
				case "HELLO":
					self._HELLO()
					continue loop

				case "CAT":
					self._CAT()
					continue loop

				case "BATCH":
					self._BATCH()
					continue loop

				case "STATION":
					self._STATION(a[2], a[3])
					continue loop

				case "SELECT":
					self._SELECT(a[2], a[3], a[4], a[5]+a[6])
					continue loop

				case "DATA":
					self._DATA(a[2], a[3], a[4], a[5], a[6], a[7], a[8])
					continue loop

				case "FETCH":
					self._FETCH(a[2], a[3], a[4], a[5], a[6], a[7], a[8])
					continue loop

				case "TIME":
					self._TIME(a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13])
					continue loop

				case "END":
					self._END()
					continue loop

				case "INFO":
					self._INFO(a[2])
					continue loop

				case "BYE":
					break loop
				}
			}
		}

		self.Println("invalid command:", cmd)

		if self.hmb != nil {
			self.Println("exiting transfer state")
			self.hmb.CancelRequest()
			self.hmb = nil
		}

		self._ERROR()
	}

	if err := scanner.Err(); err != nil {
		self.Println(err)
	}

	if self.hmb != nil {
		self.hmb.CancelRequest()
	}

	if self.infoGen != nil {
		self.infoGen.CancelRequest()
	}

	self.master.EndConnection(self.ip)
}
