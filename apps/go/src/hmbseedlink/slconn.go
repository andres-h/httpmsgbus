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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NSELECTORS  = 100
	TIME_FORMAT = "2006-01-02T15:04:05Z"
)

var sl3commands = []*regexp.Regexp{
	regexp.MustCompile("(?i)^(BATCH)\\s*$"),
	regexp.MustCompile("(?i)^(BYE)\\s*$"),
	regexp.MustCompile("(?i)^(CAT)\\s*$"),
	regexp.MustCompile("(?i)^(DATA)(?:\\s+([0-9A-Fa-f]{1,6})(?:\\s+(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}))?)?\\s*$"),
	regexp.MustCompile("(?i)^(END)\\s*$"),
	regexp.MustCompile("(?i)^(FETCH)(?:\\s+([0-9A-Fa-f]{1,6})(?:\\s+(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}))?)?\\s*$"),
	regexp.MustCompile("(?i)^(HELLO)\\s*$"),
	regexp.MustCompile("(?i)^(INFO)\\s+([A-Za-z]+)\\s*$"),
	regexp.MustCompile("(?i)^(SELECT)(?:\\s+(!)?(?:([A-Z0-9\\?]{2})?([A-Z0-9\\?]{3})(?:\\.([DETCLO\\?]))?|([DETCLO])))?\\s*$"),
	regexp.MustCompile("(?i)^(SLPROTO)\\s+([0-9.]+)\\s*$"),
	regexp.MustCompile("(?i)^(STATION)\\s+([A-Z0-9]{1,5})\\s+([A-Z0-9]{1,2})\\s*$"),
	regexp.MustCompile("(?i)^(TIME)\\s+(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2})(?:\\s+(\\d{4}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}),(\\d{1,2}))?\\s*$"),
}

var sl4commands = []*regexp.Regexp{
	regexp.MustCompile("(?i)^(AUTH)\\s+([A-Z]+)\\s*(\\S+)\\s*$"),
	regexp.MustCompile("(?i)^(BYE)\\s*$"),
	regexp.MustCompile("(?i)^(DATA)(?:\\s+([0-9]+|ALL)(?:\\s+([0-9\\-:.TZ]+)(?:\\s+([0-9\\-:.TZ]+))?)?)?\\s*$"),
	regexp.MustCompile("(?i)^(END)\\s*$"),
	regexp.MustCompile("(?i)^(ENDFETCH)\\s*$"),
	regexp.MustCompile("(?i)^(HELLO)\\s*$"),
	regexp.MustCompile("(?i)^(INFO)\\s+([A-Z]+)(?:\\s+([A-Z0-9_*?]+)(?:\\s+([A-Z0-9_*?]+)(?:\\.([A-Z0-9*?]{1,2}))?)?)?\\s*$"),
	regexp.MustCompile("(?i)^(SELECT)\\s+(!)?([A-Z0-9_*?]+)(?:\\.([A-Z0-9*?]{1,2}))?\\s*$"),
	regexp.MustCompile("(?i)^(STATION)\\s+([A-Z0-9_*?]+)\\s*$"),
	regexp.MustCompile("(?i)^(USERAGENT)\\s+(\\S+)\\s*$"),
}

func pat2rx(pat string) *regexp.Regexp {
	if pat == "" {
		return regexp.MustCompile("")
	}

	return regexp.MustCompile("^" + strings.ReplaceAll(strings.ReplaceAll(pat, "?", "."), "*", ".*") + "$")
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
	queueSet  map[string]*hmb.OpenParamQueue
	topicSet  map[string]bool
	hmb       *hmb.Client
	infoGen   InfoGenerator
	batchmode bool
	slproto   int
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

func (self *SeedlinkConnection) Println(v ...any) {
	args := make([]any, 1, len(v)+1)
	args[0] = "[" + self.conn.RemoteAddr().String() + "]"
	log.Println(append(args, v...)...)
}

func (self *SeedlinkConnection) Printf(format string, v ...any) {
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

func (self *SeedlinkConnection) _ERROR4(code, message string) {
	errmsg := "ERROR " + code + " " + message
	self.Println(errmsg)
	self.mutex.Lock()
	self.w.Write([]byte(errmsg + "\r\n"))
	self.w.Flush()
	self.mutex.Unlock()
}

func (self *SeedlinkConnection) _HELLO() {
	self.mutex.Lock()
	self.w.Write([]byte("SeedLink v4.0 [" + self.master.SoftwareId() + "] :: SLPROTO:4.0\r\n"))
	self.w.Write([]byte(self.master.Organization() + "\r\n"))
	self.w.Flush()
	self.mutex.Unlock()
}

func (self *SeedlinkConnection) _CAT() {
	self.mutex.Lock()

	for _, k := range self.master.StationList(self.ip) {
		s := self.master.StationConfig(k)
		self.w.Write(fmt.Appendf(nil, "%2s %-5s %s\r\n", k.NetworkCode, k.StationCode, s.Description))
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

func (self *SeedlinkConnection) _AUTH(string, string) {
	self._ERROR4("ARGUMENTS", "not implemented")
}

func (self *SeedlinkConnection) _SLPROTO(proto string) {
	if proto != "4.0" {
		self._ERROR4("ARGUMENTS", "invalid protocol version")
		return
	}

	self.slproto = 4
	self._OK()
}

func (self *SeedlinkConnection) _USERAGENT(proto string) {
	self._OK()
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

func (self *SeedlinkConnection) _STATION4(station string) {
	self.queueSet = map[string]*hmb.OpenParamQueue{}
	self.topicSet = map[string]bool{}

	if strings.ContainsAny(station, "*?") {
		rx := pat2rx(station)

		for _, k := range self.master.StationList(self.ip) {
			if !rx.MatchString(k.NetworkCode + "_" + k.StationCode) {
				continue
			}

			if _, ok := self.param.Queue["WAVE_"+k.NetworkCode+"_"+k.StationCode]; ok {
				continue
			}

			self.queueSet["WAVE_"+k.NetworkCode+"_"+k.StationCode] = &hmb.OpenParamQueue{Qlen: self.qlen, Oowait: self.oowait}
		}

	} else if stationId := strings.Split(station, "_"); len(stationId) == 2 {
		if s := self.master.StationConfig(StationKey{stationId[0], stationId[1]}); s == nil {
			self.Println("station not found")

		} else if len(s.ACL) != 0 && !s.ACL.Contains(self.ip) {
			self.Println("access denied")

		} else if _, ok := self.param.Queue["WAVE_"+station]; ok {
			self.Println("station already requested")

		} else {
			self.queueSet["WAVE_"+station] = &hmb.OpenParamQueue{Qlen: self.qlen, Oowait: self.oowait}
		}
	}

	self._OK()
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

func (self *SeedlinkConnection) _SELECT4(neg, stream, format string) {
	if self.queueSet == nil {
		self._ERROR4("UNEXPECTED", "no station selected")
		return
	}

	s := strings.Split(stream, "_")
	rx := regexp.MustCompile("^" + strings.ReplaceAll(strings.ReplaceAll(format, "?", "."), "*", ".*"))

	if len(s) == 4 && len(s[0]) <= 2 && len(s[1]) == 1 && len(s[2]) == 1 && len(s[3]) == 1 &&
		rx.MatchString("3D") {
		stream = s[0] + "_" + s[1] + s[2] + s[3] + "_D"

	} else {
		stream = "notexist"
	}

	if !self.topicSet[neg+stream] {
		self.topicSet[neg+stream] = true

		for _, q := range self.queueSet {
			if len(q.Topics) >= NSELECTORS {
				self._ERROR4("UNEXPECTED", "maximum number of selectors exceeded")
				return
			}
		}

		for _, q := range self.queueSet {
			if q.Topics == nil {
				q.Topics = make([]string, 0, NSELECTORS)
			}

			q.Topics = append(q.Topics, neg+stream)
		}
	}

	self._OK()
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

func (self *SeedlinkConnection) _DATA4(seq, starttime, endtime string) {
	if self.queueSet == nil {
		self._ERROR4("UNEXPECTED", "no station selected")
		return
	}

	var _seq hmb.Sequence
	var _starttime, _endtime hmb.Time

	if seq != "" {
		if strings.ToUpper(seq) == "ALL" {
			_seq = hmb.Sequence{0, true}

		} else if len(self.queueSet) > 1 {
			self._ERROR4("ARGUMENTS", "using sequence number with station wildcard is not supported")
			return

		} else if seq, err := strconv.ParseInt(seq, 10, 64); err != nil {
			self.Println(err)
			self._ERROR4("ARGUMENTS", "invalid sequence number")
			return

		} else {
			_seq = hmb.Sequence{seq, true}
		}

	}

	if starttime != "" {
		if starttime, err := time.Parse(TIME_FORMAT, starttime); err != nil {
			self.Println(err)
			self._ERROR4("ARGUMENTS", "invalid start time")
			return

		} else {
			_starttime = hmb.Time{starttime}
		}

	}

	if endtime != "" {
		if endtime, err := time.Parse(TIME_FORMAT, endtime); err != nil {
			self.Println(err)
			self._ERROR4("ARGUMENTS", "invalid end time")
			return

		} else {
			_endtime = hmb.Time{endtime}
		}
	}

	for i, q := range self.queueSet {
		q.Seq = _seq
		q.Starttime = _starttime
		q.Endtime = _endtime
		self.param.Queue[i] = q
	}

	self._OK()
}

func (self *SeedlinkConnection) _END() {
	self.hmb = hmb.NewClient(self.source, self.ip, self.param, self.timeout, self.retryWait, self)
	go self.dataServe(self.hmb)
}

func (self *SeedlinkConnection) _END4(fetch bool) {
	var keep bool = !fetch

	for _, q := range self.param.Queue {
		q.Keep = &keep
	}

	self.hmb = hmb.NewClient(self.source, self.ip, self.param, self.timeout, self.retryWait, self)
	go self.dataServe(self.hmb)
}

func (self *SeedlinkConnection) _INFO(item string) {
	if self.infoGen != nil {
		self.infoGen.ReadyWait()
	}

	var level int

	switch strings.ToUpper(item) {
	case "ID":
		level = INFO_ID

	case "CAPABILITIES":
		level = INFO_CAPABILITIES

	case "STATIONS":
		level = INFO_STATIONS

	case "STREAMS":
		level = INFO_STREAMS

	default:
		self.Println("unsupported info level")
		level = INFO_ERROR
	}

	self.infoGen = self.master.MSEEDInfoRequest(level, self.ip, self.w, &self.mutex)
	go self.infoServe(self.infoGen)
}

func (self *SeedlinkConnection) _INFO4(item, station, stream, format string) {
	if self.infoGen != nil {
		self.infoGen.ReadyWait()
	}

	var level int

	switch strings.ToUpper(item) {
	case "ID":
		level = INFO_ID

	case "FORMATS":
		level = INFO_FORMATS

	case "CAPABILITIES":
		level = INFO_CAPABILITIES

	case "STATIONS":
		level = INFO_STATIONS

	case "STREAMS":
		level = INFO_STREAMS

	default:
		self.Println("unsupported info level")
		level = INFO_ERROR
	}

	self.infoGen = self.master.JSONInfoRequest(level, pat2rx(station), pat2rx(stream), pat2rx(format), self.ip, self.w, &self.mutex)
	go self.infoServe(self.infoGen)
}

func (self *SeedlinkConnection) dataServe(h *hmb.Client) {
	defer h.Close()

	buf := [1024]byte{'S', 'E', '3', 'D'}

	var m *hmb.Message
	var err error

	for err == nil {
		if m, err = h.Recv(); err != nil {
			self.Println(err)

			if err == io.EOF {
				self.mutex.Lock()

				if _, err := self.w.Write([]byte("END")); err != nil {
					self.Println(err)
					self.conn.Close()

				} else if err := self.w.Flush(); err != nil {
					self.Println(err)
					self.conn.Close()
				}

				self.mutex.Unlock()

			} else if err != hmb.ECANCELED {
				self.conn.Close()
			}

		} else if m != nil && m.Type == "MSEED" && m.Queue[:5] == "WAVE_" {
			if data, ok := m.Data.Data.([]byte); !ok {
				self.Println("invalid MSEED message")

			} else {
				if self.slproto == 4 {
					idlen := len(m.Queue) - 5
					pllen := ms2to3(data, buf[17+idlen:])

					if pllen >= 0 {
						binary.LittleEndian.PutUint32(buf[4:8], uint32(pllen))
						binary.LittleEndian.PutUint64(buf[8:16], uint64(m.Seq.Value))
						buf[16] = byte(idlen)
						copy(buf[17:], m.Queue[5:])

						self.mutex.Lock()

						if _, err = self.w.Write(buf[:17+idlen+pllen]); err != nil {
							self.Println(err)
							self.conn.Close()

						} else if err = self.w.Flush(); err != nil {
							self.Println(err)
							self.conn.Close()
						}

						self.mutex.Unlock()
					}

				} else {
					header := fmt.Appendf(nil, "SL%06X", m.Seq.Value&0xffffff)

					self.mutex.Lock()

					if _, err = self.w.Write(header); err != nil {
						self.Println(err)
						self.conn.Close()

					} else if _, err = self.w.Write(data); err != nil {
						self.Println(err)
						self.conn.Close()

					} else if err = self.w.Flush(); err != nil {
						self.Println(err)
						self.conn.Close()
					}

					self.mutex.Unlock()
				}

				self.conn.SetReadDeadline(time.Now().Add(60 * time.Minute))
			}
		}
	}
}

func (self *SeedlinkConnection) infoServe(infoGen InfoGenerator) {
	if err := infoGen.Do(); err != nil {
		self.Println(err)
		self.conn.Close()
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
	for {
		self.conn.SetReadDeadline(time.Now().Add(60 * time.Minute))

		if !scanner.Scan() {
			break
		}

		cmd := scanner.Text()

		if self.slproto == 4 {
			for _, rx := range sl4commands {
				if a := rx.FindStringSubmatch(cmd); a != nil {
					self.Println(cmd)

					kw := strings.ToUpper(a[1])

					if self.hmb != nil && kw != "INFO" && kw != "BYE" {
						self.Println("exiting transfer state")
						self.hmb.CancelRequest()
						self.hmb = nil
					}

					switch kw {
					case "AUTH":
						self._AUTH(a[2], a[3])
						continue loop

					case "BYE":
						break loop

					case "DATA":
						self._DATA4(a[2], a[3], a[4])
						continue loop

					case "END":
						self._END4(false)
						continue loop

					case "ENDFETCH":
						self._END4(true)
						continue loop

					case "HELLO":
						self._HELLO()
						continue loop

					case "INFO":
						self._INFO4(a[2], a[3], a[4], a[5])
						continue loop

					case "SELECT":
						self._SELECT4(a[2], a[3], a[4])
						continue loop

					case "STATION":
						self._STATION4(a[2])
						continue loop

					case "USERAGENT":
						self._USERAGENT(a[2])
						continue loop
					}
				}
			}

		} else {
			for _, rx := range sl3commands {
				if a := rx.FindStringSubmatch(cmd); a != nil {
					self.Println(cmd)

					kw := strings.ToUpper(a[1])

					if self.hmb != nil && kw != "INFO" && kw != "BYE" {
						self.Println("exiting transfer state")
						self.hmb.CancelRequest()
						self.hmb = nil
					}

					switch kw {
					case "BATCH":
						self.slproto = 3
						self._BATCH()
						continue loop

					case "BYE":
						break loop

					case "CAT":
						self.slproto = 3
						self._CAT()
						continue loop

					case "DATA":
						self.slproto = 3
						self._DATA(a[2], a[3], a[4], a[5], a[6], a[7], a[8])
						continue loop

					case "END":
						self.slproto = 3
						self._END()
						continue loop

					case "FETCH":
						self.slproto = 3
						self._FETCH(a[2], a[3], a[4], a[5], a[6], a[7], a[8])
						continue loop

					case "HELLO":
						self._HELLO()
						continue loop

					case "INFO":
						self.slproto = 3
						self._INFO(a[2])
						continue loop

					case "SELECT":
						self.slproto = 3
						self._SELECT(a[2], a[3], a[4], a[5]+a[6])
						continue loop

					case "SLPROTO":
						self._SLPROTO(a[2])
						continue loop

					case "STATION":
						self.slproto = 3
						self._STATION(a[2], a[3])
						continue loop

					case "TIME":
						self.slproto = 3
						self._TIME(a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13])
						continue loop
					}
				}
			}
		}

		self.Println("invalid command:", cmd)

		if self.hmb != nil {
			self.Println("exiting transfer state")
			self.hmb.CancelRequest()
			self.hmb = nil
		}

		if self.slproto == 4 {
			self._ERROR4("UNSUPPORTED", "invalid command or syntax")

		} else {
			self._ERROR()
		}
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
