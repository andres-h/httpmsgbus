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
	"bitbucket.org/andresh/httpmsgbus/apps/go/src/regexp"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	_log "log"
	"log/syslog"
	"os"
	"strings"
	"syscall"
	"time"
)

const VERSION = "0.1 (2024.010)"

const (
	PLUGINFD        = 63
	HDRSIZE         = 60
	DATASIZE_MAX    = 4000
	SYSLOG_FACILITY = syslog.LOG_LOCAL0
	SYSLOG_SEVERITY = syslog.LOG_NOTICE
)

var regexTopicName = regexp.MustCompile("^[\\w]*$")

var log = _log.New(os.Stdout, "", _log.LstdFlags)

type StationKey struct {
	NetworkCode string
	StationCode string
}

type SeqTime struct {
	seq int64
	t   time.Time
}

type Worker struct {
	r       io.Reader
	hmb     *hmb.Client
	utrx    *regexp.Regexp
	bufsize int
	seqt    map[StationKey]*SeqTime
	buf     []byte
	ri      int
	wi      int
	msgs    []*hmb.Message
	ping    chan bool
	pong    chan bool
}

func (self *Worker) readPacket() error {
	if self.wi-self.ri < HDRSIZE {
		if n, err := io.ReadAtLeast(self.r, self.buf[self.wi:], HDRSIZE-(self.wi-self.ri)); err != nil {
			return err

		} else {
			self.wi += n
		}
	}

	hdr := self.buf[self.ri : self.ri+HDRSIZE]
	packetType := int(binary.LittleEndian.Uint32(hdr[0:4]))
	sta := string(bytes.TrimRight(hdr[4:14], "\x00"))
	cha := string(bytes.TrimRight(hdr[14:24], "\x00"))
	year := int(binary.LittleEndian.Uint32(hdr[24:28]))
	yday := int(binary.LittleEndian.Uint32(hdr[28:32]))
	hour := int(binary.LittleEndian.Uint32(hdr[32:36]))
	minute := int(binary.LittleEndian.Uint32(hdr[36:40]))
	second := int(binary.LittleEndian.Uint32(hdr[40:44]))
	usec := int(binary.LittleEndian.Uint32(hdr[44:48]))
	tqSeq := int64(int32(binary.LittleEndian.Uint32(hdr[52:56])))
	dataSize := int(binary.LittleEndian.Uint32(hdr[56:60]))

	if packetType < 8 || packetType > 13 {
		log.Fatal("invalid packet type", packetType)
	}

	if dataSize > DATASIZE_MAX {
		log.Fatal("invalid data size", dataSize)
	}

	self.ri += HDRSIZE

	if self.wi-self.ri < dataSize+HDRSIZE {
		if len(self.buf) < dataSize+HDRSIZE {
			newbuf := make([]byte, dataSize+HDRSIZE)
			copy(newbuf, self.buf[self.ri:self.wi])
			self.buf = newbuf
			self.wi -= self.ri
			self.ri = 0

		} else if len(self.buf)-self.ri < dataSize+HDRSIZE {
			copy(self.buf, self.buf[self.ri:self.wi])
			self.wi -= self.ri
			self.ri = 0
		}

		if n, err := io.ReadAtLeast(self.r, self.buf[self.wi:], dataSize-(self.wi-self.ri)); err != nil {
			return err

		} else {
			self.wi += n
		}
	}

	data := make([]byte, dataSize)
	copy(data, self.buf[self.ri : self.ri+dataSize])
	self.ri += dataSize

	if packetType == 12 {
		if !strings.Contains(sta, "_") {
			log.Printf("station ID %s is not in network_station format", sta)
		}

		timestamp := time.Date(year, time.Month(1), 1, hour, minute, second, usec*1000, time.UTC).Add(time.Duration((yday-1)*24) * time.Hour)

		if t := timestamp.Unix(); t < 0 || t > 253402297199 {
			log.Println("LOG_"+sta, "invalid time:", timestamp)
			return nil
		}

		self.msgs = append(self.msgs, &hmb.Message{
			Type:      "TEXT",
			Queue:     "LOG_" + sta,
			Starttime: hmb.Time{timestamp},
			Endtime:   hmb.Time{timestamp},
			Data:      hmb.Payload{bytes.TrimSpace(data)},
		})

	} else if packetType == 13 {
		sta := string(bytes.TrimSpace(data[8:13]))
		loc := string(bytes.TrimSpace(data[13:15]))
		mcha := string(bytes.TrimSpace(data[15:18]))
		net := string(bytes.TrimSpace(data[18:20]))
		year := int(binary.BigEndian.Uint16(data[20:22]))
		yday := int(binary.BigEndian.Uint16(data[22:24]))
		hour := int(data[24])
		minute := int(data[25])
		second := int(data[26])
		tms := int(binary.BigEndian.Uint16(data[28:30]))
		nsamp := int(binary.BigEndian.Uint16(data[30:32]))
		srfact := float64(int16(binary.BigEndian.Uint16(data[32:34])))
		srmult := float64(int16(binary.BigEndian.Uint16(data[34:36])))

		if srfact < 0 {
			srfact = -1.0 / srfact
		}

		if srmult < 0 {
			srmult = -1.0 / srmult
		}

		sr := srfact * srmult
		seqt, ok := self.seqt[StationKey{net, sta}]

		if !ok {
			seqt = &SeqTime{}
			self.seqt[StationKey{net, sta}] = seqt
		}

		var topic string
		var seq hmb.Sequence

		if cha == "" {
			topic = loc + "_" + mcha + "_D"

		} else {
			topic = cha

			if tqSeq != -1 {
				seq.Value = (seqt.seq &^ 0xffffff) | (tqSeq & 0xffffff)
				seq.Set = true

				if seq.Value < seqt.seq {
					seq.Value += 0x1000000
				}

				seqt.seq = seq.Value
			}
		}

		var stime, etime time.Time

		if self.utrx != nil && self.utrx.MatchString(topic) && !seqt.t.IsZero() {
			etime = seqt.t

			if sr != 0 {
				stime = etime.Add(-time.Duration(float64(time.Second) * float64(nsamp) / sr))
			} else {
				stime = etime
			}

		} else {
			stime = time.Date(year, time.Month(1), 1, hour, minute, second, tms*100000, time.UTC).Add(time.Duration((yday-1)*24) * time.Hour)

			if sr != 0 {
				etime = stime.Add(time.Duration(float64(time.Second) * float64(nsamp) / sr))
			} else {
				etime = stime
			}

			seqt.t = etime
		}

		if t := stime.Unix(); t < 0 || t > 253402297199 {
			log.Println("WAVE_"+net+"_"+sta, topic, seq, "invalid starttime:", stime)
			return nil
		}

		if t := etime.Unix(); t < 0 || t > 253402297199 {
			log.Println("WAVE_"+net+"_"+sta, topic, seq, "invalid endtime:", etime)
			return nil
		}

		if !regexTopicName.MatchString(topic) {
			log.Println("WAVE_"+net+"_"+sta, seq, "invalid topic:", topic)
			return nil
		}

		self.msgs = append(self.msgs, &hmb.Message{
			Type:      "MSEED",
			Queue:     "WAVE_" + net + "_" + sta,
			Topic:     topic,
			Seq:       seq,
			Starttime: hmb.Time{stime},
			Endtime:   hmb.Time{etime},
			Data:      hmb.Payload{data},
		})
	}

	return nil
}

func (self *Worker) reader() {
	for {
		if err := self.readPacket(); err != nil {
			log.Fatal(err)

		} else if len(self.msgs) == 0 {
			continue

		} else if len(self.msgs) < cap(self.msgs) {
			self.ping <- true

			select {
			case <-self.ping:
			case <-self.pong:
			}

		} else {
			self.ping <- true
			<-self.pong
		}
	}
}

func (self *Worker) writer() {
	msgs := make([]*hmb.Message, self.bufsize)

	for {
		<-self.ping
		n := len(self.msgs)
		copy(msgs, self.msgs)
		self.msgs = self.msgs[:0]
		self.pong <- true

		if err := self.hmb.Send(msgs[:n]); err != nil {
			log.Fatal(err)
		}
	}
}

func (self *Worker) Start() {
	log.Println("fetching queue info")

	if queues, err := self.hmb.Info(); err != nil {
		log.Fatal(err)

	} else {
		for k, q := range queues {
			if s := strings.Split(k, "_"); len(s) == 3 && s[0] == "WAVE" {
				self.seqt[StationKey{s[1], s[2]}] = &SeqTime{q.Endseq.Value, q.Endtime.Time}
			}
		}
	}

	log.Println("starting read-write loop")

	go self.reader()
	self.writer()
}

func main() {
	cmd := flag.String("C", "", "Plugin command line")
	sink := flag.String("H", "", "Destination HMB URL")
	showVersion := flag.Bool("V", false, "Show program's version and exit")
	utch := flag.String("X", "", "Regex matching channels with unreliable timing")
	bufsize := flag.Int("b", 1024, "Maximum number of messages to buffer")
	useSyslog := flag.Bool("s", false, "Log via syslog")
	timeout := flag.Int("t", 120, "HMB timeout in seconds")

	flag.Parse()

	if *showVersion {
		fmt.Printf("wavefeed v%s\n", VERSION)
		return
	}

	if *cmd == "" {
		log.Fatal("missing plugin command line")
	}

	if *sink == "" {
		log.Fatal("missing destination HMB URL")
	}

	var utrx *regexp.Regexp

	if *utch != "" {
		var err error
		utrx, err = regexp.Compile(*utch)

		if err != nil {
			log.Fatal(err)
		}
	}

	if *useSyslog {
		if l, err := syslog.NewLogger(SYSLOG_FACILITY|SYSLOG_SEVERITY, 0); err != nil {
			log.Fatal(err)

		} else {
			log.Println("logging via syslog")
			log = l
		}
	}

	log.Printf("wavefeed v%s started", VERSION)

	if r, w, err := os.Pipe(); err != nil {
		log.Fatal(err)

	} else if err := syscall.Dup2(int(w.Fd()), PLUGINFD); err != nil {
		log.Fatal(err)

	} else if _, err := os.StartProcess("/bin/sh", []string{"sh", "-c", *cmd},
		&os.ProcAttr{Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}}); err != nil {
		log.Fatal(err)

	} else {
		w.Close()

		h := hmb.NewClient(*sink, nil, &hmb.OpenParam{}, *timeout, 10, log)

		worker := &Worker{
			r:       r,
			hmb:     h,
			utrx:    utrx,
			bufsize: *bufsize,
			seqt:    make(map[StationKey]*SeqTime),
			buf:     make([]byte, 1024),
			msgs:    make([]*hmb.Message, 0, *bufsize),
			ping:    make(chan bool, 1),
			pong:    make(chan bool, 1),
		}

		worker.Start()
	}
}
