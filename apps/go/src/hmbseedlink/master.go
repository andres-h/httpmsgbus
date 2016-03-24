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
	"fmt"
	"hmb"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

type StationKeys []StationKey

func (self StationKeys) Len() int {
	return len(self)
}

func (self StationKeys) Less(i, j int) bool {
	return self[i].NetworkCode < self[j].NetworkCode ||
		(self[i].NetworkCode == self[j].NetworkCode && self[i].StationCode < self[j].StationCode)
}

func (self StationKeys) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

type Master struct {
	softwareId   string
	organization string
	source       string
	timeout      int
	retryWait    int
	infoRefresh  int
	connsPerIP   int
	qlen         int
	oowait       int
	hmb          *hmb.Client
	param        *hmb.OpenParam
	infoCache    *InfoCache
	started      time.Time
	stations     map[StationKey]*StationConfig
	ipCount      map[string]int
	readyNotify  chan struct{}
	mutex        sync.Mutex
}

func NewMaster(softwareId string, organization string, source string, timeout int, retryWait int, infoRefresh int, connsPerIP int, qlen int, oowait int) *Master {
	self := &Master{
		softwareId:   softwareId,
		organization: organization,
		source:       source,
		timeout:      timeout,
		retryWait:    retryWait,
		infoRefresh:  infoRefresh,
		connsPerIP:   connsPerIP,
		qlen:         qlen,
		oowait:       oowait,
		started:      time.Now().UTC(),
		stations:     make(map[StationKey]*StationConfig),
		ipCount:      make(map[string]int),
		readyNotify:  make(chan struct{}),
	}

	self.param = &hmb.OpenParam{
		HeartbeatInterval: self.timeout / 2,
		Queue: map[string]*hmb.OpenParamQueue{
			"ANNOUNCEMENT": {
				Seq: hmb.Sequence{-1, true},
			},
			"STATION_CONFIG": {
				Seq: hmb.Sequence{0, true},
			},
		},
	}

	go self.start()
	return self
}

func (self *Master) Println(v ...interface{}) {
	args := make([]interface{}, 1, len(v)+1)
	args[0] = "[master]"
	log.Println(append(args, v...)...)
}

func (self *Master) Printf(format string, v ...interface{}) {
	self.Println(fmt.Sprintf(format, v...))
}

func (self *Master) openHMB(keep bool) {
	for _, q := range self.param.Queue {
		q.Keep = &keep
	}

	self.hmb = hmb.NewClient(self.source, nil, self.param, self.timeout, self.retryWait, self)
}

func (self *Master) start() {
	self.Println("waiting for station configuration")
	self.openHMB(false)

	for {
		if m, err := self.hmb.Recv(); err == io.EOF {
			self.Println("received configuration of", len(self.stations), "stations")

			select {
			case <-self.readyNotify:
				self.Println("multiple EOF (should not happen)")
				continue

			default:
				close(self.readyNotify)
			}

			self.hmb.Close()
			self.openHMB(true)
			self.infoCache = NewInfoCache(self.hmb, self.infoRefresh)

		} else if err != nil {
			self.Println("FATAL:", err)
			os.Exit(1)

		} else if m == nil || m.Type != "STATION_CONFIG" {
			continue

		} else if data, ok := m.Data.Data.(map[string]interface{}); !ok {
			self.Println("invalid STATION_CONFIG message")

		} else if networkCode, ok := data["networkCode"].(string); !ok {
			self.Println("invalid STATION_CONFIG message (networkCode)")

		} else if stationCode, ok := data["stationCode"].(string); !ok {
			self.Println("invalid STATION_CONFIG message (stationCode)")

		} else if desc, ok := data["description"].(string); !ok {
			self.Println("invalid STATION_CONFIG message (description)")

		} else if access, ok := data["access"].([]interface{}); !ok {
			self.Println("invalid STATION_CONFIG message (access)")

		} else {
			acl := make(IPNetACL, len(access))

			for i, cidr := range access {
				if a, ok := cidr.([]byte); ok {
					var ipnet net.IPNet

					switch len(a) {
					case net.IPv4len:
						ipnet.IP = net.IP(a)
						ipnet.Mask = net.CIDRMask(net.IPv4len*8, net.IPv4len*8)

					case net.IPv4len + 1:
						ipnet.IP = net.IP(a[:net.IPv4len])
						ipnet.Mask = net.CIDRMask(int(a[net.IPv4len]), net.IPv4len*8)

					case net.IPv6len:
						ipnet.IP = net.IP(a)
						ipnet.Mask = net.CIDRMask(net.IPv6len*8, net.IPv6len*8)

					case net.IPv6len + 1:
						ipnet.IP = net.IP(a[:net.IPv6len])
						ipnet.Mask = net.CIDRMask(int(a[net.IPv6len]), net.IPv6len*8)
					}

					if ipnet.Mask != nil {
						acl[i] = ipnet
						continue
					}

				} else if a, ok := cidr.(string); ok {
					if ip := net.ParseIP(a); ip != nil {
						acl[i] = net.IPNet{ip, net.CIDRMask(8*len(ip), 8*len(ip))}
						continue

					} else if _, ipnet, err := net.ParseCIDR(a); err == nil {
						acl[i] = *ipnet
						continue
					}

				}

				self.Println("invalid address/mask:", cidr)
			}

			self.mutex.Lock()
			self.stations[StationKey{networkCode, stationCode}] = &StationConfig{desc, acl}
			self.mutex.Unlock()
		}
	}
}

func (self *Master) ReadyNotify() <-chan struct{} {
	return self.readyNotify
}

func (self *Master) SoftwareId() string {
	return self.softwareId
}

func (self *Master) Organization() string {
	return self.organization
}

func (self *Master) Started() time.Time {
	return self.started
}

func (self *Master) StationList(ip net.IP) []StationKey {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	result := StationKeys{}

	for k, s := range self.stations {
		if len(s.ACL) == 0 || s.ACL.Contains(ip) {
			result = append(result, k)
		}
	}

	sort.Sort(result)
	return result
}

func (self *Master) StationConfig(key StationKey) *StationConfig {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.stations[key]
}

func (self *Master) InfoRequest(level int, seedname string, ip net.IP, w *bufio.Writer, mutex *sync.Mutex) *InfoGenerator {
	return NewInfoGenerator(level, seedname, ip, w, mutex, self, self.infoCache)
}

func (self *Master) ApproveConnection(ip net.IP) bool {
	ipstr := ip.String()

	if self.connsPerIP > 0 && self.ipCount[ipstr] >= self.connsPerIP {
		self.Printf("maximum number of connections per IP exceeded (%s)", ip.String())
		return false
	}

	self.mutex.Lock()
	self.ipCount[ipstr]++
	self.mutex.Unlock()

	return true
}

func (self *Master) EndConnection(ip net.IP) {
	ipstr := ip.String()

	self.mutex.Lock()
	self.ipCount[ipstr]--

	if self.ipCount[ipstr] == 0 {
		delete(self.ipCount, ipstr)
	}

	self.mutex.Unlock()
}

func (self *Master) SeedlinkConnect(conn net.Conn) *SeedlinkConnection {
	ip := conn.RemoteAddr().(*net.TCPAddr).IP

	if !self.ApproveConnection(ip) {
		// wait 1 sec to prevent the client from reconnecting too soon
		time.Sleep(time.Second)
		return nil
	}

	return NewSeedlinkConnection(self, conn, ip, self.source, self.timeout, self.retryWait, self.qlen, self.oowait)
}
