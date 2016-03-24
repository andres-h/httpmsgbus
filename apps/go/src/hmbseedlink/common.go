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
	"net"
	"sync"
	"time"
)

type StationKey struct {
	NetworkCode string
	StationCode string
}

type IPNetACL []net.IPNet

func (self IPNetACL) Contains(ip net.IP) bool {
	for _, v := range self {
		if v.Contains(ip) {
			return true
		}
	}

	return false
}

type StationConfig struct {
	Description string
	ACL         IPNetACL
}

type MasterInterface interface {
	SoftwareId() string
	Organization() string
	Started() time.Time
	StationList(net.IP) []StationKey
	StationConfig(StationKey) *StationConfig
	InfoRequest(int, string, net.IP, *bufio.Writer, *sync.Mutex) *InfoGenerator
	EndConnection(net.IP)
}
