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
	"flag"
	"fmt"
	_log "log"
	"log/syslog"
	"net"
	"os"
	"runtime"
)

const VERSION = "0.1 (2016.092)"

const (
	SYSLOG_FACILITY = syslog.LOG_LOCAL0
	SYSLOG_SEVERITY = syslog.LOG_NOTICE
)

var log = _log.New(os.Stdout, "", _log.LstdFlags)

func main() {
	source := flag.String("H", "", "Source HMB URL")
	organization := flag.String("O", "", "Organization")
	port := flag.Int("P", 18000, "TCP port")
	showVersion := flag.Bool("V", false, "Show program's version and exit")
	connsPerIP := flag.Int("c", 10, "Connections per IP")
	qlen := flag.Int("d", 0, "Limit backlog of records (queue length)")
	useSyslog := flag.Bool("s", false, "Log via syslog")
	timeout := flag.Int("t", 120, "HMB timeout in seconds")
	oowait := flag.Int("w", 0, "Wait for out-of-order data in seconds")

	flag.Parse()

	if *showVersion {
		fmt.Printf("HMB SeedLink v%s\n", VERSION)
		return
	}

	if *source == "" {
		log.Fatal("missing source HMB URL")
	}

	if *useSyslog {
		if l, err := syslog.NewLogger(SYSLOG_FACILITY|SYSLOG_SEVERITY, 0); err != nil {
			log.Fatal(err)

		} else {
			log.Println("logging via syslog")
			log = l
		}
	}

	log.Printf("HMB SeedLink v%s started", VERSION)
	log.Printf("using up to %d threads", runtime.GOMAXPROCS(-1))
	log.Printf("listening at :%d", *port)

	master := NewMaster("HMB SeedLink v"+VERSION, *organization, *source, *timeout, 10, 10, *connsPerIP, *qlen, *oowait)
	<-master.ReadyNotify()

	if listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port)); err != nil {
		log.Fatal("Listen:", err)

	} else {
		for {
			if conn, err := listener.Accept(); err != nil {
				log.Println("Accept:", err)

			} else {
				master.SeedlinkConnect(conn)
			}
		}
	}
}
