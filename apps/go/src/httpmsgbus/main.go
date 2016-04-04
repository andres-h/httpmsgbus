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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tylerb/graceful.v1"
	"io"
	_log "log"
	"log/syslog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const VERSION = "0.16 (2016.095)"

const (
	// The following parameters should be tuned for optimum performance.
	// Some of those could be made user-configurable.

	// Number of goroutines that perform parallel insert operations.
	INSERTS_PARALLEL = 10

	// Number of goroutines that perform parallel query operations.
	QUERIES_PARALLEL = 10

	// Number of queries queued. HMB does not queue more than one query
	// per client, so this sets the maximum number of clients that can
	// perform a query (eg., receive non-realtime data) at the same time.
	// Connections exceeding the limit are dropped by the server.
	QUERIES_QUEUED = 1024

	// Maximum number of messages returned by one query.
	QUERY_LIMIT = 1024

	// Buffer size of reply channel. When the buffer becomes full, the
	// query is stopped with ECONTINUE (even if QUERY_LIMIT is not yet
	// reached).
	REPLY_CHAN_SIZE = 64

	// filedb: number of blocks per file by default.
	BLOCKS_PER_FILE_DEFAULT = 1024

	// filedb: default blocksize. Message size cannot exceed blocksize when
	// using filedb.
	BLOCKSIZE_DEFAULT = 1024

	// filedb: default buffer size. When performing any query, file reads
	// are done with this granularity.
	BUFSIZE_DEFAULT = 1024 * 64

	// filedb: maximum number of file handles to leave open by default.
	// LRU cache is used to cache file handles.
	MAX_OPEN_FILES_DEFAULT = 800

	// Syslog facility.
	SYSLOG_FACILITY = syslog.LOG_LOCAL0

	// Syslog severity.
	SYSLOG_SEVERITY = syslog.LOG_NOTICE

	// Number of seconds to wait for open connections to finish when
	// shutting down.
	GRACEFUL_TIMEOUT = 10
)

// log is a global logger that can be switched to Syslog.
var log = _log.New(os.Stdout, "", _log.LstdFlags)

// Handler implements the http.Handler interface
type Handler struct {
	bufferSize     int               // Commandline parameter
	postSize       int               // Commandline parameter
	sessionTimeout int               // Commandline parameter
	sessionsPerIP  int               // Commandline parameter
	delta          int               // Commandline parameter
	useXFF         bool              // Commandline parameter
	repoFactory    RepositoryFactory // Constructs mongodb or filedb repository
	stopChan       chan struct{}     // Closed when SIGINT or SIGTERM received
	busses         map[string]*Bus   // Busses managed by this HMB instance
	mutex          sync.Mutex        // Protects the above map from concurrent access
}

// recv implements the /recv and /stream (if streaming == true) methods of HMB.
func (self *Handler) recv(bus *Bus, w http.ResponseWriter, r *http.Request, args []string, streaming bool) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var sid, queue string
	var seq int64

	if len(args) == 1 {
		sid = args[0]

	} else if len(args) == 3 {
		sid = args[0]
		queue = args[1]

		var err error
		seq, err = strconv.ParseInt(args[2], 10, 64)

		if err != nil {
			LogRequestError(r, "invalid session")
			http.Error(w, "invalid session", http.StatusBadRequest)
			return
		}

	} else {
		LogRequestError(r, "not found")
		http.NotFound(w, r)
		return
	}

	// Acquire a session that must have been created previously using /open.
	if sess, err := bus.AcquireSession(sid); err != nil {
		LogRequestError(r, err.Error())
		http.Error(w, err.Error(), HttpStatus(err))

	} else {
		// Make sure that the session is released afterwards, so it
		// becomes "inactive" and can be purged after sessionTimeout.
		defer bus.ReleaseSession(sid)

		// Check for sequence continuity.
		if !sess.Validate(queue, seq) {
			LogRequestError(r, "invalid session")
			http.Error(w, "invalid session", http.StatusBadRequest)

		} else {
			// Create a WriterCounter that counts the number of
			// bytes written (needed by /status).
			// WriterCounter wraps http.ResponseWriter and
			// implements the BytesWriter interface itself.
			wc := sess.WriterCounter(w.(BytesWriter))

			var mw MessageWriter

			// Create a MessageWriter by wrapping WriterCounter in
			// JsonWriter or BsonWriter respectively.
			if sess.Format() == "JSON" {
				w.Header().Add("Content-Type", "application/json")
				mw = NewJsonWriter(wc)

			} else {
				w.Header().Add("Content-Type", "application/bson")
				mw = NewBsonWriter(wc)
			}

			// Start a producer, which pulls data from the queues
			// and sends to client.
			NewProducer(sess, streaming, mw, self.stopChan).Start()
		}
	}
}

func (self *Handler) send(bus *Bus, w http.ResponseWriter, r *http.Request, args []string) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if len(args) != 1 {
		LogRequestError(r, "not found")
		http.NotFound(w, r)
		return
	}

	sid := args[0]

	// Acquire a session that must have been created previously using /open.
	if sess, err := bus.AcquireSession(sid); err != nil {
		LogRequestError(r, err.Error())
		http.Error(w, err.Error(), HttpStatus(err))

	} else {
		// Make sure that the session is released afterwards, so it
		// becomes "inactive" and can be purged after sessionTimeout.
		defer bus.ReleaseSession(sid)

		// Create a ReaderCounter that counts the number of
		// bytes read (needed by /status).
		// ReaderCounter wraps http.Request.Body and
		// implements the BytesReader interface itself.
		rc := sess.ReaderCounter(r.Body.(BytesReader))

		var mr MessageReader

		// Create a MessageReader by wrapping ReaderCounter in
		// JsonReader or BsonReader respectively.
		if r.Header.Get("Content-Type") == "application/json" {
			mr = NewJsonReader(rc)
		} else {
			mr = NewBsonReader(rc)
		}

		// Read the messages and push them to the bus.
		for {
			if m, err := mr.Read(); err != nil {
				if err != io.EOF {
					LogRequestError(r, err.Error())
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				break

			} else {
				m.Sender = sess.Cid()

				if err := bus.Push(m); err != nil {
					LogRequestError(r, err.Error())
					http.Error(w, err.Error(), HttpStatus(err))
					return
				}
			}
		}

		// Tell the client that the messages were successful received.
		w.WriteHeader(http.StatusNoContent)
	}
}

// open implements the /open method of HMB.
func (self *Handler) open(bus *Bus, w http.ResponseWriter, r *http.Request, args []string) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if len(args) != 0 {
		LogRequestError(r, "not found")
		http.NotFound(w, r)
		return
	}

	host, port, err := net.SplitHostPort(r.RemoteAddr)

	if err != nil {
		panic(err)
	}

	if self.useXFF {
		// Get client IP from the X-Forwarded-For header if available.
		// This is useful if a front-end such as hmbseedlink is used,
		// in which case the client would be the machine where the
		// front-end is running on, not the actual user.
		if xff := strings.Split(r.Header.Get("X-Forwarded-For"), ", "); xff[0] != "" {
			host = xff[0]
		}
	}

	// If the client sends JSON, we reply in JSON, otherwise in BSON.
	if r.Header.Get("Content-Type") == "application/json" {
		decoder := json.NewDecoder(r.Body)

		var param OpenParam

		if err := decoder.Decode(&param); err != nil {
			LogRequestError(r, err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)

		} else if ack, err := bus.Open(host, port, "JSON", &param); err != nil {
			LogRequestError(r, err.Error())
			http.Error(w, err.Error(), HttpStatus(err))

		} else if ack, err := json.Marshal(ack); err != nil {
			panic(err)

		} else {
			w.Header().Add("Content-Type", "application/json")

			if _, err := w.Write(ack); err != nil {
				LogRequestError(r, err.Error())
			}
		}

	} else {
		buf := bytes.Buffer{}

		var param OpenParam

		if _, err := buf.ReadFrom(r.Body); err != nil {
			LogRequestError(r, err.Error())

		} else if err := bson.Unmarshal(buf.Bytes(), &param); err != nil {
			LogRequestError(r, err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)

		} else if ack, err := bus.Open(host, port, "BSON", &param); err != nil {
			LogRequestError(r, err.Error())
			http.Error(w, err.Error(), HttpStatus(err))

		} else if ack, err := bson.Marshal(ack); err != nil {
			panic(err)

		} else {
			w.Header().Add("Content-Type", "application/bson")

			if _, err := w.Write(ack); err != nil {
				LogRequestError(r, err.Error())
			}
		}
	}
}

// info implements the /info method of the HMB.
func (self *Handler) info(bus *Bus, w http.ResponseWriter, r *http.Request, args []string) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if len(args) != 0 {
		LogRequestError(r, "not found")
		http.NotFound(w, r)
		return
	}

	w.Header().Add("Content-Type", "application/json")

	if err := bus.Info(w); err != nil {
		LogRequestError(r, err.Error())
	}
}

// status implements the /status method of the HMB.
func (self *Handler) status(bus *Bus, w http.ResponseWriter, r *http.Request, args []string) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if len(args) != 0 {
		LogRequestError(r, "not found")
		http.NotFound(w, r)
		return
	}

	w.Header().Add("Content-Type", "application/json")

	if err := bus.Status(w); err != nil {
		LogRequestError(r, err.Error())
	}
}

// features implements the /features method of the HMB.
func (self *Handler) features(bus *Bus, w http.ResponseWriter, r *http.Request, args []string) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if len(args) != 0 {
		LogRequestError(r, "not found")
		http.NotFound(w, r)
		return
	}

	features := map[string]interface{}{
		"software":  "httpmsgbus v" + VERSION,
		"functions": []string{},
		"capabilities": []string{
			"JSON",
			"BSON",
			"INFO",
			"STREAM",
			"WINDOW",
			"FILTER",
			"OOD",
		},
	}

	if features, err := json.Marshal(features); err != nil {
		panic(err)

	} else {
		w.Header().Add("Content-Type", "application/json")

		if _, err := w.Write(features); err != nil {
			LogRequestError(r, err.Error())
		}
	}
}

// ServeHTTP is called by the http.Server for each request.
func (self *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if p := CleanPath(r.URL.Path); p != r.URL.Path {
		url := *r.URL
		url.Path = p
		http.Redirect(w, r, url.String(), http.StatusMovedPermanently)
		return
	}

	if args := strings.Split(r.URL.Path[1:], "/"); len(args) < 2 {
		LogRequestError(r, "not found")
		http.NotFound(w, r)

	} else {
		busname := args[0]
		method := args[1]
		args := args[2:]

		self.mutex.Lock()

		bus, ok := self.busses[busname]

		if !ok {
			// Create a new bus object. If database is enabled, a repository is
			// opened or created as needed.
			var repo Repository
			var err error

			if self.repoFactory == nil {
				repo = nil

			} else {
				repo, err = self.repoFactory.Repository(busname)

				if err != nil {
					LogRequestError(r, err.Error())
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					self.mutex.Unlock()
					return
				}
			}

			bus, err = NewBus(repo, self.bufferSize, QUERY_LIMIT, self.delta, self.sessionTimeout, self.sessionsPerIP)

			if err != nil {
				LogRequestError(r, err.Error())
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				self.mutex.Unlock()
				return
			}

			self.busses[busname] = bus
		}

		self.mutex.Unlock()

		// Add POST size limiter.
		r.Body = http.MaxBytesReader(w, r.Body, int64(self.postSize)*1024)

		switch method {
		case "open":
			self.open(bus, w, r, args)

		case "send":
			self.send(bus, w, r, args)

		case "recv":
			self.recv(bus, w, r, args, false)

		case "stream":
			self.recv(bus, w, r, args, true)

		case "info":
			self.info(bus, w, r, args)

		case "status":
			self.status(bus, w, r, args)

		case "features":
			self.features(bus, w, r, args)

		default:
			LogRequestError(r, "not found")
			http.NotFound(w, r)
		}
	}
}

func main() {
	database := flag.String("D", "", "Database URL")
	useXFF := flag.Bool("F", false, "Use X-Forwarded-For")
	port := flag.Int("P", 8000, "TCP port")
	showVersion := flag.Bool("V", false, "Show program's version and exit")
	bufferSize := flag.Int("b", 100, "Buffer (RAM) size in messages per queue")
	sessionsPerIP := flag.Int("c", 10, "Connections (sessions) per IP")
	delta := flag.Int("d", 0, "Maximum sequence difference into future (default 0)")
	postSize := flag.Int("p", 10240, "Maximum size of POST data in KB")
	queueSize := flag.Int("q", 256, "Queue (MongoDB capped collection) size in MB")
	useSyslog := flag.Bool("s", false, "Log via syslog")
	sessionTimeout := flag.Int("t", 120, "Session timeout in seconds")

	flag.Parse()

	if *showVersion {
		fmt.Printf("httpmsgbus v%s\n", VERSION)
		return
	}

	var repoFactory RepositoryFactory

	// Parse the database option and create the respective RepositoryFactory.
	if *database == "" {
		repoFactory = nil

	} else if u, err := url.Parse(*database); err != nil {
		log.Fatal(err)

	} else if u.Scheme == "mongodb" {
		if mgoSession, err := mgo.Dial(*database); err != nil {
			log.Fatal(err)

		} else {
			mgoSession.SetSafe(nil)

			var err error
			repoFactory, err = NewMongoRepositoryFactory(mgoSession, *queueSize, INSERTS_PARALLEL, *bufferSize/2, QUERIES_PARALLEL, QUERIES_QUEUED, REPLY_CHAN_SIZE)

			if err != nil {
				log.Fatal(err)
			}
		}

	} else if u.Scheme == "filedb" {
		blocksPerFile := BLOCKS_PER_FILE_DEFAULT
		blocksize := BLOCKSIZE_DEFAULT
		bufsize := BUFSIZE_DEFAULT
		maxOpenFiles := MAX_OPEN_FILES_DEFAULT

		if values, err := url.ParseQuery(u.RawQuery); err != nil {
			log.Fatal(err)

		} else {
			if v, ok := values["blocksPerFile"]; ok {
				if v, err := strconv.ParseUint(v[0], 10, 32); err != nil {
					log.Fatal(err)

				} else if v != 0 {
					blocksPerFile = int(v)
				}
			}

			if v, ok := values["blocksize"]; ok {
				if v, err := strconv.ParseUint(v[0], 10, 32); err != nil {
					log.Fatal(err)

				} else if v != 0 {
					blocksize = int(v)
				}
			}

			if v, ok := values["bufsize"]; ok {
				if v, err := strconv.ParseUint(v[0], 10, 32); err != nil {
					log.Fatal(err)

				} else if v != 0 {
					bufsize = int(v)
				}
			}

			if v, ok := values["maxOpenFiles"]; ok {
				if v, err := strconv.ParseUint(v[0], 10, 32); err != nil {
					log.Fatal(err)

				} else if v != 0 {
					maxOpenFiles = int(v)
				}
			}
		}

		numberOfFiles := *queueSize * 1024 * 1024 / blocksPerFile / blocksize

		if numberOfFiles < 2 {
			numberOfFiles = 2
		}

		var err error
		repoFactory, err = NewFileRepositoryFactory(u.Host+u.Path, numberOfFiles, blocksPerFile, blocksize, bufsize, maxOpenFiles, INSERTS_PARALLEL, *bufferSize/2, QUERIES_PARALLEL, QUERIES_QUEUED, REPLY_CHAN_SIZE)

		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Fatal("invalid database URL")
	}

	// If Syslog is requested, re-assign the global log variable.
	if *useSyslog {
		if l, err := syslog.NewLogger(SYSLOG_FACILITY|SYSLOG_SEVERITY, 0); err != nil {
			log.Fatal(err)

		} else {
			log.Println("logging via syslog")
			log = l
		}
	}

	log.Printf("httpmsgbus v%s started", VERSION)
	log.Printf("using up to %d threads", runtime.GOMAXPROCS(-1))
	log.Printf("listening at :%d", *port)

	// Check for the existence of Regexp.ShimTag(), which indicates that
	// a vendor regexp is used. Unfortunately the standard Go regexp is
	// broken.
	if _, ok := interface{}(regexp.Regexp{}).(interface{ShimTag()}); !ok {
		log.Println("WARNING: you appear to be using the standard regexp package, which may leak memory")
	}

	// Closing stopChan tells running producers to stop.
	stopChan := make(chan struct{})
	busses := map[string]*Bus{}

	// Create our http.Server, which is wrapped in graceful.Server,
	// enabling graceful shutdown.
	srv := &graceful.Server{
		NoSignalHandling: true,
		Server: &http.Server{
			Addr: fmt.Sprintf(":%d", *port),
			Handler: &Handler{
				bufferSize:     *bufferSize,
				postSize:       *postSize,
				sessionTimeout: *sessionTimeout,
				sessionsPerIP:  *sessionsPerIP,
				delta:          *delta,
				useXFF:         *useXFF,
				repoFactory:    repoFactory,
				stopChan:       stopChan,
				busses:         busses,
			},
		},
	}

	go func() {
		// Set up signal handler for SIGINT and SIGTERM.
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

		// Wait for a signal.
		<-interrupt
		log.Println("shutting down")

		// Tell the server to stop accepting new connections
		// and shut down after max. GRACEFUL_TIMEOUT seconds.
		srv.Stop(time.Duration(GRACEFUL_TIMEOUT) * time.Second)

		// Tell producers to stop.
		close(stopChan)
	}()

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal("ListenAndServe:", err)
	}

	// If ListenAndServe exited, we are shutting down...

	for _, bus := range busses {
		bus.Shutdown()
	}

	if repoFactory != nil {
		repoFactory.Shutdown()
	}

	log.Println("finished")
}
