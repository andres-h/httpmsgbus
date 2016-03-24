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

package hmb

import (
	"bytes"
	"encoding/json"
	"errors"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

var ECANCELED = errors.New("request canceled")

type Logger interface {
	Println(...interface{})
	Printf(string, ...interface{})
}

type TimeoutConn struct {
	net.Conn
	Timeout time.Duration
}

func (self *TimeoutConn) Read(b []byte) (int, error) {
	self.SetReadDeadline(time.Now().Add(self.Timeout))
	return self.Conn.Read(b)
}

func (self *TimeoutConn) Write(b []byte) (int, error) {
	self.SetWriteDeadline(time.Now().Add(self.Timeout))
	return self.Conn.Write(b)
}

type MessageEncoder struct {
	msgs []*Message
	buf  []byte
	i    int
}

func NewMessageEncoder(msgs []*Message) *MessageEncoder {
	return &MessageEncoder{msgs, nil, 0}
}

func (self *MessageEncoder) Read(b []byte) (int, error) {
	n := 0

	for n < len(b) && len(self.msgs) > 0 {
		if self.i == 0 {
			var err error

			self.buf, err = bson.Marshal(self.msgs[0])

			if err != nil {
				return n, err
			}
		}

		size := len(self.buf) - self.i

		if size > len(b)-n {
			size = len(b) - n
		}

		copy(b[n:n+size], self.buf[self.i:self.i+size])

		n += size
		self.i += size

		if self.i == len(self.buf) {
			self.msgs = self.msgs[1:]
			self.i = 0
		}
	}

	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

type Client struct {
	url       string
	clientIP  net.IP
	param     *OpenParam
	retryWait int
	log       Logger
	http      *http.Client
	r         MessageReader
	sid       string
	cancel    chan struct{}
}

func NewClient(url string, clientIP net.IP, param *OpenParam, timeout int, retryWait int, logger Logger) *Client {
	self := &Client{
		url:       url,
		clientIP:  clientIP,
		param:     param,
		retryWait: retryWait,
		log:       logger,
		cancel:    make(chan struct{}),
	}

	tr := *http.DefaultTransport.(*http.Transport)
	dial := tr.Dial

	tr.Dial = func(netw, addr string) (net.Conn, error) {
		if conn, err := dial(netw, addr); err != nil {
			return nil, err

		} else {
			return &TimeoutConn{conn, time.Duration(timeout) * time.Second}, nil
		}
	}

	self.http = &http.Client{Transport: &tr}

	if self.param != nil && self.param.Queue != nil && self.param.Queue["ANNOUNCEMENT"] == nil {
		self.param.Queue["ANNOUNCEMENT"] = &OpenParamQueue{}
	}

	return self
}

func (self *Client) _post(url string, bodyType string, body io.Reader) (*http.Response, error) {
	if req, err := http.NewRequest("POST", url, body); err != nil {
		return nil, err

	} else {
		req.Cancel = self.cancel
		req.Header.Set("Content-Type", bodyType)

		if self.clientIP != nil {
			req.Header.Set("X-Forwarded-For", self.clientIP.String())
		}

		if resp, err := self.http.Do(req); err != nil {
			return nil, err

		} else {
			return resp, nil
		}
	}
}

func (self *Client) _get(url string) (*http.Response, error) {
	if req, err := http.NewRequest("GET", url, nil); err != nil {
		return nil, err

	} else {
		req.Cancel = self.cancel

		if self.clientIP != nil {
			req.Header.Set("X-Forwarded-For", self.clientIP.String())
		}

		if resp, err := self.http.Do(req); err != nil {
			return nil, err

		} else {
			return resp, nil
		}
	}
}

func (self *Client) _open() error {
	data, err := bson.Marshal(self.param)

	if err != nil {
		return err
	}

	fail := false

	for {
		if resp, err := self._post(self.url+"/open", "application/bson", bytes.NewBuffer(data)); err != nil {
			if !fail {
				self.log.Println(err)
			}

			select {
			case <-self.cancel:
				return ECANCELED
			default:
			}

		} else {
			body, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			if err != nil {
				self.log.Println(err)

			} else if resp.StatusCode == 200 {
				var ack OpenAck

				if err := bson.Unmarshal(body, &ack); err != nil {
					self.log.Println(err)

				} else {
					self.sid = ack.Sid
					self.param.Cid = ack.Cid
					self.log.Printf("session opened, sid=%s, cid=%s", ack.Sid, ack.Cid)
					return nil
				}

			} else if resp.StatusCode == 400 {
				self.log.Println("bad request:", string(bytes.TrimSpace(body)))

			} else if resp.StatusCode == 503 {
				self.log.Println("service unavailable:", string(bytes.TrimSpace(body)))

			} else {
				self.log.Println(resp.Status)
			}
		}

		if !fail {
			self.log.Printf("connection to message bus failed, retrying in %d seconds", self.retryWait)
			fail = true
		}

		select {
		case <-time.After(time.Duration(self.retryWait) * time.Second):
		case <-self.cancel:
			return ECANCELED
		}
	}
}

func (self *Client) Send(msgs []*Message) error {
	for {
		if self.sid == "" {
			if err := self._open(); err != nil {
				return err
			}
		}

		if resp, err := self._post(self.url+"/send/"+self.sid, "application/bson", NewMessageEncoder(msgs)); err != nil {
			self.log.Println(err)

			select {
			case <-self.cancel:
				return ECANCELED
			default:
			}

		} else {
			body, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			if err != nil {
				self.log.Println(err)

			} else if resp.StatusCode == 204 {
				return nil

			} else if resp.StatusCode == 400 {
				self.log.Println("bad request:", string(bytes.TrimSpace(body)))

			} else if resp.StatusCode == 503 {
				self.log.Println("service unavailable:", string(bytes.TrimSpace(body)))

			} else {
				self.log.Println(resp.Status)
			}

		}

		self.sid = ""
		self.log.Printf("connection to message bus lost, retrying in %d seconds", self.retryWait)

		select {
		case <-time.After(time.Duration(self.retryWait) * time.Second):
		case <-self.cancel:
			return ECANCELED
		}
	}
}

func (self *Client) Recv() (*Message, error) {
	for {
		if self.r == nil {
			if self.sid == "" {
				if err := self._open(); err != nil {
					return nil, err
				}
			}

			if resp, err := self._get(self.url + "/stream/" + self.sid); err != nil {
				self.log.Println(err)

				select {
				case <-self.cancel:
					return nil, ECANCELED
				default:
				}

			} else {
				if resp.StatusCode == 200 {
					self.r = NewBsonReader(resp.Body)
					continue

				} else if resp.StatusCode == 400 || resp.StatusCode == 503 {
					if body, err := ioutil.ReadAll(resp.Body); err != nil {
						self.log.Println(err)

					} else if resp.StatusCode == 400 {
						self.log.Println("bad request:", string(bytes.TrimSpace(body)))

					} else {
						self.log.Println("service unavailable:", string(bytes.TrimSpace(body)))
					}

				} else {
					self.log.Println(resp.Status)
				}

				resp.Body.Close()
			}

		} else {
			if m, err := self.r.Read(); err != nil {
				self.log.Println(err)

				select {
				case <-self.cancel:
					return nil, ECANCELED
				default:
				}

			} else if m == nil {
				self.log.Println("nil message")

			} else if m.Type == "HEARTBEAT" {
				return nil, nil

			} else if m.Type == "EOF" {
				return nil, io.EOF

			} else if m.Type == "NEW_QUEUE" {
				if data, ok := m.Data.Data.(map[string]interface{}); !ok {
					self.log.Println("invalid NEW_QUEUE announcement")

				} else if qname, ok := data["name"].(string); !ok {
					self.log.Println("invalid NEW_QUEUE announcement (name)")

				} else if _, ok := self.param.Queue[qname]; ok {
					self.log.Println("subscribing to new queue:", qname)
					self.r.Close()
					self.r = nil
					self.sid = ""
					return m, nil
				}

				continue

			} else {
				self.param.Queue[m.Queue].Seq = m.Seq
				return m, nil
			}

			self.r.Close()
			self.r = nil
		}

		self.sid = ""
		self.log.Printf("connection to message bus lost, retrying in %d seconds", self.retryWait)

		select {
		case <-time.After(time.Duration(self.retryWait) * time.Second):
		case <-self.cancel:
			return nil, ECANCELED
		}
	}
}

func (self *Client) Info() (map[string]*QueueInfo, error) {
	for {
		if resp, err := self._get(self.url + "/info"); err != nil {
			self.log.Println(err)

			select {
			case <-self.cancel:
				return nil, ECANCELED
			default:
			}

		} else {
			if resp.StatusCode == 200 {
				var info struct {
					Queue map[string]*QueueInfo `json:"queue"`
				}

				decoder := json.NewDecoder(resp.Body)
				err := decoder.Decode(&info)
				resp.Body.Close()

				if err != nil {
					return nil, err

				} else {
					return info.Queue, nil
				}

			} else if resp.StatusCode == 400 || resp.StatusCode == 503 {
				if body, err := ioutil.ReadAll(resp.Body); err != nil {
					self.log.Println(err)

				} else if resp.StatusCode == 400 {
					self.log.Println("bad request:", string(bytes.TrimSpace(body)))

				} else {
					self.log.Println("service unavailable:", string(bytes.TrimSpace(body)))
				}

			} else {
				self.log.Println(resp.Status)
			}

			resp.Body.Close()
		}

		self.sid = ""
		self.log.Printf("connection to message bus lost, retrying in %d seconds", self.retryWait)

		select {
		case <-time.After(time.Duration(self.retryWait) * time.Second):
		case <-self.cancel:
			return nil, ECANCELED
		}
	}
}

func (self *Client) CancelRequest() {
	close(self.cancel)
}

func (self *Client) Close() {
	if self.r != nil {
		self.r.Close()
		self.r = nil
	}
}
