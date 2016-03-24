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
	"encoding/json"
	"errors"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"time"
)

const (
	// Time is serialized with microsecond precision; any precison is
	// supported when deserializing.
	TIME_FORMAT       = "2006-01-02T15:04:05Z"
	TIME_FORMAT_MICRO = "2006-01-02T15:04:05.000000Z"
)

// Time extends time.Time, so we can override serialization methods. Other
// methods of time.Time are inherited, but there is no implicit type conversion
// in Go.
type Time struct {
	time.Time
}

func (self Time) String() string {
	return self.Format(TIME_FORMAT_MICRO)
}

func (self Time) MarshalText() ([]byte, error) {
	if self.IsZero() {
		return []byte("null"), nil

	} else {
		return []byte(self.Format(TIME_FORMAT_MICRO)), nil
	}
}

func (self *Time) UnmarshalText(data []byte) (err error) {
	if string(data) == "null" {
		self.Time, err = time.Time{}, nil

	} else {
		self.Time, err = time.Parse(TIME_FORMAT, string(data))
	}

	return
}

func (self Time) MarshalJSON() ([]byte, error) {
	if self.IsZero() {
		return []byte("null"), nil

	} else {
		return []byte(self.Format(`"`+TIME_FORMAT_MICRO+`"`)), nil
	}
}

func (self *Time) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "null" {
		self.Time, err = time.Time{}, nil

	} else {
		self.Time, err = time.Parse(`"`+TIME_FORMAT+`"`, string(data))
	}

	return
}

func (self Time) GetBSON() (interface{}, error) {
	if self.IsZero() {
		return nil, nil

	} else {
		return self.Format(TIME_FORMAT_MICRO), nil
	}
}

func (self *Time) SetBSON(raw bson.Raw) (err error) {
	var decoded interface{}

	err = raw.Unmarshal(&decoded)

	if err != nil {
		self.Time = time.Time{}

	} else if decoded == nil {
		self.Time, err = time.Time{}, nil

	} else if decoded, ok := decoded.(string); ok {
		self.Time, err = time.Parse(TIME_FORMAT, decoded)

	} else {
		self.Time, err = time.Time{}, errors.New("invalid Time")
	}

	return
}

// Sequence contains an actual sequence number and a "Set" flag. Alternatively
// we could use a pointer that can take nil value when unset, but using a
// non-pointer type might be more efficient due to less memory allocations.
// Using -1 to denote unset sequence does not work with JSON/BSON serialization
// (unset attributes are initialized to zero, so it would be impossible to
// distinguish between a zero and an unset sequence number).
type Sequence struct {
	Value int64
	Set   bool
}

func (self Sequence) String() string {
	if !self.Set {
		return "null"

	} else {
		return strconv.FormatInt(self.Value, 10)
	}
}

func (self Sequence) MarshalText() ([]byte, error) {
	if !self.Set {
		return []byte("null"), nil

	} else {
		return []byte(strconv.FormatInt(self.Value, 10)), nil
	}
}

func (self *Sequence) UnmarshalText(data []byte) (err error) {
	if string(data) == "null" {
		*self, err = Sequence{}, nil

	} else {
		self.Value, err = strconv.ParseInt(string(data), 10, 64)
		self.Set = (err == nil)
	}

	return
}

func (self Sequence) MarshalJSON() ([]byte, error) {
	return self.MarshalText()
}

func (self *Sequence) UnmarshalJSON(data []byte) (err error) {
	return self.UnmarshalText(data)
}

func (self Sequence) GetBSON() (interface{}, error) {
	if !self.Set {
		return nil, nil

	} else {
		return self.Value, nil
	}
}

func (self *Sequence) SetBSON(raw bson.Raw) (err error) {
	var decoded interface{}

	err = raw.Unmarshal(&decoded)

	if err != nil {
		*self = Sequence{}

	} else if decoded == nil {
		*self, err = Sequence{}, nil

	} else if v, ok := decoded.(int64); ok {
		*self, err = Sequence{v, true}, nil

	} else if v, ok := decoded.(int32); ok {
		*self, err = Sequence{int64(v), true}, nil

	} else if v, ok := decoded.(int); ok {
		*self, err = Sequence{int64(v), true}, nil

	} else {
		*self, err = Sequence{}, errors.New("invalid Sequence")
	}

	return
}

// OpenParam defines the dataset that is used by the /open method of the bus.
type OpenParam struct {
	Cid               string                     `json:"cid,omitempty" bson:"cid,omitempty"`
	HeartbeatInterval int                        `json:"heartbeat,omitempty" bson:"heartbeat,omitempty"`
	RecvLimit         int                        `json:"recv_limit,omitempty" bson:"recv_limit,omitempty"`
	Queue             map[string]*OpenParamQueue `json:"queue,omitempty" bson:"queue,omitempty"`
}

// OpenParamQueue configures one queue in OpenParam. In case the default value
// is not a zero value we have to use pointers.
type OpenParamQueue struct {
	Topics     []string               `json:"topics,omitempty" bson:"topics,omitempty"`
	Seq        Sequence               `json:"seq,omitempty" bson:"seq,omitempty"`
	Endseq     Sequence               `json:"endseq,omitempty" bson:"endseq,omitempty"`
	Starttime  Time                   `json:"starttime,omitempty" bson:"starttime,omitempty"`
	Endtime    Time                   `json:"endtime,omitempty" bson:"endtime,omitempty"`
	FilterExpr map[string]interface{} `json:"filter,omitempty" bson:"filter,omitempty"`
	Qlen       *int                   `json:"qlen,omitempty" bson:"qlen,omitempty"`
	Oowait     *int                   `json:"oowait,omitempty" bson:"oowait,omitempty"`
	Keep       *bool                  `json:"keep,omitempty" bson:"keep,omitempty"`
	Seedlink   bool                   `json:"seedlink,omitempty" bson:"seedlink,omitempty"`
}

// OpenAck defines the response from /open.
type OpenAck struct {
	Sid   string                  `json:"sid" bson:"sid"`
	Cid   string                  `json:"cid" bson:"cid"`
	Queue map[string]OpenAckQueue `json:"queue" bson:"queue"`
}

// OpenAckQueue defines the status of one queue in OpenAck.
type OpenAckQueue struct {
	Seq   Sequence `json:"seq" bson:"seq"`
	Error *string  `json:"error" bson:"error"`
}

// QueueInfo defines the data that is returned by /info.
type QueueInfo struct {
	Startseq  Sequence                  `json:"startseq"`
	Starttime Time                      `json:"starttime"`
	Endseq    Sequence                  `json:"endseq"`
	Endtime   Time                      `json:"endtime"`
	Topic     map[string]QueueInfoTopic `json:"topic"`
}

// QueueInfoTopic defines one topic in QueueInfo.
type QueueInfoTopic struct {
	Starttime Time `json:"starttime"`
	Endtime   Time `json:"endtime"`
}

// SessionStatus defines the data that is returned by /status.
type SessionStatus struct {
	Cid               string                        `json:"cid"`
	Address           string                        `json:"address"`
	Ctime             Time                          `json:"ctime"`
	Sent              int64                         `json:"sent"`
	Received          int64                         `json:"received"`
	Format            string                        `json:"format"`
	HeartbeatInterval int                           `json:"heartbeat"`
	RecvLimit         int                           `json:"recv_limit"`
	Queue             map[string]SessionStatusQueue `json:"queue"`
}

// SessionStatusQueue defines the status of one queue in SessionStatus.
type SessionStatusQueue struct {
	Topics    []string `json:"topics"`
	Seq       Sequence `json:"seq"`
	Endseq    Sequence `json:"endseq"`
	Starttime Time     `json:"starttime"`
	Endtime   Time     `json:"endtime"`
	Qlen      *int     `json:"qlen"`
	Oowait    *int     `json:"oowait"`
	Keep      bool     `json:"keep"`
	Seedlink  bool     `json:"seedlink"`
	Eof       bool     `json:"eof"`
}

// Payload defines the payload of a message. It simply wraps an interface{},
// which is a generic type. The wrapper is needed to define serialization
// methods.
type Payload struct {
	Data interface{}
}

func (self Payload) MarshalText() ([]byte, error) {
	return json.Marshal(self.Data)
}

func (self *Payload) UnmarshalText(data []byte) error {
	return json.Unmarshal(data, &self.Data)
}

func (self Payload) MarshalJSON() ([]byte, error) {
	return json.Marshal(self.Data)
}

func (self *Payload) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &self.Data)
}

func (self Payload) GetBSON() (interface{}, error) {
	return self.Data, nil
}

func (self *Payload) SetBSON(raw bson.Raw) (err error) {
	// Unmarshal() seems to leave references to the original data,
	// so make a copy of the data.
	var rawcopy bson.Raw
	rawcopy.Kind = raw.Kind
	rawcopy.Data = make([]byte, len(raw.Data))
	copy(rawcopy.Data, raw.Data)

	if raw.Kind == 3 {
		// Data contains a BSON document. Anything else than
		// map[string]interface{} is ignored by the filter.
		var decoded map[string]interface{}
		err = rawcopy.Unmarshal(&decoded)
		self.Data = decoded

	} else {
		var decoded interface{}
		err = rawcopy.Unmarshal(&decoded)
		self.Data = decoded
	}

	return
}

// Message defines an HMB message.
type Message struct {
	Type      string   `json:"type" bson:"type"`
	Queue     string   `json:"queue,omitempty" bson:"queue,omitempty"`
	Sender    string   `json:"sender,omitempty" bson:"sender,omitempty"`
	Topic     string   `json:"topic,omitempty" bson:"topic,omitempty"`
	Seq       Sequence `json:"seq,omitempty" bson:"seq,omitempty"`
	Starttime Time     `json:"starttime,omitempty" bson:"starttime,omitempty"`
	Endtime   Time     `json:"endtime,omitempty" bson:"endtime,omitempty"`
	Data      Payload  `json:"data,omitempty" bson:"data,omitempty"`

	// The following fields are included for compatibility with older
	// applications and will be removed in future versions. Any extra
	// attributes should be part of Data.
	ScMessageType int `json:"scMessageType,omitempty" bson:"scMessageType,omitempty"`
	ScContentType int `json:"scContentType,omitempty" bson:"scContentType,omitempty"`
	Gdacs map[string]interface{} `json:"gdacs,omitempty" bson:"gdacs,omitempty"`
}

// MessageReader reads from underlying BytesReader and deserializes the data
// as a message.
type MessageReader interface {
	Read() (*Message, error)
	Close() error
}

// MessageWriter serializes a message and writes data to underlying
// BytesWriter.
type MessageWriter interface {
	Write(*Message) (int, error)
	Flush(bool)
	CloseNotify() <-chan bool
}

type BytesReader interface {
	Read([]byte) (int, error)
	Close() error
}

type BytesWriter interface {
	Write([]byte) (int, error)
	Flush()
	CloseNotify() <-chan bool
}
