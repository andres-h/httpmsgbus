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
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net/http"
	"path"
	"time"
)

// NewCid generates a new client ID.
func NewCid() string {
	buf := make([]byte, 12)

	if _, err := rand.Read(buf); err != nil {
		panic(err)
	} else {
		return base64.URLEncoding.EncodeToString(buf)
	}
}

// NewSid generates a new session ID.
func NewSid() string {
	buf := make([]byte, 12)

	binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()/1000))

	if _, err := rand.Read(buf[8:]); err != nil {
		panic(err)
	} else {
		return base64.URLEncoding.EncodeToString(buf)
	}
}

// EncodeNative serializes a message in native format, which is protobuf
// prefixed by size. This is used by filedb only.
func EncodeNative(m *Message, buf []byte, pb *proto.Buffer) (int, error) {
	pb.SetBuf(buf[4:4])

	if err := m.MarshalProtobuf(pb); err != nil {
		return 0, err
	}

	size := len(pb.Bytes()) + 4

	if size > len(buf) {
		return 0, errors.New(fmt.Sprintf("message size %d is larger than buffer size %d", size, len(buf)))
	}

	binary.LittleEndian.PutUint32(buf[:4], uint32(size))
	return size, nil
}

// DecodeNative deserializes a message encoded in native format.
func DecodeNative(data []byte, pb *proto.Buffer) (*Message, error) {
	size := int(binary.LittleEndian.Uint32(data[:4]))

	if size == 0 {
		return nil, nil
	}

	if size <= 4 || size > len(data) {
		return nil, errors.New("invalid data size")
	}

	pb.SetBuf(data[4:size])

	m := &Message{}

	if err := m.UnmarshalProtobuf(pb); err != nil {
		return nil, err
	}

	return m, nil
}

// CleanPath normalizes a path.
func CleanPath(p string) string {
	if p == "" {
		return "/"
	}

	if p[0] != '/' {
		p = "/" + p
	}

	np := path.Clean(p)

	// path.Clean removes trailing slash except for root;
	// put the trailing slash back if necessary.
	if p[len(p)-1] == '/' && np != "/" {
		np += "/"
	}

	return np
}

// LogRequestError logs error message together with remote address, HTTP
// method and URL.
func LogRequestError(r *http.Request, s string) {
	log.Printf("%s [%s %s %s]", s, r.RemoteAddr, r.Method, r.URL.String())
}

// HttpStatus checks the type of error and returns the corresponding HTTP code.
func HttpStatus(err error) int {
	if IsServiceUnavailable(err) {
		return http.StatusServiceUnavailable
	} else {
		return http.StatusBadRequest
	}
}
