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
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

// jsonReader implements a MessageReader for BSON format.
type jsonReader struct {
	r    BytesReader // The underlying BytesReader
	data []*Message  // Message buffer
}

// NewJsonReader creates a new jsonReader object.
func NewJsonReader(r BytesReader) MessageReader {
	return &jsonReader{r, nil}
}

// Read returns one message.
func (self *jsonReader) Read() (*Message, error) {
	// When Read() is called the first time, the underlying BytesReader
	// is read until EOF and all messages are put into the buffer.
	if self.data == nil {
		decoder := json.NewDecoder(self.r)

		var msgs map[string]*Message

		if err := decoder.Decode(&msgs); err != nil {
			return nil, err

		} else {
			// Map does not keep the order of messages, but key is
			// the message index. Put the messages into the buffer
			// in the right order.
			self.data = make([]*Message, len(msgs))

			for k, m := range msgs {
				if m == nil {
					return nil, errors.New("invalid JSON message list")

				} else if i, err := strconv.Atoi(k); err != nil || i < 0 || i >= len(msgs) || self.data[i] != nil {
					return nil, errors.New("invalid JSON message list")

				} else {
					self.data[i] = m
				}
			}
		}
	}

	// If the buffer is empty, we have EOF
	if len(self.data) == 0 {
		return nil, io.EOF
	}

	// Return the next message from the buffer.
	m := self.data[0]
	self.data = self.data[1:]
	return m, nil
}

// Close closes the underlying BytesReader.
func (self *jsonReader) Close() error {
	return self.r.Close()
}

// jsonWriter implements a MessageWriter for JSON format.
type jsonWriter struct {
	w BytesWriter // The underlying BytesWriter
	i int64       // Current message index
}

// NewJsonWriter creates a new jsonWriter object.
func NewJsonWriter(w BytesWriter) MessageWriter {
	return &jsonWriter{w, 0}
}

// Write writes one message and returns the number of bytes written.
func (self *jsonWriter) Write(m *Message) (int, error) {
	buf := make([]byte, 2, 1024) // 1024 is initial capacity, grows as needed

	if self.i == 0 {
		buf[0] = '{'
	} else {
		buf[0] = ','
	}

	buf[1] = '"'

	if data, err := json.Marshal(m); err != nil {
		// If the serialization fails, it is safer to skip the message
		// than return an error. (json.Marshal() fails to serialize NaN.)
		log.Println(err)
		return 0, nil

	} else {
		buf = append(buf, strconv.FormatInt(self.i, 10)...)
		buf = append(buf, '"', ':')
		buf = append(buf, data...)

		self.i++

		return self.w.Write(buf)
	}
}

// Flush flushes the underlying BytesWriter. Argument should be true if this
// is the final flush. In that case this jsonWriter should be no longer used.
func (self *jsonWriter) Flush(finish bool) {
	if finish && self.i > 0 {
		self.w.Write([]byte("}"))
	}

	self.w.Flush()
}

// CloseNotify returns a buffered bool channel of size 1; a value (true) is
// sent into this channel when underlying data link is closed.
func (self *jsonWriter) CloseNotify() <-chan bool {
	return self.w.CloseNotify()
}
