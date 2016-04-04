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
	"encoding/binary"
	"errors"
	"gopkg.in/mgo.v2/bson"
	"io"
)

// bsonReader implements a MessageReader for BSON format.
type bsonReader struct {
	r   BytesReader // The underlying BytesReader
	buf []byte      // Data buffer
	ri  int         // Read index into buf
	wi  int         // Write index into buf
}

// NewBsonReader creates a new bsonReader object.
func NewBsonReader(r BytesReader) MessageReader {
	// 1024 is the initial buffer size. Larger buffer will be reallocated
	// if needed.
	return &bsonReader{r, make([]byte, 1024), 0, 0}
}

// Read returns one message.
func (self *bsonReader) Read() (*Message, error) {
	if self.wi-self.ri < 4 {
		// Need at least 4 bytes to determine the size of message.
		if n, err := io.ReadAtLeast(self.r, self.buf[self.wi:], 4-(self.wi-self.ri)); err != nil {
			return nil, err

		} else {
			self.wi += n
		}
	}

	size := int(binary.LittleEndian.Uint32(self.buf[self.ri : self.ri+4]))

	if size < 0 || size > 16*1024*1024 {
		return nil, errors.New("invalid BSON size")
	}

	// Make sure we've got the whole message in buffer and there is enough
	// space for the first 4 bytes of the next message.
	if self.wi-self.ri < size+4 {
		if len(self.buf) < size+4 {
			// Have to allocate a larger buffer.
			newbuf := make([]byte, size+4)
			copy(newbuf, self.buf[self.ri:self.wi])
			self.buf = newbuf
			self.wi -= self.ri
			self.ri = 0

		} else if len(self.buf)-self.ri < size+4 {
			// Make room by moving the data to the beginning of
			// the buffer.
			copy(self.buf, self.buf[self.ri:self.wi])
			self.wi -= self.ri
			self.ri = 0
		}

		// Now read enough data to have the whole message.
		if n, err := io.ReadAtLeast(self.r, self.buf[self.wi:], size-(self.wi-self.ri)); err != nil {
			return nil, err

		} else {
			self.wi += n
		}
	}

	m := &Message{}

	// Decode the data and return the message.
	if err := bson.Unmarshal(self.buf[self.ri:self.ri+size], m); err != nil {
		return nil, errors.New("invalid BSON data: " + err.Error())

	} else {
		self.ri += size
		return m, nil
	}
}

// Close closes the underlying BytesReader.
func (self *bsonReader) Close() error {
	return self.r.Close()
}

// bsonWriter implements a MessageWriter for BSON format.
type bsonWriter struct {
	w BytesWriter // The underlying BytesWriter
}

// NewBsonWriter creates a new bsonWriter object.
func NewBsonWriter(w BytesWriter) MessageWriter {
	return &bsonWriter{w}
}

// Write writes one message and returns the number of bytes written.
func (self *bsonWriter) Write(m *Message) (int, error) {
	if data, err := bson.Marshal(m); err != nil {
		return 0, err

	} else {
		return self.w.Write(data)
	}
}

// Flush flushes the underlying BytesWriter. Argument should be true if this
// is the final flush. In that case this bsonWriter should be no longer used.
func (self *bsonWriter) Flush(bool) {
	self.w.Flush()
}

// CloseNotify returns a buffered bool channel of size 1; a value (true) is
// sent into this channel when underlying data link is closed.
func (self *bsonWriter) CloseNotify() <-chan bool {
	return self.w.CloseNotify()
}
