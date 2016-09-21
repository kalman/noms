// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package progressreader provides an io.Reader that reports progress to a callback
package progressreader

import (
	"io"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/status"
)

type Callback func(seen uint64)

func New(inner io.Reader, cb Callback) *Reader {
	return &Reader{inner, uint64(0), time.Time{}, cb}
}

type Reader struct {
	inner    io.Reader
	seen     uint64
	lastTime time.Time
	cb       Callback
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.inner.Read(p)
	r.seen += uint64(n)

	if now := time.Now(); now.Sub(r.lastTime) >= status.Rate || err == io.EOF {
		r.cb(r.seen)
		r.lastTime = now
	}
	return
}

func (r *Reader) Seek(offset int64, whence int) (ret int64, err error) {
	rs, ok := r.inner.(io.ReadSeeker)
	d.PanicIfFalse(ok, "Reader is not a Seeker")
	return rs.Seek(offset, whence)
}
