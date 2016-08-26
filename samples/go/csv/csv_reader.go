// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"bufio"
	"io"

	"github.com/attic-labs/noms/go/lang/encoding/csv"
)

var (
	rByte byte = 13 // the byte that corresponds to the '\r' rune.
	nByte byte = 10 // the byte that corresponds to the '\n' rune.
)

type reader struct {
	r *bufio.Reader
}

// Read replaces CR line endings in the source reader with LF line endings if the CR is not followed by a LF.
func (r reader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	bn, err := r.r.Peek(1)
	for i, b := range p {
		// if the current byte is a CR and the next byte is NOT a LF then replace the current byte with a LF
		if j := i + 1; b == rByte && ((j < len(p) && p[j] != nByte) || (len(bn) > 0 && bn[0] != nByte)) {
			p[i] = nByte
		}
	}
	return
}

// NewCSVReader returns a new csv.Reader that splits on comma
func NewCSVReader(res io.Reader, comma rune) *csv.Reader {
	// TODO: Is using bufio actually helping here?
	// It should at least be the job of the caller to make this buffered.
	bufRes := bufio.NewReader(res)
	r := csv.NewReader(reader{r: bufRes})
	r.Comma = comma
	r.FieldsPerRecord = -1 // Don't enforce number of fields.
	return r
}
