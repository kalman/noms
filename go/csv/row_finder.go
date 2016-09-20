// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package csv find rows in comma-separated values (CSV) files.
//
// It's actually a fork of Go's encoding/csv package, see there for docs:
// https://golang.org/src/encoding/csv/reader.go
//
// Unlike encoding/csv, this package is much more lightweight, and only supports
// finding the indices of rows in CSV files. Use Go's encoding/csv for actually
// parsing the rows.
package csv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"unicode"
)

// A ParseError is returned for parsing errors.
// The first line is 1.  The first column is 0.
type ParseError struct {
	Line   int   // Line where the error occurred
	Column int   // Column (rune index) where the error occurred
	Err    error // The actual error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("line %d, column %d: %s", e.Line, e.Column, e.Err)
}

// These are the errors that can be returned in ParseError.Error
var (
	ErrBareQuote  = errors.New("bare \" in non-quoted-field")
	ErrQuote      = errors.New("extraneous \" in field")
	ErrFieldCount = errors.New("wrong number of fields in line")
)

// RowFinder finds the indices of rows in a CSV-encoded file. It is a fork of
// Reader in Go's encoding/csv package.
//
// As returned by NewRowFinder, a RowFinder expects input conforming to RFC 4180.
// The exported fields can be changed to customize the details before the
// first call to FindNext or FindAll.
type RowFinder struct {
	// Comma is the field delimiter.
	// It is set to comma (',') by NewRowFinder.
	Comma rune
	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	Comment rune
	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	LazyQuotes bool
	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool

	line   int
	column int
	r      *bufio.Reader
	offset uint64
}

// NewRowFinder returns a new RowFinder that reads from r.
func NewRowFinder(r io.Reader) *RowFinder {
	return &RowFinder{
		Comma: ',',
		r:     bufio.NewReader(r),
	}
}

// error creates a new ParseError based on err.
func (r *RowFinder) error(err error) error {
	return &ParseError{
		Line:   r.line,
		Column: r.column,
		Err:    err,
	}
}

// FindNext returns the offset of the next row within the io.Reader, or io.EOF
// if there were no more files.
//
// Note that given a CSV file like:
//
//  a,b,cc
//  ddd,e,f
//  g,h,i
//
// The first call to FindNext will return 7 (index of ddd...), the second call
// will return 15 (index of "g,h..."), and the third call will return EOF.
//
// Blank lines are treated as the end of rows. No guarantees are made about
// whether comments are treated as the start or end of rows.
//
// It may return a different error if the io.Reader returns an error, or if the
// CSV file fails to parse.
func (r *RowFinder) FindNext() (offset uint64, err error) {
	for {
		var found bool
		found, err = r.parseRecord()
		if found {
			break
		}
		if err != nil {
			return
		}
	}

	// Skip over trailing comments.
	for {
		var didSkip bool
		if didSkip, err = r.skipComment(); !didSkip {
			break
		}
	}

	// Skip over trailing blank lines.
	for {
		var r1 rune
		if r1, err = r.peekRune(); r1 != '\n' || err != nil {
			break
		}
		r.readRune()
	}

	offset = r.offset
	return
}

// FindAll returns a slice of indices of each row break (as a result of calling
// FindNext until EOF). If a non-EOF error, returns nil offsets with that error.
func (r *RowFinder) FindAll() (offsets []uint64, err error) {
	for {
		offset, err := r.FindNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, offset)
	}
	return
}

// readRune reads one rune from r, folding \r\n to \n and keeping track
// of how far into the line we have read.  r.column will point to the start
// of this rune, not the end of this rune.
func (r *RowFinder) readRune() (rune, error) {
	r1, size, err := r.r.ReadRune()
	if err == nil {
		r.offset += uint64(size)
	}

	// Handle \r\n here. We make the simplifying assumption that
	// anytime \r is followed by \n that it can be folded to \n.
	// We will not detect files which contain both \r\n and bare \n.
	if r1 == '\r' {
		r1, size, err = r.r.ReadRune()
		if err == nil {
			if r1 != '\n' {
				r.r.UnreadRune()
				r1 = '\r'
			} else {
				r.offset += uint64(size)
			}
		}
	}
	r.column++
	return r1, err
}

// peekRune returns the next rune that readRune will return.
func (r *RowFinder) peekRune() (rune, error) {
	r1, _, err := r.r.ReadRune()

	if r1 == '\r' {
		r1, _, err = r.r.ReadRune()
		if err == nil && r1 != '\n' {
			r1 = '\r'
		}
		r.r.UnreadRune()
	}

	r.r.UnreadRune()
	return r1, err
}

// skip consumes runes up to and including the rune delim or until error.
func (r *RowFinder) skip(delim rune) error {
	for {
		r1, err := r.readRune()
		if err != nil {
			return err
		}
		if r1 == delim {
			return nil
		}
	}
}

// parseRecord tries to consume a single csv record from r up to the next line
// break. Returns true if a record was consumed, false if not (which may or may
// not imply an error).
func (r *RowFinder) parseRecord() (bool, error) {
	// Each record starts on a new line. We increment our line
	// number (lines start at 1, not 0) and set column to -1
	// so as we increment in readRune it points to the character we read.
	r.line++
	r.column = -1

	if didSkip, err := r.skipComment(); didSkip || err != nil {
		return false, err
	}

	// At this point we have at least one field.
	// NOTE: This comment was in Go's CSV Reader code - but it's not clear to me
	// what it means, or why it's correct, it seemed perfectly reasonable for
	// there *not* to be a field (e.g. a trailing blank line). In either case, we
	// use the CSV code differently (we're not extracting fields) so who cares.
	for {
		_, delim, err := r.parseField()
		if delim == '\n' || err == io.EOF {
			return true, err
		} else if err != nil {
			return false, err
		}
	}
}

// skipComment peeks at the first rune. If it is an error we are done. If we
// support comments and it is the comment character then skip to the end of
// line.
func (r *RowFinder) skipComment() (bool, error) {
	r1, size, err := r.r.ReadRune()
	if err != nil {
		return false, err
	}

	if r.Comment != 0 && r1 == r.Comment {
		r.offset += uint64(size)
		return true, r.skip('\n')
	}

	r.r.UnreadRune()
	return false, nil
}

// parseField consumes the next field in the record. Delim is the first
// character not part of the field (r.Comma or '\n').
func (r *RowFinder) parseField() (haveField bool, delim rune, err error) {
	r1, err := r.readRune()
	for err == nil && r.TrimLeadingSpace && r1 != '\n' && unicode.IsSpace(r1) {
		r1, err = r.readRune()
	}

	if err == io.EOF && r.column != 0 {
		return true, 0, err
	}
	if err != nil {
		return false, 0, err
	}

	switch r1 {
	case r.Comma:
		// will check below

	case '\n':
		// We are a trailing empty field or a blank line
		if r.column == 0 {
			return false, r1, nil
		}
		return true, r1, nil

	case '"':
		// quoted field
	Quoted:
		for {
			r1, err = r.readRune()
			if err != nil {
				if err == io.EOF {
					if r.LazyQuotes {
						return true, 0, err
					}
					return false, 0, r.error(ErrQuote)
				}
				return false, 0, err
			}
			switch r1 {
			case '"':
				r1, err = r.readRune()
				if err != nil || r1 == r.Comma {
					break Quoted
				}
				if r1 == '\n' {
					return true, r1, nil
				}
				if r1 != '"' {
					if !r.LazyQuotes {
						r.column--
						return false, 0, r.error(ErrQuote)
					}
				}
			case '\n':
				r.line++
				r.column = -1
			}
		}

	default:
		// unquoted field
		for {
			r1, err = r.readRune()
			if err != nil || r1 == r.Comma {
				break
			}
			if r1 == '\n' {
				return true, r1, nil
			}
			if !r.LazyQuotes && r1 == '"' {
				return false, 0, r.error(ErrBareQuote)
			}
		}
	}

	if err != nil {
		if err == io.EOF {
			return true, 0, err
		}
		return false, 0, err
	}

	return true, r1, nil
}
