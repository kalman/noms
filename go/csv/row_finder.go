// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package csv reads and writes comma-separated values (CSV) files.
// There are many kinds of CSV files; this package supports the format
// described in RFC 4180.
//
// A csv file contains zero or more records of one or more fields per record.
// Each record is separated by the newline character. The final record may
// optionally be followed by a newline character.
//
//	field1,field2,field3
//
// White space is considered part of a field.
//
// Carriage returns before newline characters are silently removed.
//
// Blank lines are ignored. A line with only whitespace characters (excluding
// the ending newline character) is not considered a blank line.
//
// Fields which start and stop with the quote character " are called
// quoted-fields. The beginning and ending quote are not part of the
// field.
//
// The source:
//
//	normal string,"quoted-field"
//
// results in the fields
//
//	{`normal string`, `quoted-field`}
//
// Within a quoted-field a quote character followed by a second quote
// character is considered a single quote.
//
//	"the ""word"" is true","a ""quoted-field"""
//
// results in
//
//	{`the "word" is true`, `a "quoted-field"`}
//
// Newlines and commas may be included in a quoted-field
//
//	"Multi-line
//	field","comma is ,"
//
// results in
//
//	{`Multi-line
//	field`, `comma is ,`}
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

// A RowFinder reads records from a CSV-encoded file.
//
// As returned by NewRowFinder, a RowFinder expects input conforming to RFC 4180.
// The exported fields can be changed to customize the details before the
// first call to Read or ReadAll.
//
//
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

	offset uint64
	r      *bufio.Reader

	// Used for error reporting
	line, column int
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

// FindNext returns the offset of the next row within the io.Reader.
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
		r1, err = r.peekRune()
		if err != nil || r1 != '\n' {
			break
		}
		r.readRune()
	}

	offset = r.offset
	return
}

// FindAll returns a slice of all offests within the io.Reader.
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

func (r *RowFinder) peekRune() (rune, error) {
	r1, _, err := r.r.ReadRune()

	// Handle \r\n here. We make the simplifying assumption that
	// anytime \r is followed by \n that it can be folded to \n.
	// We will not detect files which contain both \r\n and bare \n.
	if r1 == '\r' {
		r1, _, err = r.r.ReadRune()
		if err == nil {
			if r1 != '\n' {
				r1 = '\r'
			}
		}
		r.r.UnreadRune()
	}

	r.r.UnreadRune()
	return r1, err
}

// skip reads runes up to and including the rune delim or until error.
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

// parseRecord reads and parses a single csv record from r.
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
	for {
		_, delim, err := r.parseField()
		if delim == '\n' || err == io.EOF {
			return true, err
		}
		if err != nil {
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

// parseField parses the next field in the record.
// located in r.field. Delim is the first character not part of the field
// (r.Comma or '\n').
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
