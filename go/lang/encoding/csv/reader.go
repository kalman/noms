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
	"bytes"
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

// FieldRange is a 2-element integer array representing the start and end of a field within a `[]byte`.
type FieldRange []int

// Slice returns a slice of `slice` clipped to this range.
func (fr FieldRange) Slice(slice []byte) []byte {
	return slice[fr[0]:fr[1]]
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("line %d, column %d: %s", e.Line, e.Column, e.Err)
}

// These are the errors that can be returned in ParseError.Error
var (
	ErrTrailingComma = errors.New("extra delimiter at end of line") // no longer used
	ErrBareQuote     = errors.New("bare \" in non-quoted-field")
	ErrQuote         = errors.New("extraneous \" in field")
	ErrFieldCount    = errors.New("wrong number of fields in line")
)

// A Reader reads records from a CSV-encoded file.
//
// As returned by NewReader, a Reader expects input conforming to RFC 4180.
// The exported fields can be changed to customize the details before the
// first call to Read or ReadAll.
//
//
type Reader struct {
	// Comma is the field delimiter.
	// It is set to comma (',') by NewReader.
	Comma rune
	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	Comment rune
	// FieldsPerRecord is the number of expected fields per record.
	// If FieldsPerRecord is positive, Read requires each record to
	// have the given number of fields. If FieldsPerRecord is 0, Read sets it to
	// the number of fields in the first record, so that future records must
	// have the same field count. If FieldsPerRecord is negative, no check is
	// made and records may have a variable number of fields.
	FieldsPerRecord int
	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	LazyQuotes    bool
	TrailingComma bool // ignored; here for backwards compatibility
	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool

	line   int
	column int
	r      *bufio.Reader
	record bytes.Buffer // TODO: Why does this need to be heap allocated?
}

// NewReader returns a new Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comma: ',',
		r:     bufio.NewReader(r),
	}
}

// error creates a new ParseError based on err.
func (r *Reader) error(err error) error {
	return &ParseError{
		Line:   r.line,
		Column: r.column,
		Err:    err,
	}
}

// Read reads one record from r. The record is returned as a single byte slice,
// then a list of start/end indices of each field within that slice.
func (r *Reader) Read() (record []byte, fields []FieldRange, err error) {
	for {
		fields, err = r.parseRecord()
		if fields != nil {
			break
		}
		if err != nil {
			return nil, nil, err
		}
	}

	record = make([]byte, r.record.Len())
	copy(record, r.record.Bytes())

	if r.FieldsPerRecord > 0 {
		if len(fields) != r.FieldsPerRecord {
			r.column = 0 // report at start of record
			return record, fields, r.error(ErrFieldCount)
		}
	} else if r.FieldsPerRecord == 0 {
		r.FieldsPerRecord = len(fields)
	}
	return record, fields, nil
}

// ReadFields is like Read, but returns results as a `[]string`.
// This is often more convenient than Read, at the cost of an extra allocation (the string array).
// NOTE: Don't use this for reading entire CSV files, use Read instead.
func (r *Reader) ReadFields() ([]string, error) {
	record, fields, err := r.Read()
	if err != nil {
		return nil, err
	}
	res := make([]string, len(fields))
	for i, f := range fields {
		res[i] = string(f.Slice(record))
	}
	return res, nil
}

// ReadAll reads all the remaining records from r.
// Each record is a slice of fields.
// A successful call returns err == nil, not err == io.EOF. Because ReadAll is
// defined to read until EOF, it does not treat end of file as an error to be
// reported.
// NOTE: Don't use this, use multiple calls to Read instead.
func (r *Reader) ReadAll() (records [][]string, err error) {
	for {
		record, err := r.ReadFields()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

// SkipRecords moves the reader ahead `n` records.
func (r *Reader) SkipRecords(n int) (err error) {
	for i := 0; i < n && err == nil; i++ {
		// TODO: There is some low hanging fruit to make this more efficient - for a start, there is no point reading into the buffer.
		_, _, err = r.Read()
	}
	return
}

// readRune reads one rune from r, folding \r\n to \n and keeping track
// of how far into the line we have read.  r.column will point to the start
// of this rune, not the end of this rune.
func (r *Reader) readRune() (rune, error) {
	r1, _, err := r.r.ReadRune()

	// Handle \r\n here. We make the simplifying assumption that
	// anytime \r is followed by \n that it can be folded to \n.
	// We will not detect files which contain both \r\n and bare \n.
	if r1 == '\r' {
		r1, _, err = r.r.ReadRune()
		if err == nil {
			if r1 != '\n' {
				r.r.UnreadRune()
				r1 = '\r'
			}
		}
	}
	r.column++
	return r1, err
}

// skip reads runes up to and including the rune delim or until error.
func (r *Reader) skip(delim rune) error {
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
func (r *Reader) parseRecord() (fields []FieldRange, err error) {
	// Each record starts on a new line. We increment our line
	// number (lines start at 1, not 0) and set column to -1
	// so as we increment in readRune it points to the character we read.
	r.record.Reset()
	r.line++
	r.column = -1

	// Peek at the first rune. If it is an error we are done.
	// If we support comments and it is the comment character
	// then skip to the end of line.

	r1, _, err := r.r.ReadRune()
	if err != nil {
		return nil, err
	}

	if r.Comment != 0 && r1 == r.Comment {
		return nil, r.skip('\n')
	}
	r.r.UnreadRune()

	// At this point we have at least one field.
	needsComma := false
	for {
		// Add a separator to the record result, because it's useful for debugging.
		//
		// We could happily return for a CSV `abc,def` a record `abcdef` with
		// offsets [(0, 3), (3, 6)], but instead we return `abc,def` with offsets
		// [(0, 3), (4, 7)]. That way, fmt.Println(record) is easier to understand.
		//
		// This is more subtle when you consider a CSV like `ab""c,"def"`, where we
		// collapse the `""` into a single quote, and drop the quotes around the
		// `"def"`. In this case, the result will be a record `ab"c,def` with offsets
		// [(0, 4), (5, 8)].
		if needsComma {
			r.record.WriteRune(r.Comma)
			needsComma = false
		}

		start := r.record.Len()

		haveField, delim, err := r.parseField()
		if haveField {
			// If FieldsPerRecord is greater than 0 we can assume the final
			// length of fields to be equal to FieldsPerRecord.
			if r.FieldsPerRecord > 0 && fields == nil {
				fields = make([]FieldRange, 0, r.FieldsPerRecord)
			}
			fields = append(fields, FieldRange{start, r.record.Len()})
			needsComma = true
		}
		if delim == '\n' || err == io.EOF {
			return fields, err
		} else if err != nil {
			return nil, err
		}
	}
}

// parseField parses the next field in the record by moving `r.column` forward
// the size of the field, while appending to `r.record`.  Delim is the first
// character not part of the field (r.Comma or '\n').
func (r *Reader) parseField() (haveField bool, delim rune, err error) {
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
					// accept the bare quote
					r.record.WriteRune('"')
				}
			case '\n':
				r.line++
				r.column = -1
			}
			r.record.WriteRune(r1)
		}

	default:
		// unquoted field
		for {
			r.record.WriteRune(r1)
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
