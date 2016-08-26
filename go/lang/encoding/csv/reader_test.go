// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package csv

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

var readTests = []struct {
	Name               string
	Input              string
	Rows               [][]byte
	Fields             [][]FieldRange
	UseFieldsPerRecord bool // false (default) means FieldsPerRecord is -1

	// These fields are copied into the Reader
	Comma            rune
	Comment          rune
	FieldsPerRecord  int
	LazyQuotes       bool
	TrailingComma    bool
	TrimLeadingSpace bool

	Error  string
	Line   int // Expected error line if != 0
	Column int // Expected error column if line != 0
}{
	{
		Name:  "Simple",
		Input: "a,b,c\n",
		Rows: [][]byte{
			[]byte("a,b,c"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}, {4, 5}},
		},
	},
	{
		Name:  "CRLF",
		Input: "a,b\r\nc,d\r\n",
		Rows: [][]byte{
			[]byte("a,b"),
			[]byte("c,d"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}},
			{{0, 1}, {2, 3}},
		},
	},
	{
		Name:  "BareCR",
		Input: "a,b\rc,d\r\n",
		Rows: [][]byte{
			[]byte("a,b\rc,d"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 5}, {6, 7}},
		},
	},
	{
		Name:               "RFC4180test",
		UseFieldsPerRecord: true,
		Input: `#field1,field2,field3
"aaa","bb
b","ccc"
"a,a","b""bb","ccc"
zzz,yyy,xxx
`,
		Rows: [][]byte{
			[]byte("#field1,field2,field3"),
			[]byte("aaa,bb\nb,ccc"),
			[]byte(`a,a,b"bb,ccc`),
			[]byte("zzz,yyy,xxx"),
		},
		Fields: [][]FieldRange{
			{{0, 7}, {8, 14}, {15, 21}},
			{{0, 3}, {4, 8}, {9, 12}},
			{{0, 3}, {4, 8}, {9, 12}},
			{{0, 3}, {4, 7}, {8, 11}},
		},
	},
	{
		Name:  "NoEOLTest",
		Input: "a,b,c",
		Rows: [][]byte{
			[]byte("a,b,c"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}, {4, 5}},
		},
	},
	{
		Name:  "Semicolon",
		Comma: ';',
		Input: "a;b;c\n",
		Rows: [][]byte{
			[]byte("a;b;c"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}, {4, 5}},
		},
	},
	{
		Name: "MultiLine",
		Input: `"two
line","one line","three
line
field"`,
		Rows: [][]byte{
			[]byte("two\nline,one line,three\nline\nfield"),
		},
		Fields: [][]FieldRange{
			{{0, 8}, {9, 17}, {18, 34}},
		},
	},
	{
		Name:  "BlankLine",
		Input: "a,b,c\n\nd,e,f\n\n",
		Rows: [][]byte{
			[]byte("a,b,c"),
			[]byte("d,e,f"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}, {4, 5}},
			{{0, 1}, {2, 3}, {4, 5}},
		},
	},
	{
		Name:               "BlankLineFieldCount",
		Input:              "a,b,c\n\nd,e,f\n\n",
		UseFieldsPerRecord: true,
		Rows: [][]byte{
			[]byte("a,b,c"),
			[]byte("d,e,f"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}, {4, 5}},
			{{0, 1}, {2, 3}, {4, 5}},
		},
	},
	{
		Name:             "TrimSpace",
		Input:            " a,  b,   c\n",
		TrimLeadingSpace: true,
		Rows: [][]byte{
			[]byte("a,b,c"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}, {4, 5}},
		},
	},
	{
		Name:  "LeadingSpace",
		Input: " a,  b,   c\n",
		Rows: [][]byte{
			[]byte(" a,  b,   c"),
		},
		Fields: [][]FieldRange{
			{{0, 2}, {3, 6}, {7, 11}},
		},
	},
	{
		Name:    "Comment",
		Comment: '#',
		Input:   "#1,2,3\na,b,c\n#comment",
		Rows: [][]byte{
			[]byte("a,b,c"),
		},
		Fields: [][]FieldRange{
			{{0, 1}, {2, 3}, {4, 5}},
		},
	},
	/*
			{
				Name:   "NoComment",
				Input:  "#1,2,3\na,b,c",
				Output: [][]string{{"#1", "2", "3"}, {"a", "b", "c"}},
			},
			{
				Name:       "LazyQuotes",
				LazyQuotes: true,
				Input:      `a "word","1"2",a","b`,
				Output:     [][]string{{`a "word"`, `1"2`, `a"`, `b`}},
			},
			{
				Name:       "BareQuotes",
				LazyQuotes: true,
				Input:      `a "word","1"2",a"`,
				Output:     [][]string{{`a "word"`, `1"2`, `a"`}},
			},
			{
				Name:       "BareDoubleQuotes",
				LazyQuotes: true,
				Input:      `a""b,c`,
				Output:     [][]string{{`a""b`, `c`}},
			},
			{
				Name:  "BadDoubleQuotes",
				Input: `a""b,c`,
				Error: `bare " in non-quoted-field`, Line: 1, Column: 1,
			},
			{
				Name:             "TrimQuote",
				Input:            ` "a"," b",c`,
				TrimLeadingSpace: true,
				Output:           [][]string{{"a", " b", "c"}},
			},
			{
				Name:  "BadBareQuote",
				Input: `a "word","b"`,
				Error: `bare " in non-quoted-field`, Line: 1, Column: 2,
			},
			{
				Name:  "BadTrailingQuote",
				Input: `"a word",b"`,
				Error: `bare " in non-quoted-field`, Line: 1, Column: 10,
			},
			{
				Name:  "ExtraneousQuote",
				Input: `"a "word","b"`,
				Error: `extraneous " in field`, Line: 1, Column: 3,
			},
			{
				Name:               "BadFieldCount",
				UseFieldsPerRecord: true,
				Input:              "a,b,c\nd,e",
				Error:              "wrong number of fields", Line: 2,
			},
			{
				Name:               "BadFieldCount1",
				UseFieldsPerRecord: true,
				FieldsPerRecord:    2,
				Input:              `a,b,c`,
				Error:              "wrong number of fields", Line: 1,
			},
			{
				Name:   "FieldCount",
				Input:  "a,b,c\nd,e",
				Output: [][]string{{"a", "b", "c"}, {"d", "e"}},
			},
			{
				Name:   "TrailingCommaEOF",
				Input:  "a,b,c,",
				Output: [][]string{{"a", "b", "c", ""}},
			},
			{
				Name:   "TrailingCommaEOL",
				Input:  "a,b,c,\n",
				Output: [][]string{{"a", "b", "c", ""}},
			},
			{
				Name:             "TrailingCommaSpaceEOF",
				TrimLeadingSpace: true,
				Input:            "a,b,c, ",
				Output:           [][]string{{"a", "b", "c", ""}},
			},
			{
				Name:             "TrailingCommaSpaceEOL",
				TrimLeadingSpace: true,
				Input:            "a,b,c, \n",
				Output:           [][]string{{"a", "b", "c", ""}},
			},
			{
				Name:             "TrailingCommaLine3",
				TrimLeadingSpace: true,
				Input:            "a,b,c\nd,e,f\ng,hi,",
				Output:           [][]string{{"a", "b", "c"}, {"d", "e", "f"}, {"g", "hi", ""}},
			},
			{
				Name:   "NotTrailingComma3",
				Input:  "a,b,c, \n",
				Output: [][]string{{"a", "b", "c", " "}},
			},
			{
				Name:          "CommaFieldTest",
				TrailingComma: true,
				Input: `x,y,z,w
		x,y,z,
		x,y,,
		x,,,
		,,,
		"x","y","z","w"
		"x","y","z",""
		"x","y","",""
		"x","","",""
		"","","",""
		`,
				Output: [][]string{
					{"x", "y", "z", "w"},
					{"x", "y", "z", ""},
					{"x", "y", "", ""},
					{"x", "", "", ""},
					{"", "", "", ""},
					{"x", "y", "z", "w"},
					{"x", "y", "z", ""},
					{"x", "y", "", ""},
					{"x", "", "", ""},
					{"", "", "", ""},
				},
			},
			{
				Name:             "TrailingCommaIneffective1",
				TrailingComma:    true,
				TrimLeadingSpace: true,
				Input:            "a,b,\nc,d,e",
				Output: [][]string{
					{"a", "b", ""},
					{"c", "d", "e"},
				},
			},
			{
				Name:             "TrailingCommaIneffective2",
				TrailingComma:    false,
				TrimLeadingSpace: true,
				Input:            "a,b,\nc,d,e",
				Output: [][]string{
					{"a", "b", ""},
					{"c", "d", "e"},
				},
			},
	*/
}

func TestRead(t *testing.T) {
	for _, tt := range readTests {
		r := NewReader(strings.NewReader(tt.Input))
		r.Comment = tt.Comment
		if tt.UseFieldsPerRecord {
			r.FieldsPerRecord = tt.FieldsPerRecord
		} else {
			r.FieldsPerRecord = -1
		}
		r.LazyQuotes = tt.LazyQuotes
		r.TrailingComma = tt.TrailingComma
		r.TrimLeadingSpace = tt.TrimLeadingSpace
		if tt.Comma != 0 {
			r.Comma = tt.Comma
		}
		rows, fields, err := readAll(r)
		perr, _ := err.(*ParseError)
		if tt.Error != "" {
			if err == nil || !strings.Contains(err.Error(), tt.Error) {
				t.Errorf("%s: error %v, want error %q", tt.Name, err, tt.Error)
			} else if tt.Line != 0 && (tt.Line != perr.Line || tt.Column != perr.Column) {
				t.Errorf("%s: error at %d:%d expected %d:%d", tt.Name, perr.Line, perr.Column, tt.Line, tt.Column)
			}
		} else if err != nil {
			t.Errorf("%s: unexpected error %v", tt.Name, err)
		} else if !reflect.DeepEqual(rows, tt.Rows) {
			t.Errorf("%s: rows=%q want %q", tt.Name, rows, tt.Rows)
		} else if !reflect.DeepEqual(fields, tt.Fields) {
			t.Errorf("%s: fields=%v want %v", tt.Name, fields, tt.Fields)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	data := `x,y,z,w
x,y,z,
x,y,,
x,,,
,,,
"x","y","z","w"
"x","y","z",""
"x","y","",""
"x","","",""
"","","",""
`

	for i := 0; i < b.N; i++ {
		_, _, err := readAll(NewReader(strings.NewReader(data)))

		if err != nil {
			b.Fatalf("could not read data: %s", err)
		}
	}
}

func readAll(r *Reader) (allRows [][]byte, allFields [][]FieldRange, err error) {
	for {
		row, fields, err2 := r.Read()
		if err2 != nil {
			if err2 != io.EOF {
				err = err2
			}
			break
		}
		allRows = append(allRows, row)
		allFields = append(allFields, fields)
	}
	return
}
