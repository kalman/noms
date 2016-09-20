// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package csv

import (
	"reflect"
	"strings"
	"testing"
)

var readTests = []struct {
	Name   string
	Input  string
	Output []uint64

	// These fields are copied into the RowFinder
	Comma            rune
	Comment          rune
	LazyQuotes       bool
	TrimLeadingSpace bool

	Error  string
	Line   int // Expected error line if != 0
	Column int // Expected error column if line != 0
}{
	{
		Name:   "Simple",
		Input:  "a,b,c\n",
		Output: nil,
	},
	{
		Name:   "CRLF",
		Input:  "a,b\r\nc,d\r\n",
		Output: []uint64{5},
	},
	{
		Name:   "BareCR",
		Input:  "a,b\rc,d\r\n",
		Output: nil,
	},
	{
		Name: "RFC4180test",
		Input: `#field1,field2,field3
"aaa","bb
b","ccc"
"a,a","b""bb","ccc"
zzz,yyy,xxx
`,
		Output: []uint64{22, 41, 61},
	},
	{
		Name:   "NoEOLTest",
		Input:  "a,b,c",
		Output: nil,
	},
	{
		Name:   "Semicolon",
		Comma:  ';',
		Input:  "a;b;c\n",
		Output: nil,
	},
	{
		Name: "MultiLine",
		Input: `"two
line","one line","three
line
field"`,
		Output: nil,
	},
	{
		Name:   "BlankLine1",
		Input:  "a,b,c\n\nd,e,f\n\n",
		Output: []uint64{7},
	},
	{
		Name:   "BlankLine2",
		Input:  "a,b,c\n\n\n\n\nd,e,f\n\n\n\n\ng,h,i\nj,k,l\n\n\n",
		Output: []uint64{10, 20, 26},
	},
	{
		Name:   "LeadingSpace",
		Input:  " a,  b,   c\n",
		Output: nil,
	},
	{
		Name:    "Comment1",
		Comment: '#',
		Input:   "#1,2,3\na,b,c\n#comment",
		Output:  nil,
	},
	{
		Name:    "Comment2",
		Comment: '#',
		Input:   "#1,2,3\na,b,c\n#comment\n#comment2",
		Output:  nil,
	},
	{
		Name:    "Comment3",
		Comment: '#',
		Input:   "#1,2,3\na,b,c\nd,e,f\n#comment",
		Output:  []uint64{13},
	},
	{
		Name:    "Comment4",
		Comment: '#',
		Input:   "#1,2,3\na,b,c\nd,e,f\n#comment\n#comment2",
		Output:  []uint64{13},
	},
	{
		Name:    "Comment5",
		Comment: '#',
		Input:   "#1,2,3\na,b,c\n#comment\nd,e,f\n",
		Output:  []uint64{22},
	},
	{
		Name:    "Comment6",
		Comment: '#',
		Input:   "#1,2,3\na,b,c\nd,e,f\n#comment\ng,h,i\n",
		Output:  []uint64{13, 28},
	},
	{
		Name:   "NoComment",
		Input:  "#1,2,3\na,b,c",
		Output: []uint64{7},
	},
	{
		Name:       "LazyQuotes",
		LazyQuotes: true,
		Input:      `a "word","1"2",a","b`,
		Output:     nil,
	},
	{
		Name:       "BareQuotes",
		LazyQuotes: true,
		Input:      `a "word","1"2",a"`,
		Output:     nil,
	},
	{
		Name:       "BareDoubleQuotes",
		LazyQuotes: true,
		Input:      `a""b,c`,
		Output:     nil,
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
		Output:           nil,
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
		Name:   "FieldCount",
		Input:  "a,b,c\nd,e",
		Output: []uint64{6},
	},
	{
		Name:   "TrailingCommaEOF",
		Input:  "a,b,c,",
		Output: nil,
	},
	{
		Name:   "TrailingCommaEOL",
		Input:  "a,b,c,\n",
		Output: nil,
	},
	{
		Name:             "TrailingCommaSpaceEOF",
		TrimLeadingSpace: true,
		Input:            "a,b,c, ",
		Output:           nil,
	},
	{
		Name:             "TrailingCommaSpaceEOL",
		TrimLeadingSpace: true,
		Input:            "a,b,c, \n",
		Output:           nil,
	},
	{
		Name:             "TrailingCommaLine3",
		TrimLeadingSpace: true,
		Input:            "a,b,c\nd,e,f\ng,hi,",
		Output:           []uint64{6, 12},
	},
	{
		Name:   "NotTrailingComma3",
		Input:  "a,b,c, \n",
		Output: nil,
	},
	{
		Name: "CommaFieldTest",
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
		Output: []uint64{8, 15, 21, 26, 30, 46, 61, 75, 88},
	},
	{
		Name:             "TrailingCommaIneffective1",
		TrimLeadingSpace: true,
		Input:            "a,b,\nc,d,e",
		Output:           []uint64{5},
	},
}

func TestFind(t *testing.T) {
	for _, tt := range readTests {
		r := NewRowFinder(strings.NewReader(tt.Input))
		r.Comment = tt.Comment
		r.LazyQuotes = tt.LazyQuotes
		r.TrimLeadingSpace = tt.TrimLeadingSpace
		if tt.Comma != 0 {
			r.Comma = tt.Comma
		}
		out, err := r.FindAll()
		perr, _ := err.(*ParseError)
		if tt.Error != "" {
			if err == nil || !strings.Contains(err.Error(), tt.Error) {
				t.Errorf("%s: error %v, want error %q", tt.Name, err, tt.Error)
			} else if tt.Line != 0 && (tt.Line != perr.Line || tt.Column != perr.Column) {
				t.Errorf("%s: error at %d:%d expected %d:%d", tt.Name, perr.Line, perr.Column, tt.Line, tt.Column)
			}
		} else if err != nil {
			t.Errorf("%s: unexpected error %v", tt.Name, err)
		} else if !reflect.DeepEqual(out, tt.Output) {
			t.Errorf("%s: out=%v want %v", tt.Name, out, tt.Output)
		}
	}
}

func BenchmarkFind(b *testing.B) {
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
		_, err := NewRowFinder(strings.NewReader(data)).FindAll()

		if err != nil {
			b.Fatalf("could not read data: %s", err)
		}
	}
}
