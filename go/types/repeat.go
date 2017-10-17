// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

// Repeat represents the repetition of a Value in a sequence.
type Repeat struct {
	valueImpl
	count uint64
	v     Value
}

const (
	repeatPartKind = iota
	repeatPartCount
	repeatPartValue
	repeatPartEnd
)

func newRepeat(vrw ValueReadWriter, count uint64, v Value) Repeat {
	d.PanicIfTrue(vrw == nil)
	d.PanicIfTrue(count == 0)
	d.PanicIfTrue(v.Kind() == RepeatKind)

	w := newBinaryNomsWriter()
	offsets := make([]uint32, repeatPartEnd)
	offsets[repeatPartKind] = w.offset
	RepeatKind.writeTo(&w)
	offsets[repeatPartCount] = w.offset
	w.writeCount(count)
	offsets[repeatPartValue] = w.offset
	v.writeTo(&w)

	return Repeat{valueImpl{vrw, w.data(), offsets}, count, v}
}

func readRepeat(r *valueDecoder) Repeat {
	start := r.pos()

	offsets := make([]uint32, repeatPartEnd)
	offsets[repeatPartKind] = r.pos()
	r.skipKind()
	offsets[repeatPartCount] = r.pos()
	count := r.readCount()
	offsets[repeatPartValue] = r.pos()
	v := r.readValue()

	end := r.pos()
	return Repeat{valueImpl{nil, r.byteSlice(start, end), offsets}, count, v}
}

func skipRepeat(r *valueDecoder) uint64 {
	r.skipKind()
	count := r.readCount()
	r.skipValue()
	return count
}

// Value interface

func (r Repeat) Value() Value {
	return r
}

func (r Repeat) WalkValues(cb ValueCallback) {
	r.v.WalkValues(cb)
}

func (r Repeat) typeOf() *Type {
	return makeCompoundType(RepeatKind, r.v.typeOf())
}
