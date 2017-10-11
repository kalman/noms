// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type hashValueBytesFn func(item sequenceItem, rv *rollingValueHasher)

type sequenceChunker struct {
	kind                       NomsKind
	cur                        *sequenceCursor
	level                      uint64
	vrw                        ValueReadWriter
	parent                     *sequenceChunker
	current                    []sequenceEntry
	makeChunk, parentMakeChunk makeChunkFn
	isLeaf                     bool
	hashValueBytes             hashValueBytesFn
	rv                         *rollingValueHasher
	done                       bool
	unwrittenCol               Collection
}

type sequenceEntry struct {
	count uint64
	item  sequenceItem
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(level uint64, values []sequenceEntry) (Collection, orderedKey, uint64)

func newEmptySequenceChunker(kind NomsKind, vrw ValueReadWriter, makeChunk, parentMakeChunk makeChunkFn, hashValueBytes hashValueBytesFn) *sequenceChunker {
	return newSequenceChunker(kind, nil, uint64(0), vrw, makeChunk, parentMakeChunk, hashValueBytes)
}

func newSequenceChunker(kind NomsKind, cur *sequenceCursor, level uint64, vrw ValueReadWriter, makeChunk, parentMakeChunk makeChunkFn, hashValueBytes hashValueBytesFn) *sequenceChunker {
	d.PanicIfFalse(makeChunk != nil)
	d.PanicIfFalse(parentMakeChunk != nil)
	d.PanicIfFalse(hashValueBytes != nil)
	d.PanicIfTrue(vrw == nil)

	// |cur| will be nil if this is a new sequence, implying this is a new tree, or the tree has grown in height relative to its original chunked form.

	sc := &sequenceChunker{
		kind,
		cur,
		level,
		vrw,
		nil,
		make([]sequenceEntry, 0, 1<<10),
		makeChunk, parentMakeChunk,
		true,
		hashValueBytes,
		newRollingValueHasher(byte(level % 256)),
		false,
		nil,
	}

	if cur != nil {
		sc.resume()
	}

	return sc
}

func (sc *sequenceChunker) resume() {
	if sc.cur.parent != nil && sc.parent == nil {
		sc.createParent()
	}

	idx := sc.cur.idx

	// Walk backwards to the start of the existing chunk.
	for sc.cur.indexInChunk() > 0 && sc.cur.retreatMaybeAllowBeforeStart(false) {
	}

	for ; sc.cur.idx < idx; sc.cur.advance() {
		sc.appendEntry(sc.cur.currentEntry())
	}
}

// advanceTo advances the sequenceChunker to the next "spine" at which
// modifications to the prolly-tree should take place
func (sc *sequenceChunker) advanceTo(next *sequenceCursor) {
	// There are four basic situations which must be handled when advancing to a
	// new chunking position:
	//
	// Case (1): |sc.cur| and |next| are exactly aligned. In this case, there's
	//           nothing to do. Just assign sc.cur = next.
	//
	// Case (2): |sc.cur| is "ahead" of |next|. This can only have resulted from
	//           advancing of a lower level causing |sc.cur| to advance. In this
	//           case, we advance |next| until the cursors are aligned and then
	//           process as if Case (1):
	//
	// Case (3+4): |sc.cur| is "behind" |next|, we must consume elements in
	//             |sc.cur| until either:
	//
	//   Case (3): |sc.cur| aligns with |next|. In this case, we just assign
	//             sc.cur = next.
	//   Case (4): A boundary is encountered which is aligned with a boundary
	//             in the previous state. This is the critical case, as is allows
	//             us to skip over large parts of the tree. In this case, we align
	//             parent chunkers then sc.resume() at |next|

	for sc.cur.compare(next) > 0 {
		next.advance() // Case (2)
	}

	// If neither loop above and below are entered, it is Case (1). If the loop
	// below is entered but Case (4) isn't reached, then it is Case (3).
	reachedNext := true
	for sc.cur.compare(next) < 0 {
		if sc.appendEntry(sc.cur.currentEntry()) && sc.cur.atLastItem() {
			if sc.cur.parent != nil {

				if sc.cur.parent.compare(next.parent) < 0 {
					// Case (4): We stopped consuming items on this level before entering
					// the sequence referenced by |next|
					reachedNext = false
				}

				// Note: Logically, what is happening here is that we are consuming the
				// item at the current level. Logically, we'd call sc.cur.advance(),
				// but that would force loading of the next sequence, which we don't
				// need for any reason, so instead we advance the parent and take care
				// not to allow it to step outside the sequence.
				sc.cur.parent.advanceMaybeAllowPastEnd(false)

				// Invalidate this cursor, since it is now inconsistent with its parent
				sc.cur.parent = nil
				sc.cur.seq = nil
			}

			break
		}

		sc.cur.advance()
	}

	if sc.parent != nil && next.parent != nil {
		sc.parent.advanceTo(next.parent)
	}

	sc.cur = next
	if !reachedNext {
		sc.resume() // Case (4)
	}
}

func (sc *sequenceChunker) Append(item sequenceItem) {
	d.PanicIfTrue(item == nil)

	// Only support CountList/CompressedList/RepeatList/whateverList for Lists.
	if sc.kind != ListKind {
		sc.appendEntry(sequenceEntry{1, item})
		return
	}

	if len(sc.current) == 0 {
		sc.current = append(sc.current, sequenceEntry{1, item})
		return
	}

	// XXX: == doesn't really work, need to use equals(). but these aren't
	// necessarily values, so need to type check as well (or check isLeaf).
	if l := len(sc.current); sc.current[l-1].item == item {
		sc.current[l-1].count++
	} else {
		sc.consumeLastEntry()
		sc.current = append(sc.current, sequenceEntry{1, item})
	}
}

func (sc *sequenceChunker) appendEntry(entry sequenceEntry) bool {
	sc.current = append(sc.current, entry)
	return sc.consumeLastEntry()
}

func (sc *sequenceChunker) consumeLastEntry() bool {
	last := sc.current[len(sc.current)-1]

	if last.count > 1 {
		sc.hashValueBytes(Number(last.count), sc.rv)
	}
	sc.hashValueBytes(last.item, sc.rv)

	if sc.rv.crossedBoundary {
		sc.handleChunkBoundary()
		return true
	}
	return false
}

func (sc *sequenceChunker) Skip() {
	sc.cur.advance()
}

func (sc *sequenceChunker) createParent() {
	d.PanicIfFalse(sc.parent == nil)
	var parent *sequenceCursor
	if sc.cur != nil && sc.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.advance() will affect our parent - and vice versa - in surprising ways. Instead, Skip moves forward our parent's cursor if we advance across a boundary.
		parent = sc.cur.parent
	}
	sc.parent = newSequenceChunker(sc.kind, parent, sc.level+1, sc.vrw, sc.parentMakeChunk, sc.parentMakeChunk, metaHashValueBytes)
	sc.parent.isLeaf = false

	if sc.unwrittenCol != nil {
		// There is an unwritten collection, but this chunker now has a parent, so
		// write it. See createSequence().
		sc.vrw.WriteValue(sc.unwrittenCol)
		sc.unwrittenCol = nil
	}
}

// createSequence creates a sequence from the current items in |sc.current|,
// clears the current items, then returns the new sequence and a metaTuple that
// points to it.
//
// If |write| is true then the sequence is eagerly written, or if false it's
// manually constructed and stored in |sc.unwrittenCol| to possibly write later
// in createParent(). This is to hopefully avoid unnecessarily writing the root
// chunk (for example, the sequence may be stored inline).
//
// There is a catch: in the rare case that the root chunk is actually not the
// canonical root of the sequence (see Done()), then we will have ended up
// unnecessarily writing a chunk - the canonical root. However, this is a fair
// tradeoff for simplicity of the chunking algorithm.
func (sc *sequenceChunker) createSequence(write bool) (sequence, metaTuple) {
	col, key, numLeaves := sc.makeChunk(sc.level, sc.current)

	// |sc.makeChunk| copies |sc.current| so it's safe to re-use the memory.
	sc.current = sc.current[:0]

	var ref Ref
	if write {
		ref = sc.vrw.WriteValue(col)
	} else {
		ref = NewRef(col)
		sc.unwrittenCol = col
	}

	mt := newMetaTuple(ref, key, numLeaves)
	return col.asSequence(), mt
}

func (sc *sequenceChunker) handleChunkBoundary() {
	d.PanicIfFalse(len(sc.current) > 0)
	sc.rv.Reset()
	if sc.parent == nil {
		sc.createParent()
	}
	_, mt := sc.createSequence(true)
	sc.parent.Append(mt)
}

// Returns true if this chunker or any of its parents have any pending items in their |current| slice.
func (sc *sequenceChunker) anyPending() bool {
	if len(sc.current) > 0 {
		return true
	}

	if sc.parent != nil {
		return sc.parent.anyPending()
	}

	return false
}

// Returns the root sequence of the resulting tree. The logic here is subtle, but hopefully correct and understandable. See comments inline.
func (sc *sequenceChunker) Done() sequence {
	d.PanicIfTrue(sc.done)
	sc.done = true

	if sc.cur != nil {
		sc.finalizeCursor()
	}

	// There is pending content above us, so we must push any remaining items from this level up and allow some parent to find the root of the resulting tree.
	if sc.parent != nil && sc.parent.anyPending() {
		if len(sc.current) > 0 {
			// If there are items in |current| at this point, they represent the final items of the sequence which occurred beyond the previous *explicit* chunk boundary. The end of input of a sequence is considered an *implicit* boundary.
			sc.handleChunkBoundary()
		}

		return sc.parent.Done()
	}

	// At this point, we know this chunker contains, in |current| every item at this level of the resulting tree. To see this, consider that there are two ways a chunker can enter items into its |current|: (1) as the result of resume() with the cursor on anything other than the first item in the sequence, and (2) as a result of a child chunker hitting an explicit chunk boundary during either Append() or finalize(). The only way there can be no items in some parent chunker's |current| is if this chunker began with cursor within its first existing chunk (and thus all parents resume()'d with a cursor on their first item) and continued through all sebsequent items without creating any explicit chunk boundaries (and thus never sent any items up to a parent as a result of chunking). Therefore, this chunker's current must contain all items within the current sequence.

	// This level must represent *a* root of the tree, but it is possibly non-canonical. There are three cases to consider:

	// (1) This is "leaf" chunker and thus produced tree of depth 1 which contains exactly one chunk (never hit a boundary), or (2) This in an internal node of the tree which contains multiple references to child nodes. In either case, this is the canonical root of the tree.
	if sc.isLeaf || len(sc.current) > 1 {
		seq, _ := sc.createSequence(false)
		return seq
	}

	// (3) This is an internal node of the tree which contains a single reference to a child node. This can occur if a non-leaf chunker happens to chunk on the first item (metaTuple) appended. In this case, this is the root of the tree, but it is *not* canonical and we must walk down until we find cases (1) or (2), above.
	d.PanicIfFalse(!sc.isLeaf && len(sc.current) == 1)
	mt := sc.current[0].item.(metaTuple)

	for {
		child := mt.getChildSequence(sc.vrw)
		if _, ok := child.(metaSequence); !ok || child.seqLen() > 1 {
			return child
		}

		mt = child.getItem(0).(metaTuple)
	}
}

// If we are mutating an existing sequence, append subsequent items in the sequence until we reach a pre-existing chunk boundary or the end of the sequence.
func (sc *sequenceChunker) finalizeCursor() {
	for ; sc.cur.valid(); sc.cur.advance() {
		if sc.appendEntry(sc.cur.currentEntry()) && sc.cur.atLastItem() {
			break // boundary occurred at same place in old & new sequence
		}
	}

	if sc.cur.parent != nil {
		sc.cur.parent.advance()

		// Invalidate this cursor, since it is now inconsistent with its parent
		sc.cur.parent = nil
		sc.cur.seq = nil
	}
}
