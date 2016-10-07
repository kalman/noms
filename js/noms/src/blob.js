// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import * as Bytes from './bytes.js';
import Collection from './collection.js';
import RollingValueHasher from './rolling-value-hasher.js';
import SequenceChunker, {chunkSequence} from './sequence-chunker.js';
import type {ValueReader, ValueReadWriter} from './value-store.js';
import type {makeChunkFn} from './sequence-chunker.js';
import {Kind} from './noms-kind.js';
import {OrderedKey, newIndexedMetaSequenceChunkFn} from './meta-sequence.js';
import type {Sequence} from './sequence.js';
import type SequenceCursor from './sequence-cursor.js';
import {newCursorAtIndex} from './sequence-cursor.js';
import {invariant} from './assert.js';
import {hashValueByte} from './rolling-value-hasher.js';
import {newBlobLeafSequence} from './blob-leaf-sequence.js';

export default class Blob extends Collection<number, any> {
  constructor(bytes: Uint8Array) {
    const chunker = new SequenceChunker(null, null, null, newBlobLeafChunkFn(null),
        newIndexedMetaSequenceChunkFn(Kind.Blob, null), blobHashValueBytes);
    for (let i = 0; i < bytes.length; i++) {
      chunker.append(bytes[i]);
    }
    super(chunker.doneSync());
  }

  getReader(): BlobReader {
    return new BlobReader(this.sequence);
  }

  get length(): number {
    return this.sequence.numLeaves;
  }

  splice(idx: number, deleteCount: number, insert: Uint8Array): Promise<Blob> {
    const vr = this.sequence.valueReader;
    return newCursorAtIndex(this.sequence, idx).then(cursor =>
      chunkSequence(cursor, vr, Array.from(insert), deleteCount, newBlobLeafChunkFn(vr),
                    newIndexedMetaSequenceChunkFn(Kind.Blob, vr, null),
                    hashValueByte)).then(s => Blob.fromSequence(s));
  }
}

export class BlobReader {
  _sequence: Sequence<number, number>;
  _cursor: Promise<SequenceCursor<number, number>>;
  _pos: number;
  _lock: string;

  constructor(sequence: Sequence<number, number>) {
    this._sequence = sequence;
    this._cursor = newCursorAtIndex(sequence, 0);
    this._pos = 0;
    this._lock = '';
  }

  /**
   * Reads the next chunk of bytes from this blob.
   *
   * Returns {done: false, value: chunk} if there is more data, or {done: true} if there is none.
   */
  read(): Promise<{done: boolean, value?: Uint8Array}> {
    invariant(this._lock === '', `cannot read without completing current ${this._lock}`);
    this._lock = 'read';

    return this._cursor.then(cur => {
      if (!cur.valid) {
        return {done: true};
      }
      return this._readCur(cur).then(arr => ({done: false, value: arr}));
    }).then(res => {
      this._lock = '';
      return res;
    });
  }

  _readCur(cur: SequenceCursor<number, number>): Promise<Uint8Array> {
    let arr = cur.sequence.items;
    invariant(arr instanceof Uint8Array);

    const idx = cur.indexInChunk;
    if (idx > 0) {
      invariant(idx < arr.byteLength);
      arr = Bytes.subarray(arr, idx, arr.byteLength);
    }

    return cur.advanceChunk().then(() => {
      // $FlowIssue: Flow doesn't think arr is a Uint8Array.
      this._pos += arr.byteLength;
      // $FlowIssue: Flow doesn't think arr is a Uint8Array.
      return arr;
    });
  }

  /**
   * Seeks the reader to a position either relative to the start, the current position, or end of
   * the blob.
   *
   * If |whence| is 0, |offset| will be relative to the start.
   * If |whence| is 1, |offset| will be relative to the current position.
   * If |whence| is 2, |offset| will be relative to the end.
   */
  seek(offset: number, whence: number = 0): Promise<number> {
    invariant(this._lock === '', `cannot seek without completing current ${this._lock}`);
    this._lock = 'seek';

    let abs = this._pos;

    switch (whence) {
      case 0:
        abs = offset;
        break;
      case 1:
        abs += offset;
        break;
      case 2:
        abs = this._sequence.numLeaves + offset;
        break;
      default:
        throw new Error(`invalid whence ${whence}`);
    }

    invariant(abs >= 0, `cannot seek to negative position ${abs}`);

    this._cursor = newCursorAtIndex(this._sequence, abs);

    // Wait for the seek to complete so that reads will be relative to the new position.
    return this._cursor.then(() => {
      this._pos = abs;
      this._lock = '';
      return abs;
    });
  }
}

function newBlobLeafChunkFn(vr: ?ValueReader): makeChunkFn<number, number> {
  return (items: number[]) => {
    const blobLeaf = newBlobLeafSequence(vr, Bytes.fromValues(items));
    const blob = Blob.fromSequence(blobLeaf);
    const key = new OrderedKey(items.length);
    return [blob, key, items.length];
  };
}

function blobHashValueBytes(b: number, rv: RollingValueHasher) {
  rv.hashByte(b);
}

type BlobWriterState = 'writable' | 'closed';

export class BlobWriter {
  _state: BlobWriterState;
  _blob: ?Promise<Blob>;
  _chunker: SequenceChunker<number, number>;
  _vrw: ?ValueReadWriter;

  constructor(vrw: ?ValueReadWriter) {
    this._state = 'writable';
    this._chunker = new SequenceChunker(null, vrw, vrw, newBlobLeafChunkFn(vrw),
        newIndexedMetaSequenceChunkFn(Kind.Blob, vrw), blobHashValueBytes);
    this._vrw = vrw;
  }

  write(chunk: Uint8Array) {
    assert(this._state === 'writable');
    for (let i = 0; i < chunk.length; i++) {
      this._chunker.append(chunk[i]);
    }
  }

  close() {
    assert(this._state === 'writable');
    this._blob = this._chunker.done(this._vrw).then(seq => Blob.fromSequence(seq));
    this._state = 'closed';
  }

  get blob(): Promise<Blob> {
    assert(this._state === 'closed');
    invariant(this._blob);
    return this._blob;
  }
}

function assert(v: any) {
  if (!v) {
    throw new TypeError('Invalid usage of BlobWriter');
  }
}
