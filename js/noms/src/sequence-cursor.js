// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant, notNull} from './assert.js';
import search from './binary-search.js';
import type {OrderedKey} from './meta-sequence.js';
import type {Sequence} from './sequence.js';
import type Value from './value.js';

export async function newCursorAtIndex<T, K: Value>(seq: Sequence<T, K>, idx: number)
    : Promise<SequenceCursor<T, K>> {
  let cursor: ?SequenceCursor<T, K> = null;
  for (let childSeq = seq; childSeq; childSeq = await cursor.getChildSequence()) {
    cursor = new SequenceCursor(cursor, childSeq, 0);
    idx -= cursor.advanceToOffset(idx);
  }
  return notNull(cursor);
}

export default class SequenceCursor<T, K: Value> {
  parent: ?SequenceCursor<T, K>;
  sequence: Sequence<T, K>;
  idx: number;

  constructor(parent: ?SequenceCursor<T, K>, sequence: Sequence<T, K>, idx: number) {
    this.parent = parent;
    this.sequence = sequence;
    this.idx = idx;
    if (this.idx < 0) {
      this.idx = Math.max(0, this.sequence.length + this.idx);
    }
  }

  clone(): SequenceCursor<T, K> {
    return new SequenceCursor(this.parent && this.parent.clone(), this.sequence, this.idx);
  }

  get length(): number {
    return this.sequence.length;
  }

  sync(): Promise<void> {
    invariant(this.parent);
    return this.parent.getChildSequence().then(p => {
      this.sequence = notNull(p);
    });
  }

  getChildSequence(): Promise<?Sequence<T, K>> {
    return this.sequence.getChildSequence(this.idx);
  }

  // NOTE: Returning a T is a lie, it really returns "any" because Sequence items are "any".
  // However, assume that SequenceCursor only wrap leaf Sequence outside implementation details.
  getCurrent(): T {
    invariant(this.valid);
    return this.sequence.items[this.idx];
  }

  getCurrentKey(): OrderedKey<K> {
    invariant(this.valid);
    return this.sequence.getKey(this.idx);
  }

  get valid(): boolean {
    return this.idx >= 0 && this.idx < this.length;
  }

  get indexInChunk(): number {
    return this.idx;
  }

  get depth(): number {
    return 1 + (this.parent ? this.parent.depth : 0);
  }

  advance(allowPastEnd: boolean = true): Promise<boolean> {
    return this._advanceMaybeAllowPastEnd(allowPastEnd);
  }

  advanceToOffset(idx: number): number {
    this.idx = search(this.length, (i: number) => idx < this.sequence.cumulativeNumberOfLeaves(i));

    if (this.sequence.isMeta && this.idx === this.length) {
      this.idx = this.length - 1;
    }

    return this.idx > 0 ? this.sequence.cumulativeNumberOfLeaves(this.idx - 1) : 0;
  }

  /**
   * Advances the cursor within the current chunk.
   * Performance optimisation: allowing non-async resolution of leaf elements
   *
   * Returns true if the cursor advanced to a valid position within this chunk, false if not.
   *
   * If |allowPastEnd| is true, the cursor is allowed to advance one index past the end of the chunk
   * (an invalid position, so the return value will be false).
   */
  advanceLocal(allowPastEnd: boolean): boolean {
    if (this.idx === this.length) {
      return false;
    }

    if (this.idx < this.length - 1) {
      this.idx++;
      return true;
    }

    if (allowPastEnd) {
      this.idx++;
    }

    return false;
  }

  /**
   * Returns true if the cursor can advance within the current chunk to a valid position.
   * Performance optimisation: allowing non-async resolution of leaf elements
   */
  canAdvanceLocal(): boolean {
    return this.idx < this.length - 1;
  }

  async _advanceMaybeAllowPastEnd(allowPastEnd: boolean): Promise<boolean> {
    if (this.idx === this.length) {
      return Promise.resolve(false);
    }

    if (this.advanceLocal(allowPastEnd)) {
      return Promise.resolve(true);
    }

    if (this.parent && await this.parent._advanceMaybeAllowPastEnd(false)) {
      await this.sync();
      this.idx = 0;
      return true;
    }

    return false;
  }

  advanceChunk(): Promise<boolean> {
    this.idx = this.length - 1;
    return this._advanceMaybeAllowPastEnd(true);
  }

  retreat(): Promise<boolean> {
    return this._retreatMaybeAllowBeforeStart(true);
  }

  async _retreatMaybeAllowBeforeStart(allowBeforeStart: boolean): Promise<boolean> {
    // TODO: Factor this similar to advance().
    if (this.idx > 0) {
      this.idx--;
      return true;
    }
    if (this.idx === -1) {
      return false;
    }
    invariant(this.idx === 0);
    if (this.parent && await this.parent._retreatMaybeAllowBeforeStart(false)) {
      await this.sync();
      this.idx = this.length - 1;
      return true;
    }

    if (allowBeforeStart) {
      this.idx--;
    }

    return false;
  }

  // NOTE: Iterating over T is a lie, really it's iterating over "any" since the items of Sequence
  // are "any". However, assume the SequenceCursor is only exposed as cursors to leaves.
  async iter(cb: (v: T, i: number) => boolean): Promise<void> {
    let idx = 0;
    while (this.valid) {
      if (cb(this.sequence.items[this.idx], idx++)) {
        return;
      }
      this.advanceLocal(false) || await this.advance();
    }
  }

  /**
   * Moves the cursor to the first value in sequence >= key and returns true.
   * If none exists, returns false.
   * This will only work for sequences that are ordered by K.
   */
  seekTo(key: OrderedKey<K>, lastPositionIfNotfound: boolean = false): boolean {
    // Find smallest idx where key(idx) >= key
    this.idx = search(this.length, i => this.sequence.getKey(i).compare(key) >= 0);

    if (this.idx === this.length && lastPositionIfNotfound) {
      invariant(this.idx > 0);
      this.idx--;
    }

    return this.idx < this.length;
  }
}
