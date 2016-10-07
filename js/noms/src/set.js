// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type {ValueReader} from './value-store.js';
import type {makeChunkFn} from './sequence-chunker.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import {AsyncIterator} from './async-iterator.js';
import {chunkSequence, chunkSequenceSync} from './sequence-chunker.js';
import Collection from './collection.js';
import {compare, equals} from './compare.js';
import {invariant} from './assert.js';
import {
  newOrderedMetaSequenceChunkFn,
  OrderedKey,
} from './meta-sequence.js';
import type SequenceCursor from './sequence-cursor.js';
import {
  newCursorAtKey,
  newCursorAtValue,
} from './ordered-sequence.js';
import diff from './ordered-sequence-diff.js';
import {removeDuplicateFromOrdered} from './map.js';
import {Kind} from './noms-kind.js';
import {hashValueBytes} from './rolling-value-hasher.js';
import {newSetLeafSequence} from './set-leaf-sequence.js';
import SequenceIterator from './sequence-iterator.js';

function newSetLeafChunkFn<T: Value>(vr: ?ValueReader): makeChunkFn<T, T> {
  return (items: T[]) => {
    // $FlowIssue: we're forced to use false as a placeholder for no-value.
    const key = new OrderedKey(items.length > 0 ? items[items.length - 1] : false);
    const seq = newSetLeafSequence(vr, items);
    const ns = Set.fromSequence(seq);
    return [ns, key, items.length];
  };
}

function buildSetData<T: Value>(values: Array<any>): Array<T> {
  values = values.slice();
  values.sort(compare);
  return removeDuplicateFromOrdered(values, compare);
}

export default class Set<T: Value> extends Collection<T, T> {
  constructor(values: Array<T> = []) {
    super(chunkSequenceSync(
        buildSetData(values),
        newSetLeafChunkFn(null),
        newOrderedMetaSequenceChunkFn(Kind.Set, null),
        hashValueBytes));
  }

  async has(key: T): Promise<boolean> {
    const cursor = await newCursorAtValue(this.sequence, key);
    return cursor.valid && equals(cursor.getCurrentKey().value(), key);
  }

  async _firstOrLast(last: boolean): Promise<?T> {
    const cursor = await newCursorAtKey(this.sequence, null, false, last);
    return cursor.valid ? cursor.getCurrent() : null;
  }

  first(): Promise<?T> {
    return this._firstOrLast(false);
  }

  last(): Promise<?T> {
    return this._firstOrLast(true);
  }

  async forEach(cb: (v: T) => ?Promise<any>): Promise<void> {
    const cursor = await newCursorAtKey(this.sequence, null);
    const promises = [];
    return cursor.iter(v => {
      promises.push(cb(v));
      return false;
    }).then(() => Promise.all(promises)).then(() => void 0);
  }

  iterator(): AsyncIterator<T> {
    return new SequenceIterator(newCursorAtKey(this.sequence, null));
  }

  iteratorAt(v: T): AsyncIterator<T> {
    return new SequenceIterator(newCursorAtValue(this.sequence, v));
  }

  _splice(cursor: SequenceCursor<any, any>, insert: Array<T>, remove: number)
      : Promise<Set<T>> {
    const vr = this.sequence.valueReader;
    return chunkSequence(cursor, vr, insert, remove, newSetLeafChunkFn(vr),
                         newOrderedMetaSequenceChunkFn(Kind.Set, vr),
                         hashValueBytes).then(s => Set.fromSequence(s));
  }

  async add(value: T): Promise<Set<T>> {
    const cursor = await newCursorAtValue(this.sequence, value, true);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), value)) {
      return this;
    }

    return this._splice(cursor, [value], 0);
  }

  async delete(value: T): Promise<Set<T>> {
    const cursor = await newCursorAtValue(this.sequence, value);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), value)) {
      return this._splice(cursor, [], 1);
    }

    return this;
  }

  // TODO: Find some way to return a Set.
  async map<S>(cb: (v: T) => (Promise<S> | S)): Promise<Array<S>> {
    const cursor = await newCursorAtKey(this.sequence, null);
    const values = [];
    await cursor.iter(v => {
      values.push(cb(v));
      return false;
    });

    return Promise.all(values);
  }

  get size(): number {
    return this.sequence.numLeaves;
  }

  /**
   * Returns a 2-tuple [added, removed] sorted values.
   */
  diff(from: Set<T>): Promise<[Array<T> /* added */, Array<T> /* removed */]> {
    return diff(from.sequence, this.sequence).then(([added, removed, modified]) => {
      invariant(modified.length === 0);
      return [added, removed];
    });
  }
}
