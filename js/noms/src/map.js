// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type {ValueReader} from './value-store.js';
import type {makeChunkFn} from './sequence-chunker.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import type {AsyncIterator} from './async-iterator.js';
import {chunkSequence, chunkSequenceSync} from './sequence-chunker.js';
import SequenceIterator from './sequence-iterator.js';
import Collection from './collection.js';
import {compare, equals} from './compare.js';
import {
  OrderedKey,
  newOrderedMetaSequenceChunkFn,
} from './meta-sequence.js';
import type SequenceCursor from './sequence-cursor.js';
import {
  newCursorAtKey,
  newCursorAtValue,
} from './ordered-sequence.js';
import diff from './ordered-sequence-diff.js';
import {Kind} from './noms-kind.js';
import RollingValueHasher, {hashValueBytes} from './rolling-value-hasher.js';
import {newMapLeafSequence} from './map-leaf-sequence.js';

export type MapEntry<K: Value, V: Value> = [K, V];

export const KEY = 0;
export const VALUE = 1;

function newMapLeafChunkFn<K: Value, V: Value>(vr: ?ValueReader): makeChunkFn<MapEntry<K, V>, K> {
  return (items: MapEntry<K, V>[]) => {
    // $FlowIssue: we're forced to use false as a placeholder for no-value.
    const key = new OrderedKey(items.length > 0 ? items[items.length - 1][KEY] : false);
    const seq = newMapLeafSequence(vr, items);
    const nm = Map.fromSequence(seq);
    return [nm, key, seq.length];
  };
}

function mapHashValueBytes<K: Value, V: Value>(entry: MapEntry<K, V>, rv: RollingValueHasher) {
  hashValueBytes(entry[KEY], rv);
  hashValueBytes(entry[VALUE], rv);
}

export function removeDuplicateFromOrdered<T>(elems: T[], compare: (v1: T, v2: T) => number) : T[] {
  const unique = [];
  let i = -1;
  let last = null;
  elems.forEach((elem: T) => {
    if (null === elem || undefined === elem ||
        null === last || undefined === last || compare(last, elem) !== 0) {
      i++;
    }
    unique[i] = elem;
    last = elem;
  });

  return unique;
}

function compareKeys(v1, v2) {
  return compare(v1[KEY], v2[KEY]);
}

function buildMapData<K: Value, V: Value>(kvs: MapEntry<K, V>[]): MapEntry<K, V>[] {
  // TODO: Assert k & v are of correct type
  const entries = kvs.slice();
  entries.sort(compareKeys);
  return removeDuplicateFromOrdered(entries, compareKeys);
}

export default class Map<K: Value, V: Value> extends Collection<MapEntry<K, V>, K> {
  constructor(kvs: MapEntry<K, V>[] = []) {
    super(chunkSequenceSync(
        buildMapData(kvs),
        newMapLeafChunkFn(null),
        newOrderedMetaSequenceChunkFn(Kind.Map, null),
        mapHashValueBytes));
  }

  async has(key: K): Promise<boolean> {
    const cursor = await newCursorAtValue(this.sequence, key);
    return cursor.valid && equals(cursor.getCurrentKey().value(), key);
  }

  async _firstOrLast(last: boolean): Promise<?MapEntry<K, V>> {
    const cursor = await newCursorAtKey(this.sequence, null, false, last);
    if (!cursor.valid) {
      return undefined;
    }

    return cursor.getCurrent();
  }

  first(): Promise<?MapEntry<K, V>> {
    return this._firstOrLast(false);
  }

  last(): Promise<?MapEntry<K, V>> {
    return this._firstOrLast(true);
  }

  async get(key: K): Promise<?V> {
    const cursor = await newCursorAtValue(this.sequence, key);
    if (!cursor.valid) {
      return undefined;
    }

    const entry = cursor.getCurrent();
    return equals(entry[KEY], key) ? entry[VALUE] : undefined;
  }

  async forEach(cb: (v: V, k: K) => ?Promise<any>): Promise<void> {
    const cursor = await newCursorAtKey(this.sequence, null);
    const promises = [];
    return cursor.iter(entry => {
      promises.push(cb(entry[VALUE], entry[KEY]));
      return false;
    }).then(() => Promise.all(promises)).then(() => void 0);
  }

  iterator(): AsyncIterator<MapEntry<K, V>> {
    return new SequenceIterator(newCursorAtKey(this.sequence, null));
  }

  iteratorAt(k: K): AsyncIterator<MapEntry<K, V>> {
    return new SequenceIterator(newCursorAtValue(this.sequence, k));
  }

  _splice(cursor: SequenceCursor<any, any>, insert: MapEntry<K, V>[], remove: number)
      : Promise<Map<K, V>> {
    const vr = this.sequence.valueReader;
    return chunkSequence(cursor, vr, insert, remove, newMapLeafChunkFn(vr),
                         newOrderedMetaSequenceChunkFn(Kind.Map, vr),
                         mapHashValueBytes).then(s => Map.fromSequence(s));
  }

  async set(key: K, value: V): Promise<Map<K, V>> {
    let remove = 0;
    const cursor = await newCursorAtValue(this.sequence, key, true);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), key)) {
      const entry = cursor.getCurrent();
      if (equals(entry[VALUE], value)) {
        return this;
      }

      remove = 1;
    }

    return this._splice(cursor, [[key, value]], remove);
  }

  async delete(key: K): Promise<Map<K, V>> {
    const cursor = await newCursorAtValue(this.sequence, key);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), key)) {
      return this._splice(cursor, [], 1);
    }

    return this;
  }

  get size(): number {
    return this.sequence.numLeaves;
  }

  /**
   * Returns a 3-tuple [added, removed, modified] sorted by keys.
   */
  diff(from: Map<K, V>): Promise<[K[], K[], K[]]> {
    return diff(from.sequence, this.sequence);
  }
}
