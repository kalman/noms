// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {compare} from './compare.js';
import Hash from './hash.js';
import type {makeChunkFn} from './sequence-chunker.js';
import type {ValueReader} from './value-store.js';
import type Value from './value.js';
import {ValueBase} from './value.js';
import Collection from './collection.js';
import type {Type} from './type.js';
import {
  boolType,
  blobType,
  makeMapType,
  makeListType,
  makeRefType,
  makeSetType,
  makeUnionType,
  valueType,
} from './type.js';
import {invariant, notNull} from './assert.js';
import Ref, {constructRef} from './ref.js';
import type {Sequence} from './sequence.js';
import {Kind} from './noms-kind.js';
import type {NomsKind} from './noms-kind.js';
import List from './list.js';
import ListLeafSequence from './list-leaf-sequence.js';
import Map from './map.js';
import Set from './set.js';
import Blob from './blob.js';
import type {EqualsFn} from './edit-distance.js';
import {hashValueBytes} from './rolling-value-hasher.js';
import RollingValueHasher from './rolling-value-hasher.js';
import LeafSequence from './leaf-sequence.js';

/**
 * TODO: docs, what is T/K
 */
export class MetaTuple<T, K: Value> {
  ref: Ref<any>;
  key: OrderedKey<K>;
  numLeaves: number;
  child: ?Collection<T, K>;

  constructor(ref: Ref<any>, key: OrderedKey<K>, numLeaves: number, child: ?Collection<T, K>) {
    this.ref = ref;
    this.key = key;
    this.numLeaves = numLeaves;
    this.child = child;
  }

  getChildSequence(vr: ?ValueReader): Promise<Sequence<T, K>> {
    return this.child ?
        Promise.resolve(this.child.sequence) :
        notNull(vr).readValue(this.ref.targetHash).then((c: Collection<T, K>) => {
          invariant(c, () => `Could not read sequence ${this.ref.targetHash.toString()}`);
          return c.sequence;
        });
  }

  getChildSequenceSync(): Sequence<T, K> {
    return notNull(this.child).sequence;
  }
}

export function metaHashValueBytes<T, K: Value>(tuple: MetaTuple<T, K>, rv: RollingValueHasher) {
  let val = tuple.key.v;
  if (!tuple.key.isOrderedByValue) {
    // See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
    val = constructRef(makeRefType(boolType), notNull(tuple.key.h), 0);
  } else {
    invariant(val);
  }

  hashValueBytes(tuple.ref, rv);
  hashValueBytes(val, rv);
}

// TODO: OrderedKey isn't really the best name and/or not the right place for this?
export class OrderedKey<K: Value> {
  isOrderedByValue: boolean;
  v: ?K;
  h: ?Hash;

  constructor(v: K) {
    this.v = v;
    if (v instanceof ValueBase) {
      this.isOrderedByValue = false;
      this.h = v.hash;
    } else {
      this.isOrderedByValue = true;
      this.h = null;
    }
  }

  static fromHash(h: Hash): OrderedKey<any> {
    const k = Object.create(this.prototype);
    k.isOrderedByValue = false;
    k.v = null;
    k.h = h;
    return k;
  }

  value(): K {
    return notNull(this.v);
  }

  numberValue(): number {
    invariant(typeof this.v === 'number');
    return this.v;
  }

  compare(other: OrderedKey<any>): number {
    if (this.isOrderedByValue && other.isOrderedByValue) {
      return compare(notNull(this.v), notNull(other.v));
    }
    if (this.isOrderedByValue) {
      return -1;
    }
    if (other.isOrderedByValue) {
      return 1;
    }
    return notNull(this.h).compare(notNull(other.h));
  }
}

/**
 * Returns the elemTypes of the collection inside the Ref<Collection<?, ?>>.
 */
function getCollectionTypes<T, K: Value>(tuple: MetaTuple<T, K>): Type<any>[] {
  return tuple.ref.type.desc.elemTypes[0].desc.elemTypes;
}

export function newListMetaSequence<T: Value>(vr: ?ValueReader, items: MetaTuple<T, T>[])
    : MetaSequence<T, T> {
  const t = makeListType(makeUnionType(items.map(tuple => getCollectionTypes(tuple)[0])));
  return new MetaSequence(vr, t, items);
}

export function newBlobMetaSequence(vr: ?ValueReader, items: MetaTuple<number, number>[])
    : MetaSequence<number, number> {
  return new MetaSequence(vr, blobType, items);
}

export default class MetaSequence<T, K: Value> {
  _vr: ?ValueReader;
  _t: Type<any>;
  _items: MetaTuple<T, K>[];
  _offsets: number[];

  constructor(vr: ?ValueReader, t: Type<any>, items: MetaTuple<T, K>[]) {
    this._vr = vr;
    this._t = t;
    this._items = items;
    let cum = 0;
    this._offsets = items.map(i => {
      cum += i.numLeaves;
      return cum;
    });
  }

  /**
   * Returns the sequences pointed to by all items[i], s.t. start <= i < end,
   * and returns the concatentation as one long composite sequence
   */
  async getCompositeChildSequence(start: number, length: number): Promise<Sequence<T, K>> {
    if (length === 0) {
      return new EmptySequence();
    }

    const children = await Promise.all(this._items
      .slice(start, start + length)
      .map(item => item.getChildSequence(this._vr)));
    const items = children.reduce((all, child) => all.concat(child.items), []);

    if (!items[0].isMeta) {
      // Assume that getCompositeChildSequence is only called on lists.
      // $FlowIssue: doesn't know that T:Value if these are leaf sequences.
      return new ListLeafSequence(this._vr, this.type, items);
    }
    return new MetaSequence(this._vr, this.type, items);
  }

  // Sequence implementation

  get type(): Type<any> {
    return this._t;
  }

  get items(): MetaTuple<T, K>[] {
    return this._items;
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this._offsets[this._offsets.length - 1];
  }

  get valueReader(): ?ValueReader {
    return this._vr;
  }

  get chunks(): Ref<any>[] {
    return this._items.map(mt => mt.ref);
  }

  get length(): number {
    return this._items.length;
  }

  getChildSequence(idx: number): Promise<?Sequence<T, K>> {
    const mt = this.items[idx];
    return mt.getChildSequence(this._vr);
  }

  getChildSequenceSync(idx: number): ?Sequence<T, K> {
    const mt = this.items[idx];
    return mt.getChildSequenceSync();
  }

  getEqualsFn(other: Sequence<T, K>): EqualsFn {
    invariant(other instanceof MetaSequence);
    return (idx: number, otherIdx: number) =>
      this.items[idx].ref.targetHash.equals(other.items[otherIdx].ref.targetHash);
  }

  range(start: number, end: number): Promise<T[]> {
    invariant(start >= 0 && end >= 0 && end >= start);

    const childRanges = [];
    for (let i = 0; i < this.items.length && end > start; i++) {
      const cum = this.cumulativeNumberOfLeaves(i);
      const seqLength = this.items[i].key.numberValue();
      if (start < cum) {
        const seqStart = cum - seqLength;
        const childStart = start - seqStart;
        const childEnd = Math.min(seqLength, end - seqStart);
        childRanges.push(this.getChildSequence(i).then(
          child => notNull(child).range(childStart, childEnd)));
        start += childEnd - childStart;
      }
    }

    return Promise.all(childRanges).then(ranges => {
      const range = [];
      ranges.forEach(r => range.push(...r));
      return range;
    });
  }

  cumulativeNumberOfLeaves(idx: number): number {
    return this._offsets[idx];
  }

  getKey(idx: number): OrderedKey<K> {
    return this.items[idx].key;
  }
}

export function newMapMetaSequence<T, K: Value>(vr: ?ValueReader, tuples: MetaTuple<T, K>[])
    : MetaSequence<T, K> {
  const kt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0]));
  const vt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[1]));
  const t = makeMapType(kt, vt);
  return new MetaSequence(vr, t, tuples);
}

export function newSetMetaSequence<T: Value>(vr: ?ValueReader, tuples: MetaTuple<T, T>[])
    : MetaSequence<T, T> {
  const t = makeSetType(makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0])));
  return new MetaSequence(vr, t, tuples);
}

export function newOrderedMetaSequenceChunkFn<T, K: Value>(kind: NomsKind, vr: ?ValueReader)
    : makeChunkFn<T, K> {
  // $FlowIssue: this is confusing flow.
  return (tuples: MetaTuple<T, K>[]) => {
    const numLeaves = tuples.reduce((l, mt) => l + mt.numLeaves, 0);
    const last = tuples[tuples.length - 1];
    let seq, col;
    if (kind === Kind.Map) {
      seq = newMapMetaSequence(vr, tuples);
      col = Map.fromSequence(seq);
    } else {
      invariant(kind === Kind.Set);
      seq = newSetMetaSequence(vr, tuples);
      col = Set.fromSequence(seq);
    }
    return [col, last.key, numLeaves];
  };
}

export function newIndexedMetaSequenceChunkFn<T, K: Value>(kind: NomsKind, vr: ?ValueReader)
    : makeChunkFn<T, K> {
  // $FlowIssue: this is confusing Flow.
  return (tuples: MetaTuple<T, K>[]) => {
    const sum = tuples.reduce((l, mt) => {
      const nv = mt.key.numberValue();
      invariant(nv === mt.numLeaves);
      return l + nv;
    }, 0);
    let seq, col;
    if (kind === Kind.List) {
      seq = newListMetaSequence(vr, tuples);
      col = List.fromSequence(seq);
    } else {
      invariant(kind === Kind.Blob);
      seq = newBlobMetaSequence(vr, tuples);
      col = Blob.fromSequence(seq);
    }
    const key = new OrderedKey(sum);
    return [col, key, sum];
  };
}

class EmptySequence extends LeafSequence<any, any> {
  constructor() {
    super(null, valueType, []);
  }

  // Sequence implementation

  get chunks(): Ref<any>[] {
    return [];
  }

  getEqualsFn(other: Sequence<any, any>): EqualsFn {
    invariant(other instanceof EmptySequence);
    return (idx: number, otherIdx: number) => { // eslint-disable-line
      throw new Error('empty sequence');
    };
  }

  getKey(idx: number): any { // eslint-disable-line
    throw new Error('empty sequence');
  }
}
