// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type {ValueReader} from './value-store.js';
import type {Type} from './type.js';
import {ValueBase} from './value.js';
import {invariant} from './assert.js';
import type Ref from './ref.js';
import type {Sequence} from './sequence.js';
import type Value from './value.js';

export default class LeafSequence<T, K: Value> {
  _vr: ?ValueReader;
  _type: Type<any>;
  _items: T[];

  constructor(vr: ?ValueReader, type: Type<any>, items: T[]) {
    this._vr = vr;
    this._type = type;
    this._items = items;
  }

  getValueChunks(): Ref<any>[] {
    const chunks = [];
    for (const item of this._items) {
      if (item instanceof ValueBase) {
        chunks.push(...item.chunks);
      }
    }
    return chunks;
  }

  // Sequence implementation
  // Subclasses must implement chunks and getEqualsFn.

  get type(): Type<any> {
    return this._type;
  }

  get items(): T[] {
    return this._items;
  }

  get isMeta(): boolean {
    return false;
  }

  get numLeaves(): number {
    return this._items.length;
  }

  get valueReader(): ?ValueReader {
    return this._vr;
  }

  get length(): number {
    return this._items.length;
  }

  getChildSequence(_: number): Promise<?Sequence<T, K>> {
    return Promise.resolve(null);
  }

  getChildSequenceSync(_: number): ?Sequence<T, K> {
    return null;
  }

  range(start: number, end: number): Promise<T[]> {
    invariant(start >= 0 && end >= 0 && end <= this._items.length);
    return Promise.resolve(this._items.slice(start, end));
  }

  cumulativeNumberOfLeaves(idx: number): number {
    return idx;
  }
}
