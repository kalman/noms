// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {equals} from './compare.js';
import Ref from './ref.js';
import type {ValueReader} from './value-store.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import {OrderedKey} from './meta-sequence.js';
import {makeSetType, makeUnionType, getTypeOfValue} from './type.js';
import type {EqualsFn} from './edit-distance.js';
import LeafSequence from './leaf-sequence.js';
import type {Sequence} from './sequence.js';
import {invariant} from './assert.js';

/**
 * SetLeafSequence is used for the leaf items of a set prolly-tree.
 */
export default class SetLeafSequence<T: Value> extends LeafSequence<T, T> {
  // Sequence implementation

  get chunks(): Ref<any>[] {
    return this.getValueChunks();
  }

  getEqualsFn(other: Sequence<T, T>): EqualsFn {
    invariant(other instanceof SetLeafSequence);
    return (idx, otherIdx) => equals(this.items[idx], other.items[otherIdx]);
  }

  getKey(idx: number): OrderedKey<T> {
    return new OrderedKey(this.items[idx]);
  }
}

export function newSetLeafSequence<T: Value>(vr: ?ValueReader, items: T[])
    : SetLeafSequence<T> {
  const t = makeSetType(makeUnionType(items.map(getTypeOfValue)));
  return new SetLeafSequence(vr, t, items);
}
