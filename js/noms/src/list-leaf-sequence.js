// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {equals} from './compare.js';
import type {ValueReader} from './value-store.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import Ref from './ref.js';
import {makeListType, makeUnionType, getTypeOfValue} from './type.js';
import LeafSequence from './leaf-sequence.js';
import type {EqualsFn} from './edit-distance.js';
import type {OrderedKey} from './meta-sequence.js';
import type {Sequence} from './sequence.js';
import {invariant} from './assert.js';

/**
 * ListLeafSequence is used for the leaf items of a set prolly-tree.
 */
export default class ListLeafSequence<T: Value> extends LeafSequence<T, any> {
  // Sequence implementation

  get chunks(): Array<Ref<any>> {
    return this.getValueChunks();
  }

  getEqualsFn(other: Sequence<T, any>): EqualsFn {
    invariant(other instanceof ListLeafSequence);
    return (idx, otherIdx) => equals(this.items[idx], other.items[otherIdx]);
  }

  getKey(idx: number): OrderedKey<any> { // eslint-disable-line
    throw new Error('unsupported');
  }
}

export function newListLeafSequence<T: Value>(vr: ?ValueReader, items: T[])
    : ListLeafSequence<T> {
  const t = makeListType(makeUnionType(items.map(getTypeOfValue)));
  return new ListLeafSequence(vr, t, items);
}
