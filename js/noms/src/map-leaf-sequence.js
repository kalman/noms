// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Ref from './ref.js';
import type {ValueReader} from './value-store.js';
import type Value from './value.js';
import {equals} from './compare.js';
import {getTypeOfValue, makeMapType, makeUnionType} from './type.js';
import {OrderedKey} from './meta-sequence.js';
import {ValueBase} from './value.js';
import type {EqualsFn} from './edit-distance.js';
import type {MapEntry} from './map.js';
import {KEY, VALUE} from './map.js';
import LeafSequence from './leaf-sequence.js';
import type {Sequence} from './sequence.js';
import {invariant} from './assert.js';

/**
 * MapLeafSequence is used for the leaf items of a map prolly-tree.
 */
export default class MapLeafSequence<K: Value, V: Value> extends LeafSequence<MapEntry<K, V>, K> {
  // Sequence implementation

  get chunks(): Array<Ref<any>> {
    const chunks = [];
    for (const entry of this.items) {
      if (entry[KEY] instanceof ValueBase) {
        chunks.push(...entry[KEY].chunks);
      }
      if (entry[VALUE] instanceof ValueBase) {
        chunks.push(...entry[VALUE].chunks);
      }
    }
    return chunks;
  }

  getEqualsFn(other: Sequence<MapEntry<K, V>, K>): EqualsFn {
    invariant(other instanceof MapLeafSequence);
    return (idx: number, otherIdx: number) =>
      equals(this.items[idx][KEY], other.items[otherIdx][KEY]) &&
      equals(this.items[idx][VALUE], other.items[otherIdx][VALUE]);
  }

  getKey(idx: number): OrderedKey<K> {
    return new OrderedKey(this.items[idx][KEY]);
  }
}

export function newMapLeafSequence<K: Value, V: Value>(
    vr: ?ValueReader, items: MapEntry<K, V>[]): MapLeafSequence<K, V> {
  const kt = [];
  const vt = [];
  for (let i = 0; i < items.length; i++) {
    kt.push(getTypeOfValue(items[i][KEY]));
    vt.push(getTypeOfValue(items[i][VALUE]));
  }
  const t = makeMapType(makeUnionType(kt), makeUnionType(vt));
  return new MapLeafSequence(vr, t, items);
}
