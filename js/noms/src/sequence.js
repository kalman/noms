// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Ref from './ref.js';
import type {Type} from './type.js';
import type {ValueReader} from './value-store.js';
import type {OrderedKey} from './meta-sequence.js';
import type {EqualsFn} from './edit-distance.js';
import type Value from './value.js';

/**
 * TODO: docs, explain T/K, export default
 */
export interface Sequence<T, K: Value> {
  type: Type<any>;
  items: any[];
  isMeta: boolean;
  numLeaves: number;
  valueReader: ?ValueReader;
  chunks: Ref<any>[];
  length: number;
  getChildSequence: (idx: number) => Promise<?Sequence<T, K>>;
  getChildSequenceSync: (idx: number) => ?Sequence<T, K>;
  getEqualsFn: (other: Sequence<T, K>) => EqualsFn;
  range: (start: number, end: number) => Promise<T[]>;
  cumulativeNumberOfLeaves: (idx: number) => number;
  getKey: (idx: number) => OrderedKey<K>;
}
