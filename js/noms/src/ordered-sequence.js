// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {OrderedKey} from './meta-sequence.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import {notNull} from './assert.js';
import type {Sequence} from './sequence.js';
import SequenceCursor from './sequence-cursor.js';

/**
 * Returns:
 *  - null, if sequence is empty.
 *  - null, if all values in sequence are < key.
 *  - cursor positioned at
 *     - first value, if |val| is null
 *     - first value >= key of |val|
 */
export function newCursorAtValue<T, K: Value>(
    seq: Sequence<T, K>, val: ?K, forInsertion: boolean = false, last: boolean = false)
    : Promise<SequenceCursor<T, K>> {
  let key;
  if (val !== null && val !== undefined) {
    key = new OrderedKey(val);
  }
  return newCursorAtKey(seq, key, forInsertion, last);
}

/**
 * Returns:
 *  - null, if sequence is empty.
 *  - null, if all values in sequence are < key.
 *  - cursor positioned at
 *     - first value, if |key| is null
 *     - first value >= |key|
 */
export async function newCursorAtKey<T, K: Value>(
    seq: Sequence<T, K>, key: ?OrderedKey<K>, forInsertion: boolean = false, last: boolean = false)
    : Promise<SequenceCursor<T, K>> {
  let cursor: ?SequenceCursor<T, K> = null;

  for (let childSeq = seq; childSeq; childSeq = await cursor.getChildSequence()) {
    cursor = new SequenceCursor(cursor, childSeq, last ? -1 : 0);
    if (key !== null && key !== undefined) {
      const lastPositionIfNotfound = forInsertion && childSeq.isMeta;
      if (!cursor.seekTo(key, lastPositionIfNotfound)) {
        return cursor; // invalid
      }
    }
  }

  return notNull(cursor);
}
