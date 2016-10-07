// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {AsyncIterator} from './async-iterator.js';
import type {AsyncIteratorResult} from './async-iterator.js';
import SequenceCursor from './sequence-cursor.js';
import type Value from './value.js';

export default class SequenceIterator<T, K: Value> extends AsyncIterator<T> {
  _curP: Promise<SequenceCursor<T, K>>;
  _closed: boolean;

  constructor(curP: Promise<SequenceCursor<T, K>>) {
    super();
    this._curP = curP;
    this._closed = false;
  }

  next(): Promise<AsyncIteratorResult<T>> {
    if (this._closed) {
      return Promise.resolve({done: true});
    }

    return this._curP.then(cur => cur.advance() ? {
      done: false,
      value: cur.getCurrent(),
    } : {
      done: true,
    });
  }

  return(): Promise<AsyncIteratorResult<T>> {
    this._closed = true;
    return Promise.resolve({done: true});
  }
}
