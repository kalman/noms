// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type Ref from './ref.js';
import type {Sequence} from './sequence.js';
import type {Type} from './type.js';
import {ValueBase, init as initValueBase} from './value.js';
import type Value from './value.js';
import {invariant} from './assert.js';

export default class Collection<T, K: Value> extends ValueBase {
  sequence: Sequence<T, K>;

  constructor(sequence: Sequence<T, K>) {
    super();
    this.sequence = sequence;
  }

  get type(): Type<any> {
    return this.sequence.type;
  }

  isEmpty(): boolean {
    return !this.sequence.isMeta && this.sequence.items.length === 0;
  }

  get chunks(): Ref<any>[] {
    return this.sequence.chunks;
  }

  /**
   * Creates a new Collection of the type fromSequence is called on.
   */
  static fromSequence<T, K>(s: Sequence<T, K>): any {
    // NOTE: returning "any" so that callers don't need to put invariants everywhere.
    const col = Object.create(this.prototype);
    invariant(col instanceof this);
    initValueBase(col);
    col.sequence = s;
    return col;
  }
}
