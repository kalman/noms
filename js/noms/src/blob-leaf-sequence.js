// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type {ValueReader} from './value-store.js';
import {invariant} from './assert.js';
import LeafSequence from './leaf-sequence.js';
import {blobType} from './type.js';
import type Ref from './ref.js';
import type {EqualsFn} from './edit-distance.js';
import type {OrderedKey} from './meta-sequence.js';
import type {Sequence} from './sequence.js';

export default class BlobLeafSequence extends LeafSequence<number, any> {
  // Sequence implementation

  get chunks(): Ref<any>[] {
    return [];
  }

  getEqualsFn(other: Sequence<number, any>): EqualsFn {
    invariant(other instanceof BlobLeafSequence);
    return (idx: number, otherIdx: number) => this.items[idx] === other.items[otherIdx];
  }

  getKey(idx: number): OrderedKey<any> { // eslint-disable-line
    throw new Error('unsupported');
  }
}

export function newBlobLeafSequence(vr: ?ValueReader, items: Uint8Array): BlobLeafSequence {
  // $FlowIssue: The super class expects Array<T> but we sidestep that.
  return new BlobLeafSequence(vr, blobType, items);
}
