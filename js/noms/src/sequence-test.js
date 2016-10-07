// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import SequenceCursor, {newCursorAtIndex} from './sequence-cursor.js';
import {notNull} from './assert.js';
import type {Type} from './type.js';
import {makeListType, numberType} from './type.js';
import LeafSequence from './leaf-sequence.js';
import type {ValueReader} from './value-store.js';
import type {Sequence} from './sequence.js';
import type {EqualsFn} from './edit-distance.js';
import type Ref from './ref.js';

class NumberSequence {
  _items: (number[])[];

  constructor(items: (number[])[]) {
    this._items = items;
  }

  get type(): Type<any> {
    return makeListType(numberType);
  }

  get items(): (number[])[] {
    return this._items;
  }

  get isMeta(): boolean {
    return false;
  }

  get numLeaves(): number {
    throw new Error('not implemented');
  }

  get valueReader(): ?ValueReader {
    return null;
  }

  get chunks(): Ref<any>[] {
    throw new Error('not implemented');
  }

  get length(): number {
    return this._items.length;
  }

  getChildSequence(idx: number): Promise<?Sequence<number, number>> {
    return Promise.resolve(this.getChildSequenceSync(idx));
  }

  getChildSequenceSync(idx: number): ?Sequence<number, number> {
    return new LeafSequence(null, this.type, this._items[idx]);
  }

  getEqualsFn(other: Sequence<any, any>): EqualsFn { // eslint-disable-line
    throw new Error('not implemented');
  }

  range(start: number, end: number): Promise<number[]> { // eslint-disable-line
    throw new Error('not implemented');
  }

  cumulativeNumberOfLeaves(idx: number): number { // eslint-disable-line
    throw new Error('not implemented');
  }

  getKey(idx: number): any { // eslint-disable-line
    throw new Error('not implemented');
  }
}

suite('SequenceCursor', () => {
  function testCursor(data: (number[])[]): Promise<SequenceCursor<number, any>> {
    const seq = new NumberSequence(data);
    return newCursorAtIndex(seq, 0);
  }

  function expect(c: SequenceCursor<number, any>, expectIdx: number,
                  expectParentIdx: number, expectValid: boolean, expectVal: ?number) {
    assert.strictEqual(expectIdx, c.indexInChunk, 'indexInChunk');
    const parent = notNull(c.parent);
    assert.strictEqual(expectParentIdx, parent.indexInChunk, 'parentIdx');
    assert.strictEqual(expectValid, c.valid, 'valid');
    let actualVal = null;
    if (c.valid) {
      actualVal = c.getCurrent();
    }
    assert.strictEqual(expectVal, actualVal, 'value');
  }

  test('retreating past the start', async () => {
    const cur = await testCursor([[100, 101],[102]]);
    expect(cur, 0, 0, true, 100);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
  });

  test('retreating past the start, then advanding past the end', async () => {
    const cur = await testCursor([[100, 101],[102]]);
    assert.isFalse(await cur.retreat());
    assert.isTrue(await cur.advance());
    expect(cur, 0, 0, true, 100);
    assert.isTrue(await cur.advance());
    expect(cur, 1, 0, true, 101);
    assert.isTrue(await cur.advance());
    expect(cur, 0, 1, true, 102);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
  });

  test('advancing past the end', async () => {
    const cur = await testCursor([[100, 101],[102]]);
    assert.isTrue(await cur.advance());
    expect(cur, 1, 0, true, 101);
    assert.isTrue(await cur.retreat());
    expect(cur, 0, 0, true, 100);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
  });

  test('advancing past the end, then retreating past the start.', async () => {
    const cur = await testCursor([[100, 101],[102]]);
    assert.isTrue(await cur.advance());
    assert.isTrue(await cur.advance());
    expect(cur, 0, 1, true, 102);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
    assert.isTrue(await cur.retreat());
    expect(cur, 0, 1, true, 102);
    assert.isTrue(await cur.retreat());
    expect(cur, 1, 0, true, 101);
    assert.isTrue(await cur.retreat());
    expect(cur, 0, 0, true, 100);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
  });
});
