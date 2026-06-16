'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { createIdempotency, MemoryStore } = require('../src/index.js');

test('runs once and replays the stored result on repeat', async () => {
  let calls = 0;
  const idem = createIdempotency();
  const op = async () => { calls++; return { receipt: 'abc' }; };
  assert.deepStrictEqual(await idem.run('k', op), { receipt: 'abc' });
  assert.deepStrictEqual(await idem.run('k', op), { receipt: 'abc' });
  assert.strictEqual(calls, 1);
});

test('dedupes concurrent callers with the same key', async () => {
  let calls = 0;
  const idem = createIdempotency();
  const op = async () => {
    calls++;
    await new Promise((r) => setImmediate(r));
    return calls;
  };
  const [a, b] = await Promise.all([idem.run('k', op), idem.run('k', op)]);
  assert.strictEqual(calls, 1);
  assert.strictEqual(a, 1);
  assert.strictEqual(b, 1);
});

test('different keys run independently', async () => {
  let calls = 0;
  const idem = createIdempotency();
  await idem.run('a', async () => { calls++; });
  await idem.run('b', async () => { calls++; });
  assert.strictEqual(calls, 2);
});

test('retries after a failure by default', async () => {
  let calls = 0;
  const idem = createIdempotency();
  const op = async () => { calls++; if (calls === 1) throw new Error('boom'); return 'ok'; };
  await assert.rejects(() => idem.run('k', op), /boom/);
  assert.strictEqual(await idem.run('k', op), 'ok');
  assert.strictEqual(calls, 2);
});

test('retryFailed:false records the failure and refuses to re-run', async () => {
  let calls = 0;
  const idem = createIdempotency({ retryFailed: false });
  const op = async () => { calls++; throw new Error('boom'); };
  await assert.rejects(() => idem.run('k', op), /boom/);
  await assert.rejects(() => idem.run('k', op), /boom/);
  assert.strictEqual(calls, 1);
  assert.strictEqual(await idem.status('k'), 'failed');
});

test('a pending record from another process raises a conflict', async () => {
  const store = new MemoryStore();
  await store.set('k', { status: 'pending', ts: 0 });
  const idem = createIdempotency({ store, now: () => 1 });
  await assert.rejects(() => idem.run('k', async () => 'x'), { code: 'IDEMPOTENCY_CONFLICT' });
});

test('ttl lets a key be reused after it expires', async () => {
  let clock = 0;
  let calls = 0;
  const idem = createIdempotency({ ttlMs: 100, now: () => clock });
  await idem.run('k', async () => { calls++; return 1; });
  clock = 50;
  await idem.run('k', async () => { calls++; return 2; }); // within ttl -> replay
  assert.strictEqual(calls, 1);
  clock = 201;
  await idem.run('k', async () => { calls++; return 3; }); // expired -> re-run
  assert.strictEqual(calls, 2);
});

test('forget clears a key', async () => {
  let calls = 0;
  const idem = createIdempotency();
  await idem.run('k', async () => { calls++; });
  await idem.forget('k');
  await idem.run('k', async () => { calls++; });
  assert.strictEqual(calls, 2);
});
