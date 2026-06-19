'use strict';

// Regression tests for the 0.1.1 hardening pass.
const { test } = require('node:test');
const assert = require('node:assert');
const { createIdempotency, MemoryStore, IdempotencyConflict } = require('../src/index.js');

test('a caller mutating the returned result cannot corrupt the replay', async () => {
  const idem = createIdempotency();
  const first = await idem.run('k', async () => ({ amount: 100, currency: 'NGN' }));
  first.amount = 999999; // hostile/careless mutation
  delete first.currency;
  const replay = await idem.run('k', async () => ({ amount: 1, currency: 'USD' }));
  assert.deepStrictEqual(replay, { amount: 100, currency: 'NGN' }); // pristine
});

test('a store-commit failure after fn succeeds does NOT re-run fn (no double-spend)', async () => {
  let calls = 0;
  let failDoneWrite = true;
  const base = new MemoryStore();
  const store = {
    get: (k) => base.get(k),
    delete: (k) => base.delete(k),
    add: (k, v) => base.add(k, v),
    set: async (k, v) => {
      if (v.status === 'done' && failDoneWrite) {
        failDoneWrite = false;
        throw new Error('redis blip on done-write');
      }
      return base.set(k, v);
    },
  };
  const idem = createIdempotency({ store });
  const op = async () => { calls++; return 'paid'; };

  const err = await idem.run('k', op).then(() => null, (e) => e);
  assert.ok(err, 'first run should surface the commit error');
  assert.strictEqual(err.idempotencyCommitted, true);
  assert.strictEqual(calls, 1);

  // The retry must NOT execute the side effect again — key is still pending.
  await assert.rejects(() => idem.run('k', op), { code: 'IDEMPOTENCY_CONFLICT' });
  assert.strictEqual(calls, 1, 'fn must not run a second time after a committed side effect');
});

test('cross-process: two guards over one atomic store run fn exactly once', async () => {
  let calls = 0;
  const store = new MemoryStore(); // has atomic add()
  const mk = () => createIdempotency({ store }); // separate inflight maps == separate processes
  const op = async () => { calls++; await new Promise((r) => setImmediate(r)); return calls; };

  const results = await Promise.allSettled([mk().run('k', op), mk().run('k', op)]);
  assert.strictEqual(calls, 1, 'fn must execute exactly once across the two guards');
  const fulfilled = results.filter((r) => r.status === 'fulfilled');
  const conflicts = results.filter(
    (r) => r.status === 'rejected' && r.reason instanceof IdempotencyConflict
  );
  assert.strictEqual(fulfilled.length, 1);
  assert.strictEqual(conflicts.length, 1, 'the loser gets a retryable conflict, not a double-run');
});

test('rejects an empty / non-string key and a non-function fn', async () => {
  const idem = createIdempotency();
  assert.throws(() => idem.run('', async () => 1), TypeError);
  assert.throws(() => idem.run(undefined, async () => 1), TypeError);
  assert.throws(() => idem.run('k', 'not-a-fn'), TypeError);
});

test('retryFailed:false preserves a non-Error thrown value in the replayed message', async () => {
  const idem = createIdempotency({ retryFailed: false });
  await assert.rejects(() => idem.run('k', async () => { throw 'plain string boom'; }));
  await assert.rejects(() => idem.run('k', async () => 'unused'), /plain string boom/);
});

test('default MemoryStore evicts expired records (memory stays bounded with ttl)', async () => {
  let clock = 0;
  const store = new MemoryStore({ ttlMs: 100, now: () => clock });
  const idem = createIdempotency({ ttlMs: 100, now: () => clock, store });
  for (let i = 0; i < 50; i++) await idem.run('key' + i, async () => i);
  assert.strictEqual(store._m.size, 50);
  clock = 1000; // everything expired
  // touching any key triggers eviction-on-read; sweep them all
  for (let i = 0; i < 50; i++) await store.get('key' + i);
  assert.strictEqual(store._m.size, 0, 'expired records evicted on read');
});
