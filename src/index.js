'use strict';

/**
 * @goonerlabs/idempotency
 *
 * Run an operation at most once per key. Retries with the same key replay the
 * stored result instead of re-running it, and concurrent callers in the same
 * process share a single execution. Built for money-moving work where a retry,
 * a double-submit, or a race must never cause a second charge or payout.
 *
 * Zero dependencies. State lives in a pluggable async store (in-memory default);
 * the clock is injectable for testing.
 */

class MemoryStore {
  constructor() {
    this._m = new Map();
  }
  async get(key) {
    return this._m.has(key) ? this._m.get(key) : null;
  }
  async set(key, value) {
    this._m.set(key, value);
  }
  async delete(key) {
    this._m.delete(key);
  }
}

class IdempotencyConflict extends Error {
  constructor(key) {
    super(`operation for key "${key}" is already in progress`);
    this.name = 'IdempotencyConflict';
    this.code = 'IDEMPOTENCY_CONFLICT';
    this.key = key;
  }
}

function createIdempotency(options) {
  const opts = options || {};
  const store = opts.store || new MemoryStore();
  const now = opts.now || (() => Date.now());
  const ttlMs = opts.ttlMs || 0; // 0 = keep records forever
  const retryFailed = opts.retryFailed !== false; // default: a failed op may be retried
  const inflight = new Map(); // key -> Promise (same-process concurrent dedupe)

  function expired(rec, t) {
    return ttlMs > 0 && rec.ts != null && t - rec.ts > ttlMs;
  }

  async function _run(key, fn) {
    const t = now();
    const existing = await store.get(key);
    if (existing && !expired(existing, t)) {
      if (existing.status === 'done') return existing.result;
      if (existing.status === 'failed' && !retryFailed) {
        const e = new Error(existing.error || 'previous attempt failed');
        e.idempotentReplay = true;
        throw e;
      }
      if (existing.status === 'pending') {
        // Held by another process (or a crashed run — clears via ttl/forget).
        throw new IdempotencyConflict(key);
      }
      // failed + retryFailed: fall through and try again.
    }

    await store.set(key, { status: 'pending', ts: t });
    try {
      const result = await fn();
      await store.set(key, { status: 'done', result, ts: now() });
      return result;
    } catch (err) {
      if (retryFailed) {
        await store.delete(key);
      } else {
        await store.set(key, { status: 'failed', error: err && err.message, ts: now() });
      }
      throw err;
    }
  }

  /**
   * Run `fn` exactly once for `key`. Returns `fn`'s result; on a repeat call
   * with a completed key, returns the stored result without running `fn`.
   */
  function run(key, fn) {
    const current = inflight.get(key);
    if (current) return current;
    const p = _run(key, fn).finally(() => {
      if (inflight.get(key) === p) inflight.delete(key);
    });
    inflight.set(key, p);
    return p;
  }

  /** 'none' | 'pending' | 'done' | 'failed' for a key. */
  async function status(key) {
    const rec = await store.get(key);
    if (!rec || expired(rec, now())) return 'none';
    return rec.status;
  }

  /** Drop a key so it can run fresh again. */
  async function forget(key) {
    inflight.delete(key);
    await store.delete(key);
  }

  return { run, status, forget };
}

module.exports = { createIdempotency, MemoryStore, IdempotencyConflict };
