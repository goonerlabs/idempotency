'use strict';

/**
 * @goonerlabs/idempotency
 *
 * Run an operation at most once per key. Retries with the same key replay the
 * stored result instead of re-running it, and concurrent callers in the same
 * process share a single execution. Built for money-moving work where a retry,
 * a double-submit, or a race must never cause a second charge or payout.
 *
 * Guarantee:
 *   - Within one process: exactly-once, always (concurrent callers are deduped
 *     in-memory and share a single execution).
 *   - Across processes (e.g. a PM2 cluster sharing Redis): exactly-once ONLY if
 *     the store exposes an atomic `add(key, value)` (set-if-absent, like Redis
 *     `SET key v NX`). With such a store the loser of a race gets an
 *     `IdempotencyConflict` (retryable); without it, the non-atomic get→set
 *     fallback has a small TOCTOU window — pair it with an external lock for
 *     hard guarantees.
 *
 * Zero dependencies. State lives in a pluggable async store (in-memory default);
 * the clock is injectable for testing.
 */

class MemoryStore {
  constructor(options) {
    const o = options || {};
    this._m = new Map();
    this._ttlMs = o.ttlMs || 0;
    this._now = o.now || (() => Date.now());
  }
  _isExpired(e) {
    return this._ttlMs > 0 && e && e.ts != null && this._now() - e.ts > this._ttlMs;
  }
  async get(key) {
    const e = this._m.get(key);
    if (e === undefined) return null;
    if (this._isExpired(e)) {
      this._m.delete(key); // evict on read so memory stays bounded when ttlMs is set
      return null;
    }
    return e;
  }
  async set(key, value) {
    this._m.set(key, value);
  }
  async delete(key) {
    this._m.delete(key);
  }
  /**
   * Atomic set-if-absent. Returns true if this call inserted the value, false if
   * a live (non-expired) value was already present. Lets createIdempotency close
   * the cross-process check→set race; mirror this with Redis `SET key v NX`.
   */
  async add(key, value) {
    const e = this._m.get(key);
    if (e !== undefined && !this._isExpired(e)) return false;
    this._m.set(key, value);
    return true;
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
  const now = opts.now || (() => Date.now());
  const ttlMs = opts.ttlMs || 0; // 0 = keep records forever
  // The default store shares the guard's ttl + clock so eviction is consistent
  // and test clocks stay deterministic.
  const store = opts.store || new MemoryStore({ ttlMs, now });
  const retryFailed = opts.retryFailed !== false; // default: a failed op may be retried
  const inflight = new Map(); // key -> Promise (same-process concurrent dedupe)

  function expired(rec, t) {
    return ttlMs > 0 && rec.ts != null && t - rec.ts > ttlMs;
  }

  // Snapshot the cached value so a caller mutating what it gets back can never
  // corrupt the stored record (or a later replay).
  function clone(v) {
    if (v == null || typeof v !== 'object') return v;
    if (typeof structuredClone === 'function') return structuredClone(v);
    return JSON.parse(JSON.stringify(v));
  }

  // Decide what an existing record means for a fresh run() at time t.
  function decide(rec, t) {
    if (!rec || expired(rec, t)) return { go: true };
    if (rec.status === 'done') return { value: clone(rec.result) };
    if (rec.status === 'failed') {
      if (retryFailed) return { go: true };
      const e = new Error(rec.error || 'previous attempt failed');
      e.idempotentReplay = true;
      return { error: e };
    }
    return { conflict: true }; // pending
  }

  function settle(d, key) {
    if (d.value !== undefined || 'value' in d) return d.value;
    if (d.error) throw d.error;
    if (d.conflict) throw new IdempotencyConflict(key);
    return undefined;
  }

  async function _run(key, fn) {
    const t = now();

    const first = decide(await store.get(key), t);
    if (!first.go) return settle(first, key);

    // Claim the key for this run. Prefer an atomic set-if-absent when the store
    // offers one (closes the cross-process TOCTOU); otherwise fall back to set().
    const pendingRec = { status: 'pending', ts: t };
    if (typeof store.add === 'function') {
      const won = await store.add(key, pendingRec);
      if (!won) {
        const second = decide(await store.get(key), t);
        if (!second.go) return settle(second, key);
        // The record vanished/expired between add and re-read — force the claim.
        await store.set(key, pendingRec);
      }
    } else {
      await store.set(key, pendingRec);
    }

    try {
      const result = await fn();
      try {
        await store.set(key, { status: 'done', result: clone(result), ts: now() });
      } catch (persistErr) {
        // The side effect already happened. Do NOT release the key — releasing
        // would let the inevitable retry run fn() again (double-spend). Leave the
        // pending marker and flag the error so the catch below won't delete it.
        if (persistErr && typeof persistErr === 'object') persistErr.idempotencyCommitted = true;
        throw persistErr;
      }
      return result;
    } catch (err) {
      if (err && err.idempotencyCommitted) throw err;
      if (retryFailed) {
        await store.delete(key);
      } else {
        const msg = err && err.message != null ? err.message : String(err);
        await store.set(key, { status: 'failed', error: msg, ts: now() });
      }
      throw err;
    }
  }

  /**
   * Run `fn` exactly once for `key`. Returns `fn`'s result; on a repeat call
   * with a completed key, returns a clone of the stored result without running
   * `fn`.
   */
  function run(key, fn) {
    if (typeof key !== 'string' || key.length === 0) {
      throw new TypeError('idempotency key must be a non-empty string');
    }
    if (typeof fn !== 'function') {
      throw new TypeError('fn must be a function');
    }
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
