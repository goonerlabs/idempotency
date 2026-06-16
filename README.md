# @goonerlabs/idempotency

Run an operation **exactly once per key**. Retries with the same key replay the stored result instead of re-running it, and concurrent callers in the same process share a single execution.

Built for money-moving work — payouts, mints, redemptions, charges — where a retry, a double-submit, or a race must never produce a second transfer.

**Zero dependencies.** State lives in a pluggable async store (in-memory default); the clock is injectable for testing.

```
npm install @goonerlabs/idempotency
```

## Quick start

```js
const { createIdempotency } = require('@goonerlabs/idempotency');

const idem = createIdempotency();

// `key` is anything that uniquely identifies the intent — an order id,
// a redemption id, an Idempotency-Key header, `${wallet}:${nonce}`, etc.
async function redeem(redemptionId, wallet) {
  return idem.run(redemptionId, async () => {
    const tx = await chain.transfer(wallet, item);   // runs at most once
    await db.markRedeemed(redemptionId, tx);
    return tx;
  });
}

await redeem('rdm_123', wallet); // executes
await redeem('rdm_123', wallet); // returns the SAME tx, no second transfer
```

## How it behaves

- **Completed** key → the stored result is replayed; `fn` never runs again.
- **Concurrent** calls (same process, same key) → share one execution.
- **Pending elsewhere** (another process holds the key) → throws `IdempotencyConflict` (`code: 'IDEMPOTENCY_CONFLICT'`); the caller should back off and retry.
- **Failure** → by default the key is cleared so a later attempt can retry. Set `retryFailed: false` to "poison" a failed key and replay the error instead.

## API

### `createIdempotency(options)`
| Option | Meaning |
|---|---|
| `store` | Async `{ get, set, delete }` (default: in-memory) |
| `ttlMs` | Expire stored records after this long (default: keep forever) |
| `retryFailed` | Allow retry after a failed op (default `true`) |
| `now` | Clock function (default `Date.now`) |

### `idem.run(key, fn) → Promise<result>`
Runs `fn` once for `key`; replays the stored result on repeats.

### `idem.status(key) → 'none' | 'pending' | 'done' | 'failed'`

### `idem.forget(key)`
Drops a key so it can run fresh again.

## Using Redis (or any store)

```js
const idem = createIdempotency({
  ttlMs: 24 * 60 * 60 * 1000,
  store: {
    async get(k) { const v = await redis.get(k); return v ? JSON.parse(v) : null; },
    async set(k, v) { await redis.set(k, JSON.stringify(v)); },
    async delete(k) { await redis.del(k); },
  },
});
```

> Cross-process exactly-once relies on the `pending` marker. A process that crashes mid-run leaves a `pending` key; it clears via `ttlMs` or `forget(key)`. For hard guarantees under contention, pair the store with a lock.

## Testing

```
npm test   # node --test
```

## License

MIT © Owolabi Adeyemi ([goonerlabs](https://github.com/goonerlabs))
