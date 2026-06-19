export type Status = 'pending' | 'done' | 'failed';

export interface StoredRecord {
  status: Status;
  result?: unknown;
  error?: string;
  ts?: number;
}

export interface Store {
  get(key: string): Promise<StoredRecord | null> | StoredRecord | null;
  set(key: string, value: StoredRecord): Promise<void> | void;
  delete(key: string): Promise<void> | void;
  /**
   * Optional atomic set-if-absent. Return true if this call inserted the value,
   * false if a live value already existed (e.g. Redis `SET key v NX`). When
   * present, it is used to make cross-process runs exactly-once.
   */
  add?(key: string, value: StoredRecord): Promise<boolean> | boolean;
}

export interface Options {
  /** Pluggable async store (default: in-memory, ttl/clock-aware). */
  store?: Store;
  /** Time-to-live for a record, in ms. 0 (default) keeps records forever. */
  ttlMs?: number;
  /** If false, a failed run is cached and replayed instead of retried. Default true. */
  retryFailed?: boolean;
  /** Injectable clock (default: Date.now). */
  now?: () => number;
}

export interface Idempotency {
  /**
   * Run `fn` at most once for `key`. Repeat calls on a completed key return a
   * clone of the stored result without running `fn`.
   */
  run<T>(key: string, fn: () => T | Promise<T>): Promise<T>;
  status(key: string): Promise<'none' | Status>;
  forget(key: string): Promise<void>;
}

export declare class MemoryStore implements Store {
  constructor(options?: { ttlMs?: number; now?: () => number });
  get(key: string): Promise<StoredRecord | null>;
  set(key: string, value: StoredRecord): Promise<void>;
  delete(key: string): Promise<void>;
  add(key: string, value: StoredRecord): Promise<boolean>;
}

export declare class IdempotencyConflict extends Error {
  readonly name: 'IdempotencyConflict';
  readonly code: 'IDEMPOTENCY_CONFLICT';
  readonly key: string;
  constructor(key: string);
}

export declare function createIdempotency(options?: Options): Idempotency;
