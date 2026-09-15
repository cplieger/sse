/** Wire revision this client speaks; a hello outside [minWire, maxWire] is refused. */
export const WIRE = 1;

/** Largest encoded frame either side accepts, terminating blank line included. */
export const MAX_FRAME_BYTES = 1_048_576;

/** Largest offset a cursor may carry: 2^53 - 1, the JavaScript exact-integer bound. */
export const MAX_OFFSET = 9_007_199_254_740_991n;

/** MAX_OFFSET as the decimal string it takes on the wire. */
export const MAX_OFFSET_STRING = "9007199254740991";

/** Server keepalive interval the hello reports when the hub runs at its default. */
export const DEFAULT_KEEPALIVE_MS = 15_000;

/** The `retry:` value the server writes when it runs at its default. */
export const DEFAULT_RETRY_MS = 1_500;

/** Event name of the server keepalive frame when the hub runs at its default. */
export const KEEPALIVE_EVENT = "sse:keepalive";

/** Subjects one digest request may carry when the server runs at its default. */
export const DIGEST_MAX_SUBJECTS = 256;

/** Client-side timing knobs. Every field has a default in DEFAULT_TIMING. */
export interface TimingConfig {
  /** Deadline from fetch start to response headers. */
  readonly connectTimeoutMs: number;
  /** Deadline from response headers to a complete, valid hello. */
  readonly helloTimeoutMs: number;
  /** Bound on one revalidate run; the run's signal aborts at it. */
  readonly revalidateTimeoutMs: number;
  /** Deadline on one digest POST. */
  readonly digestTimeoutMs: number;
  /** Frames a hold may queue before the connection ends with hold_overflow. */
  readonly heldMaxFrames: number;
  /** Encoded bytes a hold may queue before the connection ends with hold_overflow. */
  readonly heldMaxBytes: number;
  /** Full-jitter backoff base. */
  readonly baseMs: number;
  /** Full-jitter backoff cap. */
  readonly capMs: number;
  /** Connected time after which the backoff attempt counter resets. */
  readonly stableMs: number;
  /** Hidden time after which the stream is closed deliberately. */
  readonly hiddenCloseAfterMs: number;
  /** Minimum spacing between two wake-driven revalidations. */
  readonly wakeThrottleMs: number;
  /** Worker-host port heartbeat interval. */
  readonly heartbeatMs: number;
  /** Keepalives received per alive acknowledgement POST. */
  readonly aliveEveryBeats: number;
  /** Missed keepalives before the silence watchdog fires. */
  readonly watchdogBeats: number;
  /** Lower bound on the watchdog interval whatever the keepalive is. */
  readonly watchdogFloorMs: number;
  /** Parser buffer cap; must be at least MAX_FRAME_BYTES. */
  readonly maxBufferBytes: number;
}

/**
 * The value every field a caller leaves unset takes. Each one is pinned, directly or by derivation,
 * to the repository's `timing.json` — the single source both halves of the protocol read — so a
 * client default and the server behaviour it assumes cannot drift apart.
 */
export const DEFAULT_TIMING: TimingConfig = {
  connectTimeoutMs: 15_000,
  helloTimeoutMs: 10_000,
  revalidateTimeoutMs: 30_000,
  digestTimeoutMs: 10_000,
  heldMaxFrames: 2_000,
  heldMaxBytes: 64 * MAX_FRAME_BYTES,
  baseMs: 500,
  capMs: 30_000,
  stableMs: 30_000,
  hiddenCloseAfterMs: 60_000,
  wakeThrottleMs: 1_000,
  heartbeatMs: 5_000,
  aliveEveryBeats: 1,
  watchdogBeats: 3,
  watchdogFloorMs: 15_000,
  maxBufferBytes: MAX_FRAME_BYTES,
};

/**
 * Silence watchdog derivation follows Yaffle/EventSource: max(3 beats, 15 s), 45 s at the
 * default keepalive. `keepaliveMs` is the interval the hello reported, so a server running a
 * shorter one shortens the watchdog until the floor takes over; the value is re-derived on
 * every re-arm rather than held.
 */
export function watchdogMs(keepaliveMs: number, cfg: TimingConfig = DEFAULT_TIMING): number {
  return Math.max(cfg.watchdogBeats * keepaliveMs, cfg.watchdogFloorMs);
}

/**
 * Fills a partial timing with the defaults. Throws RangeError when maxBufferBytes is
 * below MAX_FRAME_BYTES: the two caps are one wire rule, and a lower client cap would
 * make a legal ring entry abort every reconnect.
 */
export function resolveTiming(partial: Partial<TimingConfig> = {}): TimingConfig {
  const cfg: TimingConfig = { ...DEFAULT_TIMING, ...partial };
  if (cfg.maxBufferBytes < MAX_FRAME_BYTES) {
    throw new RangeError(
      `maxBufferBytes ${String(cfg.maxBufferBytes)} is below MAX_FRAME_BYTES ${String(MAX_FRAME_BYTES)}`,
    );
  }
  return cfg;
}
