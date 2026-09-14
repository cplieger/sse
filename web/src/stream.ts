import type { HeldDiscardCause, LifecycleEvent, RevalidateCause } from "./lifecycle.js";
import { type OnlineManager, createOnlineManager } from "./online.js";
import {
  type ClientEvent,
  type ClientState,
  type Effect,
  type StreamEnd,
  type Timer,
  initialState,
  reduce,
} from "./reducer.js";
import { DEFAULT_RETRY_MS, type TimingConfig, WIRE, resolveTiming } from "./timing.js";
import { type Connection, type TransportFrame, connect } from "./transport.js";
import { type VersionMap, bindListener, staleListener } from "./versions.js";
import {
  type VisibilityInput,
  type VisibilityManager,
  adaptVisibility,
  createVisibilityManager,
} from "./visibility.js";
import { type Cursor, type Hello, compareOffset } from "./wire.js";

export interface StreamOptions {
  readonly url: string;
  /** Defaults to globalThis.fetch; the place to merge `credentials: "include"` or a bearer header. */
  readonly fetch?: typeof fetch;
  /** Read at every request, so a host may fill `SSE-Client` after construction. */
  readonly headers?: Record<string, string>;
  readonly timing?: Partial<TimingConfig>;
  readonly minWire?: number;
  readonly maxWire?: number;
  readonly visibility?: VisibilityManager;
  readonly online?: OnlineManager;
  /** Keepalive acknowledgement: POST `url` after every `everyBeats`-th keepalive (default 1). */
  readonly alive?: { readonly url: string; readonly everyBeats?: number };
  /** Bound to hello.epoch on every hello; the application fills it on commit. */
  readonly versions: VersionMap;
  /** Application frames only; a throw advances the cursor and schedules revalidate("hello"). */
  readonly onFrame: (frame: Frame) => void;
  readonly onLifecycle?: (ev: LifecycleEvent) => void;
  /** The application's reconciliation, run single-flight; it must pass ctx.signal to every fetch. */
  readonly revalidate: (ctx: RevalidateContext) => Promise<void>;
}

export interface Frame {
  readonly type: string;
  readonly data: string;
  readonly id: Cursor | null;
}

export interface RevalidateContext {
  readonly cause: RevalidateCause;
  /** The version map's epoch when the run started. */
  readonly epoch: string | null;
  readonly generation: number;
  /** The map was just cleared by bind(); the application must run its full body without a digest. */
  readonly full: boolean;
  /** Aborted by stop() and at revalidateTimeoutMs, never by reconnect(). */
  readonly signal: AbortSignal;
}

export interface Stream {
  start(): void;
  /** For leaving the page: aborts the in-flight revalidate's signal. */
  stop(): void;
  /** Aborts the attempt and connects again in place; ignored in hidden_closed, offline and stopped. */
  reconnect(): void;
  state(): ClientState;
  cursor(): Cursor | null;
  /** Drops the cursor; the next connect presents none and earns a fresh hello. */
  resetCursor(): void;
}

/** Host-only controls over a stream; reached through streamInternals, not the public interface. */
export interface StreamInternals {
  /** Schedules a revalidate under the single-flight rule, as an effect would. */
  revalidate(cause: RevalidateCause, full: boolean): void;
  /** Sets the queued slot's sticky full without starting a run. */
  queueFull(): void;
}

type HeldEntry =
  | {
      readonly kind: "frame";
      readonly generation: number;
      readonly frame: Frame;
      readonly bytes: number;
    }
  | {
      readonly kind: "adopt";
      readonly generation: number;
      readonly epoch: string;
      readonly head: string;
    };

interface Slot {
  readonly cause: RevalidateCause;
  readonly full: boolean;
}

interface Run {
  readonly controller: AbortController;
  readonly cause: RevalidateCause;
  readonly startedAt: number;
  timer: ReturnType<typeof setTimeout> | null;
}

type RuntimeEnd = Extract<StreamEnd, HeldDiscardCause>;

const ALIVE_TIMEOUT_MS = 5_000;

const internals = new WeakMap<Stream, StreamInternals>();

function transportGeneration(event: ClientEvent): number | null {
  switch (event.type) {
    case "connected":
    case "connect_failed":
    case "byte":
    case "stream_ended":
      return event.generation;
    default:
      return null;
  }
}

function describe(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

/** Creates the client runtime. Throws RangeError when `timing.maxBufferBytes` is below MAX_FRAME_BYTES. */
export function createStream(opts: StreamOptions): Stream {
  const cfg = resolveTiming(opts.timing);
  const doFetch: typeof fetch = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
  const minWire = opts.minWire ?? WIRE;
  const maxWire = opts.maxWire ?? WIRE;
  const everyBeats = Math.max(1, opts.alive?.everyBeats ?? cfg.aliveEveryBeats);

  let state: ClientState = initialState();
  let cursor: Cursor | null = null;
  const timers = new Map<Timer, ReturnType<typeof setTimeout>>();

  let connection: { readonly generation: number; readonly conn: Connection } | null = null;
  let hello: Hello | null = null;
  let adopted = false;
  let retryMs = DEFAULT_RETRY_MS;
  let beats = 0;
  let aliveScope: AbortController | null = null;

  let held: HeldEntry[] = [];
  let heldFrames = 0;
  let heldBytes = 0;

  let inflight: Run | null = null;
  let queued: Slot | null = null;
  let unverified = false;
  let binding = false;
  let knownEpoch = opts.versions.epoch();
  const helloRun = { full: false, revalidated: false };
  const reducerRevalidated = (): boolean => helloRun.revalidated;
  let wasHiddenClosed = false;

  let defaultVisibility: VisibilityManager | null = null;
  let defaultOnline: OnlineManager | null = null;
  let unsubscribes: (() => void)[] = [];

  const now = (): number => Date.now();
  const rand = (): number => Math.random();

  function emit(event: LifecycleEvent): void {
    opts.onLifecycle?.(event);
  }

  bindListener(opts.versions, (epoch, _dropped) => {
    const previous = knownEpoch;
    knownEpoch = epoch;
    if (binding || inflight === null || !queued?.full) {
      return;
    }
    if (epoch === previous) {
      queued = { cause: queued.cause, full: false };
    }
  });
  staleListener(opts.versions, (subject, epoch) => {
    emit({ kind: "stale_stamp", subject, epoch });
  });

  function arm(timer: Timer, ms: number): void {
    disarm(timer);
    timers.set(
      timer,
      setTimeout(() => {
        timers.delete(timer);
        fire(timer);
      }, ms),
    );
  }

  function disarm(timer: Timer): void {
    const handle = timers.get(timer);
    if (handle !== undefined) {
      clearTimeout(handle);
      timers.delete(timer);
    }
  }

  function fire(timer: Timer): void {
    switch (timer) {
      case "watchdog":
        dispatch({ type: "watchdog_fired", now: now(), rand: rand(), retryMs });
        return;
      case "hidden":
        dispatch({ type: "hidden_timer_fired", now: now() });
        if (state.kind === "hidden_closed") {
          wasHiddenClosed = true;
          emit({ kind: "hidden_closed", arm: "timer" });
        }
        return;
      case "backoff":
        dispatch({ type: "backoff_elapsed", now: now() });
        return;
      case "stable":
        dispatch({ type: "stable_elapsed", now: now() });
        return;
    }
  }

  function execute(effect: Effect): void {
    switch (effect.kind) {
      case "connect":
        startConnection(effect.generation);
        return;
      case "abort":
        abortConnection(effect.generation, effect.reason);
        return;
      case "arm":
        arm(effect.timer, effect.ms);
        return;
      case "disarm":
        disarm(effect.timer);
        return;
      case "revalidate":
        if (effect.cause === "hello") {
          helloRun.revalidated = true;
          schedule("hello", helloRun.full);
        } else {
          schedule(effect.cause, false);
        }
        return;
      case "emit":
        emit(effect.event);
        return;
    }
  }

  function dispatch(event: ClientEvent): void {
    const eventGeneration = transportGeneration(event);
    if (eventGeneration !== null && eventGeneration !== state.generation) {
      emit({ kind: "stale_event", type: event.type, generation: eventGeneration });
      return;
    }
    const before = state;
    const reduction = reduce(state, event, cfg);
    state = reduction.state;
    for (const effect of reduction.effects) {
      execute(effect);
    }
    if (before.kind !== state.kind) {
      emit({ kind: "state", from: before.kind, to: state.kind, generation: state.generation });
    }
  }

  function startConnection(generation: number): void {
    hello = null;
    adopted = false;
    retryMs = DEFAULT_RETRY_MS;
    beats = 0;
    aliveScope?.abort();
    aliveScope = new AbortController();
    const conn = connect({
      url: opts.url,
      fetch: doFetch,
      headers: { ...opts.headers },
      cursor,
      minWire,
      maxWire,
      timing: cfg,
      generation,
      callbacks: {
        onByte(g) {
          dispatch({ type: "byte", generation: g, now: now() });
        },
        onConnected: handleConnected,
        onConnectFailed(g, reason) {
          if (g === state.generation) {
            emit({ kind: "connect_failed", reason });
          }
          dispatch({ type: "connect_failed", generation: g, now: now(), reason, rand: rand() });
        },
        onFrame: handleFrame,
        onKeepalive: handleKeepalive,
        onUnknownFrame(g, type) {
          if (g === state.generation) {
            emit({ kind: "unknown_frame", type });
          }
        },
        onBadCursor(g, id) {
          if (g === state.generation) {
            emit({ kind: "bad_cursor", id });
          }
        },
        onRetry(g, ms) {
          if (g === state.generation) {
            retryMs = ms;
          }
        },
        onEnd: handleEnd,
      },
    });
    connection = { generation, conn };
  }

  function dropConnection(generation: number, reason: string): void {
    if (connection !== null && connection.generation === generation) {
      connection.conn.abort(reason);
      connection = null;
    }
    aliveScope?.abort();
    aliveScope = null;
  }

  function abortConnection(generation: number, reason: string): void {
    dropConnection(generation, reason);
    discardHeld("abort");
  }

  function handleConnected(g: number, h: Hello): void {
    if (g !== state.generation) {
      emit({ kind: "stale_event", type: "connected", generation: g });
      return;
    }
    hello = h;
    adopted = !h.resumed;
    binding = true;
    const dropped = opts.versions.bind(h.epoch);
    binding = false;
    helloRun.full = dropped > 0;
    helloRun.revalidated = false;
    if (!h.resumed) {
      cursor = { epoch: h.epoch, offset: h.head };
    }
    dispatch({ type: "connected", generation: g, now: now(), hello: h });
    if (state.kind !== "open") {
      return;
    }
    if (wasHiddenClosed) {
      wasHiddenClosed = false;
      emit({ kind: "reopened" });
    }
    if (reducerRevalidated()) {
      return;
    }
    if (unverified || helloRun.full) {
      schedule("hello", helloRun.full);
    } else if (queued !== null && inflight === null) {
      const slot = queued;
      queued = null;
      run(slot);
    }
  }

  function handleEnd(g: number, reason: StreamEnd): void {
    if (g !== state.generation) {
      emit({ kind: "stale_event", type: "stream_ended", generation: g });
      return;
    }
    if (reason === "reset:slow" || reason === "reset:shutdown") {
      emit({ kind: "reset", reason });
    }
    dropConnection(g, reason);
    discardHeld("stream_ended");
    dispatch({ type: "stream_ended", generation: g, now: now(), reason, rand: rand(), retryMs });
  }

  function endConnection(reason: RuntimeEnd): void {
    if (state.kind !== "open") {
      return;
    }
    const g = state.generation;
    dropConnection(g, reason);
    discardHeld(reason);
    dispatch({ type: "stream_ended", generation: g, now: now(), reason, rand: rand(), retryMs });
  }

  function isReplay(frame: TransportFrame, h: Hello): boolean {
    return (
      frame.id !== null &&
      h.resumed &&
      frame.id.epoch === h.epoch &&
      compareOffset(frame.id.offset, h.head) <= 0
    );
  }

  function handleFrame(g: number, tf: TransportFrame): void {
    if (g !== state.generation || hello === null) {
      emit({ kind: "stale_event", type: "frame", generation: g });
      return;
    }
    if (!adopted && !isReplay(tf, hello)) {
      observeNonReplay(hello);
    }
    const frame: Frame = { type: tf.type, data: tf.data, id: tf.id };
    if (inflight !== null) {
      hold(frame, tf.bytes);
    } else {
      deliver(frame);
    }
  }

  function observeNonReplay(h: Hello): void {
    adopted = true;
    if (inflight !== null) {
      held.push({ kind: "adopt", generation: state.generation, epoch: h.epoch, head: h.head });
    } else {
      adopt(h.epoch, h.head);
    }
  }

  function adopt(epoch: string, head: string): void {
    if (cursor?.epoch !== epoch || compareOffset(head, cursor.offset) > 0) {
      cursor = { epoch, offset: head };
    }
  }

  function advance(id: Cursor): void {
    if (cursor?.epoch !== id.epoch || compareOffset(id.offset, cursor.offset) > 0) {
      cursor = id;
    }
  }

  function hold(frame: Frame, bytes: number): void {
    held.push({ kind: "frame", generation: state.generation, frame, bytes });
    heldFrames++;
    heldBytes += bytes;
    if (heldFrames > cfg.heldMaxFrames || heldBytes > cfg.heldMaxBytes) {
      endConnection("hold_overflow");
    }
  }

  function deliver(frame: Frame): void {
    let threw = false;
    let error: unknown;
    try {
      opts.onFrame(frame);
    } catch (e: unknown) {
      threw = true;
      error = e;
    }
    if (frame.id !== null) {
      advance(frame.id);
    }
    if (threw) {
      emit({ kind: "frame_rejected", type: frame.type, error });
      schedule("hello", false);
    }
  }

  function discardHeld(cause: HeldDiscardCause): void {
    if (held.length === 0) {
      return;
    }
    const length = held.length;
    held = [];
    heldFrames = 0;
    heldBytes = 0;
    emit({ kind: "held_discarded", cause, length });
  }

  function drain(): void {
    if (held.length === 0) {
      return;
    }
    const entries = held;
    held = [];
    heldFrames = 0;
    heldBytes = 0;
    let dropped = 0;
    for (const entry of entries) {
      if (entry.generation !== state.generation) {
        dropped++;
        continue;
      }
      if (entry.kind === "frame") {
        deliver(entry.frame);
      } else {
        adopt(entry.epoch, entry.head);
      }
    }
    emit({ kind: "drain", length: entries.length, dropped });
  }

  function handleKeepalive(g: number): void {
    if (g !== state.generation || hello === null) {
      return;
    }
    if (!adopted) {
      observeNonReplay(hello);
    }
    if (
      state.kind === "open" &&
      state.hiddenAt !== null &&
      now() - state.hiddenAt >= cfg.hiddenCloseAfterMs
    ) {
      dispatch({ type: "hidden_timer_fired", now: now() });
      if (stream.state().kind === "hidden_closed") {
        wasHiddenClosed = true;
        emit({ kind: "hidden_closed", arm: "read" });
      }
      return;
    }
    if (opts.alive === undefined) {
      return;
    }
    beats++;
    if (beats % everyBeats === 0) {
      acknowledge(opts.alive.url);
    }
  }

  function acknowledge(url: string): void {
    const scope = aliveScope;
    if (scope === null) {
      return;
    }
    const controller = new AbortController();
    const onScopeAbort = (): void => {
      controller.abort();
    };
    scope.signal.addEventListener("abort", onScopeAbort, { once: true });
    const timer = setTimeout(() => {
      controller.abort(new DOMException("alive acknowledgement timed out", "TimeoutError"));
    }, ALIVE_TIMEOUT_MS);
    void doFetch(url, {
      method: "POST",
      headers: { ...opts.headers },
      credentials: "same-origin",
      cache: "no-store",
      signal: controller.signal,
    })
      .then(
        (response) => {
          emit({ kind: "alive_ack", ok: response.ok, status: response.status });
        },
        () => {
          if (!scope.signal.aborted) {
            emit({ kind: "alive_ack", ok: false, status: null });
          }
        },
      )
      .finally(() => {
        clearTimeout(timer);
        scope.signal.removeEventListener("abort", onScopeAbort);
      });
  }

  function schedule(cause: RevalidateCause, full: boolean): void {
    if (inflight !== null) {
      const coalesces =
        cause !== "hello" &&
        inflight.cause !== "hello" &&
        now() - inflight.startedAt < cfg.wakeThrottleMs;
      if (coalesces && !full) {
        return;
      }
      queued = { cause, full: (queued?.full ?? false) || full };
      return;
    }
    const slot: Slot = { cause, full: (queued?.full ?? false) || full };
    queued = null;
    run(slot);
  }

  function run(slot: Slot): void {
    const controller = new AbortController();
    const ctx: RevalidateContext = {
      cause: slot.cause,
      epoch: opts.versions.epoch(),
      generation: state.generation,
      full: slot.full,
      signal: controller.signal,
    };
    const current: Run = { controller, cause: slot.cause, startedAt: now(), timer: null };
    current.timer = setTimeout(() => {
      timeout(current);
    }, cfg.revalidateTimeoutMs);
    inflight = current;
    emit({ kind: "revalidate", cause: slot.cause, full: slot.full });
    let promise: Promise<void>;
    try {
      promise = Promise.resolve(opts.revalidate(ctx));
    } catch (e: unknown) {
      promise = Promise.reject(e instanceof Error ? e : new Error(String(e)));
    }
    promise.then(
      () => {
        settle(current, false, undefined);
      },
      (e: unknown) => {
        settle(current, true, e);
      },
    );
  }

  function settle(current: Run, rejected: boolean, error: unknown): void {
    if (inflight !== current) {
      return;
    }
    if (current.timer !== null) {
      clearTimeout(current.timer);
    }
    inflight = null;
    if (!rejected) {
      unverified = false;
      drain();
    } else {
      unverified = true;
      const ended = state.kind === "open";
      emit({ kind: "revalidate_failed", cause: describe(error), latch: true, ended });
      if (ended) {
        endConnection("revalidate_failed");
      } else {
        drain();
      }
    }
    if (queued !== null) {
      const slot = queued;
      queued = null;
      run(slot);
    }
  }

  function timeout(current: Run): void {
    if (inflight !== current) {
      return;
    }
    inflight = null;
    current.controller.abort(new DOMException("revalidate timed out", "TimeoutError"));
    unverified = true;
    emit({ kind: "revalidate_timeout" });
    if (state.kind === "open") {
      endConnection("hold_timeout");
    } else {
      discardHeld("hold_timeout");
    }
  }

  function onVisibility(input: VisibilityInput): void {
    switch (input) {
      case "visible":
        dispatch({ type: "visible", now: now(), rand: rand(), retryMs });
        return;
      case "pageshow":
        dispatch({ type: "pageshow", now: now(), rand: rand(), retryMs });
        return;
      default:
        dispatch({ type: input, now: now() });
    }
  }

  const stream: Stream = {
    start() {
      if (state.kind !== "stopped") {
        return;
      }
      const visibility = opts.visibility ?? (defaultVisibility ??= createVisibilityManager());
      const online = opts.online ?? (defaultOnline ??= createOnlineManager());
      unsubscribes.push(
        adaptVisibility(
          visibility,
          { visible: () => visibility.isVisible(), listen: () => () => undefined },
          onVisibility,
        ),
        online.subscribe((value) => {
          dispatch({ type: value ? "online" : "offline", now: now() });
        }),
      );
      dispatch({
        type: "start",
        now: now(),
        visible: visibility.isVisible(),
        online: online.isOnline(),
      });
    },
    stop() {
      dispatch({ type: "stop" });
      for (const unsubscribe of unsubscribes) {
        unsubscribe();
      }
      unsubscribes = [];
      if (inflight !== null) {
        const current = inflight;
        inflight = null;
        if (current.timer !== null) {
          clearTimeout(current.timer);
        }
        current.controller.abort(new DOMException("stream stopped", "AbortError"));
      }
      queued = null;
    },
    reconnect() {
      dispatch({ type: "reconnect", now: now() });
    },
    state: () => state,
    cursor: () => cursor,
    resetCursor() {
      cursor = null;
    },
  };
  internals.set(stream, {
    revalidate: schedule,
    queueFull() {
      queued = { cause: queued?.cause ?? "hello", full: true };
    },
  });
  return stream;
}

/** The host-side controls of a stream created by createStream. Throws on a foreign object. */
export function streamInternals(stream: Stream): StreamInternals {
  const controls = internals.get(stream);
  if (controls === undefined) {
    throw new TypeError("streamInternals: stream was not created by createStream");
  }
  return controls;
}
