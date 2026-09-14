import type { LifecycleEvent, RevalidateCause } from "./lifecycle.js";
import { type TimingConfig, watchdogMs } from "./timing.js";
import type { Hello } from "./wire.js";

/**
 * The connection lifecycle. `generation` is monotonic for the runtime's lifetime; a state
 * reached by aborting carries a generation the aborted attempt does not have.
 */
export type ClientState =
  | { kind: "stopped"; generation: number }
  | { kind: "connecting"; generation: number; attempt: number; visible: boolean; online: boolean }
  | {
      kind: "open";
      generation: number;
      attempt: number;
      openedAt: number;
      lastByteAt: number;
      /** The hello's keepalive_ms; the watchdog interval derives from it on every re-arm. */
      keepaliveMs: number;
      visible: boolean;
      /** Invariant: hiddenAt !== null exactly when visible === false. */
      hiddenAt: number | null;
      online: boolean;
    }
  | {
      kind: "backoff";
      generation: number;
      attempt: number;
      until: number;
      visible: boolean;
      online: boolean;
    }
  | { kind: "offline"; generation: number; attempt: number; visible: boolean }
  | { kind: "hidden_closed"; generation: number; attempt: number; online: boolean };

export type StreamEnd =
  | "eof"
  | "error"
  | "reset:slow"
  | "reset:shutdown"
  | "watchdog"
  | "frame_too_large"
  | "hold_timeout"
  | "hold_overflow"
  | "revalidate_failed";

export type ConnectFailure =
  | { kind: "timeout_headers" }
  | { kind: "timeout_hello" }
  | { kind: "network" }
  | { kind: "status"; status: number }
  | { kind: "content_type"; value: string }
  | { kind: "bad_hello" }
  | { kind: "wire_unsupported"; wire: number | null };

/** Every event a connection attempt produces carries the generation of that attempt. */
interface FromTransport {
  readonly generation: number;
}

export type ClientEvent =
  | { type: "start"; now: number; visible: boolean; online: boolean }
  | { type: "stop" }
  | { type: "reconnect"; now: number }
  | ({ type: "connected"; now: number; hello: Hello } & FromTransport)
  | ({ type: "connect_failed"; now: number; reason: ConnectFailure; rand: number } & FromTransport)
  | ({ type: "byte"; now: number } & FromTransport)
  | ({
      type: "stream_ended";
      now: number;
      reason: StreamEnd;
      rand: number;
      retryMs: number;
    } & FromTransport)
  | { type: "watchdog_fired"; now: number; rand: number; retryMs: number }
  | { type: "hidden_timer_fired"; now: number }
  | { type: "backoff_elapsed"; now: number }
  | { type: "stable_elapsed"; now: number }
  | { type: "visible"; now: number; rand: number; retryMs: number }
  | { type: "hidden"; now: number }
  | { type: "online"; now: number }
  | { type: "offline"; now: number }
  | { type: "pagehide"; now: number }
  | { type: "pageshow"; now: number; rand: number; retryMs: number };

export type Timer = "watchdog" | "hidden" | "backoff" | "stable";

/**
 * What the runtime executes after a transition. `connect` carries the generation the new
 * attempt is tagged with; `abort` the generation being aborted. The runtime presents its own
 * cursor on `connect`.
 */
export type Effect =
  | { kind: "connect"; generation: number }
  | { kind: "abort"; generation: number; reason: string }
  | { kind: "arm"; timer: Timer; ms: number }
  | { kind: "disarm"; timer: Timer }
  | { kind: "revalidate"; cause: RevalidateCause }
  | { kind: "emit"; event: LifecycleEvent };

export interface Reduction {
  readonly state: ClientState;
  readonly effects: Effect[];
}

const TIMERS: readonly Timer[] = ["watchdog", "hidden", "backoff", "stable"];

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

/** The state before the first start. */
export function initialState(): ClientState {
  return { kind: "stopped", generation: 0 };
}

/** Full-jitter backoff delay for `attempt`; `rand` is drawn in [0, 1) by the runtime. */
export function jitter(attempt: number, rand: number, cfg: TimingConfig): number {
  return rand * Math.min(cfg.capMs, cfg.baseMs * 2 ** attempt);
}

function same(state: ClientState): Reduction {
  return { state, effects: [] };
}

function arm(timer: Timer, ms: number): Effect {
  return { kind: "arm", timer, ms };
}

function disarm(timer: Timer): Effect {
  return { kind: "disarm", timer };
}

function disarmAll(): Effect[] {
  return TIMERS.map(disarm);
}

function abort(generation: number, reason: string): Effect {
  return { kind: "abort", generation, reason };
}

function connect(generation: number): Effect {
  return { kind: "connect", generation };
}

function revalidate(cause: RevalidateCause): Effect {
  return { kind: "revalidate", cause };
}

function emit(event: LifecycleEvent): Effect {
  return { kind: "emit", event };
}

type OpenState = Extract<ClientState, { kind: "open" }>;
type ConnectingState = Extract<ClientState, { kind: "connecting" }>;
type BackoffState = Extract<ClientState, { kind: "backoff" }>;
type OfflineState = Extract<ClientState, { kind: "offline" }>;
type HiddenClosedState = Extract<ClientState, { kind: "hidden_closed" }>;

function connectingState(generation: number, attempt: number, online: boolean): ConnectingState {
  return { kind: "connecting", generation, attempt, visible: true, online };
}

function hiddenClosedState(
  generation: number,
  attempt: number,
  online: boolean,
): HiddenClosedState {
  return { kind: "hidden_closed", generation, attempt, online };
}

function offlineState(generation: number, attempt: number, visible: boolean): OfflineState {
  return { kind: "offline", generation, attempt, visible };
}

function endStream(
  state: OpenState,
  now: number,
  reason: StreamEnd,
  rand: number,
  retryMs: number,
  cfg: TimingConfig,
  alwaysAbort: boolean,
  lead: Effect[],
): Reduction {
  const g = state.generation;
  const effects: Effect[] = [...lead, disarm("watchdog"), disarm("stable"), disarm("hidden")];
  if (!state.visible) {
    effects.push(abort(g, reason));
    return {
      state: hiddenClosedState(g + 1, state.attempt, state.online),
      effects,
    };
  }
  if (!state.online) {
    effects.push(abort(g, reason));
    return {
      state: offlineState(g + 1, state.attempt, true),
      effects,
    };
  }
  if (now - state.openedAt < cfg.stableMs || reason === "reset:shutdown") {
    const delay = Math.max(jitter(state.attempt, rand, cfg), retryMs);
    effects.push(abort(g, reason), arm("backoff", delay));
    return {
      state: {
        kind: "backoff",
        generation: g + 1,
        attempt: state.attempt + 1,
        until: now + delay,
        visible: true,
        online: true,
      },
      effects,
    };
  }
  if (alwaysAbort) {
    effects.push(abort(g, reason));
  }
  effects.push(connect(g + 1));
  return { state: connectingState(g + 1, state.attempt, true), effects };
}

function reduceStopped(state: ClientState, event: ClientEvent): Reduction {
  if (event.type !== "start") {
    return same(state);
  }
  const g = state.generation;
  if (!event.visible) {
    return same(hiddenClosedState(g, 0, event.online));
  }
  if (!event.online) {
    return same(offlineState(g, 0, true));
  }
  return { state: connectingState(g + 1, 0, true), effects: [connect(g + 1)] };
}

function failConnect(
  state: ConnectingState,
  now: number,
  reason: ConnectFailure,
  rand: number,
  cfg: TimingConfig,
): Reduction {
  const effects: Effect[] = [];
  if (reason.kind === "wire_unsupported") {
    effects.push(emit({ kind: "wire_unsupported", wire: reason.wire }));
  }
  if (!state.online) {
    return {
      state: offlineState(state.generation, state.attempt, true),
      effects,
    };
  }
  const delay = jitter(state.attempt, rand, cfg);
  effects.push(arm("backoff", delay));
  return {
    state: {
      kind: "backoff",
      generation: state.generation,
      attempt: state.attempt + 1,
      until: now + delay,
      visible: true,
      online: true,
    },
    effects,
  };
}

function reduceConnecting(
  state: ConnectingState,
  event: ClientEvent,
  cfg: TimingConfig,
): Reduction {
  const g = state.generation;
  switch (event.type) {
    case "connected": {
      const hello = event.hello;
      const effects: Effect[] = [
        arm("watchdog", watchdogMs(hello.keepalive_ms, cfg)),
        arm("stable", cfg.stableMs),
        emit({
          kind: "hello",
          verdict: hello.verdict,
          wire: hello.wire,
          resumed: hello.resumed,
          epoch: hello.epoch,
          floor: hello.floor,
          head: hello.head,
        }),
      ];
      if (!hello.resumed) {
        effects.push(revalidate("hello"));
      }
      return {
        state: {
          kind: "open",
          generation: g,
          attempt: state.attempt,
          openedAt: event.now,
          lastByteAt: event.now,
          keepaliveMs: hello.keepalive_ms,
          visible: true,
          hiddenAt: null,
          online: state.online,
        },
        effects,
      };
    }
    case "connect_failed":
      return failConnect(state, event.now, event.reason, event.rand, cfg);
    case "stream_ended":
      return failConnect(state, event.now, { kind: "network" }, event.rand, cfg);
    case "hidden":
    case "pagehide":
      return {
        state: hiddenClosedState(g + 1, state.attempt, state.online),
        effects: [abort(g, event.type)],
      };
    case "offline":
      return {
        state: offlineState(g + 1, state.attempt, true),
        effects: [abort(g, event.type)],
      };
    default:
      return same(state);
  }
}

function wakeOpen(
  state: OpenState,
  now: number,
  cause: RevalidateCause,
  rand: number,
  retryMs: number,
  cfg: TimingConfig,
): Reduction {
  const shown: OpenState = { ...state, visible: true, hiddenAt: null };
  const lead: Effect[] = [disarm("hidden"), revalidate(cause)];
  const silence = now - state.lastByteAt;
  if (silence > watchdogMs(state.keepaliveMs, cfg)) {
    lead.push(emit({ kind: "watchdog", sinceLastByteMs: silence }));
    return endStream(shown, now, "watchdog", rand, retryMs, cfg, true, lead);
  }
  lead.push(arm("watchdog", watchdogMs(state.keepaliveMs, cfg)));
  return { state: shown, effects: lead };
}

function reduceOpen(state: OpenState, event: ClientEvent, cfg: TimingConfig): Reduction {
  const g = state.generation;
  switch (event.type) {
    case "byte": {
      const next: OpenState = { ...state, lastByteAt: event.now };
      const effects = state.visible ? [arm("watchdog", watchdogMs(state.keepaliveMs, cfg))] : [];
      return { state: next, effects };
    }
    case "watchdog_fired": {
      if (!state.visible) {
        return same(state);
      }
      const lead = [emit({ kind: "watchdog", sinceLastByteMs: event.now - state.lastByteAt })];
      return endStream(state, event.now, "watchdog", event.rand, event.retryMs, cfg, true, lead);
    }
    case "stream_ended":
      return endStream(state, event.now, event.reason, event.rand, event.retryMs, cfg, false, []);
    case "stable_elapsed":
      return same({ ...state, attempt: 0 });
    case "hidden":
      if (!state.visible) {
        return same(state);
      }
      return {
        state: { ...state, visible: false, hiddenAt: event.now },
        effects: [disarm("watchdog"), arm("hidden", cfg.hiddenCloseAfterMs)],
      };
    case "hidden_timer_fired":
      if (state.visible) {
        return same(state);
      }
      return {
        state: hiddenClosedState(g + 1, state.attempt, state.online),
        effects: [abort(g, "hidden_timer"), disarm("stable"), disarm("hidden")],
      };
    case "visible":
      if (state.visible) {
        return same(state);
      }
      return wakeOpen(state, event.now, "visible", event.rand, event.retryMs, cfg);
    case "pageshow":
      if (state.visible) {
        return { state, effects: [revalidate("pageshow")] };
      }
      return wakeOpen(state, event.now, "pageshow", event.rand, event.retryMs, cfg);
    case "pagehide":
      return {
        state: hiddenClosedState(g + 1, state.attempt, state.online),
        effects: [abort(g, "pagehide"), ...disarmAll()],
      };
    case "offline":
      return {
        state: offlineState(g + 1, state.attempt, state.visible),
        effects: [abort(g, "offline"), ...disarmAll()],
      };
    default:
      return same(state);
  }
}

function reduceBackoff(state: BackoffState, event: ClientEvent): Reduction {
  const g = state.generation;
  switch (event.type) {
    case "backoff_elapsed":
      return {
        state: connectingState(g + 1, state.attempt, state.online),
        effects: [connect(g + 1)],
      };
    case "visible":
    case "pageshow":
    case "online":
      return {
        state: connectingState(g + 1, state.attempt, true),
        effects: [disarm("backoff"), connect(g + 1), revalidate(event.type)],
      };
    case "hidden":
    case "pagehide":
      return {
        state: hiddenClosedState(g, state.attempt, state.online),
        effects: [disarm("backoff")],
      };
    case "offline":
      return {
        state: offlineState(g, state.attempt, state.visible),
        effects: [disarm("backoff")],
      };
    default:
      return same(state);
  }
}

function reduceHiddenClosed(state: HiddenClosedState, event: ClientEvent): Reduction {
  const g = state.generation;
  switch (event.type) {
    case "visible":
    case "pageshow":
      if (!state.online) {
        return {
          state: offlineState(g, state.attempt, true),
          effects: [revalidate(event.type)],
        };
      }
      return {
        state: connectingState(g + 1, state.attempt, true),
        effects: [connect(g + 1), revalidate(event.type)],
      };
    case "online":
      return same({ ...state, online: true });
    case "offline":
      return same(offlineState(g, state.attempt, false));
    default:
      return same(state);
  }
}

function reduceOffline(state: OfflineState, event: ClientEvent): Reduction {
  const g = state.generation;
  switch (event.type) {
    case "online":
      if (!state.visible) {
        return same(hiddenClosedState(g, state.attempt, true));
      }
      return {
        state: connectingState(g + 1, state.attempt, true),
        effects: [connect(g + 1), revalidate("online")],
      };
    case "visible":
    case "pageshow":
      return { state: { ...state, visible: true }, effects: [revalidate(event.type)] };
    case "hidden":
    case "pagehide":
      return same({ ...state, visible: false });
    default:
      return same(state);
  }
}

/** Total, pure transition function: no clock, no randomness, no DOM. */
export function reduce(state: ClientState, event: ClientEvent, cfg: TimingConfig): Reduction {
  if (state.kind === "stopped") {
    return reduceStopped(state, event);
  }
  if (event.type === "start") {
    return same(state);
  }
  const g = state.generation;
  if (event.type === "stop") {
    return {
      state: { kind: "stopped", generation: g + 1 },
      effects: [abort(g, "stop"), ...disarmAll()],
    };
  }
  if (event.type === "reconnect") {
    if (state.kind === "open" || state.kind === "connecting") {
      return {
        state: connectingState(g + 1, state.attempt, state.online),
        effects: [abort(g, "reconnect"), connect(g + 1)],
      };
    }
    if (state.kind === "backoff") {
      return {
        state: connectingState(g + 1, state.attempt, state.online),
        effects: [disarm("backoff"), connect(g + 1)],
      };
    }
    return same(state);
  }
  const eventGeneration = transportGeneration(event);
  if (eventGeneration !== null) {
    if (state.kind !== "connecting" && state.kind !== "open") {
      return same(state);
    }
    if (eventGeneration !== g) {
      return same(state);
    }
    if (state.kind === "open" && (event.type === "connected" || event.type === "connect_failed")) {
      return same(state);
    }
  }
  switch (state.kind) {
    case "connecting":
      return reduceConnecting(state, event, cfg);
    case "open":
      return reduceOpen(state, event, cfg);
    case "backoff":
      return reduceBackoff(state, event);
    case "hidden_closed":
      return reduceHiddenClosed(state, event);
    case "offline":
      return reduceOffline(state, event);
  }
}
