import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  type ClientEvent,
  type ClientState,
  type Effect,
  initialState,
  reduce,
} from "./reducer.js";
import { DEFAULT_TIMING } from "./timing.js";
import type { Hello } from "./wire.js";

const GOLDEN = join(import.meta.dirname, "reducer.transitions.golden.json");
const REGENERATE = "UPDATE_GOLDEN=1 npx vitest run src/reducer.node.test.ts";
const STALE_MESSAGE = `reducer.transitions.golden.json does not match reduce(); regenerate with: ${REGENERATE}`;

const T0 = 1_000_000;
const G = 3;

const HELLO: Hello = {
  wire: 1,
  epoch: "3f9a1c0e7b2d4a58",
  floor: "0",
  head: "42",
  resumed: true,
  verdict: "resumed",
  keepalive_ms: 15_000,
  keepalive_event: "sse:keepalive",
};

const STATE_KINDS: ClientState["kind"][] = [
  "stopped",
  "connecting",
  "open",
  "backoff",
  "offline",
  "hidden_closed",
];

const EVENT_TYPES: ClientEvent["type"][] = [
  "start",
  "stop",
  "reconnect",
  "connected",
  "connect_failed",
  "byte",
  "stream_ended",
  "watchdog_fired",
  "hidden_timer_fired",
  "backoff_elapsed",
  "stable_elapsed",
  "visible",
  "hidden",
  "online",
  "offline",
  "pagehide",
  "pageshow",
];

const openVisible: ClientState = {
  kind: "open",
  generation: G,
  attempt: 1,
  openedAt: T0,
  lastByteAt: T0,
  keepaliveMs: 15_000,
  visible: true,
  hiddenAt: null,
  online: true,
};

const STATES: Record<string, ClientState> = {
  stopped: { kind: "stopped", generation: G },
  connecting: { kind: "connecting", generation: G, attempt: 1, visible: true, online: true },
  "open:visible": openVisible,
  "open:hidden": { ...openVisible, visible: false, hiddenAt: T0 },
  backoff: {
    kind: "backoff",
    generation: G,
    attempt: 2,
    until: T0 + 500,
    visible: true,
    online: true,
  },
  "hidden_closed:online": { kind: "hidden_closed", generation: G, attempt: 1, online: true },
  "hidden_closed:offline": { kind: "hidden_closed", generation: G, attempt: 1, online: false },
  "offline:visible": { kind: "offline", generation: G, attempt: 1, visible: true },
  "offline:hidden": { kind: "offline", generation: G, attempt: 1, visible: false },
};

type EventBuilder = (state: ClientState) => ClientEvent;

const EVENTS: Record<string, EventBuilder> = {
  "start:visible-online": () => ({ type: "start", now: T0, visible: true, online: true }),
  "start:hidden": () => ({ type: "start", now: T0, visible: false, online: true }),
  "start:offline": () => ({ type: "start", now: T0, visible: true, online: false }),
  stop: () => ({ type: "stop" }),
  reconnect: () => ({ type: "reconnect", now: T0 + 1000 }),
  connected: (s) => ({ type: "connected", now: T0, hello: HELLO, generation: s.generation }),
  "connected:fresh": (s) => ({
    type: "connected",
    now: T0,
    hello: { ...HELLO, resumed: false, verdict: "fresh" },
    generation: s.generation,
  }),
  "connected:stale": (s) => ({
    type: "connected",
    now: T0,
    hello: HELLO,
    generation: s.generation - 1,
  }),
  connect_failed: (s) => ({
    type: "connect_failed",
    now: T0,
    reason: { kind: "network" },
    rand: 0.5,
    generation: s.generation,
  }),
  "connect_failed:wire": (s) => ({
    type: "connect_failed",
    now: T0,
    reason: { kind: "wire_unsupported", wire: 2 },
    rand: 0.5,
    generation: s.generation,
  }),
  "connect_failed:stale": (s) => ({
    type: "connect_failed",
    now: T0,
    reason: { kind: "network" },
    rand: 0.5,
    generation: s.generation - 1,
  }),
  byte: (s) => ({ type: "byte", now: T0 + 1000, generation: s.generation }),
  "byte:stale": (s) => ({ type: "byte", now: T0 + 1000, generation: s.generation - 1 }),
  "stream_ended:short": (s) => ({
    type: "stream_ended",
    now: T0 + 1000,
    reason: "eof",
    rand: 0.5,
    retryMs: 1500,
    generation: s.generation,
  }),
  "stream_ended:stable": (s) => ({
    type: "stream_ended",
    now: T0 + DEFAULT_TIMING.stableMs,
    reason: "eof",
    rand: 0.5,
    retryMs: 1500,
    generation: s.generation,
  }),
  "stream_ended:shutdown-stable": (s) => ({
    type: "stream_ended",
    now: T0 + DEFAULT_TIMING.stableMs,
    reason: "reset:shutdown",
    rand: 0.5,
    retryMs: 1500,
    generation: s.generation,
  }),
  "stream_ended:stale": (s) => ({
    type: "stream_ended",
    now: T0 + 1000,
    reason: "eof",
    rand: 0.5,
    retryMs: 1500,
    generation: s.generation - 1,
  }),
  watchdog_fired: () => ({ type: "watchdog_fired", now: T0 + 45_000, rand: 0.5, retryMs: 1500 }),
  hidden_timer_fired: () => ({ type: "hidden_timer_fired", now: T0 + 60_000 }),
  backoff_elapsed: () => ({ type: "backoff_elapsed", now: T0 + 500 }),
  stable_elapsed: () => ({ type: "stable_elapsed", now: T0 + 30_000 }),
  "visible:fresh": () => ({ type: "visible", now: T0 + 1000, rand: 0.5, retryMs: 1500 }),
  "visible:silent": () => ({ type: "visible", now: T0 + 60_000, rand: 0.5, retryMs: 1500 }),
  hidden: () => ({ type: "hidden", now: T0 + 1000 }),
  online: () => ({ type: "online", now: T0 + 1000 }),
  offline: () => ({ type: "offline", now: T0 + 1000 }),
  pagehide: () => ({ type: "pagehide", now: T0 + 1000 }),
  "pageshow:fresh": () => ({ type: "pageshow", now: T0 + 1000, rand: 0.5, retryMs: 1500 }),
  "pageshow:silent": () => ({ type: "pageshow", now: T0 + 60_000, rand: 0.5, retryMs: 1500 }),
};

interface Row {
  readonly state: string;
  readonly event: string;
  readonly next: string;
  readonly effects: string[];
}

function effectLabel(effect: Effect): string {
  switch (effect.kind) {
    case "arm":
      return `arm:${effect.timer}`;
    case "disarm":
      return `disarm:${effect.timer}`;
    case "revalidate":
      return `revalidate:${effect.cause}`;
    case "emit":
      return `emit:${effect.event.kind}`;
    default:
      return effect.kind;
  }
}

function buildTable(): Row[] {
  const rows: Row[] = [];
  for (const [stateLabel, state] of Object.entries(STATES)) {
    for (const [eventLabel, build] of Object.entries(EVENTS)) {
      const event = build(state);
      const { state: next, effects } = reduce(state, event, DEFAULT_TIMING);
      rows.push({
        state: stateLabel,
        event: eventLabel,
        next: next.kind,
        effects: effects.map(effectLabel),
      });
    }
  }
  return rows;
}

function serialise(rows: Row[]): string {
  return `${JSON.stringify(rows, null, 2)}\n`;
}

describe("reducer golden table", () => {
  it("the golden transition table covers every (state kind, event type) cell", () => {
    const rows = buildTable();
    const text = serialise(rows);
    if (process.env["UPDATE_GOLDEN"] === "1") {
      writeFileSync(GOLDEN, text);
    }
    expect(existsSync(GOLDEN), `missing golden; create it with: ${REGENERATE}`).toBe(true);
    expect(readFileSync(GOLDEN, "utf8"), STALE_MESSAGE).toBe(text);

    const cells = new Set<string>();
    for (const row of rows) {
      const stateKind = row.state.split(":")[0] ?? row.state;
      const eventType = row.event.split(":")[0] ?? row.event;
      cells.add(`${stateKind}/${eventType}`);
    }
    for (const kind of STATE_KINDS) {
      for (const type of EVENT_TYPES) {
        expect(cells.has(`${kind}/${type}`), `${kind} x ${type}`).toBe(true);
      }
    }
    expect(rows).toHaveLength(Object.keys(STATES).length * Object.keys(EVENTS).length);
  });

  it("regenerates only under UPDATE_GOLDEN=1 and names the command on mismatch", () => {
    const before = readFileSync(GOLDEN, "utf8");
    const rows = buildTable();
    const mutated = serialise([{ ...rows[0]!, next: "elsewhere" }, ...rows.slice(1)]);
    let message = "";
    try {
      if (process.env["UPDATE_GOLDEN"] !== "1") {
        expect(before, STALE_MESSAGE).toBe(mutated);
      }
    } catch (e: unknown) {
      message = e instanceof Error ? e.message : String(e);
    }
    if (process.env["UPDATE_GOLDEN"] !== "1") {
      expect(message).toContain(REGENERATE);
    }
    expect(readFileSync(GOLDEN, "utf8")).toBe(before);
  });
});

describe("reducer named cells", () => {
  it("start outside stopped is a no-op cell", () => {
    const start: ClientEvent = { type: "start", now: T0, visible: true, online: true };
    for (const [label, state] of Object.entries(STATES)) {
      if (state.kind === "stopped") {
        continue;
      }
      const out = reduce(state, start, DEFAULT_TIMING);
      expect(out.state, label).toBe(state);
      expect(out.effects, label).toEqual([]);
    }
    const started = reduce(initialState(), start, DEFAULT_TIMING);
    expect(started.state).toEqual({
      kind: "connecting",
      generation: 1,
      attempt: 0,
      visible: true,
      online: true,
    });
    expect(started.effects).toEqual([{ kind: "connect", generation: 1 }]);
  });

  it("pageshow in a visible open yields one revalidate and no abort", () => {
    const out = reduce(
      openVisible,
      { type: "pageshow", now: T0 + 1000, rand: 0.5, retryMs: 1500 },
      DEFAULT_TIMING,
    );
    expect(out.state).toBe(openVisible);
    expect(out.effects).toEqual([{ kind: "revalidate", cause: "pageshow" }]);
  });

  it("reconnect in open aborts g and connects g+1 with the same attempt", () => {
    const out = reduce(openVisible, { type: "reconnect", now: T0 + 1000 }, DEFAULT_TIMING);
    expect(out.state).toEqual({
      kind: "connecting",
      generation: G + 1,
      attempt: 1,
      visible: true,
      online: true,
    });
    expect(out.effects).toEqual([
      { kind: "abort", generation: G, reason: "reconnect" },
      { kind: "connect", generation: G + 1 },
    ]);
  });

  it("reconnect in hidden_closed yields nothing", () => {
    const state = STATES["hidden_closed:online"]!;
    const out = reduce(state, { type: "reconnect", now: T0 + 1000 }, DEFAULT_TIMING);
    expect(out.state).toBe(state);
    expect(out.effects).toEqual([]);
  });
});
