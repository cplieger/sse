import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  type ClientEvent,
  type ClientState,
  type Effect,
  initialState,
  reduce,
  type StreamEnd,
} from "./reducer.js";
import { DEFAULT_TIMING, type TimingConfig, watchdogMs } from "./timing.js";
import type { Hello } from "./wire.js";

const cfg: TimingConfig = DEFAULT_TIMING;
const T0 = 1_000_000;

function hello(keepaliveMs: number, resumed: boolean): Hello {
  return {
    wire: 1,
    epoch: "3f9a1c0e7b2d4a58",
    floor: "0",
    head: "42",
    resumed,
    verdict: resumed ? "resumed" : "fresh",
    keepalive_ms: keepaliveMs,
    keepalive_event: "sse:keepalive",
  };
}

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

const STREAM_ENDS: StreamEnd[] = [
  "eof",
  "error",
  "reset:slow",
  "reset:shutdown",
  "watchdog",
  "frame_too_large",
  "hold_timeout",
  "hold_overflow",
  "revalidate_failed",
];

interface Spec {
  readonly type: ClientEvent["type"];
  readonly dt: number;
  readonly rand: number;
  readonly retryMs: number;
  readonly stale: boolean;
  readonly visible: boolean;
  readonly online: boolean;
  readonly reason: StreamEnd;
  readonly keepaliveMs: number;
  readonly resumed: boolean;
}

const arbSpec: fc.Arbitrary<Spec> = fc.record({
  type: fc.constantFrom(...EVENT_TYPES),
  dt: fc.nat(70_000),
  rand: fc.double({ min: 0, max: 1, noNaN: true, maxExcluded: true }),
  retryMs: fc.constantFrom(0, 1500, 3000),
  stale: fc.boolean(),
  visible: fc.boolean(),
  online: fc.boolean(),
  reason: fc.constantFrom(...STREAM_ENDS),
  keepaliveMs: fc.constantFrom(9000, 15_000),
  resumed: fc.boolean(),
});

const arbSequence = fc.array(arbSpec, { minLength: 1, maxLength: 40 });

function materialise(spec: Spec, state: ClientState, now: number): ClientEvent {
  const generation = spec.stale ? state.generation - 1 : state.generation;
  switch (spec.type) {
    case "start":
      return { type: "start", now, visible: spec.visible, online: spec.online };
    case "stop":
      return { type: "stop" };
    case "reconnect":
      return { type: "reconnect", now };
    case "connected":
      return { type: "connected", now, hello: hello(spec.keepaliveMs, spec.resumed), generation };
    case "connect_failed":
      return {
        type: "connect_failed",
        now,
        reason: { kind: "network" },
        rand: spec.rand,
        generation,
      };
    case "byte":
      return { type: "byte", now, generation };
    case "stream_ended":
      return {
        type: "stream_ended",
        now,
        reason: spec.reason,
        rand: spec.rand,
        retryMs: spec.retryMs,
        generation,
      };
    case "watchdog_fired":
    case "visible":
    case "pageshow":
      return { type: spec.type, now, rand: spec.rand, retryMs: spec.retryMs };
    default:
      return { type: spec.type, now };
  }
}

interface Step {
  readonly before: ClientState;
  readonly event: ClientEvent;
  readonly after: ClientState;
  readonly effects: Effect[];
}

function run(specs: readonly Spec[]): Step[] {
  const steps: Step[] = [];
  let state = initialState();
  let now = T0;
  for (const spec of specs) {
    now += spec.dt;
    const event = materialise(spec, state, now);
    const out = reduce(state, event, cfg);
    steps.push({ before: state, event, after: out.state, effects: out.effects });
    state = out.state;
  }
  return steps;
}

function isHidden(state: ClientState): boolean {
  switch (state.kind) {
    case "open":
    case "offline":
      return !state.visible;
    case "hidden_closed":
      return true;
    default:
      return false;
  }
}

function count(effects: readonly Effect[], pred: (e: Effect) => boolean): number {
  return effects.filter(pred).length;
}

function transportEvents(generation: number, now: number): ClientEvent[] {
  return [
    { type: "connected", now, hello: hello(15_000, true), generation },
    { type: "connect_failed", now, reason: { kind: "network" }, rand: 0.5, generation },
    { type: "byte", now, generation },
    { type: "stream_ended", now, reason: "eof", rand: 0.5, retryMs: 1500, generation },
  ];
}

function nonStartEvents(now: number): ClientEvent[] {
  return [
    { type: "stop" },
    { type: "reconnect", now },
    ...transportEvents(0, now),
    ...transportEvents(1, now),
    { type: "watchdog_fired", now, rand: 0.5, retryMs: 1500 },
    { type: "hidden_timer_fired", now },
    { type: "backoff_elapsed", now },
    { type: "stable_elapsed", now },
    { type: "visible", now, rand: 0.5, retryMs: 1500 },
    { type: "hidden", now },
    { type: "online", now },
    { type: "offline", now },
    { type: "pagehide", now },
    { type: "pageshow", now, rand: 0.5, retryMs: 1500 },
  ];
}

function connectOpen(now: number, keepaliveMs = 15_000): { state: ClientState; now: number } {
  const started = reduce(
    initialState(),
    { type: "start", now, visible: true, online: true },
    cfg,
  ).state;
  const open = reduce(
    started,
    { type: "connected", now, hello: hello(keepaliveMs, true), generation: started.generation },
    cfg,
  ).state;
  return { state: open, now };
}

describe("reducer properties", () => {
  it("every visible received while hidden yields exactly one revalidate", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          if (step.event.type === "visible" && isHidden(step.before)) {
            expect(count(step.effects, (e) => e.kind === "revalidate")).toBe(1);
          }
        }
      }),
    );
  });

  it("no state arms two watchdogs", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          const arms = count(step.effects, (e) => e.kind === "arm" && e.timer === "watchdog");
          expect(arms).toBeLessThanOrEqual(1);
        }
      }),
    );
  });

  it("stop aborts, disarms everything, and stopped absorbs all but start", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          if (step.after.kind === "stopped") {
            continue;
          }
          const out = reduce(step.after, { type: "stop" }, cfg);
          expect(out.state).toEqual({ kind: "stopped", generation: step.after.generation + 1 });
          expect(out.effects).toContainEqual({
            kind: "abort",
            generation: step.after.generation,
            reason: "stop",
          });
          for (const timer of ["watchdog", "hidden", "backoff", "stable"]) {
            expect(out.effects).toContainEqual({ kind: "disarm", timer });
          }
          for (const event of nonStartEvents(T0)) {
            const absorbed = reduce(out.state, event, cfg);
            expect(absorbed.state).toBe(out.state);
            expect(absorbed.effects).toEqual([]);
          }
        }
      }),
    );
  });

  it("a stale-generation transport event changes nothing", () => {
    fc.assert(
      fc.property(arbSequence, fc.constantFrom(-1, 1, 7), (specs, delta) => {
        for (const step of run(specs)) {
          const state = step.after;
          if (state.kind !== "connecting" && state.kind !== "open") {
            continue;
          }
          for (const event of transportEvents(state.generation + delta, T0 + 5_000_000)) {
            const out = reduce(state, event, cfg);
            expect(out.state).toBe(state);
            expect(out.effects).toEqual([]);
          }
        }
      }),
    );
  });

  it("a transport event in a connection-less state changes nothing", () => {
    fc.assert(
      fc.property(arbSequence, fc.constantFrom(-1, 0, 1), (specs, delta) => {
        for (const step of run(specs)) {
          const state = step.after;
          if (state.kind === "connecting" || state.kind === "open") {
            continue;
          }
          for (const event of transportEvents(state.generation + delta, T0 + 5_000_000)) {
            const out = reduce(state, event, cfg);
            expect(out.state).toBe(state);
            expect(out.effects).toEqual([]);
          }
        }
      }),
    );
  });

  it("connect_failed in open changes nothing", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          const state = step.after;
          if (state.kind !== "open") {
            continue;
          }
          const out = reduce(
            state,
            {
              type: "connect_failed",
              now: T0 + 5_000_000,
              reason: { kind: "bad_hello" },
              rand: 0.5,
              generation: state.generation,
            },
            cfg,
          );
          expect(out.state).toBe(state);
          expect(out.effects).toEqual([]);
        }
      }),
    );
  });

  it("abort carries the pre-transition generation and connect the post-transition one", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          for (const effect of step.effects) {
            if (effect.kind === "abort") {
              expect(effect.generation).toBe(step.before.generation);
            }
            if (effect.kind === "connect") {
              expect(effect.generation).toBe(step.after.generation);
              expect(step.after.generation).toBe(step.before.generation + 1);
            }
          }
        }
      }),
    );
  });

  it("generation never decreases across stop then start", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        const steps = run(specs);
        for (const step of steps) {
          expect(step.after.generation).toBeGreaterThanOrEqual(step.before.generation);
        }
        const last = steps.at(-1)?.after ?? initialState();
        const stopped = reduce(last, { type: "stop" }, cfg).state;
        const restarted = reduce(
          stopped,
          { type: "start", now: T0, visible: true, online: true },
          cfg,
        ).state;
        expect(stopped.generation).toBeGreaterThanOrEqual(last.generation);
        expect(restarted.generation).toBeGreaterThan(last.generation);
      }),
    );
  });

  it("attempt is non-decreasing between stable_elapsed events", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          if (step.before.kind === "stopped" || step.after.kind === "stopped") {
            continue;
          }
          if (step.event.type === "stable_elapsed") {
            continue;
          }
          expect(step.after.attempt).toBeGreaterThanOrEqual(step.before.attempt);
        }
      }),
    );
  });

  it("N short opens yield N arm backoff with ms >= retryMs", () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 8 }),
        fc.array(fc.double({ min: 0, max: 1, noNaN: true, maxExcluded: true }), {
          minLength: 8,
          maxLength: 8,
        }),
        fc.constantFrom(0, 1500, 3000),
        fc.integer({ min: 0, max: cfg.stableMs - 1 }),
        (n, rands, retryMs, lived) => {
          let now = T0;
          let state = reduce(
            initialState(),
            { type: "start", now, visible: true, online: true },
            cfg,
          ).state;
          const arms: number[] = [];
          for (let i = 0; i < n; i++) {
            state = reduce(
              state,
              { type: "connected", now, hello: hello(15_000, true), generation: state.generation },
              cfg,
            ).state;
            now += lived;
            const out = reduce(
              state,
              {
                type: "stream_ended",
                now,
                reason: "eof",
                rand: rands[i]!,
                retryMs,
                generation: state.generation,
              },
              cfg,
            );
            for (const effect of out.effects) {
              if (effect.kind === "arm" && effect.timer === "backoff") {
                arms.push(effect.ms);
              }
            }
            expect(out.state.kind).toBe("backoff");
            state = out.state;
            now += 100_000;
            state = reduce(state, { type: "backoff_elapsed", now }, cfg).state;
            expect(state.kind).toBe("connecting");
          }
          expect(arms).toHaveLength(n);
          for (const ms of arms) {
            expect(ms).toBeGreaterThanOrEqual(retryMs);
          }
        },
      ),
    );
  });

  it("reset:shutdown after stableMs backs off, never connects at once", () => {
    fc.assert(
      fc.property(
        fc.double({ min: 0, max: 1, noNaN: true, maxExcluded: true }),
        fc.constantFrom(0, 1500, 3000),
        fc.nat(600_000),
        (rand, retryMs, extra) => {
          const { state, now } = connectOpen(T0);
          const out = reduce(
            state,
            {
              type: "stream_ended",
              now: now + cfg.stableMs + extra,
              reason: "reset:shutdown",
              rand,
              retryMs,
              generation: state.generation,
            },
            cfg,
          );
          expect(out.state.kind).toBe("backoff");
          expect(count(out.effects, (e) => e.kind === "connect")).toBe(0);
          const arm = out.effects.find((e) => e.kind === "arm" && e.timer === "backoff");
          expect(arm?.kind === "arm" && arm.ms >= retryMs).toBe(true);
        },
      ),
    );
  });

  it("keepalive 9 s puts watchdog_fired in the short-open branch and stays total", () => {
    fc.assert(
      fc.property(
        fc.double({ min: 0, max: 1, noNaN: true, maxExcluded: true }),
        fc.constantFrom(0, 1500, 3000),
        (rand, retryMs) => {
          const { state, now } = connectOpen(T0, 9000);
          const fireAt = now + watchdogMs(9000, cfg);
          expect(fireAt - now).toBe(27_000);
          const out = reduce(state, { type: "watchdog_fired", now: fireAt, rand, retryMs }, cfg);
          expect(out.state.kind).toBe("backoff");
          expect(out.state.generation).toBe(state.generation + 1);
          expect(out.effects).toContainEqual({
            kind: "abort",
            generation: state.generation,
            reason: "watchdog",
          });
          expect(count(out.effects, (e) => e.kind === "arm" && e.timer === "backoff")).toBe(1);
          const watchdogs = count(
            out.effects,
            (e) => e.kind === "emit" && e.event.kind === "watchdog",
          );
          expect(watchdogs).toBe(1);
        },
      ),
    );
  });

  it("visible in open always disarms hidden", () => {
    fc.assert(
      fc.property(fc.nat(100_000), fc.nat(100_000), fc.constantFrom(9000, 15_000), (hid, dt, k) => {
        const opened = connectOpen(T0, k);
        const hidden = reduce(opened.state, { type: "hidden", now: opened.now + hid }, cfg).state;
        expect(hidden.kind).toBe("open");
        const out = reduce(
          hidden,
          { type: "visible", now: opened.now + hid + dt, rand: 0.5, retryMs: 1500 },
          cfg,
        );
        expect(out.effects).toContainEqual({ kind: "disarm", timer: "hidden" });
        expect(count(out.effects, (e) => e.kind === "revalidate")).toBe(1);
      }),
    );
  });

  it("online in backoff connects", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          const state = step.after;
          if (state.kind !== "backoff") {
            continue;
          }
          const out = reduce(state, { type: "online", now: T0 + 5_000_000 }, cfg);
          expect(out.state.kind).toBe("connecting");
          expect(out.effects).toContainEqual({ kind: "connect", generation: state.generation + 1 });
          expect(out.effects).toContainEqual({ kind: "disarm", timer: "backoff" });
        }
      }),
    );
  });

  it("hidden_closed + visible while offline yields offline{visible} with revalidate and no connect", () => {
    fc.assert(
      fc.property(fc.nat(10), fc.nat(1000), (attempt, generation) => {
        const state: ClientState = { kind: "hidden_closed", generation, attempt, online: false };
        const out = reduce(state, { type: "visible", now: T0, rand: 0.5, retryMs: 1500 }, cfg);
        expect(out.state).toEqual({ kind: "offline", generation, attempt, visible: true });
        expect(out.effects).toEqual([{ kind: "revalidate", cause: "visible" }]);
      }),
    );
  });

  it("hidden_closed + online records online with no effect", () => {
    fc.assert(
      fc.property(fc.nat(10), fc.nat(1000), fc.boolean(), (attempt, generation, online) => {
        const state: ClientState = { kind: "hidden_closed", generation, attempt, online };
        const out = reduce(state, { type: "online", now: T0 }, cfg);
        expect(out.state).toEqual({ kind: "hidden_closed", generation, attempt, online: true });
        expect(out.effects).toEqual([]);
      }),
    );
  });

  it("hiddenAt and visible agree in every reachable open", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          if (step.after.kind === "open") {
            expect(step.after.hiddenAt !== null).toBe(!step.after.visible);
          }
        }
      }),
    );
  });

  it("offline.visible is defined on every path into offline", () => {
    fc.assert(
      fc.property(arbSequence, (specs) => {
        for (const step of run(specs)) {
          if (step.after.kind === "offline") {
            expect(typeof step.after.visible).toBe("boolean");
          }
        }
      }),
    );
  });
});
