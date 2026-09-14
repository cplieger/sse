import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  type FoldInput,
  type FoldOutput,
  type FoldState,
  anyVisible,
  createFoldState,
  foldTabs,
} from "./fold.js";
import { type ClientEvent, type ClientState, initialState, reduce } from "./reducer.js";
import { DEFAULT_TIMING } from "./timing.js";

// Mostly a handful of tabs, so attach/pageshow pairs on one tab are reachable; some runs spread to 64.
const tabIdArb = fc
  .oneof(
    { weight: 3, arbitrary: fc.integer({ min: 0, max: 2 }) },
    { weight: 1, arbitrary: fc.integer({ min: 0, max: 63 }) },
  )
  .map((n) => `t${String(n)}`);

const inputArb: fc.Arbitrary<FoldInput> = fc.oneof(
  fc.record({
    type: fc.constant("attach" as const),
    tabId: tabIdArb,
    visible: fc.boolean(),
    online: fc.boolean(),
  }),
  fc.record({
    type: fc.constant("visibility" as const),
    tabId: tabIdArb,
    ev: fc.constantFrom(
      "visible" as const,
      "hidden" as const,
      "pagehide" as const,
      "pageshow" as const,
    ),
  }),
  fc.record({ type: fc.constant("network" as const), tabId: tabIdArb, online: fc.boolean() }),
  fc.record({ type: fc.constant("remove" as const), tabId: tabIdArb }),
);

const sequenceArb = fc.tuple(
  fc.boolean(),
  fc.array(inputArb, { minLength: 0, maxLength: 200, size: "max" }),
);

interface Step {
  readonly before: FoldState;
  readonly after: FoldState;
  readonly input: FoldInput;
  readonly events: FoldOutput[];
}

function run(seed: boolean, inputs: readonly FoldInput[]): Step[] {
  let state = createFoldState(seed);
  const steps: Step[] = [];
  for (const input of inputs) {
    const folded = foldTabs(state, input);
    steps.push({ before: state, after: folded.state, input, events: folded.events });
    state = folded.state;
  }
  return steps;
}

function stream(steps: readonly Step[]): FoldOutput[] {
  return steps.flatMap((s) => s.events);
}

const VISIBILITY: ReadonlySet<FoldOutput> = new Set(["visible", "hidden", "pageshow"]);
const NETWORK: ReadonlySet<FoldOutput> = new Set(["online", "offline"]);

/** Feeds a fold output stream to the reducer as a single tab would and counts revalidate effects. */
function revalidates(seed: boolean, events: readonly FoldOutput[]): number {
  const cfg = DEFAULT_TIMING;
  let state: ClientState = initialState();
  let count = 0;
  const dispatch = (event: ClientEvent): void => {
    const r = reduce(state, event, cfg);
    state = r.state;
    count += r.effects.filter((e) => e.kind === "revalidate").length;
    for (const effect of r.effects) {
      if (effect.kind === "connect") {
        const connected = reduce(
          state,
          {
            type: "connected",
            generation: effect.generation,
            now: 0,
            hello: {
              wire: 1,
              epoch: "aaaaaaaaaaaaaaaa",
              floor: "0",
              head: "0",
              resumed: true,
              verdict: "resumed",
              keepalive_ms: 15000,
              keepalive_event: "sse:keepalive",
            },
          },
          cfg,
        );
        state = connected.state;
        count += connected.effects.filter((e) => e.kind === "revalidate").length;
      }
    }
  };
  dispatch({ type: "start", now: 0, visible: false, online: seed });
  for (const ev of events) {
    switch (ev) {
      case "visible":
      case "pageshow":
        dispatch({ type: ev, now: 0, rand: 0, retryMs: 1500 });
        break;
      default:
        dispatch({ type: ev, now: 0 });
    }
  }
  return count;
}

describe("profile fold", () => {
  it("never two consecutive hidden", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        const vis = stream(run(seed, inputs)).filter((e) => VISIBILITY.has(e));
        for (let i = 1; i < vis.length; i++) {
          expect(vis[i - 1] === "hidden" && vis[i] === "hidden").toBe(false);
        }
        expect(vis.filter((e) => e === "hidden").length).toBeLessThanOrEqual(
          vis.filter((e) => e === "visible").length,
        );
      }),
    );
  });

  it("never visible while already visible", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        const steps = run(seed, inputs);
        for (const step of steps) {
          if (step.events.includes("visible")) {
            expect(anyVisible(step.before)).toBe(false);
            expect(anyVisible(step.after)).toBe(true);
          }
        }
        expect(steps).toHaveLength(inputs.length);
      }),
    );
  });

  it("hidden exactly when the last visible tab leaves by any route", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        const steps = run(seed, inputs);
        for (const step of steps) {
          const left = anyVisible(step.before) && !anyVisible(step.after);
          expect(step.events.filter((e) => e === "hidden")).toHaveLength(left ? 1 : 0);
        }
        expect(steps).toHaveLength(inputs.length);
      }),
    );
  });

  it("pageshow at most once per hidden-to-visible transition", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        let sinceVisible = 0;
        for (const ev of stream(run(seed, inputs))) {
          if (ev === "visible") {
            sinceVisible = 0;
          } else if (ev === "pageshow") {
            sinceVisible++;
          }
          expect(sinceVisible).toBeLessThanOrEqual(1);
        }
        expect(sinceVisible).toBeLessThanOrEqual(1);
      }),
    );
  });

  it("never emits pagehide", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        const events: string[] = stream(run(seed, inputs));
        expect(events).not.toContain("pagehide");
      }),
    );
  });

  it("never two consecutive online or offline", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        const net = stream(run(seed, inputs)).filter((e) => NETWORK.has(e));
        for (let i = 1; i < net.length; i++) {
          expect(net[i - 1]).not.toBe(net[i]);
        }
        expect(net.every((e) => NETWORK.has(e))).toBe(true);
      }),
    );
  });

  it("online/offline exactly when the latest report flips the seeded value", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        let latest = seed;
        const steps = run(seed, inputs);
        expect(steps).toHaveLength(inputs.length);
        for (const step of steps) {
          const report =
            step.input.type === "attach" || step.input.type === "network"
              ? step.input.online
              : null;
          const net = step.events.filter((e) => NETWORK.has(e));
          if (report === null || report === latest) {
            expect(net).toEqual([]);
          } else {
            expect(net).toEqual([report ? "online" : "offline"]);
            latest = report;
          }
        }
      }),
    );
  });

  it("the fold's output yields the same revalidate count as the equivalent single-tab sequence", () => {
    fc.assert(
      fc.property(sequenceArb, ([seed, inputs]) => {
        const steps = run(seed, inputs);
        const single: FoldOutput[] = [];
        let visible = false;
        let online = seed;
        let pageshown = false;
        for (const step of steps) {
          const nowVisible = anyVisible(step.after);
          if (nowVisible && !visible) {
            single.push("visible");
            pageshown = false;
          }
          if (step.input.type === "visibility" && step.input.ev === "pageshow" && nowVisible) {
            const tab = step.before.tabs.get(step.input.tabId);
            if (tab?.visible === true && tab.pageshowPending && !pageshown) {
              single.push("pageshow");
              pageshown = true;
            }
          }
          if (!nowVisible && visible) {
            single.push("hidden");
          }
          visible = nowVisible;
          if (step.after.online !== online) {
            online = step.after.online;
            single.push(online ? "online" : "offline");
          }
        }
        expect(revalidates(seed, stream(steps))).toBe(revalidates(seed, single));
      }),
    );
  });
});
