import fc from "fast-check";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  FAKE_CLOCK,
  KEEPALIVE_FRAME,
  flush,
  frame,
  helloFrame,
} from "./test-helpers/scripted-fetch.js";
import { type Harness, harness } from "./test-helpers/stream-harness.js";
import { DEFAULT_TIMING } from "./timing.js";

const A = "aaaaaaaaaaaaaaaa";
const RUNS = { numRuns: 250 };

function id(offset: number): string {
  return `${A}:${String(offset)}`;
}

/** One connection after an established cursor at `previous`. */
interface Scenario {
  readonly previous: number;
  readonly resumed: boolean;
  readonly head: number;
  /** Live frames after head. */
  readonly live: number;
  /** A revalidate run is in flight while frames arrive (resumed connections only; a fresh hello always has one). */
  readonly hold: boolean;
  /** Index into the pushed sequence at which the run settles; past the end means never. */
  readonly settleAt: number;
  /** Index at which the stream ends; past the end means it stays open. */
  readonly endAt: number;
  /** Whether a keepalive follows the replay. */
  readonly keepalive: boolean;
}

const scenarioArb: fc.Arbitrary<Scenario> = fc
  .record({
    previous: fc.integer({ min: 0, max: 6 }),
    resumed: fc.boolean(),
    extraHead: fc.integer({ min: 0, max: 6 }),
    live: fc.integer({ min: 0, max: 4 }),
    hold: fc.boolean(),
    settleAt: fc.integer({ min: 0, max: 20 }),
    endAt: fc.integer({ min: 0, max: 20 }),
    keepalive: fc.boolean(),
  })
  .map((r) => ({
    previous: r.previous,
    resumed: r.resumed,
    head: r.resumed ? r.previous + r.extraHead : r.extraHead,
    live: r.live,
    hold: r.hold,
    settleAt: r.settleAt,
    endAt: r.endAt,
    keepalive: r.keepalive,
  }));

interface Outcome {
  readonly h: Harness;
  /** Offsets pushed on the second connection, in order. */
  readonly pushed: number[];
  /** Offsets delivered to onFrame on the second connection. */
  readonly delivered: number[];
  readonly nonReplayObserved: boolean;
  readonly settled: boolean;
  readonly ended: boolean;
  readonly endedDuringReplay: boolean;
  readonly revalidateHello: boolean;
}

async function establish(previous: number): Promise<Harness> {
  const h = harness();
  const first = await h.open({ head: String(previous) });
  h.runs[0]!.resolve();
  await flush();
  vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
  first.end();
  await flush();
  return h;
}

async function play(s: Scenario): Promise<Outcome> {
  const h = await establish(s.previous);
  const deliveredBefore = h.frames.length;
  const conn = h.last();
  conn.push(helloFrame({ resumed: s.resumed, head: String(s.head) }));
  await flush();
  const revalidateHello = h.ofKind("revalidate").some((e) => e.cause === "hello");
  if (s.resumed && s.hold) {
    h.visibility.emit("pageshow");
    await flush();
  }
  const steps: (() => void)[] = [];
  const pushed: number[] = [];
  const replayEnd = s.resumed ? s.head : s.previous;
  const replayStart = s.resumed ? s.previous + 1 : s.head + 1;
  let replaySteps = 0;
  if (s.resumed) {
    for (let o = replayStart; o <= replayEnd; o++) {
      steps.push(() => {
        pushed.push(o);
        conn.push(frame("message", "r", id(o)));
      });
      replaySteps++;
    }
  }
  if (s.keepalive) {
    steps.push(() => {
      conn.push(KEEPALIVE_FRAME);
    });
  }
  for (let i = 1; i <= s.live; i++) {
    const o = s.head + i;
    steps.push(() => {
      pushed.push(o);
      conn.push(frame("message", "l", id(o)));
    });
  }
  let settled = false;
  let ended = false;
  let endedDuringReplay = false;
  let nonReplayObserved = false;
  for (let i = 0; i < steps.length; i++) {
    if (i === s.endAt) {
      ended = true;
      endedDuringReplay = i < replaySteps;
      conn.end();
      await flush();
      break;
    }
    if (i === s.settleAt && h.runs.length > 0 && !settled) {
      settled = true;
      for (const run of h.runs) {
        run.resolve();
      }
      await flush();
    }
    steps[i]!();
    if (i >= replaySteps) {
      nonReplayObserved = true;
    }
    await flush();
  }
  if (!ended && s.endAt === steps.length) {
    ended = true;
    conn.end();
    await flush();
  }
  if (!settled && s.settleAt >= steps.length && h.runs.length > 0 && !ended) {
    settled = true;
    for (const run of h.runs) {
      run.resolve();
    }
    await flush();
  }
  const delivered = h.frames
    .slice(deliveredBefore)
    .map((f) => f.id)
    .filter((c): c is { epoch: string; offset: string } => c !== null)
    .map((c) => Number(c.offset));
  return {
    h,
    pushed,
    delivered,
    nonReplayObserved,
    settled,
    ended,
    endedDuringReplay,
    revalidateHello,
  };
}

function offset(h: Harness): number {
  const c = h.stream.cursor();
  return c === null ? -1 : Number(c.offset);
}

describe("cursor discipline (Invariant C)", () => {
  beforeEach(() => {
    vi.useFakeTimers(FAKE_CLOCK);
    vi.spyOn(Math, "random").mockReturnValue(0);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("the cursor never passes an undelivered frame except by a non-resumed adoption followed by revalidate('hello')", async () => {
    await fc.assert(
      fc.asyncProperty(scenarioArb, async (s) => {
        const o = await play(s);
        const cursor = offset(o.h);
        for (const f of o.pushed) {
          if (f <= cursor) {
            expect(o.delivered).toContain(f);
          }
        }
        if (!s.resumed) {
          expect(cursor).toBeGreaterThanOrEqual(s.head);
          expect(o.revalidateHello).toBe(true);
        }
        expect(o.h.stream.cursor()).not.toBeNull();
        o.h.stream.stop();
      }),
      RUNS,
    );
  });

  it("cursor.offset >= hello.head after the first non-replay frame is delivered or its marker drained", async () => {
    await fc.assert(
      fc.asyncProperty(scenarioArb, async (s) => {
        const o = await play(s);
        const drained = o.settled || (!s.hold && s.resumed) || !s.resumed;
        if (o.nonReplayObserved && drained && !o.ended) {
          expect(offset(o.h)).toBeGreaterThanOrEqual(s.head);
        }
        expect(o.h.stream.cursor()).not.toBeNull();
        o.h.stream.stop();
      }),
      RUNS,
    );
  });

  it("a stream end mid-replay leaves the cursor at the last delivered frame", async () => {
    await fc.assert(
      fc.asyncProperty(
        scenarioArb.filter((s) => s.resumed && s.head > s.previous),
        async (s) => {
          const o = await play(s);
          if (o.endedDuringReplay) {
            const last = o.delivered.length === 0 ? s.previous : Math.max(...o.delivered);
            expect(offset(o.h)).toBe(last);
            expect(offset(o.h)).toBeLessThan(s.head);
          }
          expect(o.h.stream.cursor()).not.toBeNull();
          o.h.stream.stop();
        },
      ),
      RUNS,
    );
  });

  it("a held resumed replay followed by a keepalive and a stream end leaves the cursor at the last delivered frame", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 6 }),
        fc.integer({ min: 1, max: 6 }),
        async (previous, extra) => {
          const h = await establish(previous);
          const conn = h.last();
          conn.push(helloFrame({ resumed: true, head: String(previous + extra) }));
          await flush();
          h.visibility.emit("pageshow");
          await flush();
          expect(h.runs).toHaveLength(2);
          for (let o = previous + 1; o <= previous + extra; o++) {
            conn.push(frame("message", "r", id(o)));
          }
          conn.push(KEEPALIVE_FRAME);
          await flush();
          const deliveredBefore = h.frames.length;
          conn.end();
          await flush();
          expect(h.frames.length).toBe(deliveredBefore);
          expect(offset(h)).toBe(previous);
          expect(h.ofKind("held_discarded").at(-1)).toMatchObject({ cause: "stream_ended" });
          h.runs[1]!.resolve();
          await flush();
          expect(h.frames.length).toBe(deliveredBefore);
          vi.advanceTimersByTime(1500);
          await flush();
          expect(h.last().request.headers["Last-Event-ID"]).toBe(id(previous));
          h.stream.stop();
        },
      ),
      RUNS,
    );
  });

  it("a rejected revalidate('hello') is followed by another on the next resumed connected, a resolved one is not", async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 0, max: 6 }), async (reject, head) => {
        const h = harness();
        const first = await h.open({ head: String(head) });
        expect(h.runs.map((r) => r.ctx.cause)).toEqual(["hello"]);
        if (reject) {
          h.runs[0]!.reject(new Error("digest failed"));
        } else {
          h.runs[0]!.resolve();
        }
        await flush();
        if (!reject) {
          vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
          first.end();
          await flush();
        } else {
          expect(first.request.aborted).toBe(true);
          vi.advanceTimersByTime(1500);
          await flush();
        }
        h.last().push(helloFrame({ resumed: true, head: String(head) }));
        await flush();
        expect(h.runs.map((r) => r.ctx.cause)).toEqual(reject ? ["hello", "hello"] : ["hello"]);
        h.stream.stop();
      }),
      RUNS,
    );
  });
});
