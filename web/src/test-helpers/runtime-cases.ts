import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { type DigestResult, createDigestClient } from "../digest.js";
import type { LifecycleEvent } from "../lifecycle.js";
import { createOnlineManager } from "../online.js";
import { type Frame, type RevalidateContext, type Stream, createStream } from "../stream.js";
import { DEFAULT_RETRY_MS, DEFAULT_TIMING, MAX_FRAME_BYTES, type TimingConfig } from "../timing.js";
import { type Subject, type VersionMap, createVersionMap } from "../versions.js";
import { createVisibilityManager } from "../visibility.js";
import { type FixtureClient, FixtureError, fixtureClient } from "./fixture-client.js";
import { FAKE_CLOCK } from "./scripted-fetch.js";
import {
  type FakeOnline,
  type FakeVisibility,
  fakeOnline,
  fakeVisibility,
} from "./stream-harness.js";

export interface CaseEnvironment {
  readonly url: string;
  readonly fetch: typeof fetch;
}

const realSetTimeout = globalThis.setTimeout.bind(globalThis);
const realNow = (): number => performance.now();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    realSetTimeout(resolve, ms);
  });
}

/** Polls a predicate on the real clock; fake timers never advance it. */
async function waitFor(
  what: string,
  pred: () => boolean | Promise<boolean>,
  timeoutMs = 4000,
): Promise<void> {
  const deadline = realNow() + timeoutMs;
  while (!(await pred())) {
    if (realNow() > deadline) {
      throw new Error(`timed out waiting for ${what}`);
    }
    await sleep(10);
  }
}

interface PendingRun {
  readonly ctx: RevalidateContext;
  resolve(): void;
  reject(error: Error): void;
}

type Body = (ctx: RevalidateContext, app: App) => Promise<void>;

interface App {
  readonly stream: Stream;
  readonly versions: VersionMap;
  readonly events: LifecycleEvent[];
  /** Every frame, the fixture's id-less connected frame included. */
  readonly frames: Frame[];
  /** Subjects the loaders have fetched at least once; the full body refetches these. */
  readonly subjects: Map<string, Subject>;
  readonly runs: PendingRun[];
  readonly digests: DigestResult[];
  readonly fetched: string[];
  readonly visibility: FakeVisibility;
  readonly online: FakeOnline;
  full: number;
  ofKind<K extends LifecycleEvent["kind"]>(kind: K): Extract<LifecycleEvent, { kind: K }>[];
  /** Id-bearing frames delivered so far, in order. */
  delivered(): Frame[];
  /** Resolves the n-th manual run and lets the runtime settle it. */
  settle(index: number): Promise<void>;
  /** Loads a subject through the REST stub and observes its stamp, epoch included. */
  load(subject: Subject, signal?: AbortSignal): Promise<void>;
  state(): string;
  waitOpen(): Promise<void>;
  waitState(kind: string): Promise<void>;
  waitEvent(kind: LifecycleEvent["kind"], count?: number): Promise<void>;
}

interface AppOptions {
  readonly visible?: boolean;
  readonly online?: boolean;
  /** "manual": runs are settled by the test; "digest": the digest-driven body; a function: custom. */
  readonly body?: "manual" | "digest" | Body;
  readonly timing?: Partial<TimingConfig>;
  readonly alive?: { url: string; everyBeats?: number };
  readonly headers?: Record<string, string>;
  readonly minWire?: number;
  readonly maxWire?: number;
  readonly onFrame?: (frame: Frame) => void;
  readonly fetch?: typeof fetch;
  readonly versions?: VersionMap;
}

function subjectKey(s: Subject): string {
  return `${s.kind}/${s.ref}`;
}

/** A subject ref no other case or project has mutated in the shared fixture store. */
function uniq(prefix: string): string {
  return `${prefix}-${String(Date.now())}-${Math.random().toString(36).slice(2, 8)}`;
}

export function runtimeCases(get: () => CaseEnvironment): void {
  let fixture: FixtureClient;
  let apps: App[] = [];

  function build(opts: AppOptions = {}): App {
    const env = get();
    const doFetch = opts.fetch ?? env.fetch;
    const events: LifecycleEvent[] = [];
    const frames: Frame[] = [];
    const runs: PendingRun[] = [];
    const digests: DigestResult[] = [];
    const fetched: string[] = [];
    const subjects = new Map<string, Subject>();
    const visibility = fakeVisibility(opts.visible ?? true);
    const online = fakeOnline(opts.online ?? true);
    const versions = opts.versions ?? createVersionMap();
    const digest = createDigestClient({ url: `${env.url}/digest`, fetch: doFetch });

    async function load(subject: Subject, signal?: AbortSignal): Promise<void> {
      fetched.push(subjectKey(subject));
      subjects.set(subjectKey(subject), subject);
      const response = await doFetch(`${env.url}/rest/${subject.kind}/${subject.ref}`, {
        ...(signal === undefined ? {} : { signal }),
      });
      if (!response.ok) {
        throw new FixtureError("/rest", response.status, await response.text());
      }
      const body = (await response.json()) as { version: string; epoch: string };
      versions.observe(subject, body.version, body.epoch);
    }

    const digestBody: Body = async (ctx, app) => {
      const known = [...subjects.values()];
      if (ctx.full) {
        app.full++;
        await Promise.all(known.map((s) => load(s, ctx.signal)));
        return;
      }
      const result = await digest.check(versions.snapshot(), ctx.signal);
      digests.push(result);
      if (result.kind === "must_refetch") {
        versions.bind(result.epoch);
        app.full++;
        await Promise.all(known.map((s) => load(s, ctx.signal)));
        return;
      }
      for (const removed of result.removed) {
        versions.forget(removed);
      }
      await Promise.all(result.changed.map((s) => load({ kind: s.kind, ref: s.ref }, ctx.signal)));
    };

    const body = opts.body ?? "digest";
    const app: App = {
      stream: createStream({
        url: `${env.url}/events`,
        fetch: doFetch,
        headers: opts.headers ?? {},
        visibility: createVisibilityManager(visibility.source),
        online: createOnlineManager(online.source),
        versions,
        ...(opts.timing !== undefined ? { timing: opts.timing } : {}),
        ...(opts.alive !== undefined ? { alive: opts.alive } : {}),
        ...(opts.minWire !== undefined ? { minWire: opts.minWire } : {}),
        ...(opts.maxWire !== undefined ? { maxWire: opts.maxWire } : {}),
        onFrame(frame) {
          frames.push(frame);
          opts.onFrame?.(frame);
        },
        onLifecycle(ev) {
          events.push(ev);
        },
        revalidate(ctx) {
          if (body === "manual") {
            return new Promise<void>((resolve, reject) => {
              runs.push({ ctx, resolve, reject });
            });
          }
          if (body === "digest") {
            return digestBody(ctx, app);
          }
          return body(ctx, app);
        },
      }),
      versions,
      events,
      frames,
      subjects,
      runs,
      digests,
      fetched,
      visibility,
      online,
      full: 0,
      ofKind(kind) {
        return events.filter((e) => e.kind === kind) as Extract<
          LifecycleEvent,
          { kind: typeof kind }
        >[];
      },
      delivered: () => frames.filter((f) => f.id !== null),
      async settle(index) {
        const run = runs[index];
        if (run === undefined) {
          throw new Error(`no manual run #${String(index)}`);
        }
        run.resolve();
        await sleep(0);
      },
      load,
      state: () => app.stream.state().kind,
      waitOpen: () => app.waitState("open"),
      waitState: (kind) => waitFor(`state ${kind}`, () => app.stream.state().kind === kind),
      waitEvent: (kind, count = 1) =>
        waitFor(`${String(count)} ${kind} event(s)`, () => app.ofKind(kind).length >= count),
    };
    apps.push(app);
    return app;
  }

  async function backoffToOpen(app: App): Promise<void> {
    await app.waitState("backoff");
    vi.advanceTimersByTime(DEFAULT_RETRY_MS);
    await app.waitOpen();
  }

  beforeEach(async () => {
    vi.useFakeTimers(FAKE_CLOCK);
    vi.spyOn(Math, "random").mockReturnValue(0);
    fixture = fixtureClient(get().url, get().fetch);
    await fixture.reset();
  });

  afterEach(async () => {
    for (const app of apps) {
      app.stream.stop();
    }
    apps = [];
    vi.useRealTimers();
    await fixture.reset();
    await sleep(50);
  });

  it("half-open: the fixture stalls and the watchdog fires at 45 s on a faked clock", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    const before = (await fixture.state()).v3_connects;
    await fixture.stall(true);
    vi.advanceTimersByTime(44_999);
    expect(app.state()).toBe("open");
    vi.advanceTimersByTime(1);
    expect(app.ofKind("watchdog")).toEqual([{ kind: "watchdog", sinceLastByteMs: 45_000 }]);
    expect(app.state()).toBe("connecting");
    await waitFor("second connect", async () => (await fixture.state()).v3_connects > before);
    await fixture.stall(false);
    vi.advanceTimersByTime(DEFAULT_TIMING.helloTimeoutMs);
    await sleep(50);
    expect((await fixture.state()).v3_connects).toBeGreaterThan(before);
  });

  it("a slowly written 200 KiB frame does not trip the watchdog", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    vi.advanceTimersByTime(40_000);
    await fixture.publish({ size: 200 * 1024 });
    await waitFor("large frame", () => app.delivered().length === 1);
    vi.advanceTimersByTime(40_000);
    await fixture.publish({ size: 200 * 1024 });
    await waitFor("second large frame", () => app.delivered().length === 2);
    expect(app.ofKind("watchdog")).toEqual([]);
    expect(app.state()).toBe("open");
    expect(app.delivered()[0]!.data.length).toBeGreaterThan(200 * 1024 - 100);
  });

  it("sse:reset keeps the cursor", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    await fixture.publish({ count: 2 });
    await waitFor("frames", () => app.delivered().length === 2);
    const held = app.stream.cursor();
    expect(held).not.toBeNull();
    await fixture.restart();
    await app.waitEvent("reset");
    expect(app.ofKind("reset")).toEqual([{ kind: "reset", reason: "reset:shutdown" }]);
    expect(app.stream.cursor()).toEqual(held);
    await backoffToOpen(app);
    const hellos = app.ofKind("hello");
    expect(hellos).toHaveLength(2);
    expect(hellos[1]!.verdict).toBe("epoch_changed");
    expect(hellos[1]!.epoch).not.toBe(hellos[0]!.epoch);
  });

  it("sse:reset shutdown on a stable connection backs off", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
    await fixture.restart();
    await app.waitState("backoff");
    const state = app.stream.state();
    if (state.kind === "backoff") {
      expect(state.until - Date.now()).toBe(1500);
    }
    expect(app.ofKind("state").map((e) => e.to)).toEqual(["connecting", "open", "backoff"]);
  });

  async function hiddenRestart(order: "hello-first" | "digest-first"): Promise<void> {
    const app = build();
    await app.load({ kind: "chat", ref: uniq(`${order}-1`) });
    await app.load({ kind: "chat", ref: uniq(`${order}-2`) });
    app.stream.start();
    await app.waitOpen();
    await waitFor("boot digest", () => app.digests.length === 1);
    expect(app.full).toBe(0);
    app.visibility.emit("hidden");
    await fixture.restart();
    await app.waitState("hidden_closed");
    if (order === "hello-first") {
      await fixture.delayDigest(300);
    } else {
      await fixture.delayHello(300);
    }
    app.visibility.emit("visible");
    await waitFor("second digest", () => app.digests.length >= 2, 6000);
    await app.waitOpen();
    await waitFor("queued hello digest", () => app.digests.length >= 3, 6000);
    await waitFor("settled", () => app.fetched.length === 4);
    expect(app.full).toBe(1);
    expect(app.ofKind("stale_stamp")).toEqual([]);
    expect(app.digests[1]!.kind).toBe("must_refetch");
    expect(app.digests[2]).toMatchObject({ kind: "ok", changed: [], removed: [] });
    const helloRuns = app.ofKind("revalidate").filter((e) => e.cause === "hello");
    expect(helloRuns.map((e) => e.full)).toEqual([false, false]);
  }

  it("fixture restart with the tab hidden: one full reconciliation, no stale_stamp, hello-first and digest-first", async () => {
    await hiddenRestart("hello-first");
    await fixture.reset();
    await hiddenRestart("digest-first");
  }, 20_000);

  it("fixture restart with the tab visible: revalidate full without a digest call", async () => {
    const app = build();
    await app.load({ kind: "chat", ref: uniq("vis") });
    app.stream.start();
    await app.waitOpen();
    await waitFor("boot digest", () => app.digests.length === 1);
    await fixture.restart();
    await backoffToOpen(app);
    await waitFor("full run", () => app.full === 1);
    await waitFor("refetch", () => app.fetched.length === 2);
    expect(app.digests).toHaveLength(1);
    expect(app.ofKind("revalidate").at(-1)).toEqual({
      kind: "revalidate",
      cause: "hello",
      full: true,
    });
  });

  it("a hook sleeping past helloTimeoutMs still yields connected then eof", async () => {
    await fixture.hookSleep(400, true);
    const app = build({ body: "manual", timing: { helloTimeoutMs: 200 } });
    app.stream.start();
    await app.waitOpen();
    vi.advanceTimersByTime(200);
    expect(app.state()).toBe("open");
    expect(app.ofKind("connect_failed")).toEqual([]);
    await app.waitState("backoff");
    expect(app.ofKind("state").map((e) => e.to)).toEqual(["connecting", "open", "backoff"]);
  });

  it("boot loaders before the first hello bind the map and are not refetched", async () => {
    const app = build();
    const boot = uniq("boot");
    await app.load({ kind: "chat", ref: boot });
    const epoch = app.versions.epoch();
    expect(epoch).not.toBeNull();
    app.stream.start();
    await app.waitOpen();
    await waitFor("boot digest", () => app.digests.length === 1);
    expect(app.ofKind("hello")[0]!.epoch).toBe(epoch);
    expect(app.digests[0]).toMatchObject({ kind: "ok", changed: [], removed: [] });
    expect(app.fetched).toEqual([`chat/${boot}`]);
    expect(app.full).toBe(0);
  });

  it("an injected VisibilitySource drives hidden-close and wake-revalidate", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    app.visibility.emit("hidden");
    vi.advanceTimersByTime(DEFAULT_TIMING.hiddenCloseAfterMs);
    expect(app.state()).toBe("hidden_closed");
    expect(app.ofKind("hidden_closed")).toEqual([{ kind: "hidden_closed", arm: "timer" }]);
    await waitFor("server saw the close", async () => (await fixture.state()).clients === 0);
    app.visibility.emit("visible");
    expect(app.runs.map((r) => r.ctx.cause)).toEqual(["hello", "visible"]);
    await app.waitOpen();
    expect(app.ofKind("reopened")).toHaveLength(1);
    expect(app.ofKind("hello")[1]!.resumed).toBe(true);
  });

  it("single-flight across two wakes", async () => {
    await fixture.delayDigest(300);
    const app = build();
    app.stream.start();
    await app.waitOpen();
    await waitFor("boot digest", () => app.digests.length === 1);
    const started = app.ofKind("revalidate").length;
    const originalDigests = app.digests.length;
    app.visibility.emit("pageshow");
    vi.advanceTimersByTime(DEFAULT_TIMING.wakeThrottleMs);
    app.visibility.emit("pageshow");
    expect(app.ofKind("revalidate").slice(started)).toHaveLength(1);
    await waitFor("both runs", () => app.digests.length === originalDigests + 2, 6000);
    const causes = app.ofKind("revalidate").slice(started);
    expect(causes.map((e) => e.cause)).toEqual(["pageshow", "pageshow"]);
    expect(app.digests.length).toBe(originalDigests + 2);
  });

  it("a full queued behind visible runs as full", async () => {
    const app = build({ body: "manual", visible: false });
    await app.load({ kind: "chat", ref: uniq("queued") });
    await fixture.restart();
    app.stream.start();
    expect(app.state()).toBe("hidden_closed");
    app.visibility.emit("visible");
    expect(app.runs.map((r) => r.ctx.cause)).toEqual(["visible"]);
    await app.waitOpen();
    expect(app.ofKind("hello")[0]!.resumed).toBe(false);
    vi.advanceTimersByTime(DEFAULT_TIMING.wakeThrottleMs);
    app.visibility.emit("pageshow");
    expect(app.runs).toHaveLength(1);
    await app.settle(0);
    await waitFor("queued run", () => app.runs.length === 2);
    expect(app.runs[1]!.ctx).toMatchObject({ cause: "pageshow", full: true });
  });

  it("hold and drain order", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    const published = await fixture.publish({ count: 5, data: "x" });
    await sleep(200);
    expect(app.frames).toEqual([]);
    await app.settle(0);
    await waitFor("drain", () => app.delivered().length === 5);
    expect(app.delivered().map((f) => f.id?.offset)).toEqual(published.offsets);
    expect(app.frames[0]).toEqual({ type: "message", data: '{"type":"connected"}', id: null });
    expect(app.ofKind("drain")).toEqual([{ kind: "drain", length: 6, dropped: 0 }]);
  });

  it("abort during a hold delivers nothing from the old generation", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await fixture.publish({ count: 3 });
    await sleep(200);
    app.visibility.emit("pagehide");
    expect(app.state()).toBe("hidden_closed");
    expect(app.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "abort", length: 4 },
    ]);
    await app.settle(0);
    await sleep(100);
    expect(app.frames).toEqual([]);
    app.visibility.emit("visible");
    await app.waitOpen();
    expect(app.ofKind("hello")[1]!.resumed).toBe(true);
    expect(app.runs).toHaveLength(2);
    await app.settle(1);
    await waitFor("replay", () => app.delivered().length === 3);
    expect(app.ofKind("drain")).toEqual([{ kind: "drain", length: 5, dropped: 0 }]);
  });

  it("hold_timeout replays the held span", async () => {
    let seen = 0;
    const app = build({
      body: (ctx, a) => {
        if (seen++ === 0) {
          return new Promise<void>((resolve, reject) => {
            a.runs.push({ ctx, resolve, reject });
          });
        }
        return Promise.resolve();
      },
    });
    app.stream.start();
    await app.waitOpen();
    await fixture.publish({ count: 3 });
    await sleep(200);
    expect(app.frames).toEqual([]);
    vi.advanceTimersByTime(DEFAULT_TIMING.revalidateTimeoutMs);
    expect(app.ofKind("revalidate_timeout")).toHaveLength(1);
    expect(app.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "hold_timeout", length: 4 },
    ]);
    expect(app.runs[0]!.ctx.signal.aborted).toBe(true);
    await app.waitOpen();
    expect(app.ofKind("hello")[1]!.resumed).toBe(true);
    await waitFor("replayed", () => app.delivered().length === 3);
    expect(app.ofKind("revalidate").map((e) => e.cause)).toEqual(["hello", "hello"]);
  });

  it("2001 held frames end with hold_overflow and are replayed", async () => {
    let seen = 0;
    const app = build({
      body: (ctx, a) => {
        if (seen++ === 0) {
          return new Promise<void>((resolve, reject) => {
            a.runs.push({ ctx, resolve, reject });
          });
        }
        return Promise.resolve();
      },
    });
    app.stream.start();
    await app.waitOpen();
    await fixture.publish({ count: 2001, data: "x" });
    await app.waitEvent("held_discarded");
    expect(app.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "hold_overflow", length: 2001 },
    ]);
    await backoffToOpen(app);
    const hello = app.ofKind("hello")[1]!;
    expect(hello.resumed).toBe(false);
    expect(hello.verdict).toMatch(/^gap_/);
    expect(app.stream.cursor()).toEqual({ epoch: hello.epoch, offset: hello.head });
    expect(app.ofKind("revalidate").map((e) => e.cause)).toEqual(["hello"]);
    await app.settle(0);
    await waitFor("queued hello run", () => app.ofKind("revalidate").length === 2);
    expect(app.ofKind("revalidate")[1]).toEqual({
      kind: "revalidate",
      cause: "hello",
      full: false,
    });
  });

  it("65 mebibyte frames end with hold_overflow at the 65th", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    for (let i = 0; i < 65; i++) {
      await fixture.publish({ size: MAX_FRAME_BYTES });
    }
    await app.waitEvent("held_discarded");
    expect(app.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "hold_overflow", length: 65 },
    ]);
    expect(app.frames).toEqual([]);
  }, 30_000);

  it("an aborted attempt's late EOF does not disturb its replacement", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    app.stream.reconnect();
    await app.waitOpen();
    await sleep(300);
    expect(app.stream.state()).toMatchObject({ kind: "open", generation: 2 });
    expect(app.ofKind("state").map((e) => e.to)).toEqual([
      "connecting",
      "open",
      "connecting",
      "open",
    ]);
    expect(app.ofKind("stale_event")).toEqual([]);
    expect((await fixture.state()).clients).toBe(1);
  });

  it("timeout_hello on a stalled retry line and on headers only", async () => {
    await fixture.stall(true, 1);
    const stalled = build({ body: "manual" });
    stalled.stream.start();
    await sleep(300);
    vi.advanceTimersByTime(DEFAULT_TIMING.helloTimeoutMs);
    await stalled.waitEvent("connect_failed");
    expect(stalled.ofKind("connect_failed")).toEqual([
      { kind: "connect_failed", reason: { kind: "timeout_hello" } },
    ]);
    expect(stalled.state()).toBe("backoff");
    stalled.stream.stop();

    // Headers alone may sit in a dev-server proxy until a body byte follows, so the browser
    // twin can see the connect deadline instead of the hello deadline; both are the covered gap.
    await fixture.stall(true, 0);
    const headersOnly = build({ body: "manual" });
    headersOnly.stream.start();
    await sleep(300);
    vi.advanceTimersByTime(DEFAULT_TIMING.connectTimeoutMs);
    await headersOnly.waitEvent("connect_failed");
    const reason = headersOnly.ofKind("connect_failed")[0]!.reason.kind;
    expect(["timeout_hello", "timeout_headers"]).toContain(reason);
    await fixture.stall(false);
  });

  it("wire 0 and wire 2 back off and keep the cursor", async () => {
    const below = build({ body: "manual", maxWire: 0 });
    below.stream.start();
    await below.waitEvent("wire_unsupported");
    expect(below.ofKind("wire_unsupported")).toEqual([{ kind: "wire_unsupported", wire: 1 }]);
    expect(below.state()).toBe("backoff");
    expect(below.stream.cursor()).toBeNull();
    expect(below.ofKind("hello")).toEqual([]);

    const above = build({ body: "manual", minWire: 2, maxWire: 2 });
    above.stream.start();
    await above.waitEvent("wire_unsupported");
    expect(above.state()).toBe("backoff");
    expect(above.stream.cursor()).toBeNull();
  });

  it("a changed subject whose refetch fails once is changed again next digest", async () => {
    const app = build();
    const retry = uniq("retry");
    await app.load({ kind: "chat", ref: retry });
    app.stream.start();
    await app.waitOpen();
    await waitFor("boot digest", () => app.digests.length === 1);
    await fixture.mutate("chat", retry);
    await fixture.restFailOnce();
    app.visibility.emit("pageshow");
    await app.waitEvent("revalidate_failed");
    expect(app.ofKind("revalidate_failed")[0]).toMatchObject({ latch: true, ended: true });
    expect(app.digests[1]).toMatchObject({
      kind: "ok",
      changed: [{ kind: "chat", ref: retry, version: "2" }],
    });
    expect(app.versions.snapshot().held).toEqual([{ kind: "chat", ref: retry, version: "1" }]);
    await backoffToOpen(app);
    await waitFor("third digest", () => app.digests.length === 3);
    expect(app.digests[2]).toMatchObject({
      kind: "ok",
      changed: [{ kind: "chat", ref: retry, version: "2" }],
    });
    await waitFor("refetched", () => app.versions.snapshot().held[0]?.version === "2");
  });

  it("resetCursor then reconnect inside a revalidate body completes its GET and runs one queued digest", async () => {
    let bodies = 0;
    let getCompleted = false;
    const app = build({
      body: async (ctx, a) => {
        bodies++;
        if (bodies === 1) {
          await a.load({ kind: "chat", ref: uniq("reset") }, ctx.signal);
          getCompleted = !ctx.signal.aborted;
          a.stream.resetCursor();
          a.stream.reconnect();
          return;
        }
        const result = await createDigestClient({
          url: `${get().url}/digest`,
          fetch: get().fetch,
        }).check(a.versions.snapshot(), ctx.signal);
        a.digests.push(result);
      },
    });
    app.stream.start();
    await app.waitOpen();
    await waitFor("second hello", () => app.ofKind("hello").length === 2);
    expect(getCompleted).toBe(true);
    expect(app.ofKind("hello")[1]!.verdict).toBe("fresh");
    await waitFor("queued digest", () => app.digests.length === 1);
    await sleep(300);
    expect(bodies).toBe(2);
    expect(app.digests[0]).toMatchObject({ kind: "ok", changed: [] });
  });

  it("revalidate('hello') failing while open ends the connection and the latch fires on the resumed reconnect", async () => {
    let bodies = 0;
    const app = build({
      body: () => {
        bodies++;
        return bodies === 1 ? Promise.reject(new Error("digest 500")) : Promise.resolve();
      },
    });
    app.stream.start();
    await app.waitEvent("revalidate_failed");
    expect(app.ofKind("revalidate_failed")).toEqual([
      { kind: "revalidate_failed", cause: "digest 500", latch: true, ended: true },
    ]);
    expect(app.ofKind("state").map((e) => e.to)).toEqual(["connecting", "open", "backoff"]);
    await backoffToOpen(app);
    expect(app.ofKind("hello")[1]!.resumed).toBe(true);
    expect(app.ofKind("revalidate").map((e) => e.cause)).toEqual(["hello", "hello"]);
    expect(bodies).toBe(2);
  });

  it("the same failure while offline sets the latch only", async () => {
    let bodies = 0;
    const app = build({
      online: false,
      body: () => {
        bodies++;
        return bodies === 1 ? Promise.reject(new Error("no route")) : Promise.resolve();
      },
    });
    app.stream.start();
    expect(app.state()).toBe("offline");
    app.visibility.emit("visible");
    await app.waitEvent("revalidate_failed");
    expect(app.ofKind("revalidate_failed")).toEqual([
      { kind: "revalidate_failed", cause: "no route", latch: true, ended: false },
    ]);
    expect(app.state()).toBe("offline");
    app.online.emit(true);
    await app.waitOpen();
    await waitFor("latch run", () => bodies === 3);
    expect(app.ofKind("revalidate").map((e) => e.cause)).toEqual(["visible", "online", "hello"]);
  });

  it("a 256-subject worst-case batch is accepted", async () => {
    const digest = createDigestClient({ url: `${get().url}/digest`, fetch: get().fetch });
    const held = Array.from({ length: 256 }, (_, i) => ({
      kind: "k".repeat(32),
      ref: `${"\\".repeat(508)}${String(i).padStart(4, "0")}`,
      version: '"'.repeat(64),
    }));
    const epoch = (await fixture.state()).position.epoch;
    const result = await digest.check({ epoch, held });
    expect(result.kind).toBe("ok");
    if (result.kind === "ok") {
      expect(result.removed).toHaveLength(256);
      expect(result.changed).toEqual([]);
    }
  });

  it("the request carries SSE-Wire: 1", async () => {
    const before = await fixture.state();
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    const after = await fixture.state();
    expect(after.v3_connects - before.v3_connects).toBe(1);
    expect(after.legacy_connects - before.legacy_connects).toBe(0);
  });

  it("a frame of exactly MaxFrameBytes is delivered and one byte more is refused server-side", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    const ok = await fixture.publish({ size: MAX_FRAME_BYTES });
    await waitFor("max frame", () => app.delivered().length === 1);
    const cursor = app.stream.cursor();
    expect(cursor?.offset).toBe(ok.offsets[0]);
    expect(new TextEncoder().encode(app.delivered()[0]!.data).length).toBeGreaterThan(
      MAX_FRAME_BYTES - 100,
    );
    await expect(fixture.publish({ size: MAX_FRAME_BYTES + 1 })).rejects.toMatchObject({
      status: 422,
    });
    await sleep(200);
    expect(app.delivered()).toHaveLength(1);
    expect(app.stream.cursor()).toEqual(cursor);
    expect((await fixture.state()).position.head).toBe(ok.head);
  });

  it("restarts mint a new epoch read from the hello", async () => {
    const app = build({ body: "manual" });
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    const first = app.ofKind("hello")[0]!.epoch;
    const minted = await fixture.restart();
    await backoffToOpen(app);
    const second = app.ofKind("hello")[1]!.epoch;
    expect(second).toBe(minted);
    expect(second).not.toBe(first);
    expect(second).toMatch(/^[0-9a-f]{16}$/);
  });

  it("presence: connected with tag, one acknowledgement per keepalive, gone after the hidden close, gone at aliveWindow while ClientCount reads one when acks stop, alive again on the next ack", async () => {
    const tag = `tag_${String(Date.now())}`;
    let suppress = false;
    const env = get();
    const gated: typeof fetch = (input, init) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      if (suppress && init?.method === "POST" && url.endsWith("/alive")) {
        return Promise.resolve(new Response(null, { status: 204 }));
      }
      return env.fetch(input, init);
    };
    const app = build({
      body: "manual",
      fetch: gated,
      headers: { "SSE-Client": tag },
      alive: { url: `${env.url}/alive` },
    });
    const row = async (): Promise<{ connected: number; gone: boolean } | undefined> =>
      (await fixture.state()).presence.find((p) => p.tag === tag);
    app.stream.start();
    await app.waitOpen();
    await app.settle(0);
    let state = await fixture.state();
    expect(state.events.filter((e) => e.kind === "connected" && e.tag === tag)).toHaveLength(1);
    expect(await row()).toMatchObject({ connected: 1, gone: false });
    const alive0 = state.transitions.alive;
    await waitFor("first acknowledgement", () => app.ofKind("alive_ack").length === 1, 20_000);
    expect(app.ofKind("alive_ack")[0]).toEqual({ kind: "alive_ack", ok: true, status: 204 });
    app.visibility.emit("hidden");
    vi.advanceTimersByTime(DEFAULT_TIMING.hiddenCloseAfterMs);
    expect(app.state()).toBe("hidden_closed");
    await waitFor("row gone", async () => (await row())?.gone === true);
    state = await fixture.state();
    const closed = state.events.filter((e) => e.kind === "disconnected" && e.tag === tag);
    expect(closed).toHaveLength(1);
    expect(closed[0]!.cause).toBe("closed");
    expect(await row()).toMatchObject({ connected: 0, gone: true });
    const expired0 = state.transitions.expired;

    suppress = true;
    await fixture.aliveWindow(1500);
    app.visibility.emit("visible");
    await app.waitOpen();
    expect(await row()).toMatchObject({ connected: 1, gone: false });
    await sleep(1700);
    state = await fixture.state();
    expect(state.clients).toBe(1);
    expect(await row()).toMatchObject({ connected: 1, gone: true });
    expect(state.transitions.expired).toBe(expired0 + 1);
    suppress = false;
    await waitFor("second acknowledgement", () => app.ofKind("alive_ack").length >= 2, 20_000);
    await sleep(50);
    state = await fixture.state();
    expect(await row()).toMatchObject({ connected: 1, gone: false });
    expect(state.transitions.alive).toBe(alive0 + 2);
  }, 60_000);
}
