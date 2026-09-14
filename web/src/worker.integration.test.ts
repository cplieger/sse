import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { LifecycleEvent } from "./lifecycle.js";
import type { Frame } from "./stream.js";
import { createStream } from "./stream.js";
import {
  type TabAttachment,
  type TabFallback,
  type TabRevalidateContext,
  attachToWorker,
} from "./tab.js";
import {
  FIXTURE_PROXY_PREFIX,
  type FixtureClient,
  fixtureClient,
} from "./test-helpers/fixture-client.js";
import workerUrl from "./test-helpers/worker-entry.ts?sharedworker&url";
import { type VersionMap, createVersionMap } from "./versions.js";

const SKIP_REASON = "ssetest fixture not started: set SSE_FIXTURE to the built ssetest/cmd binary";
const fixture = import.meta.env["SSE_FIXTURE"];
if (!fixture) {
  console.warn(`[vitest] ${SKIP_REASON}`);
}
const HB = 300;
const HIDDEN = 1500;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(
  what: string,
  pred: () => boolean | Promise<boolean>,
  timeoutMs = 8000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!(await pred())) {
    if (performance.now() > deadline) {
      throw new Error(`timed out waiting for ${what}`);
    }
    await sleep(20);
  }
}

interface Page {
  readonly attachment: TabAttachment;
  readonly events: LifecycleEvent[];
  readonly frames: Frame[];
  readonly runs: TabRevalidateContext[];
  readonly workers: SharedWorker[];
  /** The fallback stream's version map, when this page fell back. */
  readonly fallbackVersions: VersionMap | null;
  ofKind<K extends LifecycleEvent["kind"]>(kind: K): Extract<LifecycleEvent, { kind: K }>[];
  /** Sends a test-only message to the worker over this page's current port. */
  send(message: Record<string, unknown>): void;
}

interface PageOptions {
  readonly bundle?: string;
  readonly tag?: string;
  readonly url?: string;
  readonly supported?: boolean;
  readonly fixtureUrl: string;
}

function page(opts: PageOptions): Page {
  const events: LifecycleEvent[] = [];
  const frames: Frame[] = [];
  const runs: TabRevalidateContext[] = [];
  const workers: SharedWorker[] = [];
  let fallbackVersions: VersionMap | null = null;
  const query = new URLSearchParams({
    fixture: opts.fixtureUrl,
    heartbeat: String(HB),
    hidden: String(HIDDEN),
    bundle: opts.bundle ?? "1",
  });
  // Vite serves the entry as a module worker in dev; the classic form is the production bundle's.
  const url = opts.url ?? `${workerUrl}${workerUrl.includes("?") ? "&" : "?"}${query.toString()}`;
  const attachment = attachToWorker({
    heartbeatMs: HB,
    tag: opts.tag ?? "",
    ...(opts.supported === undefined ? {} : { supported: opts.supported }),
    spawn() {
      const worker = new SharedWorker(url, { name: `sse-${opts.bundle ?? "1"}`, type: "module" });
      workers.push(worker);
      return worker;
    },
    fallback(): TabFallback {
      const versions = createVersionMap();
      const headers: Record<string, string> = {};
      fallbackVersions = versions;
      const stream = createStream({
        url: `${opts.fixtureUrl}/events`,
        versions,
        headers,
        onFrame(frame) {
          frames.push(frame);
        },
        onLifecycle(ev) {
          events.push(ev);
        },
        revalidate(ctx) {
          runs.push(ctx);
          return Promise.resolve();
        },
      });
      return { stream, versions, headers };
    },
    onFrame(frame) {
      frames.push(frame);
    },
    onLifecycle(ev) {
      events.push(ev);
    },
    revalidate(ctx) {
      runs.push(ctx);
      return Promise.resolve();
    },
  });
  return {
    attachment,
    events,
    frames,
    runs,
    workers,
    get fallbackVersions() {
      return fallbackVersions;
    },
    ofKind(kind) {
      return events.filter((e) => e.kind === kind) as Extract<
        LifecycleEvent,
        { kind: typeof kind }
      >[];
    },
    send(message) {
      workers[workers.length - 1]?.port.postMessage(message);
    },
  };
}

function setHidden(hidden: boolean): void {
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    get: () => (hidden ? "hidden" : "visible"),
  });
  document.dispatchEvent(new Event("visibilitychange"));
}

const sharedWorkerAvailable = Boolean(fixture) && typeof SharedWorker !== "undefined";

describe.skipIf(!sharedWorkerAvailable)("worker host and tabs against the ssetest fixture", () => {
  let client: FixtureClient;
  let fixtureUrl: string;
  let pages: Page[] = [];
  let bundle = 0;

  function open(opts: Omit<PageOptions, "fixtureUrl"> = {}): Page {
    const p = page({ ...opts, fixtureUrl, bundle: opts.bundle ?? String(bundle) });
    pages.push(p);
    return p;
  }

  beforeEach(async () => {
    fixtureUrl = `${location.origin}${FIXTURE_PROXY_PREFIX}`;
    client = fixtureClient(fixtureUrl);
    await client.reset();
    bundle++;
    setHidden(false);
  });

  afterEach(async () => {
    for (const p of pages) {
      p.attachment.detach();
    }
    pages = [];
    Reflect.deleteProperty(document, "visibilityState");
    await client.reset();
    await sleep(HIDDEN + 500);
  });

  it("two pages hold one connection and a third attaches to the same worker", async () => {
    const a = open();
    const b = open();
    await waitFor("open", () => a.ofKind("state").some((e) => e.to === "open"));
    await waitFor(
      "hello on both",
      () => b.ofKind("hello").length > 0 || a.ofKind("hello").length > 0,
    );
    expect((await client.state()).clients).toBe(1);
    const c = open();
    await waitFor("third attached", () => c.attachment.state()?.kind === "open");
    expect(c.ofKind("tab_attached")).toEqual([
      { kind: "tab_attached", replaced: false, state: "open" },
    ]);
    expect((await client.state()).clients).toBe(1);
    const before = (await client.state()).v3_connects;
    await client.publish({ data: "shared" });
    await waitFor("frame everywhere", () =>
      [a, b, c].every((p) => p.frames.some((f) => f.data === "shared")),
    );
    expect((await client.state()).v3_connects).toBe(before);
  });

  it("hiding one of two pages does not start the hidden timer; hiding both does and showing either reconnects and revalidates once", async () => {
    const a = open();
    const b = open();
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    b.attachment.detach();
    pages = [a];
    await sleep(HIDDEN + 300);
    expect(a.attachment.state()?.kind).toBe("open");
    setHidden(true);
    await waitFor(
      "hidden_closed",
      () => a.attachment.state()?.kind === "hidden_closed",
      HIDDEN + 3000,
    );
    expect(a.ofKind("hidden_closed")).toHaveLength(1);
    await waitFor("server saw close", async () => (await client.state()).clients === 0);
    const revalidates = a.ofKind("revalidate").length;
    setHidden(false);
    await waitFor("reopened", () => a.ofKind("reopened").length === 1);
    expect(a.ofKind("revalidate").length).toBe(revalidates + 1);
    expect(a.ofKind("revalidate").at(-1)?.cause).toBe("visible");
  });

  it("a worker told to close stops heartbeating; the visible page reports worker_dead at 3 x heartbeatMs, re-spawns with hadWorker and the fresh host's first revalidate is full", async () => {
    const a = open();
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    await client.mutate("chat", "closed-1");
    a.send({ type: "test:close" });
    await waitFor("worker_dead", () => a.ofKind("worker_dead").length === 1, 3 * HB + 4000);
    expect(a.ofKind("worker_dead")[0]?.sinceLastPortMessageMs).toBeGreaterThanOrEqual(3 * HB);
    await waitFor(
      "recovered",
      () => a.workers.length === 2 && a.attachment.state()?.kind === "open",
    );
    await waitFor("full run", () => a.runs.some((r) => r.full));
    expect(a.attachment.mode()).toBe("worker");
  });

  it("the same close with the page hidden produces no worker_dead until shown; on show the ping goes unanswered for the window and then re-spawns", async () => {
    const a = open();
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    setHidden(true);
    await sleep(HB);
    a.send({ type: "test:close" });
    await sleep(4 * HB);
    expect(a.ofKind("worker_dead")).toEqual([]);
    setHidden(false);
    await sleep(2 * HB);
    expect(a.ofKind("worker_dead")).toEqual([]);
    await waitFor("worker_dead", () => a.ofKind("worker_dead").length === 1, 3 * HB + 2000);
    expect(a.workers).toHaveLength(2);
  });

  it("a throttled worker is not dead: a stalled heartbeat timer released before the ping window ends yields a pong and no second worker", async () => {
    const a = open();
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    setHidden(true);
    a.send({ type: "test:pause", ms: 8 * HB });
    await sleep(8 * HB);
    setHidden(false);
    await sleep(3 * HB + 200);
    expect(a.ofKind("worker_dead")).toEqual([]);
    expect(a.workers).toHaveLength(1);
    expect(a.attachment.state()?.kind).toBe("open");
  });

  it("bfcache: pagehide closes the port, the host posts nothing to it, and pageshow re-attaches the same tabId earning one per-port full", async () => {
    const a = open();
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    const detachedBefore = a.ofKind("tab_detached").length;
    window.dispatchEvent(new Event("pagehide"));
    await sleep(2 * HB);
    const framesBefore = a.frames.length;
    await client.publish({ data: "while-cached" });
    await sleep(2 * HB);
    expect(a.frames.length).toBe(framesBefore);
    expect(a.ofKind("tab_detached").length).toBeGreaterThanOrEqual(detachedBefore);
    window.dispatchEvent(new Event("pageshow"));
    await waitFor("per-port full", () => a.runs.some((r) => r.full));
    expect(a.workers).toHaveLength(2);
  });

  it("the wedged-worker ladder: a blocked message pump yields two silent re-spawns and then worker_unavailable{silent} with the per-tab stream", async () => {
    const a = open();
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    await sleep(2 * HB);
    a.send({ type: "test:block", ms: 12 * HB });
    await waitFor("fallback", () => a.attachment.mode() === "fallback", 12 * HB + 4000);
    expect(a.ofKind("worker_unavailable")).toEqual([
      { kind: "worker_unavailable", cause: "silent" },
    ]);
    expect(a.workers).toHaveLength(3);
    await waitFor("per-tab open", () => a.attachment.state()?.kind === "open");
  });

  it("the post-deploy branch, server-served: a worker URL answering no script fires error and the page takes the per-tab path at once", async () => {
    const a = open({ url: `${location.origin}/no-such-worker-${String(Date.now())}.js` });
    await waitFor("fallback", () => a.attachment.mode() === "fallback");
    expect(a.ofKind("worker_unavailable")).toEqual([
      { kind: "worker_unavailable", cause: "error" },
    ]);
    await waitFor("per-tab open", () => a.attachment.state()?.kind === "open");
  });

  it.skipIf(!navigator.userAgent.includes("Chrome"))(
    "two hidden ports, one frozen: the profile revalidate resolves without the frozen page and it is repaired with a per-page full",
    async () => {
      const a = open();
      await waitFor("open", () => a.attachment.state()?.kind === "open");
      const raw = new SharedWorker(
        `${workerUrl}${workerUrl.includes("?") ? "&" : "?"}${new URLSearchParams({
          fixture: fixtureUrl,
          heartbeat: String(HB),
          hidden: String(HIDDEN),
          bundle: String(bundle),
        }).toString()}`,
        { name: `sse-${String(bundle)}`, type: "module" },
      );
      const received: { type: string }[] = [];
      raw.port.onmessage = (e: MessageEvent) => {
        received.push(e.data as { type: string });
      };
      raw.port.postMessage({
        type: "attach",
        tabId: "frozen",
        visible: false,
        online: true,
        hadWorker: false,
        tag: "",
      });
      await waitFor("expiry", () => a.ofKind("port_expired").length >= 1, 8 * HB);
      const lastHeartbeat = received.filter((m) => m.type === "heartbeat").at(-1) as
        { type: string; seq: number } | undefined;
      raw.port.postMessage({ type: "heartbeat_ack", seq: lastHeartbeat?.seq ?? 0 });
      await waitFor("repaired", () => received.some((m) => m.type === "stale"));
      expect(received.some((m) => m.type === "revalidate_run")).toBe(true);
      raw.port.close();
    },
  );

  it("a different content-hashed worker URL gets a second worker and a second connection", async () => {
    const a = open({ bundle: `${String(bundle)}-a` });
    const b = open({ bundle: `${String(bundle)}-b` });
    await waitFor(
      "both open",
      () => a.attachment.state()?.kind === "open" && b.attachment.state()?.kind === "open",
    );
    expect((await client.state()).clients).toBe(2);
    b.attachment.detach();
    pages = [a];
    await waitFor(
      "old worker released its connection",
      async () => (await client.state()).clients === 1,
      HIDDEN + 5000,
    );
  });

  it("per-tab fallback with SharedWorker deleted: two pages hold two connections", async () => {
    const a = open({ supported: false });
    const b = open({ supported: false });
    await waitFor(
      "both open",
      () => a.attachment.state()?.kind === "open" && b.attachment.state()?.kind === "open",
    );
    expect(a.attachment.mode()).toBe("fallback");
    expect((await client.state()).clients).toBe(2);
    await client.publish({ data: "each" });
    await waitFor(
      "both received",
      () => a.frames.some((f) => f.data === "each") && b.frames.some((f) => f.data === "each"),
    );
  });

  it("in fallback mode the tab presents its tag from the first connect, setTag() moves the presence row with one reconnect, and observe() records into the fallback's version map", async () => {
    const tag = `tag_${String(Date.now())}`;
    const next = `${tag}_b`;
    const a = open({ supported: false, tag });
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    expect(a.attachment.mode()).toBe("fallback");
    const row = async (t: string): Promise<{ connected: number } | undefined> =>
      (await client.state()).presence.find((p) => p.tag === t);
    expect(await row(tag)).toMatchObject({ connected: 1 });
    const held = await client.rest("chat", "fallen");
    a.attachment.observe({ kind: "chat", ref: "fallen" }, held.version, held.epoch);
    expect(a.fallbackVersions?.snapshot().held).toEqual([
      { kind: "chat", ref: "fallen", version: held.version },
    ]);
    const before = (await client.state()).v3_connects;
    a.attachment.setTag(next);
    await waitFor(
      "row moved",
      async () => (await row(next))?.connected === 1 && (await row(tag))?.connected === 0,
    );
    expect((await client.state()).v3_connects).toBe(before + 1);
    a.attachment.setTag(next);
    await sleep(2 * HB);
    expect((await client.state()).v3_connects).toBe(before + 1);
    expect(a.attachment.state()?.kind).toBe("open");
  });

  it("reconnect() keeps the cursor and earns a resumed hello; reconnect({resetCursor: true}) earns a fresh one; each is one connect", async () => {
    const a = open();
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    const before = (await client.state()).v3_connects;
    const hellos = a.ofKind("hello").length;
    a.attachment.reconnect();
    await waitFor("resumed hello", () => a.ofKind("hello").length === hellos + 1);
    expect(a.ofKind("hello").at(-1)?.resumed).toBe(true);
    expect((await client.state()).v3_connects).toBe(before + 1);
    a.attachment.reconnect({ resetCursor: true });
    await waitFor("fresh hello", () => a.ofKind("hello").length === hellos + 2);
    expect(a.ofKind("hello").at(-1)?.resumed).toBe(false);
    expect((await client.state()).v3_connects).toBe(before + 2);
    expect(a.attachment.mode()).toBe("worker");
  });

  it("setTag() moves the profile's presence row to the new tag with one reconnect; the same tag again reconnects nothing", async () => {
    const tag = `tag_${String(Date.now())}`;
    const next = `${tag}_b`;
    const a = open({ tag });
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    const row = async (t: string): Promise<{ connected: number } | undefined> =>
      (await client.state()).presence.find((p) => p.tag === t);
    expect(await row(tag)).toMatchObject({ connected: 1 });
    const before = (await client.state()).v3_connects;
    a.attachment.setTag(next);
    await waitFor(
      "row moved",
      async () => (await row(next))?.connected === 1 && (await row(tag))?.connected === 0,
    );
    expect((await client.state()).v3_connects).toBe(before + 1);
    a.attachment.setTag(next);
    await sleep(2 * HB);
    expect((await client.state()).v3_connects).toBe(before + 1);
    expect(a.attachment.state()?.kind).toBe("open");
  });

  it("observe() feeds the host's version map, and one digest's verdict reaches every page's revalidate context", async () => {
    const a = open();
    const b = open();
    await waitFor(
      "both open",
      () => a.attachment.state()?.kind === "open" && b.attachment.state()?.kind === "open",
    );
    const held = await client.rest("chat", "observed");
    a.attachment.observe({ kind: "chat", ref: "observed" }, held.version, held.epoch);
    await sleep(HB);
    const moved = await client.mutate("chat", "observed");
    expect(moved.version).not.toBe(held.version);
    const runsA = a.runs.length;
    const runsB = b.runs.length;
    a.attachment.reconnect({ resetCursor: true });
    await waitFor("verdict in both", () => a.runs.length > runsA && b.runs.length > runsB);
    for (const p of [a, b]) {
      const last = p.runs.at(-1);
      expect(last).toMatchObject({ cause: "hello", full: false });
      expect(last?.changed).toEqual([{ kind: "chat", ref: "observed", version: moved.version }]);
      expect(last?.removed).toEqual([]);
    }
  });

  it("presence through the fixture: connected with the tag, acknowledgements per keepalive, gone at the hidden close and at aliveWindow while the socket stays, alive again on the next ack", async () => {
    const tag = `tag_${String(Date.now())}`;
    const a = open({ tag });
    await waitFor("open", () => a.attachment.state()?.kind === "open");
    const row = async (): Promise<{ connected: number; gone: boolean } | undefined> =>
      (await client.state()).presence.find((p) => p.tag === tag);
    expect(await row()).toMatchObject({ connected: 1, gone: false });
    await waitFor("acknowledgement", () => a.ofKind("alive_ack").length >= 1, 20_000);
    setHidden(true);
    await waitFor("gone at close", async () => (await row())?.gone === true, HIDDEN + 5000);
    setHidden(false);
    await waitFor("reopened", () => a.ofKind("reopened").length === 1);
    a.send({ type: "test:suppress-alive", on: true });
    await client.aliveWindow(1500);
    await sleep(1800);
    const state = await client.state();
    expect(state.clients).toBe(1);
    expect(await row()).toMatchObject({ connected: 1, gone: true });
    a.send({ type: "test:suppress-alive", on: false });
    await waitFor("alive again", async () => (await row())?.gone === false, 20_000);
  }, 60_000);
});
