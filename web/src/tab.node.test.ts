import { afterEach, describe, expect, it, vi } from "vitest";
import type { ClientState } from "./reducer.js";
import type { Stream } from "./stream.js";
import {
  type AttachOptions,
  type SharedWorkerLike,
  type TabAttachment,
  type TabFallback,
  type TabRevalidateContext,
  attachToWorker,
} from "./tab.js";
import type { TabToWorker, WorkerToTab } from "./port-protocol.js";
import { createVersionMap } from "./versions.js";
import {
  type VisibilityEvent,
  type VisibilitySource,
  createVisibilityManager,
} from "./visibility.js";
import { type OnlineSource, createOnlineManager } from "./online.js";

// Real timers with a short window: the ports deliver on the event loop rather than on a
// timer, so faking timers would stop the message pump these tests depend on.
const HB = 30;
const WINDOW = 3 * HB;
const RUN_TIMEOUT = 40;

function tick(ms = 0): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(what: string, pred: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!pred()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${what}`);
    }
    await tick(5);
  }
}

/** One fake SharedWorker: a MessageChannel, the host end under the test's control. */
interface FakeWorker extends SharedWorkerLike {
  /** Everything the tab posted to the host. */
  readonly sent: TabToWorker[];
  /** Posts a host-to-tab message down this worker's port. */
  host(message: WorkerToTab): void;
  /** Fires the worker's error event, as a 404 script or a construction failure would. */
  fireError(): void;
  closed: boolean;
}

function fakeWorker(): FakeWorker {
  const channel = new MessageChannel();
  const sent: TabToWorker[] = [];
  const errorListeners: (() => void)[] = [];
  channel.port2.onmessage = (event: MessageEvent) => {
    sent.push(event.data as TabToWorker);
  };
  channel.port2.start();
  return {
    port: channel.port1 as unknown as MessagePort,
    sent,
    closed: false,
    addEventListener(_type, listener) {
      errorListeners.push(listener);
    },
    host(message) {
      channel.port2.postMessage(message);
    },
    fireError() {
      for (const listener of [...errorListeners]) {
        listener();
      }
    },
  };
}

const STOPPED: ClientState = { kind: "stopped", generation: 0 };

/** A fallback stream that records the verbs the tab drives it with. */
function fakeStream(): Stream & { readonly calls: string[] } {
  const calls: string[] = [];
  return {
    calls,
    start: () => void calls.push("start"),
    stop: () => void calls.push("stop"),
    reconnect: () => void calls.push("reconnect"),
    resetCursor: () => void calls.push("resetCursor"),
    state: () => STOPPED,
    cursor: () => null,
  };
}

function visibilityControl(): {
  readonly manager: ReturnType<typeof createVisibilityManager>;
  /** Changes the on-demand reading WITHOUT emitting, as a throttled page would. */
  setReading(value: boolean): void;
  fire(ev: VisibilityEvent): void;
} {
  let visible = true;
  let emit: ((ev: VisibilityEvent) => void) | null = null;
  const source: VisibilitySource = {
    visible: () => visible,
    listen(cb) {
      emit = cb;
      return () => {
        emit = null;
      };
    },
  };
  const manager = createVisibilityManager(source);
  return {
    manager,
    setReading(value) {
      visible = value;
    },
    fire(ev) {
      if (ev === "visible") {
        visible = true;
      } else if (ev === "hidden") {
        visible = false;
      }
      emit?.(ev);
    },
  };
}

function onlineControl(): {
  readonly manager: ReturnType<typeof createOnlineManager>;
  fire(value: boolean): void;
} {
  let online = true;
  let emit: ((value: boolean) => void) | null = null;
  const source: OnlineSource = {
    online: () => online,
    listen(cb) {
      emit = cb;
      return () => {
        emit = null;
      };
    },
  };
  return {
    manager: createOnlineManager(source),
    fire(value) {
      online = value;
      emit?.(value);
    },
  };
}

interface Harness {
  readonly attachment: TabAttachment;
  readonly workers: FakeWorker[];
  readonly frames: unknown[];
  readonly events: { kind: string }[];
  readonly runs: TabRevalidateContext[];
  readonly fallbacks: (Stream & { readonly calls: string[] })[];
  readonly fallbackHeaders: Record<string, string>[];
  kinds(): string[];
  latest(): FakeWorker;
}

interface HarnessOptions {
  readonly supported?: boolean;
  readonly tag?: string;
  readonly spawnThrows?: boolean;
  readonly revalidate?: (ctx: TabRevalidateContext) => Promise<void>;
  readonly onFrame?: (frame: unknown) => void;
  readonly visibility?: AttachOptions["visibility"];
  readonly online?: AttachOptions["online"];
}

const live: TabAttachment[] = [];

function harness(opts: HarnessOptions = {}): Harness {
  const workers: FakeWorker[] = [];
  const frames: unknown[] = [];
  const events: { kind: string }[] = [];
  const runs: TabRevalidateContext[] = [];
  const fallbacks: (Stream & { readonly calls: string[] })[] = [];
  const fallbackHeaders: Record<string, string>[] = [];

  const attachment = attachToWorker({
    heartbeatMs: HB,
    revalidateTimeoutMs: RUN_TIMEOUT,
    ...(opts.supported === undefined ? {} : { supported: opts.supported }),
    ...(opts.tag === undefined ? {} : { tag: opts.tag }),
    ...(opts.visibility === undefined ? {} : { visibility: opts.visibility }),
    ...(opts.online === undefined ? {} : { online: opts.online }),
    ...(opts.revalidate === undefined ? {} : { revalidate: opts.revalidate }),
    spawn() {
      if (opts.spawnThrows === true) {
        throw new Error("no such worker script");
      }
      const worker = fakeWorker();
      workers.push(worker);
      return worker;
    },
    fallback(): TabFallback {
      const stream = fakeStream();
      const headers: Record<string, string> = {};
      fallbacks.push(stream);
      fallbackHeaders.push(headers);
      return { stream, versions: createVersionMap(), headers };
    },
    onFrame(frame) {
      if (opts.onFrame !== undefined) {
        opts.onFrame(frame);
        return;
      }
      frames.push(frame);
    },
    onLifecycle(ev) {
      events.push(ev);
      runs.length += 0;
    },
  });
  live.push(attachment);
  return {
    attachment,
    workers,
    frames,
    events,
    runs,
    fallbacks,
    fallbackHeaders,
    kinds: () => events.map((e) => e.kind),
    latest() {
      const worker = workers[workers.length - 1];
      if (worker === undefined) {
        throw new Error("no worker was spawned");
      }
      return worker;
    },
  };
}

afterEach(() => {
  for (const attachment of live.splice(0)) {
    attachment.detach();
  }
  vi.unstubAllGlobals();
});

describe("attachToWorker: choosing the worker or the per-tab stream", () => {
  it("takes the per-tab stream where the runtime has no SharedWorker", () => {
    const h = harness();
    expect(h.attachment.mode()).toBe("fallback");
    expect(h.workers).toHaveLength(0);
    expect(h.fallbacks[0]?.calls).toEqual(["start"]);
  });

  it("spawns a worker where the runtime has SharedWorker", () => {
    vi.stubGlobal("SharedWorker", class {});
    const h = harness();
    expect(h.attachment.mode()).toBe("worker");
    expect(h.workers).toHaveLength(1);
  });

  it("honours an explicit supported:false over a present SharedWorker", () => {
    vi.stubGlobal("SharedWorker", class {});
    const h = harness({ supported: false });
    expect(h.attachment.mode()).toBe("fallback");
    expect(h.workers).toHaveLength(0);
  });

  it("presents the tab's tag to the per-tab stream it starts", () => {
    const h = harness({ supported: false, tag: "tab_a" });
    expect(h.fallbackHeaders[0]).toEqual({ "SSE-Client": "tab_a" });
  });

  it("falls back at once when spawning the worker throws", () => {
    const h = harness({ supported: true, spawnThrows: true });
    expect(h.attachment.mode()).toBe("fallback");
    expect(h.events).toEqual([{ kind: "worker_unavailable", cause: "error" }]);
    expect(h.fallbacks[0]?.calls).toEqual(["start"]);
  });

  it("falls back when the worker fires error, and reports the cause", async () => {
    const h = harness({ supported: true });
    h.latest().fireError();
    await waitFor("fallback", () => h.attachment.mode() === "fallback");
    expect(h.events.at(-1)).toEqual({ kind: "worker_unavailable", cause: "error" });
  });

  it("presents its tabId and readings in the first attach message", async () => {
    const h = harness({ supported: true, tag: "tab_b" });
    await waitFor("attach", () => h.latest().sent.length > 0);
    expect(h.latest().sent[0]).toMatchObject({
      type: "attach",
      hadWorker: false,
      tag: "tab_b",
      visible: true,
    });
  });
});

describe("attachToWorker: the port conversation", () => {
  it("answers a heartbeat with an acknowledgement carrying the same seq", async () => {
    const h = harness({ supported: true });
    h.latest().host({ type: "heartbeat", seq: 7 });
    await waitFor("ack", () => h.latest().sent.some((m) => m.type === "heartbeat_ack"));
    expect(h.latest().sent.filter((m) => m.type === "heartbeat_ack")).toEqual([
      { type: "heartbeat_ack", seq: 7 },
    ]);
  });

  it("hands a routed frame to onFrame", async () => {
    const h = harness({ supported: true });
    const frame = { type: "chat", data: "hi", id: null };
    h.latest().host({ type: "frame", frame, generation: 3 });
    await waitFor("frame", () => h.frames.length === 1);
    expect(h.frames[0]).toEqual(frame);
  });

  it("reports frame_failed with the frame's cursor when onFrame throws", async () => {
    const id = { epoch: "e1", offset: "12" };
    const h = harness({
      supported: true,
      onFrame() {
        throw new Error("could not apply");
      },
    });
    h.latest().host({ type: "frame", frame: { type: "chat", data: "x", id }, generation: 5 });
    await waitFor("frame_failed", () => h.latest().sent.some((m) => m.type === "frame_failed"));
    expect(h.latest().sent.filter((m) => m.type === "frame_failed")).toEqual([
      { type: "frame_failed", generation: 5, cursor: id },
    ]);
  });

  it("surfaces the host's lifecycle records to its own consumer", async () => {
    const h = harness({ supported: true });
    h.latest().host({ type: "lifecycle", event: { kind: "reopened" } });
    await waitFor("record", () => h.kinds().includes("reopened"));
    expect(h.kinds()).toContain("reopened");
  });

  it("reads the host's last state broadcast", async () => {
    const h = harness({ supported: true });
    expect(h.attachment.state()).toBeNull();
    const state: ClientState = { kind: "stopped", generation: 4 };
    h.latest().host({ type: "state", state });
    await waitFor("state", () => h.attachment.state() !== null);
    expect(h.attachment.state()).toEqual(state);
  });

  it("answers a routed run with revalidate_done when its body resolves", async () => {
    const seen: TabRevalidateContext[] = [];
    const h = harness({
      supported: true,
      revalidate(ctx) {
        seen.push(ctx);
        return Promise.resolve();
      },
    });
    h.latest().host({
      type: "revalidate_run",
      runId: 11,
      ctx: { cause: "hello", epoch: "e1", generation: 1, full: true },
    });
    await waitFor("done", () => h.latest().sent.some((m) => m.type === "revalidate_done"));
    expect(h.latest().sent.filter((m) => m.type === "revalidate_done")).toEqual([
      { type: "revalidate_done", runId: 11 },
    ]);
    expect(seen[0]).toMatchObject({ cause: "hello", full: true });
  });

  it("answers a routed run with revalidate_failed carrying the body's message", async () => {
    const h = harness({
      supported: true,
      revalidate: () => Promise.reject(new Error("digest refused")),
    });
    h.latest().host({
      type: "revalidate_run",
      runId: 12,
      ctx: { cause: "visible", epoch: "e1", generation: 1, full: false },
    });
    await waitFor("failed", () => h.latest().sent.some((m) => m.type === "revalidate_failed"));
    expect(h.latest().sent.filter((m) => m.type === "revalidate_failed")).toEqual([
      { type: "revalidate_failed", runId: 12, cause: "digest refused" },
    ]);
  });

  it("answers a routed run with revalidate_failed when the body throws synchronously", async () => {
    const h = harness({
      supported: true,
      revalidate() {
        throw new Error("threw at once");
      },
    });
    h.latest().host({
      type: "revalidate_run",
      runId: 13,
      ctx: { cause: "visible", epoch: null, generation: 1, full: false },
    });
    await waitFor("failed", () => h.latest().sent.some((m) => m.type === "revalidate_failed"));
    expect(h.latest().sent.filter((m) => m.type === "revalidate_failed")).toEqual([
      { type: "revalidate_failed", runId: 13, cause: "threw at once" },
    ]);
  });

  it("reports a run as done without a body, since the host's own reconciled", async () => {
    const h = harness({ supported: true });
    h.latest().host({
      type: "revalidate_run",
      runId: 14,
      ctx: { cause: "hello", epoch: "e1", generation: 1, full: true },
    });
    await waitFor("done", () => h.latest().sent.some((m) => m.type === "revalidate_done"));
    expect(h.latest().sent.filter((m) => m.type === "revalidate_done")).toEqual([
      { type: "revalidate_done", runId: 14 },
    ]);
  });

  it("aborts a routed run's signal with a TimeoutError once its bound elapses", async () => {
    const reasons: { name: string }[] = [];
    const h = harness({
      supported: true,
      revalidate(ctx) {
        return new Promise<void>((_resolve, reject) => {
          ctx.signal.addEventListener("abort", () => {
            reasons.push(ctx.signal.reason as { name: string });
            reject(new Error("aborted"));
          });
        });
      },
    });
    h.latest().host({
      type: "revalidate_run",
      runId: 15,
      ctx: { cause: "visible", epoch: "e1", generation: 1, full: false },
    });
    await waitFor("abort", () => reasons.length === 1, 4000);
    expect(reasons[0]?.name).toBe("TimeoutError");
  });
});

describe("attachToWorker: the page lifecycle", () => {
  it("forwards a network transition to the host", async () => {
    const net = onlineControl();
    const h = harness({ supported: true, online: net.manager });
    net.fire(false);
    await waitFor("network", () => h.latest().sent.some((m) => m.type === "network"));
    expect(h.latest().sent.filter((m) => m.type === "network")).toEqual([
      { type: "network", online: false },
    ]);
  });

  it("closes the port on pagehide and tells the host why", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: true, visibility: vis.manager });
    const first = h.latest();
    vis.fire("pagehide");
    await waitFor("pagehide", () => first.sent.some((m) => m.type === "visibility"));
    expect(first.sent.filter((m) => m.type === "visibility")).toEqual([
      { type: "visibility", ev: "pagehide" },
    ]);
  });

  // The port is closed, not merely told: anything the host says afterwards reaches nothing.
  // Asserted inside the silent window, so a watchdog respawn cannot supply the answer.
  it("stops answering the host once pagehide has closed the port", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: true, visibility: vis.manager });
    const first = h.latest();
    vis.fire("pagehide");
    await waitFor("pagehide", () => first.sent.some((m) => m.type === "visibility"));
    first.host({ type: "heartbeat", seq: 4 });
    await tick(WINDOW / 2);
    expect(first.sent.some((m) => m.type === "heartbeat_ack")).toBe(false);
  });

  it("spawns a replacement on pageshow when pagehide left no port, declaring hadWorker", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: true, visibility: vis.manager });
    vis.fire("pagehide");
    await tick(5);
    vis.fire("pageshow");
    await waitFor("respawn", () => h.workers.length === 2);
    await waitFor("attach", () => h.latest().sent.length > 0);
    expect(h.latest().sent[0]).toMatchObject({ type: "attach", hadWorker: true });
  });

  it("forwards pageshow over a live port rather than spawning again", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: true, visibility: vis.manager });
    vis.fire("pageshow");
    await waitFor("pageshow", () =>
      h.latest().sent.some((m) => m.type === "visibility" && m.ev === "pageshow"),
    );
    expect(h.workers).toHaveLength(1);
  });

  it("forwards a hidden and a visible transition to the host", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: true, visibility: vis.manager });
    vis.fire("hidden");
    vis.fire("visible");
    await waitFor(
      "both",
      () => h.latest().sent.filter((m) => m.type === "visibility").length === 2,
    );
    expect(h.latest().sent.filter((m) => m.type === "visibility")).toEqual([
      { type: "visibility", ev: "hidden" },
      { type: "visibility", ev: "visible" },
    ]);
  });
});

describe("attachToWorker: the wedged-worker ladder", () => {
  it("declares the worker dead and respawns when the window passes with no message", async () => {
    const h = harness({ supported: true });
    await waitFor("worker_dead", () => h.kinds().includes("worker_dead"), 4000);
    expect(h.workers.length).toBeGreaterThanOrEqual(2);
  });

  it("does not declare the worker dead while the page is no longer visible", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: true, visibility: vis.manager });
    // A throttled page changes what isVisible() reads without delivering an event, which is
    // the case the watchdog's own guard exists for.
    vis.setReading(false);
    await tick(WINDOW + 150);
    expect(h.kinds()).not.toContain("worker_dead");
  });

  it("gives up on the worker after two silent windows and takes the per-tab stream", async () => {
    const h = harness({ supported: true });
    await waitFor("fallback", () => h.attachment.mode() === "fallback", 6000);
    expect(h.events.at(-1)).toEqual({ kind: "worker_unavailable", cause: "silent" });
    expect(h.workers).toHaveLength(2);
  });

  it("lets a heartbeat buy back the whole silent-window allowance", async () => {
    const h = harness({ supported: true });
    // A heartbeat on the first link resets the ladder, so the tab spends two FURTHER
    // silent windows (a third worker) before giving up rather than one.
    h.latest().host({ type: "heartbeat", seq: 1 });
    await waitFor("ack", () => h.latest().sent.some((m) => m.type === "heartbeat_ack"));
    await waitFor("fallback", () => h.attachment.mode() === "fallback", 6000);
    expect(h.workers).toHaveLength(3);
  });
});

describe("attachToWorker: recovering a worker from the per-tab stream", () => {
  it("promotes back to the worker when a recovery probe heartbeats", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: false, visibility: vis.manager });
    expect(h.attachment.mode()).toBe("fallback");
    vis.fire("hidden");
    vis.fire("visible");
    await waitFor("probe", () => h.workers.length === 1);
    h.latest().host({ type: "heartbeat", seq: 2 });
    await waitFor("recovered", () => h.attachment.mode() === "worker", 4000);
    expect(h.kinds()).toContain("worker_recovered");
    expect(h.fallbacks[0]?.calls).toEqual(["start", "stop"]);
  });

  it("abandons a recovery probe that fires error, so its later heartbeat is ignored", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: false, visibility: vis.manager });
    vis.fire("hidden");
    vis.fire("visible");
    await waitFor("probe", () => h.workers.length === 1);
    const probe = h.latest();
    probe.fireError();
    probe.host({ type: "heartbeat", seq: 3 });
    await tick(60);
    expect(h.attachment.mode()).toBe("fallback");
  });

  // A worker's error event can arrive after its probe was abandoned (a 404 script resolving
  // late), and closing the CURRENT probe on that report would take down a live recovery.
  it("ignores a late error from an abandoned probe and still promotes the current one", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: false, visibility: vis.manager });
    vis.fire("hidden");
    vis.fire("visible");
    await waitFor("first probe", () => h.workers.length === 1);
    const stale = h.latest();
    vis.fire("hidden");
    vis.fire("visible");
    await waitFor("second probe", () => h.workers.length === 2);
    stale.fireError();
    h.latest().host({ type: "heartbeat", seq: 5 });
    await waitFor("recovered", () => h.attachment.mode() === "worker", 4000);
    expect(h.kinds()).toContain("worker_recovered");
  });

  it("probes at most once per visible period", async () => {
    const vis = visibilityControl();
    const h = harness({ supported: false, visibility: vis.manager });
    vis.fire("hidden");
    vis.fire("visible");
    await waitFor("probe", () => h.workers.length === 1);
    vis.fire("visible");
    await tick(30);
    expect(h.workers).toHaveLength(1);
  });
});

describe("attachToWorker: the tab's verbs", () => {
  it("routes observe, reconnect and setTag over the port in worker mode", async () => {
    const h = harness({ supported: true, tag: "tab_a" });
    h.attachment.observe({ kind: "chat", ref: "c1" }, "41", "e1");
    h.attachment.reconnect({ resetCursor: true });
    h.attachment.setTag("tab_z");
    await waitFor("all three", () => h.latest().sent.length >= 4);
    expect(h.latest().sent.slice(1)).toEqual([
      { type: "observed", subject: { kind: "chat", ref: "c1" }, version: "41", epoch: "e1" },
      { type: "reconnect", resetCursor: true },
      { type: "set_tag", tag: "tab_z" },
    ]);
  });

  it("drives the per-tab stream directly in fallback mode", () => {
    const h = harness({ supported: false });
    h.attachment.reconnect({ resetCursor: true });
    h.attachment.reconnect();
    expect(h.fallbacks[0]?.calls).toEqual(["start", "resetCursor", "reconnect", "reconnect"]);
  });

  it("reads the per-tab stream's own state in fallback mode", () => {
    const h = harness({ supported: false });
    expect(h.attachment.state()).toEqual(STOPPED);
  });

  it("ignores an empty tag rather than displacing the one it holds", async () => {
    const h = harness({ supported: true, tag: "tab_a" });
    h.attachment.setTag("");
    await tick(10);
    expect(h.latest().sent.some((m) => m.type === "set_tag")).toBe(false);
  });

  it("reconnects the per-tab stream once for a tag change and not for a repeat", () => {
    const h = harness({ supported: false, tag: "tab_a" });
    h.attachment.setTag("tab_b");
    expect(h.fallbackHeaders[0]).toEqual({ "SSE-Client": "tab_b" });
    expect(h.fallbacks[0]?.calls).toEqual(["start", "reconnect"]);
    h.attachment.setTag("tab_b");
    expect(h.fallbacks[0]?.calls).toEqual(["start", "reconnect"]);
  });

  it("tells the host why it is leaving and stops answering afterwards", async () => {
    const h = harness({ supported: true });
    const worker = h.latest();
    h.attachment.detach("logout");
    await waitFor("detach", () => worker.sent.some((m) => m.type === "detach"));
    expect(worker.sent.filter((m) => m.type === "detach")).toEqual([
      { type: "detach", cause: "logout" },
    ]);
    worker.host({ type: "heartbeat", seq: 9 });
    await tick(30);
    expect(worker.sent.some((m) => m.type === "heartbeat_ack")).toBe(false);
  });

  it("stops the per-tab stream on detach", () => {
    const h = harness({ supported: false });
    h.attachment.detach();
    expect(h.fallbacks[0]?.calls).toEqual(["start", "stop"]);
  });

  it("is idempotent on a second detach", async () => {
    const h = harness({ supported: true });
    const worker = h.latest();
    h.attachment.detach();
    h.attachment.detach();
    await tick(20);
    expect(worker.sent.filter((m) => m.type === "detach")).toHaveLength(1);
  });
});
