import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LifecycleEvent } from "./lifecycle.js";
import { createOnlineManager } from "./online.js";
import type { TabToWorker, WorkerToTab } from "./port-protocol.js";
import type { RevalidateContext } from "./stream.js";
import {
  type SharedWorkerLike,
  type TabAttachment,
  type TabRevalidateContext,
  attachToWorker,
} from "./tab.js";
import {
  FAKE_CLOCK,
  type ScriptedFetch,
  flush,
  frame,
  helloFrame,
  scriptedFetch,
} from "./test-helpers/scripted-fetch.js";
import { fakeOnline, fakeVisibility } from "./test-helpers/stream-harness.js";
import { type VersionMap, createVersionMap } from "./versions.js";
import { createVisibilityManager } from "./visibility.js";
import { type TabSet, type WorkerHost, createWorkerHost } from "./worker.js";

const A = "aaaaaaaaaaaaaaaa";
const B = "bbbbbbbbbbbbbbbb";
const HB = 5000;

function id(offset: number): string {
  return `${A}:${String(offset)}`;
}

interface HostRun {
  readonly ctx: RevalidateContext;
  readonly tabs: TabSet;
  resolve(): void;
  reject(error: Error): void;
}

interface Host {
  readonly host: WorkerHost;
  readonly sf: ScriptedFetch;
  readonly versions: VersionMap;
  readonly events: LifecycleEvent[];
  readonly runs: HostRun[];
  ofKind<K extends LifecycleEvent["kind"]>(kind: K): Extract<LifecycleEvent, { kind: K }>[];
}

function hostHarness(): Host {
  const sf = scriptedFetch();
  const versions = createVersionMap();
  const events: LifecycleEvent[] = [];
  const runs: HostRun[] = [];
  const host = createWorkerHost({
    url: "/events",
    fetch: sf.fetch,
    versions,
    heartbeatMs: HB,
    onlineSeed: true,
    onLifecycle(ev) {
      events.push(ev);
    },
    revalidate(ctx, tabs) {
      return new Promise<void>((resolve, reject) => {
        runs.push({ ctx, tabs, resolve, reject });
      });
    },
  });
  return {
    host,
    sf,
    versions,
    events,
    runs,
    ofKind(kind) {
      return events.filter((e) => e.kind === kind) as Extract<
        LifecycleEvent,
        { kind: typeof kind }
      >[];
    },
  };
}

interface RawTab {
  readonly received: WorkerToTab[];
  readonly port: MessagePort;
  send(message: TabToWorker): void;
  ofType<T extends WorkerToTab["type"]>(type: T): Extract<WorkerToTab, { type: T }>[];
  close(): void;
}

interface RawTabOptions {
  readonly tabId?: string;
  readonly visible?: boolean;
  readonly online?: boolean;
  readonly hadWorker?: boolean;
  readonly tag?: string;
  /** Answer every heartbeat, as a live tab does. */
  readonly ack?: boolean;
}

function rawTab(h: Host, opts: RawTabOptions = {}): RawTab {
  const channel = new MessageChannel();
  h.host.attach(channel.port1);
  const received: WorkerToTab[] = [];
  const send = (message: TabToWorker): void => {
    channel.port2.postMessage(message);
  };
  channel.port2.onmessage = (event: MessageEvent) => {
    const message = event.data as WorkerToTab;
    received.push(message);
    if ((opts.ack ?? true) && message.type === "heartbeat") {
      send({ type: "heartbeat_ack", seq: message.seq });
    }
  };
  send({
    type: "attach",
    tabId: opts.tabId ?? "tab-1",
    visible: opts.visible ?? true,
    online: opts.online ?? true,
    hadWorker: opts.hadWorker ?? false,
    tag: opts.tag ?? "",
  });
  return {
    received,
    port: channel.port2,
    send,
    ofType(type) {
      return received.filter((m) => m.type === type) as Extract<
        WorkerToTab,
        { type: typeof type }
      >[];
    },
    close() {
      channel.port2.close();
    },
  };
}

async function settle(): Promise<void> {
  await flush(10);
}

/** Advances one heartbeat at a time so acknowledgements are delivered between beats. */
async function beats(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    vi.advanceTimersByTime(HB);
    await settle();
  }
}

/** Opens the host's stream and pushes a hello; returns nothing, the run (if any) is in h.runs. */
async function hello(h: Host, overrides: Record<string, unknown> = {}): Promise<void> {
  await settle();
  const conn = h.sf.connections[h.sf.connections.length - 1];
  if (conn === undefined) {
    throw new Error("host opened no connection");
  }
  conn.push(helloFrame(overrides));
  await settle();
}

interface FakeWorker {
  readonly worker: SharedWorkerLike;
  /** The other end, held by the host or by the test. */
  readonly peer: MessagePort;
  fireError(): void;
}

function fakeWorker(host: WorkerHost | null): FakeWorker {
  const channel = new MessageChannel();
  if (host !== null) {
    host.attach(channel.port1);
  }
  const listeners: (() => void)[] = [];
  return {
    worker: {
      port: channel.port2,
      addEventListener(_type, listener) {
        listeners.push(listener);
      },
    },
    peer: channel.port1,
    fireError() {
      for (const listener of listeners) {
        listener();
      }
    },
  };
}

interface Tab {
  readonly attachment: TabAttachment;
  readonly events: LifecycleEvent[];
  readonly runs: TabRevalidateContext[];
  /** The fallback's version map, the one `fallback()` hands the attachment. */
  readonly versions: VersionMap;
  readonly spawned: FakeWorker[];
  readonly fallbacks: number;
  readonly fallbackStates: string[];
  /** The latest fallback's headers record, as the attachment left it. */
  readonly fallbackHeaders: Record<string, string>;
  readonly visibility: ReturnType<typeof fakeVisibility>;
  ofKind<K extends LifecycleEvent["kind"]>(kind: K): Extract<LifecycleEvent, { kind: K }>[];
}

function tabHarness(spawnTargets: (WorkerHost | null)[], visible = true, tag = ""): Tab {
  const events: LifecycleEvent[] = [];
  const runs: TabRevalidateContext[] = [];
  const versions = createVersionMap();
  const spawned: FakeWorker[] = [];
  const visibility = fakeVisibility(visible);
  const online = fakeOnline(true);
  const fallbackStates: string[] = [];
  const counters = { fallbacks: 0 };
  let fallbackHeaders: Record<string, string> = {};
  const attachment = attachToWorker({
    supported: true,
    heartbeatMs: HB,
    tag,
    visibility: createVisibilityManager(visibility.source),
    online: createOnlineManager(online.source),
    onFrame: () => undefined,
    onLifecycle(ev) {
      events.push(ev);
    },
    revalidate(ctx) {
      runs.push(ctx);
      return Promise.resolve();
    },
    spawn() {
      const target = spawnTargets[spawned.length] ?? null;
      const w = fakeWorker(target);
      spawned.push(w);
      return w.worker;
    },
    fallback() {
      counters.fallbacks++;
      fallbackHeaders = {};
      let state = "stopped";
      return {
        versions,
        headers: fallbackHeaders,
        stream: {
          start() {
            state = "connecting";
            fallbackStates.push("start");
          },
          stop() {
            state = "stopped";
            fallbackStates.push("stop");
          },
          reconnect() {
            fallbackStates.push("reconnect");
          },
          state: () => ({ kind: state as "stopped", generation: 0 }),
          cursor: () => null,
          resetCursor() {
            fallbackStates.push("resetCursor");
          },
        },
      };
    },
  });
  return {
    attachment,
    events,
    runs,
    versions,
    spawned,
    get fallbacks() {
      return counters.fallbacks;
    },
    fallbackStates,
    get fallbackHeaders() {
      return fallbackHeaders;
    },
    visibility,
    ofKind(kind) {
      return events.filter((e) => e.kind === kind) as Extract<
        LifecycleEvent,
        { kind: typeof kind }
      >[];
    },
  };
}

describe("port protocol", () => {
  const hosts: WorkerHost[] = [];
  const tabs: TabAttachment[] = [];
  const ports: MessagePort[] = [];

  function track(h: Host): Host {
    hosts.push(h.host);
    return h;
  }

  beforeEach(() => {
    vi.useFakeTimers(FAKE_CLOCK);
    vi.spyOn(Math, "random").mockReturnValue(0);
  });
  afterEach(async () => {
    for (const tab of tabs.splice(0)) {
      tab.detach();
    }
    for (const host of hosts.splice(0)) {
      host.close();
    }
    for (const port of ports.splice(0)) {
      port.close();
    }
    await flush(4);
    vi.useRealTimers();
  });

  it("frames published on the host's fixture stream reach every attached, acknowledging port in order after a hold drains", async () => {
    const h = track(hostHarness());
    const a = rawTab(h, { tabId: "a" });
    const b = rawTab(h, { tabId: "b" });
    await hello(h, { head: "0" });
    expect(h.runs).toHaveLength(1);
    const conn = h.sf.connections[0]!;
    conn.push(frame("message", "one", id(1)));
    conn.push(frame("message", "two", id(2)));
    conn.push(frame("message", "three", id(3)));
    await settle();
    expect(a.ofType("frame")).toEqual([]);
    h.runs[0]!.resolve();
    await settle();
    expect(a.ofType("frame").map((m) => m.frame.data)).toEqual(["one", "two", "three"]);
    expect(b.ofType("frame").map((m) => m.frame.data)).toEqual(["one", "two", "three"]);
    expect(a.ofType("frame")[0]!.generation).toBe(1);
  });

  it("a tab's frame_failed triggers exactly one revalidate('hello') on the host and no other tab's delivery is affected", async () => {
    const h = track(hostHarness());
    const a = rawTab(h, { tabId: "a" });
    const b = rawTab(h, { tabId: "b" });
    await hello(h, { resumed: true, head: "0" });
    expect(h.runs).toHaveLength(0);
    const conn = h.sf.connections[0]!;
    conn.push(frame("message", "one", id(1)));
    await settle();
    a.send({ type: "frame_failed", generation: 1, cursor: { epoch: A, offset: "1" } });
    a.send({ type: "frame_failed", generation: 1, cursor: { epoch: A, offset: "1" } });
    await settle();
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["hello"]);
    h.runs[0]!.resolve();
    await settle();
    conn.push(frame("message", "two", id(2)));
    await settle();
    expect(b.ofType("frame").map((m) => m.frame.data)).toEqual(["one", "two"]);
    expect(a.ofType("frame").map((m) => m.frame.data)).toEqual(["one", "two"]);
  });

  it("attach{hadWorker: true} on a fresh host sets the queued slot's full and the first run is full; attach{hadWorker: false} does not", async () => {
    const full = track(hostHarness());
    rawTab(full, { hadWorker: true });
    await hello(full, { head: "3" });
    expect(full.runs).toHaveLength(1);
    expect(full.runs[0]!.ctx).toMatchObject({ cause: "hello", full: true });

    const plain = track(hostHarness());
    rawTab(plain, { hadWorker: false });
    await hello(plain, { head: "3" });
    expect(plain.runs[0]!.ctx).toMatchObject({ cause: "hello", full: false });
  });

  it("attach{hadWorker: true} on a host in any non-stopped state yields one revalidate_run{full} to that port and nothing to the others; the hidden_closed case connects", async () => {
    const open = track(hostHarness());
    const openA = rawTab(open, { tabId: "a" });
    await hello(open, { resumed: true });
    const openB = rawTab(open, { tabId: "b", hadWorker: true });
    await settle();
    expect(openB.ofType("revalidate_run")).toMatchObject([{ ctx: { full: true } }]);
    expect(openA.ofType("revalidate_run")).toEqual([]);
    expect(open.runs).toHaveLength(0);
    expect(open.host.stream().state().kind).toBe("open");

    const connecting = track(hostHarness());
    rawTab(connecting, { tabId: "a" });
    await settle();
    expect(connecting.host.stream().state().kind).toBe("connecting");
    const connectingB = rawTab(connecting, { tabId: "b", hadWorker: true });
    await settle();
    expect(connectingB.ofType("revalidate_run")).toMatchObject([{ ctx: { full: true } }]);

    const backoff = track(hostHarness());
    rawTab(backoff, { tabId: "a" });
    await hello(backoff, { resumed: true });
    backoff.sf.connections[0]!.end();
    await settle();
    expect(backoff.host.stream().state().kind).toBe("backoff");
    const backoffB = rawTab(backoff, { tabId: "b", hadWorker: true });
    await settle();
    expect(backoffB.ofType("revalidate_run")).toMatchObject([{ ctx: { full: true } }]);
    expect(backoff.runs).toHaveLength(0);

    const offline = track(hostHarness());
    rawTab(offline, { tabId: "a", online: false });
    await settle();
    expect(offline.host.stream().state().kind).toBe("offline");
    const offlineB = rawTab(offline, { tabId: "b", online: false, hadWorker: true });
    await settle();
    expect(offlineB.ofType("revalidate_run")).toMatchObject([{ ctx: { full: true } }]);

    const hidden = track(hostHarness());
    rawTab(hidden, { tabId: "a", visible: false });
    await settle();
    expect(hidden.host.stream().state().kind).toBe("hidden_closed");
    expect(hidden.sf.connections).toHaveLength(0);
    const hiddenB = rawTab(hidden, { tabId: "b", visible: true, hadWorker: true });
    await settle();
    expect(hiddenB.ofType("revalidate_run")).toMatchObject([{ ctx: { full: true } }]);
    expect(hidden.host.stream().state().kind).toBe("connecting");
    expect(hidden.sf.connections).toHaveLength(1);
    expect(hidden.runs.map((r) => r.ctx.cause)).toEqual(["visible"]);
  });

  it("an attach whose tabId matches a fold member replaces it: the old port receives nothing further and the run in flight resolves without it", async () => {
    const h = track(hostHarness());
    const old = rawTab(h, { tabId: "same" });
    await hello(h, { head: "0" });
    expect(h.runs).toHaveLength(1);
    let settled = false;
    const run = h.runs[0]!.tabs.run(h.runs[0]!.ctx).then(() => {
      settled = true;
    });
    await settle();
    expect(old.ofType("revalidate_run")).toHaveLength(1);
    const replacement = rawTab(h, { tabId: "same", hadWorker: true });
    await settle();
    expect(h.ofKind("tab_attached")).toEqual([
      { kind: "tab_attached", replaced: false, state: "connecting" },
      { kind: "tab_attached", replaced: true, state: "open" },
    ]);
    expect(h.ofKind("tab_detached")).toEqual([{ kind: "tab_detached", cause: "replaced" }]);
    await run;
    expect(settled).toBe(true);
    old.send({ type: "revalidate_done", runId: old.ofType("revalidate_run")[0]!.runId });
    const before = old.received.length;
    h.runs[0]!.resolve();
    await settle();
    h.sf.connections[0]!.push(frame("message", "x", id(1)));
    vi.advanceTimersByTime(HB);
    await settle();
    expect(old.received.length).toBe(before);
    expect(replacement.ofType("frame")).toHaveLength(1);
    expect(replacement.ofType("heartbeat")).toHaveLength(1);
    expect(h.runs[0]!.tabs.size()).toBe(1);
  });

  it("observed with a foreign epoch is refused and reported stale_stamp; the bound epoch is recorded", async () => {
    const h = track(hostHarness());
    const a = rawTab(h);
    await hello(h, { epoch: A, resumed: true });
    a.send({ type: "observed", subject: { kind: "chat", ref: "c1" }, version: "3", epoch: B });
    a.send({ type: "observed", subject: { kind: "chat", ref: "c2" }, version: "4", epoch: A });
    await settle();
    expect(h.ofKind("stale_stamp")).toEqual([
      { kind: "stale_stamp", subject: { kind: "chat", ref: "c1" }, epoch: B },
    ]);
    expect(h.runs).toHaveLength(0);
    expect(h.host.stream().state().kind).toBe("open");
  });

  it("a detached port receives nothing, and detach{logout} stops the host's stream while another port is attached; the next attach starts it again", async () => {
    const h = track(hostHarness());
    const a = rawTab(h, { tabId: "a" });
    const b = rawTab(h, { tabId: "b" });
    await hello(h, { resumed: true });
    a.send({ type: "detach", cause: "logout" });
    await settle();
    expect(h.host.stream().state().kind).toBe("stopped");
    expect(h.ofKind("tab_detached")).toEqual([{ kind: "tab_detached", cause: "logout" }]);
    expect(h.sf.connections[0]!.request.aborted).toBe(true);
    const before = a.received.length;
    vi.advanceTimersByTime(HB);
    await settle();
    expect(a.received.length).toBe(before);
    expect(b.ofType("heartbeat").length).toBeGreaterThan(0);
    rawTab(h, { tabId: "c" });
    await settle();
    expect(h.host.stream().state().kind).toBe("connecting");
    expect(h.sf.connections).toHaveLength(2);
  });

  it("detach('logout') on the attachment sends detach{logout}, which stops the host's stream, and stops a fallback stream too", async () => {
    const h = track(hostHarness());
    const other = rawTab(h, { tabId: "other" });
    const t = tabHarness([h.host]);
    tabs.push(t.attachment);
    await hello(h, { resumed: true });
    expect(h.host.stream().state().kind).toBe("open");
    t.attachment.detach("logout");
    await settle();
    expect(h.ofKind("tab_detached")).toEqual([{ kind: "tab_detached", cause: "logout" }]);
    expect(h.host.stream().state().kind).toBe("stopped");
    expect(h.sf.connections[0]!.request.aborted).toBe(true);
    expect(other.ofType("state").at(-1)?.state.kind).toBe("stopped");

    const fallen = tabHarness([null]);
    tabs.push(fallen.attachment);
    ports.push(fallen.spawned[0]!.peer);
    await settle();
    fallen.spawned[0]!.fireError();
    await settle();
    expect(fallen.attachment.mode()).toBe("fallback");
    fallen.attachment.detach("logout");
    expect(fallen.fallbackStates).toEqual(["start", "stop"]);
    expect(fallen.attachment.state()?.kind).toBe("stopped");
  });

  it("visibility{pagehide} stops delivery to the port at once and the same tabId re-enters with a per-port full", async () => {
    const h = track(hostHarness());
    const a = rawTab(h, { tabId: "a" });
    const b = rawTab(h, { tabId: "b" });
    await hello(h, { resumed: true });
    a.send({ type: "visibility", ev: "pagehide" });
    await settle();
    expect(h.ofKind("tab_detached")).toEqual([{ kind: "tab_detached", cause: "pagehide" }]);
    const before = a.received.length;
    h.sf.connections[0]!.push(frame("message", "x", id(1)));
    vi.advanceTimersByTime(HB);
    await settle();
    expect(a.received.length).toBe(before);
    expect(b.ofType("frame")).toHaveLength(1);
    expect(h.host.stream().state().kind).toBe("open");
    const again = rawTab(h, { tabId: "a", hadWorker: true });
    await settle();
    expect(again.ofType("revalidate_run")).toMatchObject([{ ctx: { full: true } }]);
    expect(h.runs).toHaveLength(0);
  });

  it("heartbeats arrive at heartbeatMs on a faked clock; a visible tab whose port is silent for 3 x heartbeatMs reports worker_dead, sends detach{respawn}, closes its port and calls spawn(); a hidden tab waits for visible", async () => {
    const h = track(hostHarness());
    const a = rawTab(h);
    await settle();
    vi.advanceTimersByTime(HB);
    await settle();
    expect(a.ofType("heartbeat").map((m) => m.seq)).toEqual([1]);
    vi.advanceTimersByTime(HB);
    await settle();
    expect(a.ofType("heartbeat").map((m) => m.seq)).toEqual([1, 2]);

    const t = tabHarness([null, null]);
    tabs.push(t.attachment);
    const peerMessages: TabToWorker[] = [];
    t.spawned[0]!.peer.onmessage = (e: MessageEvent) => {
      peerMessages.push(e.data as TabToWorker);
    };
    ports.push(t.spawned[0]!.peer);
    await settle();
    vi.advanceTimersByTime(3 * HB - 1);
    await settle();
    expect(t.ofKind("worker_dead")).toEqual([]);
    vi.advanceTimersByTime(1);
    await settle();
    expect(t.ofKind("worker_dead")).toEqual([
      { kind: "worker_dead", sinceLastPortMessageMs: 3 * HB },
    ]);
    expect(peerMessages.map((m) => m.type)).toEqual(["attach", "detach"]);
    expect(peerMessages[1]).toEqual({ type: "detach", cause: "respawn" });
    expect(t.spawned).toHaveLength(2);
    expect(t.ofKind("worker_spawned")).toHaveLength(2);

    const hiddenTab = tabHarness([null], false);
    tabs.push(hiddenTab.attachment);
    ports.push(hiddenTab.spawned[0]!.peer);
    await settle();
    vi.advanceTimersByTime(10 * HB);
    await settle();
    expect(hiddenTab.ofKind("worker_dead")).toEqual([]);
    expect(hiddenTab.spawned).toHaveLength(1);
    hiddenTab.visibility.emit("visible");
    await settle();
    vi.advanceTimersByTime(3 * HB);
    await settle();
    expect(hiddenTab.ofKind("worker_dead")).toHaveLength(1);
    expect(hiddenTab.spawned).toHaveLength(2);
  });

  it("on visible the tab sends ping and waits: a pong 14 s later clears the suspicion, and silence for the full window is worker_dead", async () => {
    const t = tabHarness([null], false);
    tabs.push(t.attachment);
    const peer = t.spawned[0]!.peer;
    ports.push(peer);
    const peerMessages: TabToWorker[] = [];
    peer.onmessage = (e: MessageEvent) => {
      peerMessages.push(e.data as TabToWorker);
    };
    await settle();
    vi.advanceTimersByTime(20 * HB);
    t.visibility.emit("visible");
    await settle();
    const ping = peerMessages.find((m) => m.type === "ping");
    expect(ping).toEqual({ type: "ping", seq: 1 });
    expect(t.ofKind("worker_dead")).toEqual([]);
    vi.advanceTimersByTime(14_000);
    peer.postMessage({ type: "pong", seq: 1 } satisfies WorkerToTab);
    await settle();
    vi.advanceTimersByTime(1000);
    await settle();
    expect(t.ofKind("worker_dead")).toEqual([]);
    expect(t.spawned).toHaveLength(1);
    t.visibility.emit("hidden");
    vi.advanceTimersByTime(HB);
    t.visibility.emit("visible");
    await settle();
    vi.advanceTimersByTime(3 * HB);
    await settle();
    expect(t.ofKind("worker_dead")).toEqual([
      { kind: "worker_dead", sinceLastPortMessageMs: 3 * HB },
    ]);
  });

  it("two silent spawns then fallback() once with worker_unavailable{silent}; an error on construction falls back at once", async () => {
    const silent = tabHarness([null, null, null]);
    tabs.push(silent.attachment);
    await settle();
    vi.advanceTimersByTime(3 * HB);
    await settle();
    expect(silent.spawned).toHaveLength(2);
    expect(silent.fallbacks).toBe(0);
    vi.advanceTimersByTime(3 * HB);
    await settle();
    expect(silent.spawned).toHaveLength(2);
    expect(silent.fallbacks).toBe(1);
    expect(silent.attachment.mode()).toBe("fallback");
    expect(silent.ofKind("worker_unavailable")).toEqual([
      { kind: "worker_unavailable", cause: "silent" },
    ]);
    expect(silent.fallbackStates).toEqual(["start"]);
    for (const w of silent.spawned) {
      ports.push(w.peer);
    }

    const errored = tabHarness([null]);
    tabs.push(errored.attachment);
    ports.push(errored.spawned[0]!.peer);
    await settle();
    errored.spawned[0]!.fireError();
    await settle();
    expect(errored.attachment.mode()).toBe("fallback");
    expect(errored.ofKind("worker_unavailable")).toEqual([
      { kind: "worker_unavailable", cause: "error" },
    ]);
    expect(errored.fallbacks).toBe(1);
    vi.advanceTimersByTime(10 * HB);
    await settle();
    expect(errored.spawned).toHaveLength(1);
  });

  it("a fallen-back tab retries spawn() once per visible and recovers with worker_recovered when the worker heartbeats", async () => {
    const h = track(hostHarness());
    const t = tabHarness([null, null, h.host, null]);
    tabs.push(t.attachment);
    await settle();
    vi.advanceTimersByTime(6 * HB);
    await settle();
    expect(t.attachment.mode()).toBe("fallback");
    expect(t.spawned).toHaveLength(2);
    ports.push(t.spawned[0]!.peer, t.spawned[1]!.peer);
    t.visibility.emit("visible");
    await settle();
    expect(t.spawned).toHaveLength(2);
    t.visibility.emit("hidden");
    t.visibility.emit("visible");
    await settle();
    expect(t.spawned).toHaveLength(3);
    expect(t.attachment.mode()).toBe("fallback");
    vi.advanceTimersByTime(HB);
    await settle();
    expect(t.attachment.mode()).toBe("worker");
    expect(t.ofKind("worker_recovered")).toHaveLength(1);
    expect(t.fallbackStates).toEqual(["start", "stop"]);
    expect(h.ofKind("tab_attached")).toEqual([
      { kind: "tab_attached", replaced: false, state: "connecting" },
    ]);
    await hello(h, { resumed: true });
    expect(h.host.stream().state().kind).toBe("open");

    const stays = tabHarness([null, null, null]);
    tabs.push(stays.attachment);
    await settle();
    vi.advanceTimersByTime(6 * HB);
    await settle();
    expect(stays.attachment.mode()).toBe("fallback");
    stays.visibility.emit("hidden");
    stays.visibility.emit("visible");
    await settle();
    expect(stays.spawned).toHaveLength(3);
    vi.advanceTimersByTime(3 * HB);
    await settle();
    expect(stays.attachment.mode()).toBe("fallback");
    expect(stays.fallbackStates).toEqual(["start"]);
    expect(stays.ofKind("worker_recovered")).toEqual([]);
    for (const w of stays.spawned) {
      ports.push(w.peer);
    }
  });

  it("a port that sends no heartbeat_ack for 3 x heartbeatMs is expired: it leaves the fold and the quorum and is reported port_expired", async () => {
    const h = track(hostHarness());
    const a = rawTab(h, { tabId: "a", ack: false });
    const b = rawTab(h, { tabId: "b" });
    await hello(h, { head: "0" });
    let settled = false;
    void h.runs[0]!.tabs.run(h.runs[0]!.ctx).then(() => {
      settled = true;
    });
    await settle();
    const runId = a.ofType("revalidate_run")[0]!.runId;
    b.send({ type: "revalidate_done", runId });
    await settle();
    expect(settled).toBe(false);
    b.send({ type: "visibility", ev: "hidden" });
    await settle();
    expect(h.host.stream().state()).toMatchObject({ kind: "open", visible: true });
    await beats(3);
    expect(h.ofKind("port_expired")).toEqual([]);
    await beats(1);
    expect(h.ofKind("port_expired")).toEqual([{ kind: "port_expired", sinceLastAckMs: 4 * HB }]);
    expect(settled).toBe(true);
    expect(h.host.stream().state()).toMatchObject({ kind: "open", visible: false });
    a.send({ type: "revalidate_done", runId });
    await settle();
    expect(h.runs).toHaveLength(1);
    h.sf.connections[0]!.push(frame("message", "x", id(1)));
    h.runs[0]!.resolve();
    await settle();
    expect(a.ofType("frame")).toEqual([]);
    expect(b.ofType("frame")).toHaveLength(1);
  });

  it("the expired port's next heartbeat_ack earns exactly one stale and one revalidate_run{full} addressed to it, and the queued slot is unchanged", async () => {
    const h = track(hostHarness());
    const a = rawTab(h, { tabId: "a", ack: false });
    const b = rawTab(h, { tabId: "b" });
    await hello(h, { resumed: true });
    await beats(4);
    expect(h.ofKind("port_expired")).toHaveLength(1);
    a.send({ type: "heartbeat_ack", seq: a.ofType("heartbeat").at(-1)!.seq });
    await settle();
    expect(a.ofType("stale")).toHaveLength(1);
    expect(a.ofType("revalidate_run")).toMatchObject([{ ctx: { full: true } }]);
    expect(b.ofType("stale")).toEqual([]);
    expect(b.ofType("revalidate_run")).toEqual([]);
    expect(h.ofKind("port_repaired")).toHaveLength(1);
    expect(h.runs).toHaveLength(0);
    h.sf.connections[0]!.push(frame("message", "x", id(1)));
    await settle();
    expect(a.ofType("frame")).toHaveLength(1);
  });

  it("outbound frames to a port that has stopped acknowledging stop at heldMaxFrames, and the port is expired at that count before 3 x heartbeatMs", async () => {
    const h = track(hostHarness());
    const a = rawTab(h, { tabId: "a", ack: false });
    await hello(h, { resumed: true });
    const conn = h.sf.connections[0]!;
    let chunk = "";
    for (let i = 1; i <= 2001; i++) {
      chunk += frame("message", "x", id(i));
    }
    conn.push(chunk);
    await settle();
    expect(h.ofKind("port_expired")).toHaveLength(1);
    expect(a.ofType("frame")).toHaveLength(2000);
    expect(h.host.stream().state()).toMatchObject({ kind: "open", visible: false });
  });

  it("observe() on a worker-mode attachment records into the host's version map; in fallback mode it records into the attachment's own versions", async () => {
    const h = track(hostHarness());
    const t = tabHarness([h.host]);
    tabs.push(t.attachment);
    await hello(h, { epoch: A, resumed: true });
    t.attachment.observe({ kind: "chat", ref: "c1" }, "3", A);
    await settle();
    expect(h.versions.snapshot()).toEqual({
      epoch: A,
      held: [{ kind: "chat", ref: "c1", version: "3" }],
    });
    expect(t.versions.snapshot().held).toEqual([]);

    const fallen = tabHarness([null]);
    tabs.push(fallen.attachment);
    ports.push(fallen.spawned[0]!.peer);
    await settle();
    fallen.spawned[0]!.fireError();
    await settle();
    expect(fallen.attachment.mode()).toBe("fallback");
    fallen.attachment.observe({ kind: "chat", ref: "c2" }, "7", B);
    expect(fallen.versions.snapshot()).toEqual({
      epoch: B,
      held: [{ kind: "chat", ref: "c2", version: "7" }],
    });
    expect(h.versions.snapshot().held).toHaveLength(1);
  });

  it("tabs.run(ctx, verdict) carries changed and removed to every tab's context; without a verdict the context has neither key", async () => {
    const h = track(hostHarness());
    const t = tabHarness([h.host]);
    tabs.push(t.attachment);
    const raw = rawTab(h, { tabId: "raw" });
    await hello(h, { head: "0" });
    expect(h.runs).toHaveLength(1);
    const run = h.runs[0]!;
    const verdict = {
      changed: [{ kind: "chat", ref: "c1", version: "4" }],
      removed: [{ kind: "chat", ref: "c2", reason: "gone" as const }],
    };
    const withVerdict = run.tabs.run(run.ctx, verdict);
    await settle();
    raw.send({ type: "revalidate_done", runId: raw.ofType("revalidate_run")[0]!.runId });
    await withVerdict;
    expect(t.runs).toHaveLength(1);
    expect(t.runs[0]).toMatchObject({
      cause: "hello",
      full: false,
      changed: [{ kind: "chat", ref: "c1", version: "4" }],
      removed: [{ kind: "chat", ref: "c2", reason: "gone" }],
    });
    expect(t.runs[0]!.signal).toBeInstanceOf(AbortSignal);
    expect(raw.ofType("revalidate_run")[0]!.ctx).toEqual({
      cause: "hello",
      epoch: A,
      generation: 1,
      full: false,
      changed: [{ kind: "chat", ref: "c1", version: "4" }],
      removed: [{ kind: "chat", ref: "c2", reason: "gone" }],
    });
    const withoutVerdict = run.tabs.run(run.ctx);
    await settle();
    raw.send({ type: "revalidate_done", runId: raw.ofType("revalidate_run")[1]!.runId });
    await withoutVerdict;
    const plain = raw.ofType("revalidate_run")[1]!.ctx;
    expect(plain).toEqual({ cause: "hello", epoch: A, generation: 1, full: false });
    expect("changed" in plain).toBe(false);
    expect("removed" in plain).toBe(false);
  });

  it("reconnect() on a worker-mode attachment reconnects the host's stream once, keeping the cursor unless resetCursor is set; in fallback mode it drives the fallback stream", async () => {
    const h = track(hostHarness());
    const t = tabHarness([h.host]);
    tabs.push(t.attachment);
    await hello(h, { epoch: A, resumed: true });
    h.sf.connections[0]!.push(frame("message", "one", id(1)));
    await settle();
    t.attachment.reconnect();
    await settle();
    expect(h.sf.connections).toHaveLength(2);
    expect(h.sf.connections[0]!.request.aborted).toBe(true);
    expect(h.sf.connections[1]!.request.headers["Last-Event-ID"]).toBe(`${A}:1`);
    await hello(h, { epoch: A, resumed: true });
    t.attachment.reconnect({ resetCursor: true });
    await settle();
    expect(h.sf.connections).toHaveLength(3);
    expect(h.sf.connections[2]!.request.headers["Last-Event-ID"]).toBeUndefined();
    expect(h.host.stream().state().kind).toBe("connecting");

    const fallen = tabHarness([null]);
    tabs.push(fallen.attachment);
    ports.push(fallen.spawned[0]!.peer);
    await settle();
    fallen.spawned[0]!.fireError();
    await settle();
    fallen.attachment.reconnect();
    fallen.attachment.reconnect({ resetCursor: true });
    expect(fallen.fallbackStates).toEqual(["start", "reconnect", "resetCursor", "reconnect"]);
  });

  it("the first non-empty tag on attach is presented as SSE-Client from the first connect; a differing tag on a later attach replaces it with exactly one reconnect, and the empty tag changes nothing", async () => {
    const h = track(hostHarness());
    rawTab(h, { tabId: "a", tag: "" });
    await settle();
    expect(h.sf.connections).toHaveLength(1);
    expect(h.sf.connections[0]!.request.headers["SSE-Client"]).toBeUndefined();
    rawTab(h, { tabId: "b", tag: "profile_one" });
    await settle();
    expect(h.sf.connections).toHaveLength(2);
    expect(h.sf.connections[0]!.request.aborted).toBe(true);
    expect(h.sf.connections[1]!.request.headers["SSE-Client"]).toBe("profile_one");
    await hello(h, { resumed: true });
    rawTab(h, { tabId: "c", tag: "profile_one" });
    rawTab(h, { tabId: "d", tag: "" });
    await settle();
    expect(h.sf.connections).toHaveLength(2);
    expect(h.host.stream().state().kind).toBe("open");
    rawTab(h, { tabId: "e", tag: "profile_two" });
    await settle();
    expect(h.sf.connections).toHaveLength(3);
    expect(h.sf.connections[2]!.request.headers["SSE-Client"]).toBe("profile_two");
    expect(h.host.stream().state().kind).toBe("connecting");

    const fresh = track(hostHarness());
    rawTab(fresh, { tabId: "a", tag: "profile_one" });
    await settle();
    expect(fresh.sf.connections).toHaveLength(1);
    expect(fresh.sf.connections[0]!.request.headers["SSE-Client"]).toBe("profile_one");
  });

  it("setTag() posts set_tag: the host replaces SSE-Client and reconnects once, an unchanged or empty tag reconnects nothing, and a respawn onto a fresh host attaches with the new tag; in fallback mode it writes the fallback's header and reconnects once per change", async () => {
    const h = track(hostHarness());
    const fresh = track(hostHarness());
    const t = tabHarness([h.host, fresh.host], true, "profile_one");
    tabs.push(t.attachment);
    await hello(h, { resumed: true });
    expect(h.sf.connections[0]!.request.headers["SSE-Client"]).toBe("profile_one");
    t.attachment.setTag("profile_two");
    await settle();
    expect(h.sf.connections).toHaveLength(2);
    expect(h.sf.connections[1]!.request.headers["SSE-Client"]).toBe("profile_two");
    await hello(h, { resumed: true });
    t.attachment.setTag("profile_two");
    t.attachment.setTag("");
    await settle();
    expect(h.sf.connections).toHaveLength(2);
    expect(h.host.stream().state().kind).toBe("open");
    vi.advanceTimersByTime(3 * HB);
    await settle();
    expect(t.ofKind("worker_dead")).toHaveLength(1);
    expect(t.spawned).toHaveLength(2);
    expect(fresh.ofKind("tab_attached")).toHaveLength(1);
    expect(fresh.sf.connections).toHaveLength(1);
    expect(fresh.sf.connections[0]!.request.headers["SSE-Client"]).toBe("profile_two");
    expect(h.sf.connections).toHaveLength(2);

    const fallen = tabHarness([null], true, "profile_one");
    tabs.push(fallen.attachment);
    ports.push(fallen.spawned[0]!.peer);
    await settle();
    fallen.spawned[0]!.fireError();
    await settle();
    expect(fallen.fallbackHeaders).toEqual({ "SSE-Client": "profile_one" });
    fallen.attachment.setTag("profile_one");
    fallen.attachment.setTag("");
    expect(fallen.fallbackStates).toEqual(["start"]);
    expect(fallen.fallbackHeaders).toEqual({ "SSE-Client": "profile_one" });
    fallen.attachment.setTag("profile_two");
    fallen.attachment.setTag("profile_two");
    expect(fallen.fallbackStates).toEqual(["start", "reconnect"]);
    expect(fallen.fallbackHeaders).toEqual({ "SSE-Client": "profile_two" });

    const untagged = tabHarness([null]);
    tabs.push(untagged.attachment);
    ports.push(untagged.spawned[0]!.peer);
    await settle();
    untagged.spawned[0]!.fireError();
    await settle();
    expect(untagged.fallbackHeaders).toEqual({});
  });

  it("a state record reaches the tab after the state broadcast, so state() is current inside the handler; a tab attaching to a live stream receives state then tab_attached naming that state", async () => {
    const h = track(hostHarness());
    const seen: { to: string; state: string | undefined }[] = [];
    const observer = attachToWorker({
      supported: true,
      heartbeatMs: HB,
      visibility: createVisibilityManager(fakeVisibility(true).source),
      online: createOnlineManager(fakeOnline(true).source),
      onFrame: () => undefined,
      onLifecycle(ev) {
        if (ev.kind === "state") {
          seen.push({ to: ev.to, state: observer.state()?.kind });
        }
      },
      spawn: () => fakeWorker(h.host).worker,
      fallback: () => {
        throw new Error("not reached");
      },
    });
    tabs.push(observer);
    await hello(h, { resumed: true });
    expect(seen).toEqual([
      { to: "connecting", state: "connecting" },
      { to: "open", state: "open" },
    ]);

    const late = rawTab(h, { tabId: "late" });
    await settle();
    const types = late.received.map((m) => m.type);
    expect(types.indexOf("state")).toBeGreaterThanOrEqual(0);
    expect(types.indexOf("state")).toBeLessThan(types.indexOf("lifecycle"));
    expect(late.ofType("lifecycle")[0]!.event).toEqual({
      kind: "tab_attached",
      replaced: false,
      state: "open",
    });
    expect(late.ofType("state")[0]!.state.kind).toBe("open");
  });
});
