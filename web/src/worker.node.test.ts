import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LifecycleEvent } from "./lifecycle.js";
import type { TabToWorker, WorkerToTab } from "./port-protocol.js";
import type { RevalidateContext } from "./stream.js";
import {
  FAKE_CLOCK,
  KEEPALIVE_FRAME,
  type ScriptedFetch,
  flush,
  helloFrame,
  scriptedFetch,
} from "./test-helpers/scripted-fetch.js";
import { type VersionMap, createVersionMap } from "./versions.js";
import { type TabSet, type WorkerHost, createWorkerHost } from "./worker.js";

const A = "aaaaaaaaaaaaaaaa";
const HB = 1000;
const URL_ALIVE = "/alive";

/** One host revalidation, held open until the test settles it. */
interface HostRun {
  readonly ctx: RevalidateContext;
  readonly tabs: TabSet;
  reject(reason: unknown): void;
}

interface Host {
  readonly host: WorkerHost;
  readonly sf: ScriptedFetch;
  readonly versions: VersionMap;
  readonly runs: HostRun[];
  ofKind<K extends LifecycleEvent["kind"]>(kind: K): Extract<LifecycleEvent, { kind: K }>[];
}

const hosts: WorkerHost[] = [];

function hostHarness(opts: { readonly alive?: boolean } = {}): Host {
  const sf = scriptedFetch();
  const versions = createVersionMap();
  const events: LifecycleEvent[] = [];
  const runs: HostRun[] = [];
  const host = createWorkerHost({
    url: "/events",
    fetch: sf.fetch,
    versions,
    heartbeatMs: HB,
    ...(opts.alive === true ? { alive: { url: URL_ALIVE, everyBeats: 1 } } : {}),
    onLifecycle(ev) {
      events.push(ev);
    },
    revalidate(ctx, tabs) {
      return new Promise<void>((_resolve, reject) => {
        runs.push({ ctx, tabs, reject });
      });
    },
  });
  hosts.push(host);
  return {
    host,
    sf,
    versions,
    runs,
    ofKind(kind) {
      return events.filter((e) => e.kind === kind) as Extract<
        LifecycleEvent,
        { kind: typeof kind }
      >[];
    },
  };
}

/** It does not acknowledge heartbeats automatically. */
interface Tab {
  send(message: TabToWorker): void;
  attach(over?: Partial<Extract<TabToWorker, { type: "attach" }>>): void;
  ofType<K extends WorkerToTab["type"]>(type: K): Extract<WorkerToTab, { type: K }>[];
}

function tab(h: Host, tabId: string): Tab {
  const channel = new MessageChannel();
  const received: WorkerToTab[] = [];
  channel.port2.onmessage = (event: MessageEvent) => {
    received.push(event.data as WorkerToTab);
  };
  h.host.attach(channel.port1);
  const send = (message: TabToWorker): void => {
    channel.port2.postMessage(message);
  };
  return {
    send,
    attach(over = {}) {
      send({
        type: "attach",
        tabId,
        visible: true,
        online: true,
        hadWorker: false,
        tag: "",
        ...over,
      });
    },
    ofType(type) {
      return received.filter((m) => m.type === type) as Extract<
        WorkerToTab,
        { type: typeof type }
      >[];
    },
  };
}

async function settle(): Promise<void> {
  await flush(10);
}

async function beats(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    vi.advanceTimersByTime(HB);
    await settle();
  }
}

/** Pushes a hello on the host's latest connection; a fresh hello earns one host run. */
async function hello(h: Host, overrides: Record<string, unknown> = {}): Promise<void> {
  await settle();
  const conn = h.sf.connections.at(-1);
  if (conn === undefined) {
    throw new Error("host opened no connection");
  }
  conn.push(helloFrame({ epoch: A, ...overrides }));
  await settle();
}

beforeEach(() => {
  vi.useFakeTimers(FAKE_CLOCK);
});

afterEach(async () => {
  for (const host of hosts.splice(0)) {
    host.close();
  }
  await flush(4);
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("createWorkerHost: the port conversation", () => {
  it("answers a ping with a pong carrying the same seq, before any attach", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.send({ type: "ping", seq: 21 });
    await settle();
    expect(t.ofType("pong")).toEqual([{ type: "pong", seq: 21 }]);
  });

  it("discards everything but ping from a port that has not attached", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.send({ type: "observed", subject: { kind: "chat", ref: "c1" }, version: "4", epoch: A });
    t.send({ type: "ping", seq: 1 });
    await settle();
    expect(t.ofType("pong")).toEqual([{ type: "pong", seq: 1 }]);
    expect(h.versions.snapshot()).toEqual({ epoch: null, held: [] });
    expect(h.ofKind("tab_attached")).toEqual([]);
  });

  it("reports a tab's arrival with the stream state it joined", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await settle();
    expect(h.ofKind("tab_attached")).toEqual([
      { kind: "tab_attached", replaced: false, state: "connecting" },
    ]);
    expect(t.ofType("state").at(-1)?.state.kind).toBe("connecting");
    expect(h.sf.connections).toHaveLength(1);
  });

  it("heartbeats every attached port and expires one that never acknowledges", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await settle();
    await beats(3);
    expect(t.ofType("heartbeat").map((m) => m.seq)).toEqual([1, 2, 3]);
    expect(h.ofKind("port_expired")).toEqual([]);
    await beats(1);
    expect(h.ofKind("port_expired")).toEqual([{ kind: "port_expired", sinceLastAckMs: 4 * HB }]);
  });

  it("repairs an expired port on its next acknowledgement", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await settle();
    await beats(4);
    expect(h.ofKind("port_expired")).toHaveLength(1);
    t.send({ type: "heartbeat_ack", seq: 4 });
    await settle();
    expect(h.ofKind("port_repaired")).toEqual([{ kind: "port_repaired" }]);
    expect(t.ofType("stale")).toEqual([{ type: "stale" }]);
    expect(t.ofType("revalidate_run")).toMatchObject([{ ctx: { cause: "hello", full: true } }]);
  });

  it("records a tab's stamp into the profile's one version map", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    t.send({ type: "observed", subject: { kind: "chat", ref: "c1" }, version: "41", epoch: A });
    await settle();
    expect(h.versions.snapshot()).toEqual({
      epoch: A,
      held: [{ kind: "chat", ref: "c1", version: "41" }],
    });
  });
});

describe("createWorkerHost: folding the tabs' readings", () => {
  it("takes the profile offline when a tab reports the network gone", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await hello(h, { resumed: true });
    expect(h.host.stream().state().kind).toBe("open");
    t.send({ type: "network", online: false });
    await settle();
    expect(h.host.stream().state().kind).toBe("offline");
  });

  it("ignores a network report from a port it has already expired", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await hello(h, { resumed: true });
    await beats(4);
    expect(h.ofKind("port_expired")).toHaveLength(1);
    expect(h.host.stream().state()).toMatchObject({ kind: "open", visible: false });
    t.send({ type: "network", online: false });
    await settle();
    expect(h.host.stream().state()).toMatchObject({ kind: "open", online: true });
  });

  it("revalidates when a hidden profile's only tab becomes visible", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await hello(h, { resumed: true });
    t.send({ type: "visibility", ev: "hidden" });
    await settle();
    expect(h.host.stream().state()).toMatchObject({ kind: "open", visible: false });
    expect(h.runs).toEqual([]);
    t.send({ type: "visibility", ev: "visible" });
    await settle();
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["visible"]);
    expect(h.host.stream().state()).toMatchObject({ kind: "open", visible: true });
  });

  it("carries a tab's recorded visibility back into the fold when the port is repaired", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach({ visible: false });
    await settle();
    expect(h.host.stream().state().kind).toBe("hidden_closed");
    await beats(4);
    expect(h.ofKind("port_expired")).toHaveLength(1);
    t.send({ type: "visibility", ev: "visible" });
    await settle();
    expect(h.host.stream().state().kind).toBe("hidden_closed");
    expect(h.sf.connections).toEqual([]);
    t.send({ type: "heartbeat_ack", seq: 4 });
    await settle();
    expect(h.ofKind("port_repaired")).toHaveLength(1);
    expect(h.host.stream().state().kind).toBe("connecting");
    expect(h.sf.connections).toHaveLength(1);
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["visible"]);
  });

  it("removes a tab that reports pagehide", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await settle();
    t.send({ type: "visibility", ev: "pagehide" });
    await settle();
    expect(h.ofKind("tab_detached")).toEqual([{ kind: "tab_detached", cause: "pagehide" }]);
  });

  // With no tab attached nothing has reported a network reading, so the stream reads the seed alone.
  it("seeds the profile's network reading from the worker's own navigator", () => {
    vi.stubGlobal("navigator", { onLine: false });
    const h = hostHarness();
    h.host.stream().start();
    expect(h.host.stream().state()).toMatchObject({ kind: "hidden_closed", online: false });
  });

  it("reads a navigator that reports online as online", () => {
    vi.stubGlobal("navigator", { onLine: true });
    const h = hostHarness();
    h.host.stream().start();
    expect(h.host.stream().state()).toMatchObject({ kind: "hidden_closed", online: true });
  });

  it("reads a navigator with no onLine reading as online", () => {
    vi.stubGlobal("navigator", {});
    const h = hostHarness();
    h.host.stream().start();
    expect(h.host.stream().state()).toMatchObject({ kind: "hidden_closed", online: true });
  });
});

describe("createWorkerHost: routing a run to the tabs", () => {
  it("resolves a run with no acknowledging tab without routing anything", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await hello(h);
    const run = h.runs[0]!;
    t.send({ type: "detach", cause: "unload" });
    await settle();
    expect(run.tabs.size()).toBe(0);
    let settled = false;
    void run.tabs.run(run.ctx).then(() => {
      settled = true;
    });
    await settle();
    expect(settled).toBe(true);
    expect(t.ofType("revalidate_run")).toEqual([]);
  });

  it("routes the run to each tab and settles when every one answers done", async () => {
    const h = hostHarness();
    const a = tab(h, "a");
    const b = tab(h, "b");
    a.attach();
    b.attach();
    await hello(h);
    const run = h.runs[0]!;
    let settled = false;
    void run.tabs.run(run.ctx, { changed: [], removed: [] }).then(() => {
      settled = true;
    });
    await settle();
    const routedA = a.ofType("revalidate_run");
    const routedB = b.ofType("revalidate_run");
    expect(routedA).toMatchObject([{ ctx: { cause: "hello", changed: [], removed: [] } }]);
    expect(routedB).toMatchObject([{ runId: routedA[0]!.runId }]);
    a.send({ type: "revalidate_done", runId: routedA[0]!.runId });
    await settle();
    expect(settled).toBe(false);
    b.send({ type: "revalidate_done", runId: routedB[0]!.runId });
    await settle();
    expect(settled).toBe(true);
  });

  it("rejects the run with the cause the tab reported", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await hello(h);
    const run = h.runs[0]!;
    const outcome = expect(run.tabs.run(run.ctx)).rejects.toThrow(/^tab could not apply$/);
    await settle();
    const routed = t.ofType("revalidate_run")[0]!;
    t.send({ type: "revalidate_failed", runId: routed.runId, cause: "tab could not apply" });
    await settle();
    await outcome;
  });

  it("rejects a routed run whose signal aborts before the tabs answer", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await hello(h);
    const run = h.runs[0]!;
    const outcome = expect(run.tabs.run(run.ctx)).rejects.toThrow(/^revalidate run aborted$/);
    await settle();
    expect(t.ofType("revalidate_run")).toHaveLength(1);
    h.host.stream().stop();
    await outcome;
  });
});

describe("createWorkerHost: losing authorisation", () => {
  async function digestRejects(reason: unknown): Promise<Host> {
    const h = hostHarness();
    tab(h, "t1").attach();
    await hello(h);
    expect(h.runs).toHaveLength(1);
    h.runs[0]!.reject(reason);
    await settle();
    return h;
  }

  it("stops the profile's stream when the digest answers 401", async () => {
    const h = await digestRejects({ status: 401 });
    expect(h.ofKind("auth_lost")).toEqual([{ kind: "auth_lost", route: "digest", status: 401 }]);
    expect(h.host.stream().state().kind).toBe("stopped");
  });

  it("leaves the stream running when the digest fails with another status", async () => {
    const h = await digestRejects(Object.assign(new Error("server error"), { status: 500 }));
    expect(h.ofKind("auth_lost")).toEqual([]);
    expect(h.ofKind("revalidate_failed").map((e) => e.cause)).toEqual(["server error"]);
    expect(h.host.stream().state().kind).toBe("backoff");
  });

  it("leaves the stream running when the digest rejects with no status at all", async () => {
    const h = await digestRejects(new Error("network"));
    expect(h.ofKind("auth_lost")).toEqual([]);
    expect(h.ofKind("revalidate_failed").map((e) => e.cause)).toEqual(["network"]);
    expect(h.host.stream().state().kind).toBe("backoff");
  });

  // null is typeof "object": without statusOf's null arm the status read throws, and the run fails
  // with that TypeError instead of the rejection the application raised.
  it("leaves the stream running when the digest rejects with null", async () => {
    const h = await digestRejects(null);
    expect(h.ofKind("auth_lost")).toEqual([]);
    expect(h.ofKind("revalidate_failed").map((e) => e.cause)).toEqual(["null"]);
    expect(h.host.stream().state().kind).toBe("backoff");
  });

  it("leaves the stream running when the digest rejects with a string", async () => {
    const h = await digestRejects("refused");
    expect(h.ofKind("auth_lost")).toEqual([]);
    expect(h.ofKind("revalidate_failed").map((e) => e.cause)).toEqual(["refused"]);
    expect(h.host.stream().state().kind).toBe("backoff");
  });

  it("stops the profile's stream when an alive acknowledgement answers 401", async () => {
    const h = hostHarness({ alive: true });
    h.sf.onPost(() => new Response(null, { status: 401 }));
    tab(h, "t1").attach();
    await hello(h, { resumed: true });
    h.sf.connections[0]!.push(KEEPALIVE_FRAME);
    await settle();
    expect(h.sf.requests.filter((r) => r.method === "POST").map((r) => r.url)).toEqual([URL_ALIVE]);
    expect(h.ofKind("auth_lost")).toEqual([{ kind: "auth_lost", route: "alive", status: 401 }]);
    expect(h.host.stream().state().kind).toBe("stopped");
  });

  it("leaves the stream running when an alive acknowledgement answers 200", async () => {
    const h = hostHarness({ alive: true });
    h.sf.onPost(() => new Response(null, { status: 200 }));
    tab(h, "t1").attach();
    await hello(h, { resumed: true });
    h.sf.connections[0]!.push(KEEPALIVE_FRAME);
    await settle();
    expect(h.ofKind("alive_ack")).toEqual([{ kind: "alive_ack", ok: true, status: 200 }]);
    expect(h.ofKind("auth_lost")).toEqual([]);
    expect(h.host.stream().state().kind).toBe("open");
  });
});

describe("createWorkerHost: leaving", () => {
  it("stops the profile's stream on a logout detach", async () => {
    const h = hostHarness();
    const t = tab(h, "t1");
    t.attach();
    await settle();
    t.send({ type: "detach", cause: "logout" });
    await settle();
    expect(h.ofKind("tab_detached")).toEqual([{ kind: "tab_detached", cause: "logout" }]);
    expect(h.host.stream().state().kind).toBe("stopped");
  });

  it("leaves the stream running on an unload detach", async () => {
    const h = hostHarness();
    const a = tab(h, "a");
    const b = tab(h, "b");
    a.attach();
    b.attach();
    await settle();
    expect(h.ofKind("tab_attached")).toHaveLength(2);
    a.send({ type: "detach", cause: "unload" });
    await settle();
    expect(h.ofKind("tab_detached")).toEqual([{ kind: "tab_detached", cause: "unload" }]);
    expect(h.host.stream().state().kind).toBe("connecting");
    expect(h.sf.connections[0]!.request.aborted).toBe(false);
  });
});
