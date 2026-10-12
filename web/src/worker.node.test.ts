import { afterEach, describe, expect, it, vi } from "vitest";
import type { LifecycleEvent } from "./lifecycle.js";
import type { TabToWorker, WorkerToTab } from "./port-protocol.js";
import type { RevalidateContext } from "./stream.js";
import { createVersionMap } from "./versions.js";
import { type TabSet, type WorkerHost, createWorkerHost } from "./worker.js";

const HB = 40;
const URL_EVENTS = "https://sse.invalid/events";
const URL_ALIVE = "https://sse.invalid/alive";

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

/** A fetch that serves one controllable event stream and answers the alive POST. */
function transport(): {
  readonly fetch: typeof fetch;
  readonly alivePosts: number[];
  aliveStatus: number;
  write(text: string): void;
  hello(over?: Record<string, unknown>): void;
  keepalive(): void;
} {
  const encoder = new TextEncoder();
  let ctl: ReadableStreamDefaultController<Uint8Array> | null = null;
  const alivePosts: number[] = [];
  const self = {
    fetch: ((input: unknown, init?: RequestInit) => {
      if (init?.method === "POST") {
        alivePosts.push(self.aliveStatus);
        return Promise.resolve(new Response(null, { status: self.aliveStatus }));
      }
      void input;
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          ctl = controller;
        },
      });
      return Promise.resolve(
        new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } }),
      );
    }) as unknown as typeof fetch,
    alivePosts,
    aliveStatus: 200,
    write(text: string) {
      ctl?.enqueue(encoder.encode(text));
    },
    hello(over: Record<string, unknown> = {}) {
      const payload = {
        wire: 1,
        epoch: "e1",
        floor: "0",
        head: "0",
        resumed: false,
        verdict: "fresh",
        keepalive_ms: 15_000,
        keepalive_event: "sse:keepalive",
        ...over,
      };
      self.write(`event: sse:hello\ndata: ${JSON.stringify(payload)}\n\n`);
    },
    keepalive() {
      self.write("event: sse:keepalive\ndata: \n\n");
    },
  };
  return self;
}

/** One fake tab: a MessageChannel whose host end the host owns. */
interface Tab {
  readonly sent: WorkerToTab[];
  post(message: TabToWorker): void;
  attach(over?: Partial<Extract<TabToWorker, { type: "attach" }>>): void;
  ofType<K extends WorkerToTab["type"]>(type: K): Extract<WorkerToTab, { type: K }>[];
  readonly id: string;
}

const hosts: WorkerHost[] = [];
let tabSeq = 0;

function harness(
  opts: {
    readonly revalidate?: (ctx: RevalidateContext, tabs: TabSet) => Promise<void>;
    readonly onlineSeed?: boolean;
    readonly alive?: boolean;
    readonly fetch?: typeof fetch;
  } = {},
): {
  readonly host: WorkerHost;
  readonly events: LifecycleEvent[];
  kinds(): string[];
  tab(): Tab;
} {
  const events: LifecycleEvent[] = [];
  const host = createWorkerHost({
    url: URL_EVENTS,
    ...(opts.fetch === undefined ? {} : { fetch: opts.fetch }),
    versions: createVersionMap(),
    heartbeatMs: HB,
    timing: { heartbeatMs: HB },
    ...(opts.alive === true ? { alive: { url: URL_ALIVE } } : {}),
    ...(opts.onlineSeed === undefined ? {} : { onlineSeed: opts.onlineSeed }),
    revalidate: opts.revalidate ?? (() => Promise.resolve()),
    onLifecycle(ev) {
      events.push(ev);
    },
  });
  hosts.push(host);
  return {
    host,
    events,
    kinds: () => events.map((e) => e.kind),
    tab(): Tab {
      const channel = new MessageChannel();
      const sent: WorkerToTab[] = [];
      channel.port2.onmessage = (event: MessageEvent) => {
        sent.push(event.data as WorkerToTab);
      };
      channel.port2.start();
      host.attach(channel.port1 as unknown as MessagePort);
      const id = `tab_${String(++tabSeq)}`;
      return {
        sent,
        id,
        post(message) {
          channel.port2.postMessage(message);
        },
        attach(over = {}) {
          channel.port2.postMessage({
            type: "attach",
            tabId: id,
            visible: true,
            online: true,
            hadWorker: false,
            tag: "",
            ...over,
          });
        },
        ofType(type) {
          return sent.filter((m) => m.type === type) as Extract<
            WorkerToTab,
            { type: typeof type }
          >[];
        },
      };
    },
  };
}

afterEach(() => {
  for (const host of hosts.splice(0)) {
    host.close();
  }
  vi.unstubAllGlobals();
});

describe("createWorkerHost: the port conversation", () => {
  it("answers a ping with a pong carrying the same seq, before any attach", async () => {
    const h = harness();
    const tab = h.tab();
    tab.post({ type: "ping", seq: 21 });
    await waitFor("pong", () => tab.ofType("pong").length === 1);
    expect(tab.ofType("pong")).toEqual([{ type: "pong", seq: 21 }]);
  });

  it("discards everything but ping from a port that has not attached", async () => {
    const h = harness();
    const tab = h.tab();
    tab.post({ type: "reconnect", resetCursor: true });
    tab.post({ type: "ping", seq: 1 });
    await waitFor("pong", () => tab.ofType("pong").length === 1);
    expect(h.kinds()).not.toContain("tab_attached");
  });

  it("reports a tab's arrival with the stream state it joined", async () => {
    const h = harness();
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    expect(h.events.filter((e) => e.kind === "tab_attached")).toEqual([
      { kind: "tab_attached", replaced: false, state: "connecting" },
    ]);
    expect(tab.ofType("state")).toHaveLength(1);
  });

  it("heartbeats every attached port and expires one that never acknowledges", async () => {
    const h = harness();
    const tab = h.tab();
    tab.attach();
    await waitFor("heartbeat", () => tab.ofType("heartbeat").length >= 1);
    await waitFor("expired", () => h.kinds().includes("port_expired"), 4000);
    expect(h.kinds()).toContain("port_expired");
  });

  it("repairs an expired port on its next acknowledgement", async () => {
    const h = harness();
    const tab = h.tab();
    tab.attach();
    await waitFor("expired", () => h.kinds().includes("port_expired"), 4000);
    tab.post({ type: "heartbeat_ack", seq: 1 });
    await waitFor("repaired", () => h.kinds().includes("port_repaired"), 4000);
    expect(tab.ofType("stale")).toHaveLength(1);
  });

  it("records a tab's stamp into the profile's one version map", async () => {
    const map = createVersionMap();
    const host = createWorkerHost({
      url: URL_EVENTS,
      fetch: transport().fetch,
      versions: map,
      heartbeatMs: HB,
      revalidate: () => Promise.resolve(),
    });
    hosts.push(host);
    const channel = new MessageChannel();
    channel.port2.start();
    host.attach(channel.port1 as unknown as MessagePort);
    channel.port2.postMessage({
      type: "attach",
      tabId: "t1",
      visible: true,
      online: true,
      hadWorker: false,
      tag: "",
    });
    channel.port2.postMessage({
      type: "observed",
      subject: { kind: "chat", ref: "c1" },
      version: "41",
      epoch: "e9",
    });
    await waitFor("observed", () => map.has({ kind: "chat", ref: "c1" }));
    expect(map.snapshot()).toEqual({
      epoch: "e9",
      held: [{ kind: "chat", ref: "c1", version: "41" }],
    });
  });
});

describe("createWorkerHost: folding the tabs' readings", () => {
  it("takes the profile offline when a tab reports the network gone", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    tab.post({ type: "network", online: false });
    await waitFor("offline", () => h.host.stream().state().kind === "offline", 4000);
    expect(h.host.stream().state().kind).toBe("offline");
  });

  it("ignores a network report from a port it has already expired", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach();
    await waitFor("expired", () => h.kinds().includes("port_expired"), 4000);
    const before = h.host.stream().state().kind;
    tab.post({ type: "network", online: false });
    await tick(3 * HB);
    expect(h.host.stream().state().kind).toBe(before);
  });

  it("revalidates when a hidden profile's only tab becomes visible", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach({ visible: false });
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    t.hello();
    await waitFor("open", () => h.host.stream().state().kind === "open", 4000);
    tab.post({ type: "visibility", ev: "visible" });
    await waitFor(
      "revalidate",
      () => h.events.some((e) => e.kind === "revalidate" && e.cause === "visible"),
      4000,
    );
    expect(h.events.some((e) => e.kind === "revalidate" && e.cause === "visible")).toBe(true);
  });

  it("carries a tab's recorded visibility back into the fold when the port is repaired", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach({ visible: false });
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    t.hello();
    await waitFor("open", () => h.host.stream().state().kind === "open", 4000);
    tab.post({ type: "visibility", ev: "visible" });
    await waitFor(
      "visible",
      () => h.events.some((e) => e.kind === "revalidate" && e.cause === "visible"),
      4000,
    );
    await waitFor("expired", () => h.kinds().includes("port_expired"), 4000);
    const runsBefore = h.events.filter((e) => e.kind === "revalidate").length;
    tab.post({ type: "heartbeat_ack", seq: 1 });
    await waitFor("repaired", () => h.kinds().includes("port_repaired"), 4000);
    // The repair re-attaches the tab at the visibility it last reported, so the profile
    // becomes visible again and earns a run; a repair at hidden would earn none.
    await waitFor(
      "run after repair",
      () => h.events.filter((e) => e.kind === "revalidate").length > runsBefore,
      4000,
    );
    expect(h.events.filter((e) => e.kind === "revalidate").length).toBeGreaterThan(runsBefore);
  });

  it("removes a tab that reports pagehide", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    tab.post({ type: "visibility", ev: "pagehide" });
    await waitFor("detached", () => h.kinds().includes("tab_detached"));
    expect(h.events.filter((e) => e.kind === "tab_detached")).toEqual([
      { kind: "tab_detached", cause: "pagehide" },
    ]);
  });

  it("seeds the profile's network reading from the worker's own navigator", async () => {
    vi.stubGlobal("navigator", { onLine: false });
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach({ online: false });
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    expect(h.host.stream().state().kind).toBe("offline");
  });

  it("treats a navigator that reports online as a reason to connect", async () => {
    vi.stubGlobal("navigator", { onLine: true });
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    expect(h.host.stream().state().kind).toBe("connecting");
  });

  it("treats a navigator with no onLine reading as a reason to connect", async () => {
    vi.stubGlobal("navigator", {});
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    expect(h.host.stream().state().kind).toBe("connecting");
  });
});

describe("createWorkerHost: routing a run to the tabs", () => {
  it("resolves a run with no acknowledging tab without routing anything", async () => {
    const t = transport();
    const settled: string[] = [];
    const h = harness({
      fetch: t.fetch,
      async revalidate(ctx, tabs) {
        expect(tabs.size()).toBe(0);
        await tabs.run(ctx);
        settled.push("resolved");
      },
    });
    h.host.stream().start();
    t.hello();
    await waitFor("resolved", () => settled.length === 1, 4000);
    expect(settled).toEqual(["resolved"]);
  });

  it("routes the run to each tab and settles when every one answers done", async () => {
    const t = transport();
    const settled: string[] = [];
    const h = harness({
      fetch: t.fetch,
      async revalidate(ctx, tabs) {
        await tabs.run(ctx, { changed: [], removed: [] });
        settled.push("resolved");
      },
    });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    t.hello();
    await waitFor("routed", () => tab.ofType("revalidate_run").length >= 1, 4000);
    const run = tab.ofType("revalidate_run")[0];
    expect(run?.ctx).toMatchObject({ changed: [], removed: [] });
    tab.post({ type: "revalidate_done", runId: run?.runId ?? 0 });
    await waitFor("resolved", () => settled.length === 1, 4000);
    expect(settled).toEqual(["resolved"]);
  });

  it("rejects the run with the cause the tab reported", async () => {
    const t = transport();
    const failures: string[] = [];
    const h = harness({
      fetch: t.fetch,
      async revalidate(ctx, tabs) {
        try {
          await tabs.run(ctx);
        } catch (e: unknown) {
          failures.push(e instanceof Error ? e.message : String(e));
        }
      },
    });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    t.hello();
    await waitFor("routed", () => tab.ofType("revalidate_run").length >= 1, 4000);
    const run = tab.ofType("revalidate_run")[0];
    tab.post({
      type: "revalidate_failed",
      runId: run?.runId ?? 0,
      cause: "tab could not apply",
    });
    await waitFor("rejected", () => failures.length === 1, 4000);
    expect(failures).toEqual(["tab could not apply"]);
  });

  it("rejects a routed run whose signal aborts before the tabs answer", async () => {
    const t = transport();
    const failures: string[] = [];
    const h = harness({
      fetch: t.fetch,
      async revalidate(ctx, tabs) {
        try {
          await tabs.run(ctx);
        } catch (e: unknown) {
          failures.push(e instanceof Error ? e.message : String(e));
        }
      },
    });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    t.hello();
    await waitFor("routed", () => tab.ofType("revalidate_run").length >= 1, 4000);
    h.host.stream().stop();
    await waitFor("rejected", () => failures.length === 1, 4000);
    expect(failures).toEqual(["revalidate run aborted"]);
  });
});

describe("createWorkerHost: losing authorisation", () => {
  it("stops the profile's stream when the digest answers 401", async () => {
    const t = transport();
    const h = harness({
      fetch: t.fetch,
      revalidate: () => Promise.reject({ status: 401 }),
    });
    h.host.stream().start();
    t.hello();
    await waitFor("auth_lost", () => h.kinds().includes("auth_lost"), 4000);
    expect(h.events.filter((e) => e.kind === "auth_lost")).toEqual([
      { kind: "auth_lost", route: "digest", status: 401 },
    ]);
    expect(h.host.stream().state().kind).toBe("stopped");
  });

  it("leaves the stream running when the digest fails with another status", async () => {
    const t = transport();
    const h = harness({
      fetch: t.fetch,
      revalidate: () => Promise.reject({ status: 500 }),
    });
    h.host.stream().start();
    t.hello();
    await waitFor("failed", () => h.kinds().includes("revalidate_failed"), 4000);
    expect(h.kinds()).not.toContain("auth_lost");
  });

  it("leaves the stream running when the digest rejects with no status at all", async () => {
    const t = transport();
    const h = harness({
      fetch: t.fetch,
      revalidate: () => Promise.reject(new Error("network")),
    });
    h.host.stream().start();
    t.hello();
    await waitFor("failed", () => h.kinds().includes("revalidate_failed"), 4000);
    expect(h.kinds()).not.toContain("auth_lost");
  });

  // null is typeof "object", so the guard's second arm is the only thing standing between
  // this rejection and a property read on null.
  it("leaves the stream running when the digest rejects with null", async () => {
    const t = transport();
    const h = harness({
      fetch: t.fetch,
      revalidate: () => Promise.reject(null),
    });
    h.host.stream().start();
    t.hello();
    await waitFor("failed", () => h.kinds().includes("revalidate_failed"), 4000);
    expect(h.kinds()).not.toContain("auth_lost");
  });

  it("leaves the stream running when the digest rejects with a string", async () => {
    const t = transport();
    const h = harness({
      fetch: t.fetch,
      revalidate: () => Promise.reject("refused"),
    });
    h.host.stream().start();
    t.hello();
    await waitFor("failed", () => h.kinds().includes("revalidate_failed"), 4000);
    expect(h.kinds()).not.toContain("auth_lost");
  });

  it("stops the profile's stream when an alive acknowledgement answers 401", async () => {
    const t = transport();
    t.aliveStatus = 401;
    const h = harness({ fetch: t.fetch, alive: true });
    h.host.stream().start();
    t.hello();
    await waitFor("open", () => h.host.stream().state().kind === "open", 4000);
    t.keepalive();
    await waitFor("auth_lost", () => h.kinds().includes("auth_lost"), 4000);
    expect(h.events.filter((e) => e.kind === "auth_lost")).toEqual([
      { kind: "auth_lost", route: "alive", status: 401 },
    ]);
    expect(h.host.stream().state().kind).toBe("stopped");
  });

  it("leaves the stream running when an alive acknowledgement answers 200", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch, alive: true });
    h.host.stream().start();
    t.hello();
    await waitFor("open", () => h.host.stream().state().kind === "open", 4000);
    t.keepalive();
    await waitFor("ack", () => h.kinds().includes("alive_ack"), 4000);
    expect(h.kinds()).not.toContain("auth_lost");
    expect(h.host.stream().state().kind).toBe("open");
  });
});

describe("createWorkerHost: leaving", () => {
  it("stops the profile's stream on a logout detach", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const tab = h.tab();
    tab.attach();
    await waitFor("attached", () => h.kinds().includes("tab_attached"));
    tab.post({ type: "detach", cause: "logout" });
    await waitFor("stopped", () => h.host.stream().state().kind === "stopped", 4000);
    expect(h.host.stream().state().kind).toBe("stopped");
  });

  it("leaves the stream running on an unload detach", async () => {
    const t = transport();
    const h = harness({ fetch: t.fetch });
    const a = h.tab();
    const b = h.tab();
    a.attach();
    b.attach();
    await waitFor("both", () => h.events.filter((e) => e.kind === "tab_attached").length === 2);
    a.post({ type: "detach", cause: "unload" });
    await waitFor("detached", () => h.kinds().includes("tab_detached"));
    expect(h.host.stream().state().kind).not.toBe("stopped");
  });
});
