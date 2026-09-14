import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createStream, streamInternals } from "./stream.js";
import {
  FAKE_CLOCK,
  KEEPALIVE_FRAME,
  flush,
  frame,
  helloFrame,
} from "./test-helpers/scripted-fetch.js";
import { harness } from "./test-helpers/stream-harness.js";
import { DEFAULT_TIMING, MAX_FRAME_BYTES } from "./timing.js";
import { createVersionMap } from "./versions.js";

const A = "aaaaaaaaaaaaaaaa";
const B = "bbbbbbbbbbbbbbbb";

function id(offset: number | string, epoch = A): string {
  return `${epoch}:${String(offset)}`;
}

/** A frame whose encoded size is exactly `size` bytes at offset `offset`. */
function sizedFrame(offset: number, size: number): string {
  const head = `id: ${id(offset)}\ndata: `;
  const payload = "x".repeat(size - head.length - 2);
  return `${head}${payload}\n\n`;
}

describe("stream runtime", () => {
  beforeEach(() => {
    vi.useFakeTimers(FAKE_CLOCK);
    vi.spyOn(Math, "random").mockReturnValue(0);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("the watchdog fires after max(3k, 15 s) of silence while visible", async () => {
    const h = harness();
    await h.open({ keepalive_ms: 15000, resumed: true });
    vi.advanceTimersByTime(44_999);
    await flush();
    expect(h.stream.state().kind).toBe("open");
    vi.advanceTimersByTime(1);
    await flush();
    expect(h.ofKind("watchdog")).toEqual([{ kind: "watchdog", sinceLastByteMs: 45_000 }]);
    expect(h.stream.state().kind).toBe("connecting");
    expect(h.sf.connections[0]!.request.aborted).toBe(true);
    expect(h.sf.connections).toHaveLength(2);

    const floor = harness();
    await floor.open({ keepalive_ms: 1000, resumed: true });
    vi.advanceTimersByTime(14_999);
    await flush();
    expect(floor.stream.state().kind).toBe("open");
    vi.advanceTimersByTime(1);
    await flush();
    expect(floor.ofKind("watchdog")).toEqual([{ kind: "watchdog", sinceLastByteMs: 15_000 }]);
  });

  it("a slowly delivered large frame keeps the watchdog fed", async () => {
    const h = harness();
    const conn = await h.open({ resumed: true });
    conn.push(`id: ${id(1)}\ndata: `);
    for (let i = 0; i < 4; i++) {
      vi.advanceTimersByTime(30_000);
      await flush();
      conn.push("y".repeat(100_000));
      await flush();
    }
    conn.push("\n\n");
    await flush();
    expect(h.ofKind("watchdog")).toEqual([]);
    expect(h.stream.state().kind).toBe("open");
    expect(h.frames).toHaveLength(1);
    expect(h.frames[0]!.data).toHaveLength(400_000);
  });

  it("hidden disarms the watchdog and visible re-checks lastByteAt", async () => {
    const h = harness();
    await h.open({ resumed: true });
    h.visibility.emit("hidden");
    vi.advanceTimersByTime(50_000);
    await flush();
    expect(h.ofKind("watchdog")).toEqual([]);
    expect(h.stream.state().kind).toBe("open");
    h.visibility.emit("visible");
    await flush();
    expect(h.ofKind("watchdog")).toEqual([{ kind: "watchdog", sinceLastByteMs: 50_000 }]);
    expect(h.stream.state().kind).toBe("connecting");
    expect(h.sf.connections[0]!.request.aborted).toBe(true);
    expect(h.ofKind("revalidate").map((e) => e.cause)).toEqual(["visible"]);

    const fed = harness();
    const conn = await fed.open({ resumed: true });
    fed.visibility.emit("hidden");
    vi.advanceTimersByTime(30_000);
    conn.push(KEEPALIVE_FRAME);
    await flush();
    vi.advanceTimersByTime(10_000);
    fed.visibility.emit("visible");
    await flush();
    expect(fed.ofKind("watchdog")).toEqual([]);
    expect(fed.stream.state().kind).toBe("open");
    fed.runs[0]!.resolve();
    await flush();
    vi.advanceTimersByTime(45_000);
    await flush();
    expect(fed.ofKind("watchdog")).toEqual([{ kind: "watchdog", sinceLastByteMs: 55_000 }]);
  });

  it("the hidden timer closes at hiddenCloseAfterMs", async () => {
    const h = harness();
    const conn = await h.open({ resumed: true });
    h.visibility.emit("hidden");
    vi.advanceTimersByTime(59_999);
    await flush();
    expect(h.stream.state().kind).toBe("open");
    vi.advanceTimersByTime(1);
    await flush();
    expect(h.stream.state().kind).toBe("hidden_closed");
    expect(h.ofKind("hidden_closed")).toEqual([{ kind: "hidden_closed", arm: "timer" }]);
    expect(conn.request.aborted).toBe(true);
    h.visibility.emit("visible");
    await flush();
    h.last().push(helloFrame({ resumed: true }));
    await flush();
    expect(h.ofKind("reopened")).toHaveLength(1);
  });

  it("a keepalive parsed while hidden past the threshold closes from the read path with arm read", async () => {
    const h = harness({ alive: { url: "/alive" } });
    const conn = await h.open({ resumed: true });
    h.visibility.emit("hidden");
    vi.setSystemTime(Date.now() + 60_000);
    conn.push(KEEPALIVE_FRAME);
    await flush();
    expect(h.stream.state().kind).toBe("hidden_closed");
    expect(h.ofKind("hidden_closed")).toEqual([{ kind: "hidden_closed", arm: "read" }]);
    expect(h.sf.requests.filter((r) => r.method === "POST")).toHaveLength(0);
  });

  it("a short-lived connection backs off at max(jitter, retryMs)", async () => {
    const h = harness();
    const conn = await h.open({ resumed: true });
    conn.end();
    await flush();
    const state = h.stream.state();
    expect(state.kind).toBe("backoff");
    if (state.kind === "backoff") {
      expect(state.until - Date.now()).toBe(1500);
      expect(state.attempt).toBe(1);
    }

    const hinted = harness();
    const c2 = await hinted.open({ resumed: true });
    c2.push("retry: 5000\n\n");
    await flush();
    c2.end();
    await flush();
    const s2 = hinted.stream.state();
    expect(s2.kind).toBe("backoff");
    if (s2.kind === "backoff") {
      expect(s2.until - Date.now()).toBe(5000);
    }
  });

  it("reset:shutdown after stableMs still backs off", async () => {
    const h = harness();
    const conn = await h.open({ resumed: true });
    vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
    conn.push(`event: sse:reset\ndata: {"reason":"shutdown"}\n\n`);
    await flush();
    expect(h.ofKind("reset")).toEqual([{ kind: "reset", reason: "reset:shutdown" }]);
    const state = h.stream.state();
    expect(state.kind).toBe("backoff");
    if (state.kind === "backoff") {
      expect(state.until - Date.now()).toBe(1500);
    }
    expect(h.sf.connections).toHaveLength(1);

    const eof = harness();
    const c2 = await eof.open({ resumed: true });
    vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
    c2.end();
    await flush();
    expect(eof.stream.state().kind).toBe("connecting");
    expect(eof.sf.connections).toHaveLength(2);
  });

  it("a wake bypasses backoff without resetting attempt", async () => {
    const h = harness();
    const conn = await h.open({ resumed: true });
    conn.end();
    await flush();
    expect(h.stream.state()).toMatchObject({ kind: "backoff", attempt: 1 });
    h.visibility.emit("visible");
    await flush();
    expect(h.stream.state()).toMatchObject({ kind: "connecting", attempt: 1 });
    expect(h.sf.connections).toHaveLength(2);
    expect(h.ofKind("revalidate").map((e) => e.cause)).toEqual(["visible"]);
  });

  it("a non-resumed hello adopts head at once and emits revalidate('hello')", async () => {
    const h = harness();
    await h.open({ head: "42", floor: "10" });
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "42" });
    expect(h.ofKind("revalidate")).toEqual([{ kind: "revalidate", cause: "hello", full: false }]);
    expect(h.runs).toHaveLength(1);
    expect(h.runs[0]!.ctx).toMatchObject({ cause: "hello", epoch: A, full: false, generation: 1 });
    expect(h.versions.epoch()).toBe(A);
  });

  it("a resumed hello keeps the cursor and adopts head after the first non-replay frame", async () => {
    const h = harness();
    const first = await h.open({ head: "5" });
    h.runs[0]!.resolve();
    await flush();
    vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
    first.end();
    await flush();
    const second = h.last();
    expect(second.request.headers["Last-Event-ID"]).toBe(id(5));
    second.push(helloFrame({ resumed: true, head: "9", floor: "1" }));
    await flush();
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "5" });
    expect(h.runs).toHaveLength(1);
    second.push(frame("message", "six", id(6)));
    second.push(frame("message", "seven", id(7)));
    await flush();
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "7" });
    second.push(KEEPALIVE_FRAME);
    await flush();
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "9" });

    const idless = harness();
    const c1 = await idless.open({ head: "5" });
    idless.runs[0]!.resolve();
    await flush();
    vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
    c1.end();
    await flush();
    idless.last().push(helloFrame({ resumed: true, head: "9" }));
    idless.last().push(frame("connected", "{}", null));
    await flush();
    expect(idless.stream.cursor()).toEqual({ epoch: A, offset: "9" });
    expect(idless.frames.map((f) => f.type)).toEqual(["connected"]);
  });

  it("frames during a revalidate are held and drained in order after it settles", async () => {
    const h = harness();
    const conn = await h.open({ head: "0" });
    conn.push(frame("message", "one", id(1)));
    conn.push(frame("message", "two", id(2)));
    conn.push(frame("connected", "{}", null));
    conn.push(frame("message", "three", id(3)));
    await flush();
    expect(h.frames).toEqual([]);
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "0" });
    h.runs[0]!.resolve();
    await flush();
    expect(h.frames.map((f) => f.data)).toEqual(["one", "two", "{}", "three"]);
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "3" });
    expect(h.ofKind("drain")).toEqual([{ kind: "drain", length: 4, dropped: 0 }]);
  });

  it("an abort during a hold discards the held queue and a reconnect delivers nothing from the old generation", async () => {
    const h = harness();
    const conn = await h.open({ head: "0" });
    conn.push(frame("message", "one", id(1)));
    conn.push(frame("message", "two", id(2)));
    await flush();
    h.visibility.emit("pagehide");
    await flush();
    expect(h.stream.state().kind).toBe("hidden_closed");
    expect(conn.request.aborted).toBe(true);
    expect(h.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "abort", length: 2 },
    ]);
    h.visibility.emit("visible");
    await flush();
    const next = h.last();
    next.push(helloFrame({ resumed: true, head: "2" }));
    await flush();
    h.runs[0]!.resolve();
    await flush();
    expect(h.frames).toEqual([]);
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "0" });
  });

  it("the 2001st held frame ends the connection with hold_overflow", async () => {
    const h = harness();
    const conn = await h.open({ head: "0" });
    let chunk = "";
    for (let i = 1; i <= 2000; i++) {
      chunk += frame("message", "x", id(i));
    }
    conn.push(chunk);
    await flush();
    expect(h.stream.state().kind).toBe("open");
    conn.push(frame("message", "x", id(2001)));
    await flush();
    expect(h.stream.state().kind).toBe("backoff");
    expect(h.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "hold_overflow", length: 2001 },
    ]);
    expect(h.ofKind("state").map((e) => e.to)).toEqual(["connecting", "open", "backoff"]);
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "0" });
  });

  it("held bytes reaching 64 MiB end the connection with hold_overflow before the count", async () => {
    const h = harness();
    const conn = await h.open({ head: "0" });
    const encoded = new TextEncoder().encode(sizedFrame(1, MAX_FRAME_BYTES));
    expect(encoded.length).toBe(MAX_FRAME_BYTES);
    for (let i = 1; i <= 64; i++) {
      conn.push(sizedFrame(i, MAX_FRAME_BYTES));
    }
    await flush(8);
    expect(h.stream.state().kind).toBe("open");
    conn.push(sizedFrame(65, MAX_FRAME_BYTES));
    await flush(8);
    expect(h.stream.state().kind).toBe("backoff");
    expect(h.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "hold_overflow", length: 65 },
    ]);
  });

  it("a revalidate that never settles ends with hold_timeout at revalidateTimeoutMs and the queued slot survives", async () => {
    const h = harness();
    const conn = await h.open({ head: "0" });
    conn.push(frame("message", "held", id(1)));
    await flush();
    streamInternals(h.stream).queueFull();
    vi.advanceTimersByTime(DEFAULT_TIMING.revalidateTimeoutMs - 1);
    await flush();
    expect(h.stream.state().kind).toBe("open");
    vi.advanceTimersByTime(1);
    await flush();
    expect(h.runs[0]!.ctx.signal.aborted).toBe(true);
    expect(h.ofKind("revalidate_timeout")).toHaveLength(1);
    expect(h.ofKind("held_discarded")).toEqual([
      { kind: "held_discarded", cause: "hold_timeout", length: 1 },
    ]);
    expect(h.stream.state().kind).toBe("connecting");
    expect(h.runs).toHaveLength(1);
    h.last().push(helloFrame({ resumed: true, head: "1" }));
    await flush();
    expect(h.runs).toHaveLength(2);
    expect(h.runs[1]!.ctx.full).toBe(true);
    h.runs[0]!.resolve();
    await flush();
    expect(h.runs).toHaveLength(2);
  });

  it("two wakes inside one slow revalidate produce two runs, never concurrent", async () => {
    const h = harness();
    await h.open({ resumed: true });
    h.visibility.emit("pageshow");
    await flush();
    expect(h.runs).toHaveLength(1);
    vi.advanceTimersByTime(DEFAULT_TIMING.wakeThrottleMs);
    h.visibility.emit("pageshow");
    await flush();
    vi.advanceTimersByTime(DEFAULT_TIMING.wakeThrottleMs);
    h.visibility.emit("pageshow");
    await flush();
    expect(h.runs).toHaveLength(1);
    h.runs[0]!.resolve();
    await flush();
    expect(h.runs).toHaveLength(2);
    expect(h.runs[1]!.ctx.cause).toBe("pageshow");
    h.runs[1]!.resolve();
    await flush();
    expect(h.runs).toHaveLength(2);
    expect(h.ofKind("revalidate")).toHaveLength(2);
  });

  it("a full run queued behind a later cause still runs as full", async () => {
    const versions = createVersionMap();
    versions.observe({ kind: "chat", ref: "c1" }, "3", A);
    const h = harness({ visible: false, versions });
    h.stream.start();
    await flush();
    expect(h.stream.state().kind).toBe("hidden_closed");
    h.visibility.emit("visible");
    await flush();
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["visible"]);
    h.last().push(helloFrame({ epoch: B, head: "7" }));
    await flush();
    vi.advanceTimersByTime(DEFAULT_TIMING.wakeThrottleMs);
    h.visibility.emit("pageshow");
    await flush();
    expect(h.runs).toHaveLength(1);
    h.runs[0]!.resolve();
    await flush();
    expect(h.runs).toHaveLength(2);
    expect(h.runs[1]!.ctx).toMatchObject({ cause: "pageshow", full: true, epoch: B });
    expect(versions.snapshot().held).toEqual([]);
  });

  it("a bind of the current epoch during a run discharges the queued full", async () => {
    const versions = createVersionMap();
    versions.observe({ kind: "chat", ref: "c1" }, "3", A);
    const h = harness({ visible: false, versions });
    h.stream.start();
    await flush();
    h.visibility.emit("visible");
    await flush();
    h.last().push(helloFrame({ epoch: B, head: "7" }));
    await flush();
    expect(h.ofKind("revalidate")).toEqual([{ kind: "revalidate", cause: "visible", full: false }]);
    versions.bind(B);
    h.runs[0]!.resolve();
    await flush();
    expect(h.runs).toHaveLength(2);
    expect(h.runs[1]!.ctx).toMatchObject({ cause: "hello", full: false });

    const other = createVersionMap();
    other.observe({ kind: "chat", ref: "c1" }, "3", A);
    const kept = harness({ visible: false, versions: other });
    kept.stream.start();
    await flush();
    kept.visibility.emit("visible");
    await flush();
    kept.last().push(helloFrame({ epoch: B, head: "7" }));
    await flush();
    other.bind("cccccccccccccccc");
    kept.runs[0]!.resolve();
    await flush();
    expect(kept.runs[1]!.ctx.full).toBe(true);
  });

  it("a rejected revalidate while open ends the connection with revalidate_failed and the next connected emits revalidate('hello')", async () => {
    const h = harness();
    const conn = await h.open({ head: "3" });
    h.runs[0]!.reject(new Error("digest 500"));
    await flush();
    expect(h.ofKind("revalidate_failed")).toEqual([
      { kind: "revalidate_failed", cause: "digest 500", latch: true, ended: true },
    ]);
    expect(conn.request.aborted).toBe(true);
    expect(h.stream.state()).toMatchObject({ kind: "backoff", attempt: 1 });
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "3" });
    vi.advanceTimersByTime(1500);
    await flush();
    h.last().push(helloFrame({ resumed: true, head: "3" }));
    await flush();
    expect(h.runs).toHaveLength(2);
    expect(h.runs[1]!.ctx.cause).toBe("hello");
    h.runs[1]!.resolve();
    await flush();
    vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
    h.last().end();
    await flush();
    h.last().push(helloFrame({ resumed: true, head: "3" }));
    await flush();
    expect(h.runs).toHaveLength(2);
  });

  it("a rejected revalidate while offline sets the latch and ends nothing", async () => {
    const h = harness();
    await h.open({ resumed: true });
    h.online.emit(false);
    await flush();
    expect(h.stream.state().kind).toBe("offline");
    h.visibility.emit("visible");
    await flush();
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["visible"]);
    h.runs[0]!.reject(new Error("offline"));
    await flush();
    expect(h.ofKind("revalidate_failed")).toEqual([
      { kind: "revalidate_failed", cause: "offline", latch: true, ended: false },
    ]);
    expect(h.stream.state().kind).toBe("offline");
    h.online.emit(true);
    await flush();
    h.last().push(helloFrame({ resumed: true }));
    await flush();
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["visible", "online"]);
    h.runs[1]!.resolve();
    await flush();
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["visible", "online", "hello"]);
    expect(h.stream.state().kind).toBe("open");
  });

  it("onFrame throwing advances the cursor, reports frame_rejected and schedules revalidate('hello')", async () => {
    const h = harness({
      onFrame(f) {
        if (f.data === "bad") {
          throw new Error("decoder rejected");
        }
      },
    });
    const conn = await h.open({ resumed: true, head: "0" });
    conn.push(frame("message", "bad", id(1)));
    await flush();
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "1" });
    expect(h.ofKind("frame_rejected")).toMatchObject([{ kind: "frame_rejected", type: "message" }]);
    expect(h.runs.map((r) => r.ctx.cause)).toEqual(["hello"]);
    expect(h.stream.state().kind).toBe("open");
  });

  it("alive POSTs once per everyBeats keepalives, never for application frames, never after the connection ends, failures reported and not retried", async () => {
    const h = harness({ alive: { url: "/alive", everyBeats: 2 } });
    let status = 204;
    h.sf.onPost(() => new Response(null, { status }));
    const conn = await h.open({ resumed: true });
    const posts = (): number => h.sf.requests.filter((r) => r.method === "POST").length;
    conn.push(KEEPALIVE_FRAME);
    conn.push(frame("message", "x", id(1)));
    await flush();
    expect(posts()).toBe(0);
    conn.push(KEEPALIVE_FRAME);
    await flush();
    expect(posts()).toBe(1);
    expect(h.sf.requests[1]).toMatchObject({ method: "POST", url: "/alive" });
    expect(h.ofKind("alive_ack")).toEqual([{ kind: "alive_ack", ok: true, status: 204 }]);
    status = 500;
    conn.push(KEEPALIVE_FRAME);
    conn.push(KEEPALIVE_FRAME);
    await flush();
    expect(posts()).toBe(2);
    expect(h.ofKind("alive_ack")[1]).toEqual({ kind: "alive_ack", ok: false, status: 500 });
    vi.advanceTimersByTime(10_000);
    await flush();
    expect(posts()).toBe(2);
    h.sf.onPost(() => Promise.reject(new TypeError("network")));
    conn.push(KEEPALIVE_FRAME);
    conn.push(KEEPALIVE_FRAME);
    await flush();
    expect(h.ofKind("alive_ack")[2]).toEqual({ kind: "alive_ack", ok: false, status: null });
    conn.end();
    await flush();
    expect(h.stream.state().kind).toBe("backoff");
    expect(posts()).toBe(3);
  });

  it("the alive POST carries the stream headers", async () => {
    const h = harness({ alive: { url: "/alive" }, headers: { "SSE-Client": "tag_1" } });
    const conn = await h.open({ resumed: true });
    conn.push(KEEPALIVE_FRAME);
    await flush();
    const post = h.sf.requests.find((r) => r.method === "POST");
    expect(post?.headers).toEqual({ "SSE-Client": "tag_1" });
    expect(h.sf.requests[0]!.headers["SSE-Client"]).toBe("tag_1");
  });

  it("resetCursor then reconnect presents no Last-Event-ID and earns fresh", async () => {
    const h = harness();
    const conn = await h.open({ head: "5" });
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "5" });
    h.stream.resetCursor();
    expect(h.stream.cursor()).toBeNull();
    h.stream.reconnect();
    await flush();
    expect(conn.request.aborted).toBe(true);
    const next = h.last();
    expect(next.request.headers).not.toHaveProperty("Last-Event-ID");
    next.push(helloFrame({ head: "8", verdict: "fresh" }));
    await flush();
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "8" });
    expect(h.ofKind("hello").map((e) => e.verdict)).toEqual(["fresh", "fresh"]);
  });

  it("reconnect does not abort the revalidate signal", async () => {
    const h = harness();
    await h.open({ head: "1" });
    const run = h.runs[0]!;
    h.stream.reconnect();
    await flush();
    expect(run.ctx.signal.aborted).toBe(false);
    expect(h.stream.state().kind).toBe("connecting");
    expect(h.sf.connections).toHaveLength(2);
  });

  it("stop aborts the revalidate signal", async () => {
    const h = harness();
    const conn = await h.open({ head: "1" });
    const run = h.runs[0]!;
    h.stream.stop();
    expect(run.ctx.signal.aborted).toBe(true);
    expect(conn.request.aborted).toBe(true);
    expect(h.stream.state().kind).toBe("stopped");
  });

  it("wire_unsupported leaves the cursor where it was", async () => {
    const h = harness();
    const conn = await h.open({ head: "5" });
    h.runs[0]!.resolve();
    vi.advanceTimersByTime(DEFAULT_TIMING.stableMs + 1);
    conn.end();
    await flush();
    h.last().push(helloFrame({ wire: 2, head: "9" }));
    await flush();
    expect(h.ofKind("wire_unsupported")).toEqual([{ kind: "wire_unsupported", wire: 2 }]);
    expect(h.stream.state().kind).toBe("backoff");
    expect(h.stream.cursor()).toEqual({ epoch: A, offset: "5" });
    vi.advanceTimersByTime(0);
    await flush();
    expect(h.last().request.headers["Last-Event-ID"]).toBe(id(5));
  });

  it("resolveTiming refusal is thrown by createStream", () => {
    expect(() =>
      createStream({
        url: "/events",
        versions: createVersionMap(),
        onFrame: () => undefined,
        revalidate: () => Promise.resolve(),
        timing: { maxBufferBytes: MAX_FRAME_BYTES - 1 },
      }),
    ).toThrow(RangeError);
  });

  it("every lifecycle record of section 9.3 is emitted on its path", async () => {
    const versions = createVersionMap();
    const h = harness({
      versions,
      alive: { url: "/alive" },
      onFrame(f) {
        if (f.data === "bad") {
          throw new Error("no");
        }
      },
    });
    const conn = await h.open({ head: "1" });
    conn.push(frame("message", "held", id(2)));
    conn.push(frame("sse:future", "{}", null));
    conn.push(frame("message", "x", "not-a-cursor"));
    conn.push(KEEPALIVE_FRAME);
    await flush();
    h.runs[0]!.resolve();
    await flush();
    conn.push(frame("message", "bad", id(3)));
    await flush();
    h.runs[1]!.reject(new Error("digest"));
    await flush();
    vi.advanceTimersByTime(1500);
    await flush();
    const second = h.last();
    second.push(helloFrame({ wire: 2 }));
    await flush();
    vi.advanceTimersByTime(0);
    await flush();
    const third = h.last();
    third.push(helloFrame({ resumed: true, head: "3" }));
    third.push(frame("message", "held", id(4)));
    await flush();
    vi.advanceTimersByTime(DEFAULT_TIMING.revalidateTimeoutMs);
    await flush();
    vi.advanceTimersByTime(1500);
    await flush();
    const fourth = h.last();
    fourth.push(helloFrame({ resumed: true, head: "3" }));
    await flush();
    h.runs[3]!.resolve();
    await flush();
    fourth.push(`event: sse:reset\ndata: {"reason":"slow"}\n\n`);
    await flush();
    vi.advanceTimersByTime(1500);
    await flush();
    const fifth = h.last();
    fifth.push(helloFrame({ resumed: true, head: "3" }));
    await flush();
    vi.advanceTimersByTime(45_000);
    await flush();
    vi.advanceTimersByTime(1500);
    await flush();
    const sixth = h.last();
    sixth.push(helloFrame({ resumed: true, head: "3" }));
    await flush();
    h.visibility.emit("hidden");
    vi.advanceTimersByTime(60_000);
    await flush();
    h.visibility.emit("visible");
    await flush();
    h.last().push(helloFrame({ resumed: true, head: "3" }));
    await flush();
    versions.observe({ kind: "chat", ref: "c" }, "1", B);
    const kinds = new Set(h.events.map((e) => e.kind));
    expect([...kinds].sort()).toEqual(
      [
        "alive_ack",
        "bad_cursor",
        "connect_failed",
        "drain",
        "frame_rejected",
        "held_discarded",
        "hello",
        "hidden_closed",
        "reopened",
        "reset",
        "revalidate",
        "revalidate_failed",
        "revalidate_timeout",
        "stale_stamp",
        "state",
        "unknown_frame",
        "watchdog",
        "wire_unsupported",
      ].sort(),
    );
  });
});
