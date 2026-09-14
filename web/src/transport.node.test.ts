import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectFailure, StreamEnd } from "./reducer.js";
import {
  FAKE_CLOCK,
  KEEPALIVE_FRAME,
  type ScriptedFetch,
  flush,
  frame,
  helloFrame,
  scriptedFetch,
} from "./test-helpers/scripted-fetch.js";
import { DEFAULT_TIMING } from "./timing.js";
import {
  type Connection,
  type TransportCallbacks,
  type TransportFrame,
  connect,
} from "./transport.js";
import type { Cursor, Hello } from "./wire.js";

const EPOCH = "aaaaaaaaaaaaaaaa";

interface Log {
  bytes: number;
  hello: Hello | null;
  failed: ConnectFailure | null;
  frames: TransportFrame[];
  keepalives: number;
  unknown: string[];
  badCursors: string[];
  retries: number[];
  ended: StreamEnd | null;
}

function harness(
  sf: ScriptedFetch,
  overrides: { cursor?: Cursor | null; minWire?: number; maxWire?: number } = {},
): { log: Log; connection: Connection } {
  const log: Log = {
    bytes: 0,
    hello: null,
    failed: null,
    frames: [],
    keepalives: 0,
    unknown: [],
    badCursors: [],
    retries: [],
    ended: null,
  };
  const callbacks: TransportCallbacks = {
    onByte: () => {
      log.bytes++;
    },
    onConnected: (_g, hello) => {
      log.hello = hello;
    },
    onConnectFailed: (_g, reason) => {
      log.failed = reason;
    },
    onFrame: (_g, f) => {
      log.frames.push(f);
    },
    onKeepalive: () => {
      log.keepalives++;
    },
    onUnknownFrame: (_g, type) => {
      log.unknown.push(type);
    },
    onBadCursor: (_g, id) => {
      log.badCursors.push(id);
    },
    onRetry: (_g, ms) => {
      log.retries.push(ms);
    },
    onEnd: (_g, reason) => {
      log.ended = reason;
    },
  };
  const connection = connect({
    url: "/events",
    fetch: sf.fetch,
    headers: { "SSE-Client": "tag_1" },
    cursor: overrides.cursor ?? null,
    minWire: overrides.minWire ?? 1,
    maxWire: overrides.maxWire ?? 1,
    timing: DEFAULT_TIMING,
    generation: 7,
    callbacks,
  });
  return { log, connection };
}

describe("transport", () => {
  beforeEach(() => {
    vi.useFakeTimers(FAKE_CLOCK);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("headers carry Accept, SSE-Wire and Last-Event-ID when a cursor is held", async () => {
    const sf = scriptedFetch();
    harness(sf, { cursor: { epoch: EPOCH, offset: "42" } });
    await flush();
    const first = sf.requests[0]!;
    expect(first.headers).toEqual({
      Accept: "text/event-stream",
      "SSE-Wire": "1",
      "Last-Event-ID": `${EPOCH}:42`,
      "SSE-Client": "tag_1",
    });
    const bare = scriptedFetch();
    harness(bare);
    await flush();
    expect(bare.requests[0]!.headers).not.toHaveProperty("Last-Event-ID");
  });

  it("connectTimeoutMs is armed before fetch and yields timeout_headers", async () => {
    const sf = scriptedFetch();
    sf.expect({ holdHeaders: true });
    const { log } = harness(sf);
    await flush();
    expect(log.failed).toBeNull();
    vi.advanceTimersByTime(DEFAULT_TIMING.connectTimeoutMs - 1);
    await flush();
    expect(log.failed).toBeNull();
    vi.advanceTimersByTime(1);
    await flush();
    expect(log.failed).toEqual({ kind: "timeout_headers" });
    expect(sf.connections[0]!.request.aborted).toBe(true);
    expect(log.ended).toBeNull();
  });

  it("helloTimeoutMs runs from headers to a complete validated hello and yields timeout_hello on a stalled retry line", async () => {
    const sf = scriptedFetch();
    const { log } = harness(sf);
    await flush();
    const conn = sf.connections[0]!;
    conn.push("retry: 1500\n\n");
    await flush();
    expect(log.retries).toEqual([1500]);
    vi.advanceTimersByTime(DEFAULT_TIMING.helloTimeoutMs - 1);
    await flush();
    expect(log.failed).toBeNull();
    vi.advanceTimersByTime(1);
    await flush();
    expect(log.failed).toEqual({ kind: "timeout_hello" });
    expect(log.hello).toBeNull();
    expect(log.ended).toBeNull();
  });

  it("a non-200 status yields connect_failed{status}", async () => {
    const sf = scriptedFetch();
    sf.expect({ status: 503 });
    const { log } = harness(sf);
    await flush();
    expect(log.failed).toEqual({ kind: "status", status: 503 });
  });

  it("a wrong content-type yields connect_failed{content_type}", async () => {
    const sf = scriptedFetch();
    sf.expect({ contentType: "text/html; charset=utf-8" });
    const { log } = harness(sf);
    await flush();
    expect(log.failed).toEqual({ kind: "content_type", value: "text/html; charset=utf-8" });
    const okParams = scriptedFetch();
    okParams.expect({ contentType: "text/event-stream; charset=utf-8" });
    const { log: okLog } = harness(okParams);
    await flush();
    expect(okLog.failed).toBeNull();
  });

  it("a first frame that is not sse:hello yields bad_hello", async () => {
    const sf = scriptedFetch();
    const { log } = harness(sf);
    await flush();
    sf.connections[0]!.push(frame("message", "{}", null));
    await flush();
    expect(log.failed).toEqual({ kind: "bad_hello" });
    const malformed = scriptedFetch();
    const { log: mLog } = harness(malformed);
    await flush();
    malformed.connections[0]!.push(helloFrame({ resumed: "true" }));
    await flush();
    expect(mLog.failed).toEqual({ kind: "bad_hello" });
  });

  it("wire 0 and wire 2 yield wire_unsupported", async () => {
    for (const wire of [0, 2]) {
      const sf = scriptedFetch();
      const { log } = harness(sf);
      await flush();
      sf.connections[0]!.push(helloFrame({ wire }));
      await flush();
      expect(log.failed).toEqual({ kind: "wire_unsupported", wire });
      expect(sf.connections[0]!.request.aborted).toBe(true);
    }
  });

  it("a second hello ends the connection with error", async () => {
    const sf = scriptedFetch();
    const { log } = harness(sf);
    await flush();
    const conn = sf.connections[0]!;
    conn.push(helloFrame());
    await flush();
    expect(log.hello?.epoch).toBe(EPOCH);
    conn.push(helloFrame());
    await flush();
    expect(log.ended).toBe("error");
    expect(conn.request.aborted).toBe(true);
  });

  it("sse:reset ends with reset:slow or reset:shutdown", async () => {
    for (const [reason, end] of [
      ["slow", "reset:slow"],
      ["shutdown", "reset:shutdown"],
    ] as const) {
      const sf = scriptedFetch();
      const { log } = harness(sf);
      await flush();
      const conn = sf.connections[0]!;
      conn.push(helloFrame());
      conn.push(`event: sse:reset\ndata: {"reason":"${reason}"}\n\n`);
      await flush();
      expect(log.ended).toBe(end);
    }
  });

  it("an unknown sse: frame is reported unknown_frame and ignored", async () => {
    const sf = scriptedFetch();
    const { log } = harness(sf);
    await flush();
    const conn = sf.connections[0]!;
    conn.push(helloFrame());
    conn.push(frame("sse:future", "{}", `${EPOCH}:1`));
    conn.push(frame("message", "{}", `${EPOCH}:2`));
    await flush();
    expect(log.unknown).toEqual(["sse:future"]);
    expect(log.frames.map((f) => f.id?.offset)).toEqual(["2"]);
    expect(log.ended).toBeNull();
  });

  it("a malformed id is reported bad_cursor and dropped", async () => {
    const sf = scriptedFetch();
    const { log } = harness(sf);
    await flush();
    const conn = sf.connections[0]!;
    conn.push(helloFrame());
    conn.push(frame("message", "a", "42"));
    conn.push(frame("message", "b", `${EPOCH}:007`));
    conn.push(frame("message", "c", `${EPOCH}:7`));
    await flush();
    expect(log.badCursors).toEqual(["42", `${EPOCH}:007`]);
    expect(log.frames.map((f) => f.data)).toEqual(["c"]);
  });

  it("an empty id is bad_cursor", async () => {
    const sf = scriptedFetch();
    const { log } = harness(sf);
    await flush();
    const conn = sf.connections[0]!;
    conn.push(helloFrame());
    conn.push("id\ndata: x\n\n");
    conn.push(frame("message", "idless", null));
    await flush();
    expect(log.badCursors).toEqual([""]);
    expect(log.frames).toEqual([{ type: "message", data: "idless", id: null, bytes: 14 }]);
  });

  it("byte fires before the parser sees the chunk", async () => {
    const sf = scriptedFetch();
    const order: string[] = [];
    const connection = connect({
      url: "/events",
      fetch: sf.fetch,
      headers: {},
      cursor: null,
      minWire: 1,
      maxWire: 1,
      timing: DEFAULT_TIMING,
      generation: 1,
      callbacks: {
        onByte: () => order.push("byte"),
        onConnected: () => order.push("hello"),
        onConnectFailed: () => order.push("failed"),
        onFrame: () => order.push("frame"),
        onKeepalive: () => order.push("keepalive"),
        onUnknownFrame: () => order.push("unknown"),
        onBadCursor: () => order.push("bad_cursor"),
        onRetry: () => order.push("retry"),
        onEnd: () => order.push("end"),
      },
    });
    await flush();
    const conn = sf.connections[0]!;
    conn.push(helloFrame());
    await flush();
    conn.push(frame("message", "x", `${EPOCH}:1`));
    await flush();
    conn.push(KEEPALIVE_FRAME);
    await flush();
    expect(order).toEqual(["byte", "hello", "byte", "frame", "byte", "keepalive"]);
    connection.abort("test");
  });

  it("EOF and read errors map to eof and error", async () => {
    const eof = scriptedFetch();
    const { log: eofLog } = harness(eof);
    await flush();
    eof.connections[0]!.push(helloFrame());
    eof.connections[0]!.end();
    await flush();
    expect(eofLog.ended).toBe("eof");

    const err = scriptedFetch();
    const { log: errLog } = harness(err);
    await flush();
    err.connections[0]!.push(helloFrame());
    err.connections[0]!.fail(new TypeError("network error"));
    await flush();
    expect(errLog.ended).toBe("error");

    const beforeHello = scriptedFetch();
    const { log: bhLog } = harness(beforeHello);
    await flush();
    beforeHello.connections[0]!.end();
    await flush();
    expect(bhLog.ended).toBe("eof");
    expect(bhLog.failed).toBeNull();

    const network = scriptedFetch();
    network.expect({ holdHeaders: true });
    const { log: netLog } = harness(network);
    await flush();
    network.connections[0]!.request.signal?.dispatchEvent(new Event("abort"));
    await flush();
    expect(netLog.failed).toEqual({ kind: "network" });
  });

  it("an abort dispatches nothing", async () => {
    const sf = scriptedFetch();
    const { log, connection } = harness(sf);
    await flush();
    const conn = sf.connections[0]!;
    conn.push(helloFrame());
    await flush();
    connection.abort("hidden");
    await flush();
    conn.push(frame("message", "late", `${EPOCH}:1`));
    conn.end();
    await flush();
    vi.advanceTimersByTime(DEFAULT_TIMING.connectTimeoutMs + DEFAULT_TIMING.helloTimeoutMs);
    await flush();
    expect(conn.request.aborted).toBe(true);
    expect(log.frames).toEqual([]);
    expect(log.ended).toBeNull();
    expect(log.failed).toBeNull();

    const early = scriptedFetch();
    early.expect({ holdHeaders: true });
    const { log: earlyLog, connection: earlyConnection } = harness(early);
    await flush();
    earlyConnection.abort("stop");
    await flush();
    expect(earlyLog.failed).toBeNull();
    expect(earlyLog.ended).toBeNull();
  });
});
