import { createParser } from "./parser.js";
import type { ConnectFailure, StreamEnd } from "./reducer.js";
import type { TimingConfig } from "./timing.js";
import {
  type Cursor,
  HELLO_EVENT,
  type Hello,
  RESERVED_PREFIX,
  RESET_EVENT,
  cursorToString,
  parseCursor,
  parseResetReason,
  validateHello,
} from "./wire.js";

/** An application frame as the transport hands it up: the id already parsed as a cursor. */
export interface TransportFrame {
  readonly type: string;
  readonly data: string;
  readonly id: Cursor | null;
  /** Encoded bytes of the frame as received; feeds the hold's byte bound. */
  readonly bytes: number;
}

/**
 * What one connection attempt reports. Every callback carries the attempt's generation.
 * Exactly one of onConnected-then-onEnd, onConnectFailed, or nothing (after abort) happens.
 */
export interface TransportCallbacks {
  readonly onByte: (generation: number) => void;
  readonly onConnected: (generation: number, hello: Hello) => void;
  readonly onConnectFailed: (generation: number, reason: ConnectFailure) => void;
  readonly onFrame: (generation: number, frame: TransportFrame) => void;
  readonly onKeepalive: (generation: number) => void;
  readonly onUnknownFrame: (generation: number, type: string) => void;
  readonly onBadCursor: (generation: number, id: string) => void;
  readonly onRetry: (generation: number, ms: number) => void;
  readonly onEnd: (generation: number, reason: StreamEnd) => void;
}

export interface ConnectOptions {
  readonly url: string;
  readonly fetch: typeof fetch;
  readonly headers: Readonly<Record<string, string>>;
  readonly cursor: Cursor | null;
  readonly minWire: number;
  readonly maxWire: number;
  readonly timing: TimingConfig;
  readonly generation: number;
  readonly callbacks: TransportCallbacks;
}

export interface Connection {
  /** Ends the attempt; nothing is dispatched afterwards. */
  abort(reason: string): void;
}

const EVENT_STREAM = "text/event-stream";

function mediaType(contentType: string | null): string {
  if (contentType === null) {
    return "";
  }
  const semicolon = contentType.indexOf(";");
  return (semicolon === -1 ? contentType : contentType.slice(0, semicolon)).trim().toLowerCase();
}

/** Opens one stream attempt: fetch, the two connect-phase deadlines, and the frame translation. */
export function connect(opts: ConnectOptions): Connection {
  const { generation: g, callbacks: cb, timing } = opts;
  const controller = new AbortController();
  let abortReason: string | null = null;
  let finished = false;
  let helloSeen = false;
  let keepaliveEvent = "";
  let deadline: ReturnType<typeof setTimeout> | null = null;

  function clearDeadline(): void {
    if (deadline !== null) {
      clearTimeout(deadline);
      deadline = null;
    }
  }

  function abort(reason: string): void {
    abortReason ??= reason;
    controller.abort(new DOMException(reason, "AbortError"));
  }

  const reason = (): string | null => abortReason;
  const done = (): boolean => finished;

  function fail(reason: ConnectFailure): void {
    if (finished) {
      return;
    }
    finished = true;
    clearDeadline();
    abort("refused");
    cb.onConnectFailed(g, reason);
  }

  function end(reason: StreamEnd): void {
    if (finished) {
      return;
    }
    finished = true;
    clearDeadline();
    abort("ended");
    cb.onEnd(g, reason);
  }

  const parser = createParser(
    {
      onEvent(ev) {
        if (finished) {
          return;
        }
        if (!helloSeen) {
          if (ev.type !== HELLO_EVENT) {
            fail({ kind: "bad_hello" });
            return;
          }
          let data: unknown;
          try {
            data = JSON.parse(ev.data);
          } catch {
            fail({ kind: "bad_hello" });
            return;
          }
          const result = validateHello(data, opts.minWire, opts.maxWire);
          if (!result.ok) {
            fail(result.reason);
            return;
          }
          helloSeen = true;
          keepaliveEvent = result.hello.keepalive_event;
          clearDeadline();
          cb.onConnected(g, result.hello);
          return;
        }
        if (ev.type === HELLO_EVENT) {
          end("error");
          return;
        }
        if (ev.type === keepaliveEvent) {
          cb.onKeepalive(g);
          return;
        }
        if (ev.type === RESET_EVENT) {
          end(parseResetReason(ev.data) ?? "error");
          return;
        }
        if (ev.type.startsWith(RESERVED_PREFIX)) {
          cb.onUnknownFrame(g, ev.type);
          return;
        }
        let id: Cursor | null = null;
        if (ev.id !== null) {
          id = parseCursor(ev.id);
          if (id === null) {
            cb.onBadCursor(g, ev.id);
            return;
          }
        }
        cb.onFrame(g, { type: ev.type, data: ev.data, id, bytes: ev.bytes });
      },
      onRetry(ms) {
        if (!finished) {
          cb.onRetry(g, ms);
        }
      },
      onOverflow() {
        end("frame_too_large");
      },
    },
    { maxBufferBytes: timing.maxBufferBytes },
  );

  function requestHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      Accept: EVENT_STREAM,
      "SSE-Wire": String(opts.maxWire),
    };
    if (opts.cursor !== null) {
      headers["Last-Event-ID"] = cursorToString(opts.cursor);
    }
    return { ...headers, ...opts.headers };
  }

  async function run(): Promise<void> {
    deadline = setTimeout(() => {
      abort("timeout_headers");
    }, timing.connectTimeoutMs);
    let response: Response;
    try {
      response = await opts.fetch(opts.url, {
        headers: requestHeaders(),
        cache: "no-store",
        credentials: "same-origin",
        signal: controller.signal,
      });
    } catch {
      clearDeadline();
      if (reason() === "timeout_headers") {
        fail({ kind: "timeout_headers" });
      } else if (reason() === null) {
        fail({ kind: "network" });
      }
      return;
    }
    clearDeadline();
    if (reason() !== null) {
      return;
    }
    if (response.status !== 200) {
      fail({ kind: "status", status: response.status });
      return;
    }
    const contentType = response.headers.get("content-type");
    if (mediaType(contentType) !== EVENT_STREAM) {
      fail({ kind: "content_type", value: contentType ?? "" });
      return;
    }
    if (response.body === null) {
      end("eof");
      return;
    }
    deadline = setTimeout(() => {
      abort("timeout_hello");
    }, timing.helloTimeoutMs);
    const reader = response.body.getReader();
    for (;;) {
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await reader.read();
      } catch {
        if (done()) {
          return;
        }
        if (reason() === "timeout_hello") {
          fail({ kind: "timeout_hello" });
        } else if (reason() === null) {
          end("error");
        }
        return;
      }
      if (done() || reason() !== null) {
        return;
      }
      if (result.done) {
        end("eof");
        return;
      }
      if (result.value.length === 0) {
        continue;
      }
      cb.onByte(g);
      parser.feed(result.value);
      if (done()) {
        return;
      }
    }
  }

  void run();

  return {
    abort(reason) {
      if (finished) {
        return;
      }
      finished = true;
      clearDeadline();
      abort(reason);
    },
  };
}
