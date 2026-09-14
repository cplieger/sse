/** How the next stream request is answered; every field has a healthy default. */
export interface ScriptedAnswer {
  readonly status?: number;
  /** null omits the header entirely. */
  readonly contentType?: string | null;
  /** Hold the headers until the test calls respond(). */
  readonly holdHeaders?: boolean;
  /** Answer with no body at all. */
  readonly noBody?: boolean;
}

export interface ScriptedRequest {
  readonly url: string;
  readonly method: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly signal: AbortSignal | null;
  aborted: boolean;
  abortReason: unknown;
}

/** One scripted stream connection; the test feeds bytes and decides how it ends. */
export interface ScriptedConnection {
  readonly request: ScriptedRequest;
  readonly answer: ScriptedAnswer;
  /** Releases held headers; a no-op when they were not held. */
  respond(): void;
  push(chunk: string | Uint8Array): void;
  end(): void;
  fail(error?: unknown): void;
  readonly closed: boolean;
}

export interface ScriptedFetch {
  readonly fetch: typeof fetch;
  /** Every request in call order, POSTs included. */
  readonly requests: ScriptedRequest[];
  /** Every GET as a connection, in call order. */
  readonly connections: ScriptedConnection[];
  /** Queues the answer for the next stream request. */
  expect(answer: ScriptedAnswer): void;
  /** Resolves with the next connection opened after the call. */
  nextConnection(): Promise<ScriptedConnection>;
  /** Answers POSTs; the default is 204. */
  onPost(handler: (request: ScriptedRequest) => Response | Promise<Response>): void;
}

const encoder = new TextEncoder();

function headerRecord(init: RequestInit | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  const h = init?.headers;
  if (h === undefined) {
    return out;
  }
  if (h instanceof Headers) {
    h.forEach((value, key) => {
      out[key] = value;
    });
    return out;
  }
  if (Array.isArray(h)) {
    for (const [key, value] of h) {
      out[key] = value;
    }
    return out;
  }
  return { ...h };
}

function abortError(reason: unknown): DOMException {
  if (reason instanceof DOMException) {
    return reason;
  }
  return new DOMException(typeof reason === "string" ? reason : "aborted", "AbortError");
}

/** A fetch whose GET bodies are streams the test writes into. */
export function scriptedFetch(): ScriptedFetch {
  const requests: ScriptedRequest[] = [];
  const connections: ScriptedConnection[] = [];
  const answers: ScriptedAnswer[] = [];
  const waiters: ((c: ScriptedConnection) => void)[] = [];
  let postHandler: (request: ScriptedRequest) => Response | Promise<Response> = () =>
    new Response(null, { status: 204 });

  function open(request: ScriptedRequest, answer: ScriptedAnswer): Promise<Response> {
    let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
    let closed = false;
    const body = answer.noBody
      ? null
      : new ReadableStream<Uint8Array>({
          start(c) {
            controller = c;
          },
        });
    const headers = new Headers();
    const contentType = answer.contentType === undefined ? "text/event-stream" : answer.contentType;
    if (contentType !== null) {
      headers.set("content-type", contentType);
    }
    const response = new Response(body, { status: answer.status ?? 200, headers });

    let release: (() => void) | null = null;
    let reject: ((e: unknown) => void) | null = null;
    const promise = new Promise<Response>((resolve, rej) => {
      reject = rej;
      release = () => {
        resolve(response);
      };
    });
    let released = false;

    function respond(): void {
      if (released) {
        return;
      }
      released = true;
      release?.();
    }

    function close(): void {
      if (closed) {
        return;
      }
      closed = true;
      controller?.close();
    }

    function fail(error?: unknown): void {
      if (closed) {
        return;
      }
      closed = true;
      controller?.error(error ?? new TypeError("scripted network failure"));
    }

    request.signal?.addEventListener(
      "abort",
      () => {
        request.aborted = true;
        request.abortReason = request.signal?.reason;
        if (!released) {
          released = true;
          reject?.(abortError(request.signal?.reason));
          return;
        }
        if (!closed) {
          closed = true;
          controller?.error(abortError(request.signal?.reason));
        }
      },
      { once: true },
    );

    const connection: ScriptedConnection = {
      request,
      answer,
      respond,
      push(chunk) {
        if (closed) {
          return;
        }
        controller?.enqueue(typeof chunk === "string" ? encoder.encode(chunk) : chunk);
      },
      end: close,
      fail,
      get closed() {
        return closed;
      },
    };
    connections.push(connection);
    for (const waiter of waiters.splice(0)) {
      waiter(connection);
    }
    if (!answer.holdHeaders) {
      respond();
    }
    return promise;
  }

  const impl = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request: ScriptedRequest = {
      url: typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
      method: init?.method ?? "GET",
      headers: headerRecord(init),
      signal: init?.signal ?? null,
      aborted: false,
      abortReason: undefined,
    };
    requests.push(request);
    if (request.method !== "GET") {
      return Promise.resolve(postHandler(request));
    }
    return open(request, answers.shift() ?? {});
  };

  return {
    fetch: impl,
    requests,
    connections,
    expect(answer) {
      answers.push(answer);
    },
    nextConnection() {
      return new Promise((resolve) => {
        waiters.push(resolve);
      });
    },
    onPost(handler) {
      postHandler = handler;
    },
  };
}

/** Encodes a hello frame with the given overrides on top of a healthy default payload. */
export function helloFrame(overrides: Record<string, unknown> = {}): string {
  const payload = {
    wire: 1,
    epoch: "aaaaaaaaaaaaaaaa",
    floor: "0",
    head: "0",
    resumed: false,
    verdict: "fresh",
    keepalive_ms: 15000,
    keepalive_event: "sse:keepalive",
    ...overrides,
  };
  return `event: sse:hello\ndata: ${JSON.stringify(payload)}\n\n`;
}

/** Encodes one application frame; `id` null omits the id line. */
export function frame(type: string, data: string, id: string | null): string {
  const idLine = id === null ? "" : `id: ${id}\n`;
  const eventLine = type === "message" ? "" : `event: ${type}\n`;
  return `${idLine}${eventLine}data: ${data}\n\n`;
}

/** The default keepalive frame. */
export const KEEPALIVE_FRAME = "event: sse:keepalive\ndata: {}\n\n";

/** Timer set to fake so stream reads, which ride macrotasks, keep settling under a faked clock. */
export const FAKE_CLOCK: {
  toFake: ("setTimeout" | "clearTimeout" | "setInterval" | "clearInterval" | "Date")[];
} = {
  toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"],
};

/** Lets pending microtasks and stream reads settle; independent of faked timers. */
export async function flush(rounds = 4): Promise<void> {
  for (let i = 0; i < rounds; i++) {
    await new Promise<void>((resolve) => {
      const channel = new MessageChannel();
      channel.port1.onmessage = () => {
        channel.port1.close();
        resolve();
      };
      channel.port2.postMessage(0);
    });
  }
}
