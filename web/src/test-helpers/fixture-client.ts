/** Typed client for the ssetest fixture's control routes. */

/** Same-origin path the vitest dev server forwards to the fixture for the browser projects. */
export const FIXTURE_PROXY_PREFIX = "/__fixture";

export interface FixturePosition {
  readonly epoch: string;
  readonly floor: string;
  readonly head: string;
}

export interface FixturePresence {
  readonly tag: string;
  readonly last_alive_at: string;
  readonly connected: number;
  readonly gone: boolean;
}

export interface FixturePresenceEvent {
  readonly at: string;
  readonly kind: "connected" | "disconnected";
  readonly topic: string;
  readonly verdict: string;
  readonly cause: string;
  readonly write: string;
  readonly epoch: string;
  readonly tag: string;
  readonly client_id: number;
}

export interface FixtureState {
  readonly position: FixturePosition;
  readonly presence: FixturePresence[];
  readonly events: FixturePresenceEvent[];
  readonly transitions: { readonly alive: number; readonly expired: number };
  readonly clients: number;
  readonly queued: number;
  readonly legacy_connects: number;
  readonly v3_connects: number;
}

export interface PublishRequest {
  readonly topic?: string;
  readonly name?: string;
  readonly data?: string;
  readonly count?: number;
  /** Replaces data so the encoded frame at the next offset is exactly this many bytes. */
  readonly size?: number;
}

export interface PublishResponse {
  readonly head: string;
  readonly offsets: string[];
}

export interface MutateResponse {
  readonly kind: string;
  readonly ref: string;
  readonly version: string;
  readonly status: string;
}

export interface RestResponse {
  readonly kind: string;
  readonly ref: string;
  readonly version: string;
  readonly epoch: string;
  readonly payload: string;
}

export class FixtureError extends Error {
  readonly status: number;

  constructor(route: string, status: number, body: string) {
    super(`${route}: ${String(status)} ${body}`);
    this.name = "FixtureError";
    this.status = status;
  }
}

export interface FixtureClient {
  readonly url: string;
  publish(req?: PublishRequest): Promise<PublishResponse>;
  /** Refused publishes reject with a FixtureError carrying 422. */
  stall(on: boolean, passWrites?: number): Promise<void>;
  restart(): Promise<string>;
  delayHello(ms: number): Promise<void>;
  delayDigest(ms: number): Promise<void>;
  hookSleep(ms: number, fail?: boolean): Promise<void>;
  mutate(kind: string, ref: string, status?: "" | "gone" | "forbidden"): Promise<MutateResponse>;
  restFailOnce(): Promise<void>;
  closeAfter(frames: number): Promise<void>;
  aliveWindow(ms: number): Promise<void>;
  state(): Promise<FixtureState>;
  rest(kind: string, ref: string, init?: RequestInit): Promise<RestResponse>;
  /** Clears every flag a case may have left behind. */
  reset(): Promise<void>;
}

export function fixtureClient(
  url: string,
  doFetch: typeof fetch = (input, init) => globalThis.fetch(input, init),
): FixtureClient {
  async function call<T>(route: string, body: unknown, method = "POST"): Promise<T> {
    const response = await doFetch(`${url}${route}`, {
      method,
      headers: body === undefined ? {} : { "Content-Type": "application/json" },
      body: body === undefined ? null : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new FixtureError(route, response.status, await response.text());
    }
    if (response.status === 204) {
      return undefined as T;
    }
    return (await response.json()) as T;
  }

  const client: FixtureClient = {
    url,
    publish: (req = {}) => call("/control/publish", req),
    stall: (on, passWrites = 0) => call("/control/stall", { on, pass_writes: passWrites }),
    restart: async () => (await call<{ epoch: string }>("/control/restart", undefined)).epoch,
    delayHello: (ms) => call("/control/delay-hello", { ms }),
    delayDigest: (ms) => call("/control/delay-digest", { ms }),
    hookSleep: (ms, fail = false) => call("/control/hook-sleep", { ms, fail }),
    mutate: (kind, ref, status = "") => call("/control/mutate", { kind, ref, status }),
    restFailOnce: () => call("/control/rest-fail-once", undefined),
    closeAfter: (frames) => call("/control/close-after", { frames }),
    aliveWindow: (ms) => call("/control/alive-window", { ms }),
    state: () => call("/control/state", undefined, "GET"),
    async rest(kind, ref, init = {}) {
      const response = await doFetch(`${url}/rest/${kind}/${ref}`, init);
      if (!response.ok) {
        throw new FixtureError("/rest", response.status, await response.text());
      }
      return (await response.json()) as RestResponse;
    },
    async reset() {
      await client.stall(false);
      await client.delayHello(0);
      await client.delayDigest(0);
      await client.hookSleep(0, false);
      await client.closeAfter(0);
      await client.aliveWindow(30_000);
    },
  };
  return client;
}
