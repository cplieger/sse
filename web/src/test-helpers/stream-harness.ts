import type { LifecycleEvent } from "../lifecycle.js";
import { type OnlineSource, createOnlineManager } from "../online.js";
import {
  type Frame,
  type RevalidateContext,
  type Stream,
  type StreamOptions,
  createStream,
} from "../stream.js";
import { type VersionMap, createVersionMap } from "../versions.js";
import {
  type VisibilityEvent,
  type VisibilitySource,
  createVisibilityManager,
} from "../visibility.js";
import {
  type ScriptedConnection,
  type ScriptedFetch,
  flush,
  helloFrame,
  scriptedFetch,
} from "./scripted-fetch.js";

export interface FakeVisibility {
  readonly source: VisibilitySource;
  emit(ev: VisibilityEvent): void;
  set(visible: boolean): void;
}

/** A visibility source the test drives; `hidden`, `pagehide` and `freeze` flip the reading. */
export function fakeVisibility(initial = true): FakeVisibility {
  let visible = initial;
  const listeners = new Set<(ev: VisibilityEvent) => void>();
  return {
    source: {
      visible: () => visible,
      listen(cb) {
        listeners.add(cb);
        return () => {
          listeners.delete(cb);
        };
      },
    },
    emit(ev) {
      if (ev === "visible" || ev === "pageshow" || ev === "resume") {
        visible = true;
      } else {
        visible = false;
      }
      for (const cb of [...listeners]) {
        cb(ev);
      }
    },
    set(value) {
      visible = value;
    },
  };
}

export interface FakeOnline {
  readonly source: OnlineSource;
  emit(online: boolean): void;
}

export function fakeOnline(initial = true): FakeOnline {
  let online = initial;
  const listeners = new Set<(online: boolean) => void>();
  return {
    source: {
      online: () => online,
      listen(cb) {
        listeners.add(cb);
        return () => {
          listeners.delete(cb);
        };
      },
    },
    emit(value) {
      online = value;
      for (const cb of [...listeners]) {
        cb(value);
      }
    },
  };
}

/**
 * One revalidate call the test settles by hand. Shared with runtime-cases.ts,
 * which drives the same shape from its own harness.
 *
 * `reject` takes an `Error` rather than `unknown` because that is what every
 * caller passes, and narrowing it here is what lets both harnesses hand the
 * promise's own `reject` straight through — the wider signature needed a
 * normalizing wrapper whose non-Error branch no test could reach.
 */
export interface PendingRun {
  readonly ctx: RevalidateContext;
  resolve(): void;
  reject(error: Error): void;
}

export interface Harness {
  readonly stream: Stream;
  readonly sf: ScriptedFetch;
  readonly events: LifecycleEvent[];
  readonly frames: Frame[];
  readonly runs: PendingRun[];
  readonly versions: VersionMap;
  readonly visibility: FakeVisibility;
  readonly online: FakeOnline;
  /** Starts the stream, waits for the connection and pushes a hello; returns the connection. */
  open(hello?: Record<string, unknown>): Promise<ScriptedConnection>;
  /** Events of one kind, typed. */
  ofKind<K extends LifecycleEvent["kind"]>(kind: K): Extract<LifecycleEvent, { kind: K }>[];
  /** The connection opened last. */
  last(): ScriptedConnection;
}

export interface HarnessOptions {
  readonly visible?: boolean;
  readonly online?: boolean;
  readonly headers?: Record<string, string>;
  readonly timing?: StreamOptions["timing"];
  readonly alive?: StreamOptions["alive"];
  readonly versions?: VersionMap;
  readonly onFrame?: (frame: Frame) => void;
  readonly minWire?: number;
  readonly maxWire?: number;
}

/** Builds a stream over a scripted fetch, fake platform sources and hand-settled revalidations. */
export function harness(opts: HarnessOptions = {}): Harness {
  const sf = scriptedFetch();
  const events: LifecycleEvent[] = [];
  const frames: Frame[] = [];
  const runs: PendingRun[] = [];
  const versions = opts.versions ?? createVersionMap();
  const visibility = fakeVisibility(opts.visible ?? true);
  const online = fakeOnline(opts.online ?? true);
  const stream = createStream({
    url: "/events",
    fetch: sf.fetch,
    headers: opts.headers ?? {},
    visibility: createVisibilityManager(visibility.source),
    online: createOnlineManager(online.source),
    versions,
    ...(opts.timing !== undefined ? { timing: opts.timing } : {}),
    ...(opts.alive !== undefined ? { alive: opts.alive } : {}),
    ...(opts.minWire !== undefined ? { minWire: opts.minWire } : {}),
    ...(opts.maxWire !== undefined ? { maxWire: opts.maxWire } : {}),
    onFrame(frame) {
      frames.push(frame);
      opts.onFrame?.(frame);
    },
    onLifecycle(ev) {
      events.push(ev);
    },
    revalidate(ctx) {
      return new Promise<void>((resolve, reject) => {
        runs.push({ ctx, resolve, reject });
      });
    },
  });
  const h: Harness = {
    stream,
    sf,
    events,
    frames,
    runs,
    versions,
    visibility,
    online,
    async open(hello = {}) {
      if (stream.state().kind === "stopped") {
        stream.start();
      }
      await flush();
      const conn = h.last();
      conn.push(helloFrame(hello));
      await flush();
      return conn;
    },
    ofKind(kind) {
      return events.filter((e) => e.kind === kind) as Extract<
        LifecycleEvent,
        { kind: typeof kind }
      >[];
    },
    last() {
      const conn = sf.connections[sf.connections.length - 1];
      if (conn === undefined) {
        throw new Error("no connection was opened");
      }
      return conn;
    },
  };
  return h;
}
