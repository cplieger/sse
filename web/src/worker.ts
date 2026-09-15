import type { Removed, State } from "./digest.js";
import { createProfileOnlineSource, createProfileVisibilitySource } from "./fold.js";
import type { LifecycleEvent } from "./lifecycle.js";
import { createOnlineManager } from "./online.js";
import {
  type PortRevalidateContext,
  type TabToWorker,
  type WorkerToTab,
  isTabMessage,
} from "./port-protocol.js";
import {
  type RevalidateContext,
  type Stream,
  type StreamOptions,
  createStream,
  streamInternals,
} from "./stream.js";
import { DEFAULT_TIMING, resolveTiming } from "./timing.js";
import { createVisibilityManager } from "./visibility.js";

/** The changed and removed sets of one host-side digest, fanned to the tabs with the run. */
export interface DigestVerdict {
  readonly changed: readonly State[];
  readonly removed: readonly Removed[];
}

/** The attached tabs a profile revalidation may be routed to. */
export interface TabSet {
  /**
   * Fans the run to every acknowledging tab; settles when each answered, expired or left.
   * With `verdict`, each tab's context carries `changed` and `removed`, so one digest serves them all.
   */
  run(ctx: RevalidateContext, verdict?: DigestVerdict): Promise<void>;
  size(): number;
}

/**
 * The host's options: a stream's, minus the three it supplies itself — the visibility and network
 * readings, which it folds from its tabs, and `onFrame`, which it spends fanning frames to the ports.
 * `revalidate` is widened to receive the attached TabSet beside the context, so the application, not
 * this package, decides whether the reconciliation body runs in the worker or in the tabs. Everything
 * else is a stream's, `versions` included: the profile has one map, and it lives here.
 */
export interface WorkerHostOptions extends Omit<
  StreamOptions,
  "visibility" | "online" | "onFrame" | "onLifecycle" | "revalidate"
> {
  /** The application decides whether the body runs here or in the tabs it names. */
  readonly revalidate: (ctx: RevalidateContext, tabs: TabSet) => Promise<void>;
  readonly onLifecycle?: (ev: LifecycleEvent) => void;
  /** Seed for the profile's network reading; defaults to the worker's navigator.onLine. */
  readonly onlineSeed?: boolean;
  readonly heartbeatMs?: number;
}

/**
 * The profile's stream owner: one stream, one cursor and one version map behind however many tabs.
 * The stream starts when the first tab attaches, and it is stopped by `close()`, by a `logout` detach
 * or by a 401 — not by the last port going away, since the browser keeps a SharedWorker alive for its
 * owner documents and a tab that navigates is expected back.
 */
export interface WorkerHost {
  /** Called from the SharedWorker's onconnect with each new port. */
  attach(port: MessagePort): void;
  stream(): Stream;
  /** Stops the stream and closes every port. */
  close(): void;
}

interface PortEntry {
  readonly port: MessagePort;
  tabId: string | null;
  visible: boolean;
  online: boolean;
  lastAckAt: number;
  unacked: number;
  expired: boolean;
}

interface Quorum {
  readonly pending: Set<PortEntry>;
  readonly failures: string[];
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
}

const EXPIRY_BEATS = 3;

function statusOf(error: unknown): number | null {
  if (typeof error !== "object" || error === null) {
    return null;
  }
  const status = (error as { status?: unknown }).status;
  return typeof status === "number" ? status : null;
}

function cloneSafe(event: LifecycleEvent): LifecycleEvent {
  if (event.kind === "frame_rejected") {
    return {
      ...event,
      error: event.error instanceof Error ? event.error.message : String(event.error),
    };
  }
  return event;
}

function defaultOnline(): boolean {
  const onLine = (globalThis as { readonly navigator?: { readonly onLine?: boolean } }).navigator
    ?.onLine;
  return onLine !== false;
}

/** Creates the profile's stream owner: one stream, one cursor, one version map, many tabs. */
export function createWorkerHost(opts: WorkerHostOptions): WorkerHost {
  const cfg = resolveTiming(opts.timing);
  const heartbeatMs = opts.heartbeatMs ?? DEFAULT_TIMING.heartbeatMs;
  const headers: Record<string, string> = { ...opts.headers };
  const visibility = createProfileVisibilitySource();
  const online = createProfileOnlineSource(opts.onlineSeed ?? defaultOnline());
  const entries = new Map<MessagePort, PortEntry>();
  const byTab = new Map<string, PortEntry>();
  const quorums = new Map<number, Quorum>();
  let runSeq = 0;
  let heartbeatSeq = 0;
  let heartbeat: ReturnType<typeof setInterval> | null = null;
  let tag: string | null = null;
  let lastFrameFailed = -1;

  const now = (): number => Date.now();

  function active(): PortEntry[] {
    const out: PortEntry[] = [];
    for (const entry of entries.values()) {
      if (entry.tabId !== null && !entry.expired) {
        out.push(entry);
      }
    }
    return out;
  }

  function post(entry: PortEntry, message: WorkerToTab): void {
    entry.port.postMessage(message);
  }

  function broadcast(message: WorkerToTab): void {
    for (const entry of active()) {
      post(entry, message);
    }
  }

  /** Every record, the host's own included, reaches the application and every attached tab. */
  function emit(event: LifecycleEvent): void {
    opts.onLifecycle?.(event);
    broadcast({ type: "lifecycle", event: cloneSafe(event) });
  }

  function portContext(ctx: RevalidateContext, verdict?: DigestVerdict): PortRevalidateContext {
    const base = { cause: ctx.cause, epoch: ctx.epoch, generation: ctx.generation, full: ctx.full };
    return verdict === undefined
      ? base
      : { ...base, changed: [...verdict.changed], removed: [...verdict.removed] };
  }

  const tabs: TabSet = {
    run(ctx, verdict) {
      const members = active();
      if (members.length === 0) {
        return Promise.resolve();
      }
      const runId = ++runSeq;
      return new Promise<void>((resolve, reject) => {
        const quorum: Quorum = { pending: new Set(members), failures: [], resolve, reject };
        quorums.set(runId, quorum);
        ctx.signal.addEventListener(
          "abort",
          () => {
            if (quorums.delete(runId)) {
              reject(new Error("revalidate run aborted"));
            }
          },
          { once: true },
        );
        const message: WorkerToTab = {
          type: "revalidate_run",
          runId,
          ctx: portContext(ctx, verdict),
        };
        for (const member of members) {
          post(member, message);
        }
      });
    },
    size: () => active().length,
  };

  function settleQuorum(runId: number, quorum: Quorum): void {
    if (quorum.pending.size > 0) {
      return;
    }
    quorums.delete(runId);
    const failure = quorum.failures[0];
    if (failure === undefined) {
      quorum.resolve();
    } else {
      quorum.reject(new Error(failure));
    }
  }

  function leaveQuorums(entry: PortEntry): void {
    for (const [runId, quorum] of quorums) {
      if (quorum.pending.delete(entry)) {
        settleQuorum(runId, quorum);
      }
    }
  }

  function authLost(route: string, status: number): void {
    stream.stop();
    emit({ kind: "auth_lost", route, status });
  }

  const stream = createStream({
    ...opts,
    headers,
    visibility: createVisibilityManager(visibility),
    online: createOnlineManager(online),
    onFrame(frame) {
      const message: WorkerToTab = { type: "frame", frame, generation: stream.state().generation };
      for (const entry of active()) {
        if (entry.unacked >= cfg.heldMaxFrames) {
          expire(entry);
          continue;
        }
        post(entry, message);
        entry.unacked++;
      }
    },
    onLifecycle(event) {
      if (event.kind === "state") {
        broadcast({ type: "state", state: stream.state() });
      }
      emit(event);
      if (event.kind === "alive_ack" && event.status === 401) {
        authLost("alive", 401);
      }
    },
    async revalidate(ctx) {
      try {
        await opts.revalidate(ctx, tabs);
      } catch (e: unknown) {
        if (statusOf(e) === 401) {
          authLost("digest", 401);
        }
        throw e;
      }
    },
  });
  const controls = streamInternals(stream);

  function expire(entry: PortEntry): void {
    if (entry.expired || entry.tabId === null) {
      return;
    }
    entry.expired = true;
    visibility.detach(entry.tabId);
    online.detach(entry.tabId);
    leaveQuorums(entry);
    emit({ kind: "port_expired", sinceLastAckMs: now() - entry.lastAckAt });
  }

  function repair(entry: PortEntry): void {
    if (entry.tabId === null) {
      return;
    }
    entry.expired = false;
    entry.unacked = 0;
    visibility.attach(entry.tabId, entry.visible);
    online.attach(entry.tabId, entry.online);
    post(entry, { type: "stale" });
    perPortFull(entry);
    emit({ kind: "port_repaired" });
  }

  function perPortFull(entry: PortEntry): void {
    post(entry, {
      type: "revalidate_run",
      runId: ++runSeq,
      ctx: {
        cause: "hello",
        epoch: opts.versions.epoch(),
        generation: stream.state().generation,
        full: true,
      },
    });
  }

  // A replaced member keeps its fold entry: the new port takes it over without a hidden flip.
  function remove(entry: PortEntry, cause: string, replaced = false): void {
    entries.delete(entry.port);
    if (entry.tabId !== null) {
      if (byTab.get(entry.tabId) === entry) {
        byTab.delete(entry.tabId);
      }
      if (!replaced) {
        visibility.detach(entry.tabId);
        online.detach(entry.tabId);
      }
      leaveQuorums(entry);
    }
    entry.port.onmessage = null;
    entry.port.close();
    emit({ kind: "tab_detached", cause });
    if (entries.size === 0) {
      stopHeartbeat();
    }
  }

  // The empty tag means "none", so it never displaces one a tab already presented.
  function adoptTag(next: string): void {
    if (next === "" || next === tag) {
      return;
    }
    tag = next;
    headers["SSE-Client"] = next;
    stream.reconnect();
  }

  function onAttach(entry: PortEntry, message: Extract<TabToWorker, { type: "attach" }>): void {
    const previous = byTab.get(message.tabId);
    const replaced = previous !== undefined && previous !== entry;
    if (replaced) {
      remove(previous, "replaced", true);
    }
    adoptTag(message.tag);
    entry.tabId = message.tabId;
    entry.visible = message.visible;
    entry.online = message.online;
    entry.lastAckAt = now();
    entry.unacked = 0;
    entry.expired = false;
    byTab.set(message.tabId, entry);
    const stopped = stream.state().kind === "stopped";
    if (stopped && message.hadWorker) {
      controls.queueFull();
    }
    visibility.attach(message.tabId, message.visible);
    online.attach(message.tabId, message.online);
    if (stopped) {
      stream.start();
    } else if (message.hadWorker) {
      perPortFull(entry);
    }
    const state = stream.state();
    post(entry, { type: "state", state });
    emit({ kind: "tab_attached", replaced, state: state.kind });
  }

  function onMessage(entry: PortEntry, message: TabToWorker): void {
    if (message.type === "attach") {
      onAttach(entry, message);
      return;
    }
    if (message.type === "ping") {
      post(entry, { type: "pong", seq: message.seq });
      return;
    }
    if (entry.tabId === null) {
      return;
    }
    switch (message.type) {
      case "visibility":
        if (message.ev === "pagehide") {
          remove(entry, "pagehide");
          return;
        }
        if (message.ev === "visible") {
          entry.visible = true;
        } else if (message.ev === "hidden") {
          entry.visible = false;
        }
        if (!entry.expired) {
          visibility.report(entry.tabId, message.ev);
        }
        return;
      case "network":
        entry.online = message.online;
        if (!entry.expired) {
          online.report(entry.tabId, message.online);
        }
        return;
      case "detach":
        remove(entry, message.cause);
        if (message.cause === "logout") {
          stream.stop();
        }
        return;
      case "heartbeat_ack":
        entry.lastAckAt = now();
        entry.unacked = 0;
        if (entry.expired) {
          repair(entry);
        }
        return;
      case "observed":
        opts.versions.observe(message.subject, message.version, message.epoch);
        return;
      case "reconnect":
        if (message.resetCursor) {
          stream.resetCursor();
        }
        stream.reconnect();
        return;
      case "set_tag":
        adoptTag(message.tag);
        return;
      case "revalidate_done":
      case "revalidate_failed": {
        const quorum = quorums.get(message.runId);
        if (!quorum?.pending.delete(entry)) {
          return;
        }
        if (message.type === "revalidate_failed") {
          quorum.failures.push(message.cause);
        }
        settleQuorum(message.runId, quorum);
        return;
      }
      case "frame_failed": {
        const generation = stream.state().generation;
        if (message.generation !== generation || lastFrameFailed === generation) {
          return;
        }
        lastFrameFailed = generation;
        emit({ kind: "frame_rejected", type: "tab", error: message.cursor });
        controls.revalidate("hello", false);
        return;
      }
    }
  }

  function tick(): void {
    const seq = ++heartbeatSeq;
    const threshold = EXPIRY_BEATS * heartbeatMs;
    for (const entry of [...entries.values()]) {
      post(entry, { type: "heartbeat", seq });
      if (entry.tabId !== null && !entry.expired && now() - entry.lastAckAt > threshold) {
        expire(entry);
      }
    }
  }

  function startHeartbeat(): void {
    heartbeat ??= setInterval(tick, heartbeatMs);
  }

  function stopHeartbeat(): void {
    if (heartbeat !== null) {
      clearInterval(heartbeat);
      heartbeat = null;
    }
  }

  return {
    attach(port) {
      const entry: PortEntry = {
        port,
        tabId: null,
        visible: false,
        online: true,
        lastAckAt: now(),
        unacked: 0,
        expired: false,
      };
      entries.set(port, entry);
      port.onmessage = (event: MessageEvent) => {
        const data: unknown = event.data;
        if (entries.get(port) === entry && isTabMessage(data)) {
          onMessage(entry, data);
        }
      };
      port.start();
      startHeartbeat();
    },
    stream: () => stream,
    close() {
      stopHeartbeat();
      for (const entry of [...entries.values()]) {
        remove(entry, "closed");
      }
      stream.stop();
    },
  };
}
