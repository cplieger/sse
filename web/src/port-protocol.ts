import type { Removed, State } from "./digest.js";
import type { LifecycleEvent, RevalidateCause } from "./lifecycle.js";
import type { ClientState } from "./reducer.js";
import type { Frame } from "./stream.js";
import type { Subject } from "./versions.js";
import type { VisibilityInput } from "./visibility.js";
import type { Cursor } from "./wire.js";

/**
 * Why a tab is leaving the host. `unload` and `logout` are the application's, through
 * `TabAttachment.detach`; `respawn` and `fallback` are the tab's own ladder giving up on a worker that
 * stopped answering. Only `logout` stops the profile's stream — the other three leave it running for
 * whatever tabs remain.
 */
export type DetachCause = "unload" | "respawn" | "fallback" | "logout";

/** RevalidateContext without its signal, which cannot cross a port; the tab supplies its own. */
export interface PortRevalidateContext {
  readonly cause: RevalidateCause;
  readonly epoch: string | null;
  readonly generation: number;
  readonly full: boolean;
  /** The host's digest verdict for this run, when its `revalidate` body performed one. */
  readonly changed?: readonly State[];
  readonly removed?: readonly Removed[];
}

/**
 * Every message a tab sends the host. `attach` comes first and identifies the tab: until the host has
 * one, only `ping` is answered and everything else is discarded. Its `hadWorker` says this tab held a
 * worker link before this attach, which is what earns it a full revalidation where a first attach to a
 * live stream is given none — the state it holds was built against a stream it can no longer account
 * for.
 */
export type TabToWorker =
  | {
      readonly type: "attach";
      readonly tabId: string;
      readonly visible: boolean;
      readonly online: boolean;
      readonly hadWorker: boolean;
      readonly tag: string;
    }
  | { readonly type: "visibility"; readonly ev: VisibilityInput }
  | { readonly type: "network"; readonly online: boolean }
  | { readonly type: "ping"; readonly seq: number }
  | { readonly type: "detach"; readonly cause: DetachCause }
  | { readonly type: "heartbeat_ack"; readonly seq: number }
  | {
      readonly type: "observed";
      readonly subject: Subject;
      readonly version: string;
      readonly epoch: string;
    }
  | { readonly type: "reconnect"; readonly resetCursor: boolean }
  | { readonly type: "set_tag"; readonly tag: string }
  | { readonly type: "revalidate_done"; readonly runId: number }
  | { readonly type: "revalidate_failed"; readonly runId: number; readonly cause: string }
  | { readonly type: "frame_failed"; readonly generation: number; readonly cursor: Cursor | null };

/**
 * Every message the host sends a tab. Two of them are obligations rather than notifications: a
 * `heartbeat` must be answered with `heartbeat_ack` or the host expires the port and stops folding this
 * tab's readings, and a `revalidate_run` must be answered with `revalidate_done` or `revalidate_failed`
 * carrying the same `runId`, or the host's run stays pending until this port expires, leaves, or the
 * run's own signal aborts, which the runtime does at `revalidateTimeoutMs` and on `stop()`. A `state`
 * message precedes the `lifecycle` record of the same transition, so a tab reading its own state inside
 * that handler reads the new one.
 */
export type WorkerToTab =
  | { readonly type: "frame"; readonly frame: Frame; readonly generation: number }
  | { readonly type: "lifecycle"; readonly event: LifecycleEvent }
  | { readonly type: "heartbeat"; readonly seq: number }
  | { readonly type: "pong"; readonly seq: number }
  | { readonly type: "revalidate_run"; readonly runId: number; readonly ctx: PortRevalidateContext }
  | { readonly type: "stale" }
  | { readonly type: "state"; readonly state: ClientState };

const TAB_TYPES: ReadonlySet<string> = new Set<TabToWorker["type"]>([
  "attach",
  "visibility",
  "network",
  "ping",
  "detach",
  "heartbeat_ack",
  "observed",
  "reconnect",
  "set_tag",
  "revalidate_done",
  "revalidate_failed",
  "frame_failed",
]);

const WORKER_TYPES: ReadonlySet<string> = new Set<WorkerToTab["type"]>([
  "frame",
  "lifecycle",
  "heartbeat",
  "pong",
  "revalidate_run",
  "stale",
  "state",
]);

function typeOf(value: unknown): string | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const type = (value as { type?: unknown }).type;
  return typeof type === "string" ? type : null;
}

/** Narrows a port message to the tab-to-worker vocabulary. */
export function isTabMessage(value: unknown): value is TabToWorker {
  const type = typeOf(value);
  return type !== null && TAB_TYPES.has(type);
}

/** Narrows a port message to the worker-to-tab vocabulary. */
export function isWorkerMessage(value: unknown): value is WorkerToTab {
  const type = typeOf(value);
  return type !== null && WORKER_TYPES.has(type);
}
