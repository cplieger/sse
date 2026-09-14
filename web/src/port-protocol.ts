import type { Removed, State } from "./digest.js";
import type { LifecycleEvent, RevalidateCause } from "./lifecycle.js";
import type { ClientState } from "./reducer.js";
import type { Frame } from "./stream.js";
import type { Subject } from "./versions.js";
import type { VisibilityInput } from "./visibility.js";
import type { Cursor } from "./wire.js";

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
