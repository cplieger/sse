import type { ClientState, ConnectFailure } from "./reducer.js";
import type { Subject } from "./versions.js";
import type { ResetReason } from "./wire.js";

export type RevalidateCause = "visible" | "pageshow" | "online" | "hello";

export type HeldDiscardCause =
  "abort" | "stream_ended" | "hold_timeout" | "hold_overflow" | "revalidate_failed";

/** Every record the observability feed (`onLifecycle`) can carry. Nothing branches on one. */
export type LifecycleEvent =
  | { kind: "state"; from: ClientState["kind"]; to: ClientState["kind"]; generation: number }
  | {
      kind: "hello";
      verdict: string;
      wire: number;
      resumed: boolean;
      epoch: string;
      floor: string;
      head: string;
    }
  | { kind: "watchdog"; sinceLastByteMs: number }
  | { kind: "connect_failed"; reason: ConnectFailure }
  | { kind: "reset"; reason: ResetReason }
  | { kind: "stale_event"; type: string; generation: number }
  | { kind: "drain"; length: number; dropped: number }
  | { kind: "held_discarded"; cause: HeldDiscardCause; length: number }
  | { kind: "revalidate_timeout" }
  | { kind: "revalidate_failed"; cause: string; latch: boolean; ended: boolean }
  | { kind: "wire_unsupported"; wire: number | null }
  | { kind: "stale_stamp"; subject: Subject; epoch: string }
  | { kind: "revalidate"; cause: RevalidateCause; full: boolean }
  | { kind: "digest"; changed: number; removed: number; mustRefetch: boolean }
  | { kind: "hidden_closed"; arm: "timer" | "read" }
  | { kind: "reopened" }
  | { kind: "alive_ack"; ok: boolean; status: number | null }
  | { kind: "unknown_frame"; type: string }
  | { kind: "bad_cursor"; id: string }
  | { kind: "frame_rejected"; type: string; error: unknown }
  | { kind: "worker_spawned" }
  | { kind: "worker_dead"; sinceLastPortMessageMs: number }
  | { kind: "worker_unavailable"; cause: "silent" | "error" }
  | { kind: "worker_recovered" }
  | { kind: "auth_lost"; route: string; status: number }
  | { kind: "tab_attached"; replaced: boolean; state: ClientState["kind"] }
  | { kind: "tab_detached"; cause: string }
  | { kind: "port_expired"; sinceLastAckMs: number }
  | { kind: "port_repaired" };
