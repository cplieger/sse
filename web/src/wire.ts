import { MAX_OFFSET_STRING } from "./timing.js";

/** Event name of the handshake frame the server writes once per connection. */
export const HELLO_EVENT = "sse:hello";

/** Event name of the frame the server writes immediately before a deliberate close. */
export const RESET_EVENT = "sse:reset";

/** Prefix reserved for library-owned frames; an application never publishes one. */
export const RESERVED_PREFIX = "sse:";

/** A resume position: the hub epoch and a decimal offset, presented as `Last-Event-ID`. */
export interface Cursor {
  readonly epoch: string;
  readonly offset: string;
}

/** The hello frame's payload after validation. `resumed === true` is the only branch point. */
export interface Hello {
  readonly wire: number;
  readonly epoch: string;
  readonly floor: string;
  readonly head: string;
  readonly resumed: boolean;
  readonly verdict: string;
  readonly keepalive_ms: number;
  readonly keepalive_event: string;
}

/** Why a hello was refused; both reasons take the ordinary backoff ladder. */
export type HelloRefusal =
  { kind: "bad_hello" } | { kind: "wire_unsupported"; wire: number | null };

export type HelloResult = { ok: true; hello: Hello } | { ok: false; reason: HelloRefusal };

const EPOCH_RE = /^[0-9a-f]{16}$/;
const OFFSET_RE = /^(0|[1-9][0-9]*)$/;
const MAX_CURSOR_LENGTH = 33;

/** Orders two offsets in the wire grammar exactly, above 2^53 included. */
export function compareOffset(a: string, b: string): -1 | 0 | 1 {
  const x = BigInt(a);
  const y = BigInt(b);
  if (x < y) {
    return -1;
  }
  return x > y ? 1 : 0;
}

function isOffset(value: unknown): value is string {
  return (
    typeof value === "string" &&
    OFFSET_RE.test(value) &&
    compareOffset(value, MAX_OFFSET_STRING) <= 0
  );
}

function isEpoch(value: unknown): value is string {
  return typeof value === "string" && EPOCH_RE.test(value);
}

/** Parses `<epoch>:<offset>`; null for anything else, a bare integer included. */
export function parseCursor(id: string): Cursor | null {
  if (id.length === 0 || id.length > MAX_CURSOR_LENGTH) {
    return null;
  }
  const colon = id.indexOf(":");
  if (colon === -1) {
    return null;
  }
  const epoch = id.slice(0, colon);
  const offset = id.slice(colon + 1);
  if (!isEpoch(epoch) || !isOffset(offset)) {
    return null;
  }
  return { epoch, offset };
}

/** Renders a cursor as the `Last-Event-ID` value the server parses. */
export function cursorToString(cursor: Cursor): string {
  return `${cursor.epoch}:${cursor.offset}`;
}

const BAD_HELLO: HelloResult = { ok: false, reason: { kind: "bad_hello" } };

// Structural pessimism follows Centrifugo's recovered/was_recovering: only `resumed === true` resumes.
/** Validates a decoded hello payload against the wire contract and the accepted revision range. */
export function validateHello(data: unknown, minWire: number, maxWire: number): HelloResult {
  if (typeof data !== "object" || data === null) {
    return BAD_HELLO;
  }
  const record = data as Record<string, unknown>;
  const wire = record["wire"];
  if (typeof wire !== "number" || !Number.isInteger(wire)) {
    return BAD_HELLO;
  }
  if (wire < minWire || wire > maxWire) {
    return { ok: false, reason: { kind: "wire_unsupported", wire } };
  }
  const epoch = record["epoch"];
  const floor = record["floor"];
  const head = record["head"];
  const resumed = record["resumed"];
  const keepaliveMs = record["keepalive_ms"];
  const keepaliveEvent = record["keepalive_event"];
  if (!isEpoch(epoch) || !isOffset(floor) || !isOffset(head)) {
    return BAD_HELLO;
  }
  if (typeof resumed !== "boolean") {
    return BAD_HELLO;
  }
  if (typeof keepaliveMs !== "number" || !Number.isInteger(keepaliveMs) || keepaliveMs <= 0) {
    return BAD_HELLO;
  }
  if (typeof keepaliveEvent !== "string") {
    return BAD_HELLO;
  }
  const verdict = record["verdict"];
  return {
    ok: true,
    hello: {
      wire,
      epoch,
      floor,
      head,
      resumed,
      verdict: typeof verdict === "string" ? verdict : "",
      keepalive_ms: keepaliveMs,
      keepalive_event: keepaliveEvent,
    },
  };
}

/** The two stream-end reasons an `sse:reset` frame can carry. */
export type ResetReason = "reset:slow" | "reset:shutdown";

/** Maps an `sse:reset` payload to its reason; null when the payload is not one of the two. */
export function parseResetReason(data: string): ResetReason | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) {
    return null;
  }
  const reason = (parsed as Record<string, unknown>)["reason"];
  if (reason === "slow") {
    return "reset:slow";
  }
  if (reason === "shutdown") {
    return "reset:shutdown";
  }
  return null;
}
