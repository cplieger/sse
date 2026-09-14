import { DEFAULT_TIMING, DIGEST_MAX_SUBJECTS } from "./timing.js";
import type { Held, Subject } from "./versions.js";
import { compareOffset } from "./wire.js";

/** A subject the server reports at a version other than the presented one: a fetch instruction. */
export type State = Held;

/** A subject the server no longer answers for. */
export interface Removed extends Subject {
  readonly reason: "gone" | "forbidden";
}

export type DigestResult =
  | { kind: "must_refetch"; epoch: string; head: string }
  | { kind: "ok"; epoch: string; head: string; changed: State[]; removed: Removed[] };

export interface DigestSnapshot {
  readonly epoch: string | null;
  readonly held: readonly Held[];
}

export interface DigestClient {
  /** Asks the server which held versions moved; the optional signal cancels every POST. */
  check(snapshot: DigestSnapshot, signal?: AbortSignal): Promise<DigestResult>;
}

export interface DigestClientOptions {
  readonly url: string;
  readonly fetch?: typeof fetch;
  readonly headers?: Record<string, string>;
  readonly timeoutMs?: number;
  readonly maxSubjects?: number;
}

/** A digest POST answered with a status other than 200; `status` lets a host recognise a 401. */
export class DigestStatusError extends Error {
  readonly status: number;

  constructor(status: number) {
    super(`digest: unexpected status ${String(status)}`);
    this.name = "DigestStatusError";
    this.status = status;
  }
}

const EPOCH_RE = /^[0-9a-f]{16}$/;
const OFFSET_RE = /^(0|[1-9][0-9]*)$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isOffset(value: unknown): value is string {
  return (
    typeof value === "string" &&
    OFFSET_RE.test(value) &&
    compareOffset(value, "9007199254740991") <= 0
  );
}

function subjectKey(subject: Subject): string {
  return `${subject.kind}\0${subject.ref}`;
}

interface Position {
  readonly epoch: string;
  readonly head: string;
}

type BatchAnswer =
  | { kind: "must_refetch"; position: Position }
  | { kind: "ok"; position: Position; changed: State[]; removed: Removed[] };

/** Reads the position; null when the body is not even a positioned digest answer. */
function readPosition(body: unknown): Position | null {
  if (!isRecord(body)) {
    return null;
  }
  const epoch = body["epoch"];
  const head = body["head"];
  if (typeof epoch !== "string" || !EPOCH_RE.test(epoch) || !isOffset(head)) {
    return null;
  }
  return { epoch, head };
}

function readAnswer(
  body: Record<string, unknown>,
  position: Position,
  batch: readonly Held[],
): BatchAnswer {
  const refuse: BatchAnswer = { kind: "must_refetch", position };
  if (body["must_refetch"] === true) {
    return refuse;
  }
  if (body["must_refetch"] !== false || body["checked"] !== batch.length) {
    return refuse;
  }
  const changedRaw = body["changed"] ?? [];
  const removedRaw = body["removed"] ?? [];
  if (!Array.isArray(changedRaw) || !Array.isArray(removedRaw)) {
    return refuse;
  }
  const requested = new Set(batch.map(subjectKey));
  const seen = new Set<string>();
  const changed: State[] = [];
  const removed: Removed[] = [];
  for (const entry of changedRaw as unknown[]) {
    if (!isRecord(entry)) {
      return refuse;
    }
    const kind = entry["kind"];
    const ref = entry["ref"];
    const version = entry["version"];
    if (typeof kind !== "string" || typeof ref !== "string" || typeof version !== "string") {
      return refuse;
    }
    const k = subjectKey({ kind, ref });
    if (!requested.has(k) || seen.has(k)) {
      return refuse;
    }
    seen.add(k);
    changed.push({ kind, ref, version });
  }
  for (const entry of removedRaw as unknown[]) {
    if (!isRecord(entry)) {
      return refuse;
    }
    const kind = entry["kind"];
    const ref = entry["ref"];
    const reason = entry["reason"];
    if (typeof kind !== "string" || typeof ref !== "string") {
      return refuse;
    }
    if (reason !== "gone" && reason !== "forbidden") {
      return refuse;
    }
    const k = subjectKey({ kind, ref });
    if (!requested.has(k) || seen.has(k)) {
      return refuse;
    }
    seen.add(k);
    removed.push({ kind, ref, reason });
  }
  return { kind: "ok", position, changed, removed };
}

interface Deadline {
  readonly signal: AbortSignal;
  release(): void;
}

function composeSignal(signal: AbortSignal | undefined, timeoutMs: number): Deadline {
  const controller = new AbortController();
  if (signal !== undefined) {
    if (signal.aborted) {
      controller.abort(signal.reason);
    } else {
      signal.addEventListener(
        "abort",
        () => {
          controller.abort(signal.reason);
        },
        { once: true, signal: controller.signal },
      );
    }
  }
  const timer = setTimeout(() => {
    controller.abort(new DOMException("digest request timed out", "TimeoutError"));
  }, timeoutMs);
  return {
    signal: controller.signal,
    release: () => {
      clearTimeout(timer);
    },
  };
}

/** Creates the client of the state-digest route. It never touches a version map. */
export function createDigestClient(opts: DigestClientOptions): DigestClient {
  const doFetch: typeof fetch = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMING.digestTimeoutMs;
  const maxSubjects = opts.maxSubjects ?? DIGEST_MAX_SUBJECTS;
  const headers: Record<string, string> = {
    ...opts.headers,
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  async function post(
    epoch: string | null,
    batch: readonly Held[],
    signal: AbortSignal | undefined,
  ): Promise<BatchAnswer> {
    const body: Record<string, unknown> = { subjects: batch };
    if (epoch !== null) {
      body["epoch"] = epoch;
    }
    const deadline = composeSignal(signal, timeoutMs);
    let response: Response;
    try {
      response = await doFetch(opts.url, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        credentials: "same-origin",
        cache: "no-store",
        signal: deadline.signal,
      });
    } finally {
      deadline.release();
    }
    if (response.status !== 200) {
      throw new DigestStatusError(response.status);
    }
    const parsed: unknown = await response.json();
    const position = readPosition(parsed);
    if (position === null || !isRecord(parsed)) {
      throw new Error("digest: malformed response");
    }
    return readAnswer(parsed, position, batch);
  }

  return {
    async check(snapshot, signal) {
      const batches: Held[][] = [];
      for (let i = 0; i < snapshot.held.length; i += maxSubjects) {
        batches.push(snapshot.held.slice(i, i + maxSubjects));
      }
      const [first = [], ...rest] = batches;
      const changed: State[] = [];
      const removed: Removed[] = [];
      let answer = await post(snapshot.epoch, first, signal);
      for (let i = 0; ; i++) {
        if (answer.kind === "must_refetch") {
          return { kind: "must_refetch", ...answer.position };
        }
        changed.push(...answer.changed);
        removed.push(...answer.removed);
        const batch = rest[i];
        if (batch === undefined) {
          return { kind: "ok", ...answer.position, changed, removed };
        }
        answer = await post(snapshot.epoch, batch, signal);
      }
    },
  };
}
