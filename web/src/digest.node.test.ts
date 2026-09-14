import { afterEach, describe, expect, it, vi } from "vitest";
import { createDigestClient } from "./digest.js";
import type { Held } from "./versions.js";

const A = "aaaaaaaaaaaaaaaa";
const B = "bbbbbbbbbbbbbbbb";

interface Call {
  readonly url: string;
  readonly init: RequestInit;
  readonly body: { epoch?: string; subjects: Held[] };
}

type Answer = (call: Call) => Response | Promise<Response>;

function scripted(...answers: Answer[]): { calls: Call[]; fetch: typeof fetch } {
  const calls: Call[] = [];
  const impl = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const call: Call = {
      url: String(input),
      init: init ?? {},
      body: JSON.parse(String(init?.body)) as Call["body"],
    };
    calls.push(call);
    const answer = answers[calls.length - 1];
    if (answer === undefined) {
      throw new Error(`unexpected digest POST #${String(calls.length)}`);
    }
    return Promise.resolve(answer(call));
  };
  return { calls, fetch: impl as typeof fetch };
}

function ok(call: Call, epoch = A, extra: Record<string, unknown> = {}): Response {
  return Response.json({
    epoch,
    head: "100",
    checked: call.body.subjects.length,
    must_refetch: false,
    changed: [],
    removed: [],
    ...extra,
  });
}

function held(n: number): Held[] {
  return Array.from({ length: n }, (_, i) => ({
    kind: "chat",
    ref: `c_${String(i)}`,
    version: "1",
  }));
}

describe("digest client", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("splits held into batches of the default 256 subjects with the same epoch", async () => {
    const script = scripted(
      (c) => ok(c, A, { changed: [{ kind: "chat", ref: "c_0", version: "2" }] }),
      (c) => ok(c),
      (c) => ok(c, A, { removed: [{ kind: "chat", ref: "c_599", reason: "gone" }] }),
    );
    const client = createDigestClient({ url: "/digest", fetch: script.fetch });
    const result = await client.check({ epoch: A, held: held(600) });
    expect(script.calls.map((c) => c.body.subjects.length)).toEqual([256, 256, 88]);
    expect(script.calls.map((c) => c.body.epoch)).toEqual([A, A, A]);
    expect(script.calls.map((c) => c.init.method)).toEqual(["POST", "POST", "POST"]);
    expect(result).toEqual({
      kind: "ok",
      epoch: A,
      head: "100",
      changed: [{ kind: "chat", ref: "c_0", version: "2" }],
      removed: [{ kind: "chat", ref: "c_599", reason: "gone" }],
    });
  });

  it("sends one empty batch for an empty snapshot and omits an absent epoch", async () => {
    const script = scripted((c) => ok(c, B));
    const client = createDigestClient({ url: "/digest", fetch: script.fetch });
    const result = await client.check({ epoch: null, held: [] });
    expect(script.calls).toHaveLength(1);
    expect(script.calls[0]?.body).toEqual({ subjects: [] });
    expect(result).toEqual({ kind: "ok", epoch: B, head: "100", changed: [], removed: [] });
  });

  it("rejects an extra key", async () => {
    const script = scripted((c) => ok(c, A, { checked: 2 }));
    const client = createDigestClient({ url: "/digest", fetch: script.fetch });
    const result = await client.check({ epoch: A, held: held(1) });
    expect(result).toEqual({ kind: "must_refetch", epoch: A, head: "100" });
  });

  it("rejects a duplicate key", async () => {
    const script = scripted((c) =>
      ok(c, A, {
        changed: [
          { kind: "chat", ref: "c_0", version: "2" },
          { kind: "chat", ref: "c_0", version: "3" },
        ],
      }),
    );
    const client = createDigestClient({ url: "/digest", fetch: script.fetch });
    const result = await client.check({ epoch: A, held: held(2) });
    expect(result).toEqual({ kind: "must_refetch", epoch: A, head: "100" });
    const across = scripted((c) =>
      ok(c, A, {
        changed: [{ kind: "chat", ref: "c_0", version: "2" }],
        removed: [{ kind: "chat", ref: "c_0", reason: "gone" }],
      }),
    );
    const second = createDigestClient({ url: "/digest", fetch: across.fetch });
    expect(await second.check({ epoch: A, held: held(2) })).toEqual({
      kind: "must_refetch",
      epoch: A,
      head: "100",
    });
  });

  it("rejects an unrequested key", async () => {
    const script = scripted((c) =>
      ok(c, A, { removed: [{ kind: "chat", ref: "c_9", reason: "forbidden" }] }),
    );
    const client = createDigestClient({ url: "/digest", fetch: script.fetch });
    const result = await client.check({ epoch: A, held: held(2) });
    expect(result).toEqual({ kind: "must_refetch", epoch: A, head: "100" });
  });

  it("any batch must_refetch is global and carries the server epoch", async () => {
    const script = scripted(
      (c) => ok(c, A, { changed: [{ kind: "chat", ref: "c_0", version: "2" }] }),
      () => Response.json({ epoch: B, head: "3", checked: 0, must_refetch: true }),
    );
    const client = createDigestClient({ url: "/digest", fetch: script.fetch, maxSubjects: 2 });
    const result = await client.check({ epoch: A, held: held(4) });
    expect(result).toEqual({ kind: "must_refetch", epoch: B, head: "3" });
    expect(script.calls).toHaveLength(2);
  });

  it("never calls observe", async () => {
    const snapshot = { epoch: A, held: held(3) };
    const observe = vi.fn();
    const decoy = { ...snapshot, observe };
    const script = scripted((c) =>
      ok(c, A, { changed: [{ kind: "chat", ref: "c_1", version: "9" }] }),
    );
    const client = createDigestClient({ url: "/digest", fetch: script.fetch });
    const result = await client.check(decoy);
    expect(result.kind).toBe("ok");
    expect(observe).not.toHaveBeenCalled();
    expect(snapshot.held.map((h) => h.version)).toEqual(["1", "1", "1"]);
  });

  it("an aborted signal rejects check and cancels the POST", async () => {
    const controller = new AbortController();
    let posted: AbortSignal | null = null;
    const client = createDigestClient({
      url: "/digest",
      fetch: (_url, init) => {
        const signal = init?.signal ?? null;
        posted = signal;
        return new Promise<Response>((_resolve, reject) => {
          signal?.addEventListener("abort", () => {
            reject(signal.reason as Error);
          });
        });
      },
    });
    const pending = client.check({ epoch: A, held: held(1) }, controller.signal);
    controller.abort(new DOMException("leaving", "AbortError"));
    await expect(pending).rejects.toThrow("leaving");
    expect(posted !== null && (posted as AbortSignal).aborted).toBe(true);
  });

  it("timeoutMs bounds each POST", async () => {
    vi.useFakeTimers();
    const client = createDigestClient({
      url: "/digest",
      timeoutMs: 250,
      fetch: (_url, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(init.signal?.reason as Error);
          });
        }),
    });
    const pending = client.check({ epoch: A, held: held(1) });
    const outcome = expect(pending).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(249);
    await vi.advanceTimersByTimeAsync(1);
    await outcome;
  });

  it("SSE-Client rides headers when set and is absent otherwise", async () => {
    const tagged = scripted((c) => ok(c));
    await createDigestClient({
      url: "/digest",
      fetch: tagged.fetch,
      headers: { "SSE-Client": "tab-1" },
    }).check({ epoch: A, held: held(1) });
    expect(tagged.calls[0]?.init.headers).toEqual({
      "SSE-Client": "tab-1",
      Accept: "application/json",
      "Content-Type": "application/json",
    });
    const plain = scripted((c) => ok(c));
    await createDigestClient({ url: "/digest", fetch: plain.fetch }).check({
      epoch: A,
      held: held(1),
    });
    expect(plain.calls[0]?.init.headers).toEqual({
      Accept: "application/json",
      "Content-Type": "application/json",
    });
  });

  it("rejects on a non-200 status and on a body without a position", async () => {
    const status = scripted(() => new Response("nope", { status: 500 }));
    await expect(
      createDigestClient({ url: "/digest", fetch: status.fetch }).check({
        epoch: A,
        held: held(1),
      }),
    ).rejects.toThrow("unexpected status 500");
    const shape = scripted(() => Response.json({ checked: 1 }));
    await expect(
      createDigestClient({ url: "/digest", fetch: shape.fetch }).check({ epoch: A, held: held(1) }),
    ).rejects.toThrow("malformed response");
  });
});
