import { createDigestClient } from "../digest.js";
import type { RevalidateContext } from "../stream.js";
import { createVersionMap } from "../versions.js";
import { type TabSet, createWorkerHost } from "../worker.js";

/**
 * The SharedWorker script of the browser suite. The fixture URL and timing overrides ride the
 * worker URL's query (`fixture`, `heartbeat`, `hidden`); test-only messages on a port
 * (`test:close`, `test:block`, `test:pause`, `test:suppress-alive`) drive the failure cases.
 */

interface SharedScope {
  onconnect: ((event: MessageEvent) => void) | null;
  close(): void;
  readonly location: { readonly href: string };
}

interface TestMessage {
  readonly type: "test:close" | "test:block" | "test:pause" | "test:suppress-alive";
  readonly ms?: number;
  readonly on?: boolean;
}

const scope = globalThis as unknown as SharedScope;
const params = new URL(scope.location.href).searchParams;
const fixture = params.get("fixture") ?? "";
const heartbeatMs = Number(params.get("heartbeat") ?? "5000");
const hiddenCloseAfterMs = Number(params.get("hidden") ?? "60000");

let paused = false;
let suppressAlive = false;

// The heartbeat interval is the one timer the suite stalls; wrapping setInterval is how a
// page can hold it without touching the host.
const realSetInterval = globalThis.setInterval.bind(globalThis);
function pausableInterval(handler: TimerHandler, timeout?: number): ReturnType<typeof setInterval> {
  return realSetInterval(() => {
    if (!paused && typeof handler === "function") {
      handler();
    }
  }, timeout);
}
globalThis.setInterval = pausableInterval as unknown as typeof setInterval;

const gatedFetch: typeof fetch = (input, init) => {
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
  if (suppressAlive && init?.method === "POST" && url.endsWith("/alive")) {
    return Promise.resolve(new Response(null, { status: 204 }));
  }
  return fetch(input, init);
};

const versions = createVersionMap();
const digest = createDigestClient({ url: `${fixture}/digest`, fetch: gatedFetch });

async function revalidate(ctx: RevalidateContext, tabs: TabSet): Promise<void> {
  if (ctx.full) {
    await tabs.run(ctx);
    return;
  }
  const result = await digest.check(versions.snapshot(), ctx.signal);
  if (result.kind === "must_refetch") {
    versions.bind(result.epoch);
    await tabs.run(ctx);
    return;
  }
  if (result.changed.length === 0 && result.removed.length === 0) {
    return;
  }
  await tabs.run(ctx, { changed: result.changed, removed: result.removed });
}

const host = createWorkerHost({
  url: `${fixture}/events`,
  fetch: gatedFetch,
  versions,
  heartbeatMs,
  timing: { hiddenCloseAfterMs },
  alive: { url: `${fixture}/alive` },
  revalidate,
});

function isTestMessage(value: unknown): value is TestMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { type?: unknown }).type === "string" &&
    (value as { type: string }).type.startsWith("test:")
  );
}

function block(ms: number): void {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    // Busy loop: the message pump is wedged for `ms`.
  }
}

scope.onconnect = (event: MessageEvent) => {
  const port = event.ports[0];
  if (port === undefined) {
    return;
  }
  port.addEventListener("message", (message: MessageEvent) => {
    const data: unknown = message.data;
    if (!isTestMessage(data)) {
      return;
    }
    switch (data.type) {
      case "test:close":
        scope.close();
        return;
      case "test:block":
        block(data.ms ?? 0);
        return;
      case "test:pause":
        paused = true;
        setTimeout(() => {
          paused = false;
        }, data.ms ?? 0);
        return;
      case "test:suppress-alive":
        suppressAlive = data.on ?? false;
        return;
    }
  });
  host.attach(port);
};
