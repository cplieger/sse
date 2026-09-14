# @cplieger/sse

> A Server-Sent-Events client that resumes exactly where it stopped, notices a dead stream, and reconciles after a sleep.

The TypeScript half of [cplieger/sse](https://github.com/cplieger/sse), speaking the wire the Go `sse` package serves: every frame carries an `epoch:offset` cursor, every connection opens with a hello that says whether that cursor was honoured, and the client turns both into a projection an application can trust across a page hide, a phone lock or a network change. A native `EventSource` cannot set a request header, cannot tell a silent stream from an idle one, and reconnects on its own schedule with whatever `Last-Event-ID` it last saw; this client owns the connection over `fetch` and `ReadableStream` instead, presents the cursor it holds, measures liveness on bytes, closes a hidden tab's stream after a minute and reopens it on return, backs off with full jitter, and holds incoming frames while the application asks the server what changed. It has no runtime dependencies, and the DOM readers (`document.visibilityState`, `navigator.onLine`) are injectable, so the same runtime runs in Node against the Go test fixture.

## Install

```sh
npx jsr add @cplieger/sse
# or
npm i @cplieger/sse
```

## Usage

```ts
import { createStream, createVersionMap } from "@cplieger/sse";

const versions = createVersionMap();

const stream = createStream({
  url: "/api/events",
  headers: { "SSE-Client": profileTag },
  alive: { url: "/api/alive" },
  versions,
  onFrame: (frame) => {
    applyToStore(frame.type, frame.data);
  },
  revalidate: async (ctx) => {
    // Runs once per hello and once per wake, never concurrently. Every fetch in
    // here takes ctx.signal; ctx.full means the map was cleared, so skip the digest.
    await reconcile(versions.snapshot(), ctx);
  },
  onLifecycle: (ev) => {
    log.debug("sse", ev);
  },
});

stream.start();
```

The first frame of every connection is a hello: it names the server epoch, the ring's floor and head, the keepalive interval and event name, and whether the cursor the client presented was resumed; a hello whose `wire` is outside `[minWire, maxWire]` (both default to the package's `WIRE`) ends the connection with a `wire_unsupported` record and keeps the cursor.

The client keeps its cursor as `epoch:offset`, presents it as `Last-Event-ID` on every connect, and advances it only as frames are delivered to `onFrame`, so a reconnect after a gap either replays exactly the missed frames or reports through the hello that the gap could not be covered, and the application reconciles instead of trusting the replay.

While `revalidate` runs, incoming frames are held and delivered in order once it settles; runs are single-flight, a second cause queues at most one more run, and a held queue past 2000 frames or 64 MiB ends the connection so the next connect resumes from the cursor instead.

With `alive` set, every keepalive (or every `everyBeats`-th) is acknowledged with a `POST` to that URL carrying the stream's headers, so a server-side presence table keyed on `SSE-Client` sees which clients are still reading; a failed acknowledgement is reported as an `alive_ack` record.

`createWorkerHost` runs one stream per browser profile inside a `SharedWorker`, and `attachToWorker` connects each tab to it over a `MessagePort`, folding the tabs' visibility and network readings into that one connection; where `SharedWorker` is absent or the worker never answers, the tab falls back to its own stream and reports the switch. The `fallback` option returns a `TabFallback`: the `createStream` result together with the `versions` map and the `headers` record it was built on, which is what lets the `TabAttachment` work the same in both modes. `observe(subject, version, epoch)` reports a stamp the tab applied, into the host's version map or the fallback's; `reconnect({ resetCursor })` reconnects the stream the tab is attached to; `setTag(tag)` presents a new `SSE-Client` value with one reconnect, written into the host's headers or the fallback's; and `detach("logout")` ends the profile's stream. The tab writes its `tag` into the fallback's headers before the first connect, so a consumer never fills `SSE-Client` itself.

The host presents the first non-empty tag a tab attaches with and replaces it, reconnecting once, when a later attach or `setTag` carries a different one; the empty tag is ignored.

The host's `revalidate(ctx, tabs)` body performs one digest for the profile and fans the verdict to every tab through `tabs.run(ctx, { changed, removed })`, so each tab's `revalidate` receives a `TabRevalidateContext` carrying `changed` and `removed` beside its own `signal`. A `state` message reaches a tab before the `lifecycle` record of the same transition, so `attachment.state()` is current inside the handler, and a tab attaching to a live stream receives `state` and then a `tab_attached` record naming that state.

The package typechecks under `lib: ["ESNext", "WebWorker"]` as well as under the DOM lib (`tsconfig.worker.json` is the check), so a consumer's worker entry can import `createWorkerHost` from a worker-only program: the DOM visibility and online sources read `document` and `window` through `globalThis` and are inert where they are absent.

`onLifecycle` receives every state change and diagnostic as one `LifecycleEvent` record (`state`, `hello`, `watchdog`, `reset`, `revalidate`, `hidden_closed`, `alive_ack`, `worker_dead` and the rest of the union), which is the surface to log and to count.

## Browser floor

`Response.body` as a readable stream (Firefox 65, Chrome 43, Safari 10.1), `AbortSignal.reason` (Firefox 97, Chrome 98, Safari 15.4), `visibilitychange` (Chrome 62, Firefox 56, Safari 14.1), `pagehide` and `pageshow`, and `BigInt` (Chrome 67, Firefox 68, Safari 14). `AbortSignal.any` is not used; two signals are composed by hand. `SharedWorker` is needed by the worker host only and is not part of the floor: where it is absent (Chrome for Android before 148, Samsung Internet) the tab runs its own stream through the fallback.

## Full documentation

The Go server API, the hub options, the hello's verdicts, the digest and the presence hook are in the [repository README](https://github.com/cplieger/sse#readme). The shared timing constants both halves pin are in the repository's `timing.json`.

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude](https://claude.com), [GPT](https://openai.com), and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

Apache-2.0. See [LICENSE](LICENSE).
