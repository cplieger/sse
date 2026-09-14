# sse

[![Go Reference](https://pkg.go.dev/badge/github.com/cplieger/sse.svg)](https://pkg.go.dev/github.com/cplieger/sse)
[![npm](https://img.shields.io/npm/v/@cplieger/sse)](https://www.npmjs.com/package/@cplieger/sse)
[![JSR](https://jsr.io/badges/@cplieger/sse)](https://jsr.io/@cplieger/sse)
[![Test coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/sse/badges/coverage.json)](https://github.com/cplieger/sse/actions/workflows/coverage.yml)
[![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/sse/badges/mutation.json)](https://github.com/cplieger/sse/issues?q=label%3Agremlins-tracker)
[![Mutation (TS)](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/sse/badges/mutation-ts.json)](https://github.com/cplieger/sse/issues?q=label%3Astryker-tracker)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/PROJECT_ID/badge)](https://www.bestpractices.dev/projects/PROJECT_ID)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cplieger/sse/badge)](https://scorecard.dev/viewer/?uri=github.com/cplieger/sse)

> Server-Sent Events that resume from an exact cursor: a Go broadcast hub and a TypeScript client speaking one wire.

`github.com/cplieger/sse` is a broadcast hub for SSE endpoints whose clients resume from an exact position: every frame carries an `<epoch>:<offset>` cursor, every connection opens with a hello that says whether the presented cursor was honoured, and a hub refuses at construction any retention it could not keep. `@cplieger/sse`, published from [web/](web/README.md), is the browser half of the same wire: it owns the connection over `fetch`, presents the cursor it holds, measures liveness on bytes, closes a hidden tab's stream and reopens it on return, and holds incoming frames while the application asks the server what changed. Both halves ship from one repository and one tag, and every timing constant they share is written once in `timing.json` and pinned by a test in each language.

The Go module has one dependency, [webhttp](https://github.com/cplieger/webhttp), for the JSON error envelope its refusals answer with; webhttp itself is standard-library only. The TypeScript package has no runtime dependencies.

## Install

- Go: `go get github.com/cplieger/sse@latest`
- TS: `npx jsr add @cplieger/sse` or `npm i @cplieger/sse`

## Usage

```go
hub, err := sse.New(
	sse.WithReplay(1024),
	sse.WithReplayTTL(10*time.Minute),
	sse.WithReplyMaxEvents(256),
	sse.WithPresence(func(ev sse.PresenceEvent) { presence.Record(ev) }),
)
if err != nil {
	return err // wraps sse.ErrConfig and names the offending option
}

mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
	hub.Serve(w, r,
		sse.WithTopic(r.URL.Query().Get("chat_id")),
		sse.OnConnect(func(w *sse.Writer, h sse.Hello) error {
			return w.Event("connected", fmt.Appendf(nil, `{"resumed":%t}`, h.Resumed))
		}),
		sse.WithClientTag(r.Header.Get("SSE-Client")),
	)
})
mux.Handle("POST /api/sync", webhttp.RouteTimeout(hub.DigestHandler(resolve), 10*time.Second, "digest timed out"))

offset, err := hub.Publish(sse.Event{Name: "notify", Topic: chatID, Data: payload})
if errors.Is(err, sse.ErrFrameTooLarge) || errors.Is(err, sse.ErrInvalidUTF8) {
	// Refused before any state changed: no offset was consumed. Publish a
	// fetch instruction instead of the payload.
}

// Inside webhttp.Run's WithPreDrain hook, so streams release before the HTTP drain:
if err := hub.Shutdown(ctx); err != nil {
	slog.Warn("sse: streams still draining", "error", err)
}
```

`ExampleNew`, `ExampleHub_Serve` and `ExampleHub_DigestHandler` in `example_test.go` run this shape end to end. The browser side of the same endpoint is the `createStream` example in [web/README.md](web/README.md).

## API

### Constructing a hub

`New(opts ...Option) (*Hub, error)` returns an error wrapping `ErrConfig` for an option set the hub could not honour; `MustNew` panics with the same message, for package-level construction in `main`. Each option refuses its own value on the spot. The rules that compare two options (a ring without a TTL, a TTL below the client's watchdog window, a reply cap above the ring, a write timeout at or below the keepalive) and the derived defaults run once after every option has been applied, so option order never changes the verdict.

| Option | Default | Refused |
| --- | --- | --- |
| `WithReplay(n)` | `0` (no replay) | negative; non-zero without `WithReplayTTL` |
| `WithReplayTTL(d)` | none | at or below zero; below `max(3 × keepalive, 15s) + 30s` (75s at the default keepalive) |
| `WithReplayMaxBytes(n)` | `64 × MaxFrameBytes` | below `MaxFrameBytes` |
| `WithReplyMaxEvents(n)` | `min(ring, 256)` | negative; above the ring |
| `WithClientBuffer(n)` | `max(ring, 256)` | at or below zero |
| `WithMaxClients(n)` | `0` (unlimited) | never; zero or negative means unlimited |
| `WithKeepalive(d)` | `15s` | below 1ms |
| `WithKeepaliveEvent(name)` | `sse:keepalive` | CR or LF in the name; `""` selects the `: keepalive` comment form |
| `WithReconnectDelay(d)` | `1500ms` | below 1ms |
| `WithWriteTimeout(d)` | `2 × keepalive` | at or below the keepalive |
| `WithLogger(l)` | `slog.Default()` | never; nil keeps the default |
| `WithPresence(fn)` | none | never; nil removes the hook |

The ring is bounded three ways at once (count, age, bytes) and the oldest entries are evicted first; `WithReplyMaxEvents` caps how much of it one resume may be sent, so a long replay cannot hold a reconnecting client.

### Serving and publishing

- `(*Hub).Serve(w, r, opts ...ServeOption)`: subscribes the request and streams until the peer leaves, the request context ends, the client is reset as slow, a write fails, or the hub shuts down. It owns the proxy-defensive headers (`Cache-Control: no-cache, no-transform`, `X-Accel-Buffering: no`), the `retry:` field, the hello, the `Last-Event-ID` replay, keepalives and the `sse:reset` frame. Every write runs under `WithWriteTimeout` and the deadline is cleared after it, so a peer that stopped reading ends its own connection instead of the stream goroutine. A `http.Flusher` reachable through an `Unwrap()` chain works; the 500 `streaming_unsupported` refusal fires only when no flusher exists at any depth, and 503 `sse_unavailable` answers a connection over the client cap or after `Shutdown`.
- Serve options: `WithTopic(t)` (receive broadcasts plus events scoped to `t`; the empty topic receives everything), `OnConnect(fn func(w *Writer, h Hello) error)` (runs after the `retry:` field, the hello and the replay have been flushed, so the client's connect deadline never measures the hook; it receives the `Hello` the client received and writes initial-state frames through `Writer.Event(name, data)`, each its own bounded write and flush, none carrying an id; an error ends the connection), `WithClientTag(tag)` (the application's presence key, carried on `PresenceEvent.Tag`; the empty string means no tag, so `r.Header.Get("SSE-Client")` can be passed through unconditionally, and any other value outside `[A-Za-z0-9_-]{1,64}` is treated as absent with one Warn).
- `(*Hub).Publish(Event) (uint64, error)`: assigns the next offset, appends to the ring and fans out to every matching client without blocking. Checks run in a fixed order before the frame is accepted: a name starting with `sse:`, holding CR or LF, or equal to the configured keepalive name panics (a fixed property of the call site); `Data` that is not UTF-8 returns `ErrInvalidUTF8`; an encoded frame above `MaxFrameBytes` (1 MiB, terminating blank line included) returns `ErrFrameTooLarge`. A refused frame consumes no offset and leaves the ring untouched, before and after `Shutdown` alike; a valid frame after `Shutdown` is dropped with `(0, nil)`, as is any frame on a nil hub. `Data` is UTF-8 text split on CRLF, CR and LF into `data:` lines, and the hub owns the slice from the call on: marshal fresh per publish. A client whose channel is full is reset as slow and dropped.
- `(*Hub).Position() Position`: the epoch, the oldest offset the ring can replay and the newest offset published, after evicting expired entries, so the floor a gauge reports is the floor a subscription at that instant would be judged against.
- `(*Hub).Snapshot() []ReplayEvent`: a copy of the ring, oldest first, each entry's `Data` cloned; a diagnostic surface. `(*Hub).ClientCount()` counts subscribed clients and `(*Hub).QueuedFrames()` sums the frames waiting in their channels, both gauge sources.
- `(*Hub).SetMaxClients(n)`: replaces the cap at runtime for hot-reloaded configuration; lowering it evicts nobody.
- `(*Hub).Shutdown(ctx) error`: refuses new subscriptions, signals every client to write its `sse:reset {"reason":"shutdown"}` and blocks until the stream goroutines return, bounded by `ctx`. Per client the wait is the remainder of a running `OnConnect` hook plus `max(write timeout, 250ms)`. Call it from `webhttp.Run`'s `WithPreDrain` hook so streams release before the HTTP drain.
- Wire constants: `Wire` (the contract revision every hello declares), `MaxOffset` (`2^53 - 1`, so a JavaScript peer never rounds an offset), `MaxFrameBytes`. `ParseCursor(s) (Cursor, error)` parses a `Last-Event-ID` value (16 lowercase hex characters, a colon, a decimal offset; the empty string is the zero `Cursor` with no error; anything else malformed wraps `ErrCursor`) and `Cursor.String()` renders it back.

### The hello and its verdicts

The first frame of every connection is `event: sse:hello` carrying `Hello{Wire, Epoch, Floor, Head, Resumed, Verdict, KeepaliveMS, KeepaliveEvent}` (`floor` and `head` are decimal strings on the wire). A client branches on `Resumed` only; `Verdict` feeds logs and counters:

- `fresh`: no cursor was presented.
- `resumed`: the ring covers every missed frame within the reply cap, and they follow the hello in order.
- `gap_floor`: the cursor is below the ring's floor, so frames were lost to retention.
- `gap_budget`: the ring covers the gap but the reply cap would truncate it.
- `gap_ahead`: the cursor is beyond the head.
- `epoch_changed`: the cursor belongs to another process lifetime.
- `cursor_invalid`: the cursor did not parse.

Every verdict other than `resumed` yields `Resumed: false` and no replay; the client reconciles from authoritative state instead.

### The digest

`(*Hub).DigestHandler(resolve Resolver, opts ...DigestOption) http.Handler` answers a client returning from sleep: given the versions it holds, pinned to an epoch, which subjects changed or were removed. `Resolver` is `func(ctx, held []Held) ([]State, error)`, answering exactly one `State{Subject, Version, Status}` per requested `(Kind, Ref)` and none the request did not carry (`StatusCurrent`, `StatusGone`, `StatusForbidden`); it runs on the request goroutine, so the application bounds it in time and in flight (`webhttp.RouteTimeout` in the example above). Mount the handler inside the application's authentication and cross-origin middleware; it performs neither check. Options: `WithDigestMaxSubjects(n)` (default 256; a larger request is 400) and `WithDigestMaxBody(n)` (default 512 KiB; a larger body is 413).

| Direction | JSON |
| --- | --- |
| Request (`POST`, `application/json`) | `{"epoch": "<hex16>", "subjects": [{"kind": "chat", "ref": "c1", "version": "7"}]}` |
| Response (200 for every well-formed request) | `{"epoch": "<hex16>", "floor": "0", "head": "42", "checked": 1, "must_refetch": false, "changed": [{"kind": "chat", "ref": "c1", "version": "9"}], "removed": [{"kind": "chat", "ref": "c2", "reason": "gone"}]}`; `reason` is `gone` or `forbidden` |

`must_refetch` is true when the request epoch is absent or not this hub's, when the resolver fails, or when its output does not match the request by key; `changed` then holds nothing and the client refetches everything. Validation refusals are 400 `digest_invalid` naming the first violated rule, 405 with `Allow: POST`, 413 `digest_too_large`, and 415 `digest_content_type`.

### Presence

`WithPresence(fn func(PresenceEvent))` installs one hook that sees each arrival after its hello was flushed and exactly one departure per arrival, on the stream's own goroutine and never under the hub lock; a panic in it is recovered and logged. `PresenceEvent{At, Kind, Topic, Verdict, Cause, Write, Epoch, Tag, ClientID}` carries `Kind` `PresenceConnected` or `PresenceDisconnected` (the `PresenceKind` strings `connected` and `disconnected`); a departure's `Cause` is `closed` (the request context ended), `dead` (a write failed with the socket still open; `Write` names `keepalive`, `frame` or `hook`), `evicted` (reset as slow), `shutdown`, or `hook_failed` (`OnConnect` returned an error). `ClientID` is minted per connection, so two tabs are two clients; `Tag` is the `WithClientTag` value the application folds them by. Dead is observability of socket death, not a liveness bound: the kernel may take minutes to notice.

### Testing against the hub

`github.com/cplieger/sse/ssetest` is the test seam consumers reuse: `Serve(t, hub, opts...)` starts an `httptest` server for a hub and returns its URL, `ReadFrames(r, n)` parses dispatched frames off the wire in one call, `FrameReader` reads successive batches off one live stream through a single buffered reader, `Recorder` is a `ResponseRecorder` that flushes but answers `http.ErrNotSupported` to the deadline setters, and `Fixture` is the controllable server (publish, stall, restart, delay, mutate) the TypeScript suites drive through the `ssetest/cmd` binary.

### The TypeScript client

`@cplieger/sse` is published from [web/](web/README.md) to npm and JSR at the repository's release tag. `createStream` owns the connection over `fetch` and `ReadableStream`: it presents the held cursor, validates the hello, measures liveness on bytes against `max(3 × keepalive_ms, 15s)`, closes a hidden tab's stream and reopens it on visibility, backs off with full jitter, and holds incoming frames while the application's `revalidate` runs; `createVersionMap` and `createDigestClient` drive the digest, and `createWorkerHost` with `attachToWorker` share one connection across a profile's tabs through a `SharedWorker` with a per-tab fallback.

## Contributing

Issues and PRs are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
conventions and how to run the checks locally.

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude](https://claude.com), [GPT](https://openai.com), and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

Apache-2.0. See [LICENSE](LICENSE).
