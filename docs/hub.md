# Running the hub

This page covers every option and method of the Go hub, for anyone wiring it into a server. [The wire contract](wire.md) describes the bytes it writes.

## Building a hub

`New(opts ...Option)` returns a `*Hub`, or an error that wraps `ErrConfig` and names the option the hub could not honour. `MustNew` panics with the same message, for construction at package level in `main`. A nil option is skipped.

Each option refuses its own value at once. The rules that compare two options run once after every option is applied, so option order never changes the result. The defaults that depend on another option are derived at the same point. Those rules refuse four combinations:

- a replay buffer without a TTL
- a TTL below the client's watchdog window plus its backoff cap
- a reply cap above the replay size
- a write timeout at or below the keepalive

| Option | Default | Refused |
| --- | --- | --- |
| `WithReplay(n)` | `0`, no replay | negative, or non-zero without `WithReplayTTL` |
| `WithReplayTTL(d)` | none | at or below zero, or below `max(3 × keepalive, 15s) + 30s`, which is 75s at the default keepalive |
| `WithReplayMaxBytes(n)` | `64 × MaxFrameBytes` | below `MaxFrameBytes` |
| `WithReplyMaxEvents(n)` | `min(replay size, 256)` | negative, or above the replay size |
| `WithClientBuffer(n)` | `max(replay size, 256)` | at or below zero |
| `WithMaxClients(n)` | `0`, unlimited | never, and zero or negative means unlimited |
| `WithKeepalive(d)` | `15s` | below 1ms |
| `WithKeepaliveEvent(name)` | `sse:keepalive` | CR or LF in the name, and `""` selects the `: keepalive` comment form |
| `WithReconnectDelay(d)` | `1500ms` | below 1ms |
| `WithWriteTimeout(d)` | `2 × keepalive` | at or below the keepalive |
| `WithLogger(l)` | `slog.Default()` | never, and nil keeps the default |
| `WithPresence(fn)` | none | never, and nil removes the hook |

The replay size is the `WithReplay` capacity. A client that falls `WithClientBuffer` frames behind is reset as slow.

## The replay buffer

The hub keeps recent frames in memory, in one buffer bounded three ways at once. `WithReplay` bounds the count, `WithReplayTTL` the age and `WithReplayMaxBytes` the encoded bytes. The oldest entries are evicted first.

The TTL floor is the client's watchdog window plus its 30-second backoff cap. That is the longest a healthy client can be away before it reconnects. A shorter TTL would evict frames a resuming client is entitled to.

`WithReplyMaxEvents` caps how many frames one reconnecting client may be sent, so a long replay cannot hold up a reconnect. A resume that needs more gets the `gap_budget` verdict and no replay.

## Serving a stream

`(*Hub).Serve(w, r, opts ...ServeOption)` subscribes one request and streams until the peer leaves, the request context ends, the client is reset as slow, a write fails, or the hub shuts down. It writes the response headers, the `retry:` field, the hello, the replay, the keepalives and the `sse:reset` frame.

When the `ResponseWriter` supports write deadlines, every write runs under `WithWriteTimeout`, and the deadline is cleared after it. A peer that stopped reading then ends its own connection, and the stream goroutine is not held. When it does not support them, `Serve` logs one warning and its streams run with no write bound. `Serve` also clears the read deadline when the stream opens, so a stream stays open on a server that sets `ReadTimeout`.

`Serve` finds an `http.Flusher` through any `Unwrap()` chain of the `ResponseWriter`. It answers 500 `streaming_unsupported` only when no flusher exists at any depth. It answers 503 `sse_unavailable` to a connection over the client cap or after `Shutdown`.

`Serve` takes three options:

- `WithTopic(t)` receives broadcasts plus the events published to topic `t`. The empty topic, the default, receives everything.
- `OnConnect(fn func(w *Writer, h Hello) error)` runs after the `retry:` field, the hello and the replay have been flushed, so the client's connect deadline never measures the hook. It receives the `Hello` the client received. It writes initial-state frames with `Writer.Event(name, data)`, each its own bounded write and flush, and none carries an id. An error from the hook ends the connection.
- `WithClientTag(tag)` sets the key your application groups clients by, carried on `PresenceEvent.Tag`. The empty string means no tag, so `r.Header.Get("SSE-Client")` can be passed through unconditionally. Any other value outside `[A-Za-z0-9_-]{1,64}` is treated as absent, with one Warn log line.

## Publishing

`(*Hub).Publish(Event) (uint64, error)` assigns the next offset, appends the frame to the replay buffer and sends it to every matching client without blocking. A client whose buffer is full is reset as slow and dropped.

An `Event` with the empty `Topic` reaches every client. An empty `Name` writes no `event:` line, so a browser dispatches the frame as `message`.

Checks run in a fixed order before a frame is accepted:

1. A name that starts with `sse:` or holds CR or LF panics. A non-empty name that equals the configured keepalive name also panics. Each is a fixed property of the call site.
2. `Data` that is not valid UTF-8 returns `ErrInvalidUTF8`.
3. An encoded frame above `MaxFrameBytes` returns `ErrFrameTooLarge`. The limit is 1 MiB and includes the blank line that ends the frame.

A refused frame consumes no offset and leaves the replay buffer untouched, before and after `Shutdown` alike. A valid frame after `Shutdown` is dropped with `(0, nil)`, and so is any frame on a nil hub.

`Data` is UTF-8 text, split on CRLF, CR and LF into `data:` lines. The hub owns the slice from the call on, so marshal a fresh one for each publish. When a payload is too large, publish a small frame that tells the client to fetch it instead.

## Inspecting and stopping the hub

- `Position()` returns the epoch, the oldest offset the hub can replay and the newest offset published. It evicts expired entries first, so the floor a gauge reports is the floor a new subscription would be judged against.
- `Snapshot()` returns a copy of the replay buffer, oldest first, with each entry's `Data` cloned. It is meant for diagnostics.
- `ClientCount()` counts subscribed clients and `QueuedFrames()` sums the frames waiting in their channels. Both are gauge sources.
- `SetMaxClients(n)` replaces the client cap at runtime, for configuration you reload. Lowering it evicts nobody.
- `Shutdown(ctx)` refuses new subscriptions, tells every client to write `sse:reset {"reason":"shutdown"}` and waits until the stream goroutines return, bounded by `ctx`. It returns `ctx.Err()` when `ctx` expires first.

Per client, the `Shutdown` wait is the rest of a running `OnConnect` hook plus `max(write timeout, 250ms)`. With [webhttp](https://github.com/cplieger/webhttp), call `Shutdown` from the `WithPreDrain` hook of `webhttp.Run`, so streams end before the HTTP server drains.

## Presence

`WithPresence(fn func(PresenceEvent))` installs one hook. It sees each arrival after its hello was flushed, and exactly one departure per arrival. It runs on the stream's own goroutine and never under the hub lock, so a slow hook delays only that stream. A panic in it is recovered and logged.

A `PresenceEvent` carries `At`, `Kind`, `Topic`, `Verdict`, `Cause`, `Write`, `Epoch`, `Tag` and `ClientID`. `Kind` is `PresenceConnected` or `PresenceDisconnected`, the strings `connected` and `disconnected`. An arrival carries the hello's `Verdict`. A departure's `Cause` is one of five values:

- `closed`: the request context ended.
- `dead`: a write failed with the socket still open. `Write` names `keepalive`, `frame` or `hook`.
- `evicted`: the client was reset as slow.
- `shutdown`: the hub shut down.
- `hook_failed`: `OnConnect` returned an error.

On an `evicted` or `shutdown` departure whose reset frame could not be written, `Write` is `reset`.

`ClientID` is minted per connection, so two tabs are two clients. `Tag` is the `WithClientTag` value your application groups them by.

`dead` reports that a socket died. It is not a liveness bound, because the kernel may take minutes to notice a dead peer. For liveness, have the client acknowledge keepalives, which the `alive` option of `createStream` does.

## Testing against the hub

`github.com/cplieger/sse/ssetest` holds test helpers for code that uses the hub:

- `Serve(t, hub, opts...)` starts an `httptest` server for a hub and returns its URL.
- `ReadFrames(r, n)` parses dispatched frames off the wire in one call.
- `NewFrameReader(r)` returns a `FrameReader` whose `Read(n)` reads successive batches off one live stream through a single buffered reader. Use it when you read one stream more than once, because each `ReadFrames` call discards the bytes it read past frame `n`.
- `NewRecorder()` returns a `Recorder`, a `ResponseRecorder` that flushes but answers `http.ErrNotSupported` to the deadline setters.
- `Fixture` is a controllable server that publishes, stalls, restarts, delays and mutates on request. The TypeScript integration tests drive it through the `ssetest/cmd` binary.
