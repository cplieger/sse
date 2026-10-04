# The wire contract

This page describes the bytes the Go hub writes and the JSON its digest endpoint accepts, for anyone writing a client or reading a stream by hand.

## The cursor

Every frame `Publish` sends carries `id: <epoch>:<offset>`. The epoch is 16 random lowercase hex characters, minted when the hub is created. The offset grows by one with each published frame. A client presents the last id it received as `Last-Event-ID` when it reconnects.

`ParseCursor(s)` parses that value and `Cursor.String()` renders it back. The empty string is the zero `Cursor` with no error. Any other value must be the epoch, a colon and a decimal offset with no leading zero, at most `MaxOffset`, or the error wraps `ErrCursor`.

`MaxOffset` is `2^53 - 1`, so a JavaScript peer that holds an offset as a `Number` never rounds it.

The epoch changes whenever a hub is created. A client that reconnects after a restart, or to another process, presents a cursor from another epoch and is told to reconcile.

## The stream

`Serve` answers with `Content-Type: text/event-stream`, `Cache-Control: no-cache, no-transform` and `X-Accel-Buffering: no`. `no-transform` asks a proxy not to compress or rewrite the stream, and `X-Accel-Buffering: no` turns off nginx response buffering. The stream then carries, in order:

1. `retry: <ms>`, the wait before a client reconnects. It is always written and is never 0. The default is 1500.
2. The hello, `event: sse:hello`.
3. The missed frames, when the hello says `resumed`.
4. Any frames the `OnConnect` hook writes. They carry no id.
5. Live frames from `Publish`.

The hub decides the hello, the replay and the start of live delivery under one lock. So the head the hello reports is the offset where the replay ends and live frames begin.

A keepalive is the named frame `event: sse:keepalive` with `data: {}`, every 15 seconds by default. It carries no id, so it moves no cursor. With `WithKeepaliveEvent("")` the hub writes the comment `: keepalive` instead, which an `EventSource` never dispatches.

The hub ends a stream with `event: sse:reset`. Its data is `{"reason":"slow"}` for a client whose buffer filled, written under a 250ms deadline, or `{"reason":"shutdown"}` when the hub shuts down.

Event names that start with `sse:` are reserved for these frames. `Wire` is the revision of this contract, and every hello declares it.

## The hello and its verdicts

The hello's data is a JSON object with eight fields:

| Field | Meaning |
| --- | --- |
| `wire` | the contract revision, `Wire` |
| `epoch` | the hub's epoch |
| `floor` | the oldest offset the hub can replay, as a decimal string, `"0"` when it holds nothing |
| `head` | the newest offset published, as a decimal string |
| `resumed` | true when the missed frames follow the hello |
| `verdict` | why, for logs and counters |
| `keepalive_ms` | the keepalive interval in milliseconds |
| `keepalive_event` | the keepalive frame's name, empty for the comment form |

A client branches on `resumed` only. The verdict is one of seven values:

- `fresh`: the client presented no cursor.
- `resumed`: the hub holds every missed frame within the reply cap, and they follow the hello in order.
- `gap_floor`: the cursor is below the floor, so frames were lost to retention.
- `gap_budget`: the hub holds the gap, but the reply cap would cut the replay short.
- `gap_ahead`: the cursor is beyond the head.
- `epoch_changed`: the cursor belongs to another hub, such as the process before a restart.
- `cursor_invalid`: the cursor did not parse.

Every verdict other than `resumed` comes with `resumed: false` and no replay, and the client reconciles from the application's state. The hub checks the three gaps in the order ahead, floor, budget, so the verdict names the first reason a resume failed.

## The digest

`(*Hub).DigestHandler(resolve Resolver, opts ...DigestOption)` returns an `http.Handler` that answers a client back from sleep. The client sends the versions it holds, pinned to an epoch. The handler answers which items changed or were removed.

`Resolver` is `func(ctx context.Context, held []Held) ([]State, error)`. It answers exactly one `State{Subject, Version, Status}` per requested `(Kind, Ref)` and none the request did not carry. `Status` is `StatusCurrent`, `StatusGone` or `StatusForbidden`.

The resolver runs on the request goroutine, and the library puts no bound on it. Bound it in time and in flight yourself. `ExampleHub_DigestHandler` in `example_test.go` wraps the handler in `webhttp.RouteTimeout` for that.

Mount the handler inside your authentication and cross-origin middleware, because it performs neither check. The resolver reads the caller from `ctx`, however your middleware stored it. When several users share a server, the resolver decides whether a caller may learn that an item exists. When it may not, the resolver answers `gone` for both cases.

`WithDigestMaxSubjects(n)` caps the items in one request at 256 by default, and a larger request is 400. `WithDigestMaxBody(n)` caps the body at 512 KiB by default, and a larger body is 413.

| Direction | JSON |
| --- | --- |
| Request (`POST`, `application/json`) | `{"epoch": "<hex16>", "subjects": [{"kind": "chat", "ref": "c1", "version": "7"}]}` |
| Response (200 for every well-formed request) | `{"epoch": "<hex16>", "floor": "0", "head": "42", "checked": 1, "must_refetch": false, "changed": [{"kind": "chat", "ref": "c1", "version": "9"}], "removed": [{"kind": "chat", "ref": "c2", "reason": "gone"}]}` |

A removed item's `reason` is `gone` or `forbidden`. The handler reads `floor` and `head` after the resolver returns, so `head` is at or after every version it compared.

For a non-empty `subjects` list, `must_refetch` is true when the request epoch is absent or not this hub's, when the resolver fails, or when its answer does not match the request by key. `changed` is then empty and the client refetches everything. A request with an empty `subjects` list gets `checked: 0` and `must_refetch: false`.

## Digest refusals

The handler refuses a request with a JSON error body:

- 405 `digest_method`, with `Allow: POST`, for any method other than `POST`.
- 415 `digest_content_type` when `Content-Type` is not `application/json`.
- 413 `digest_too_large` for a body over the cap.
- 400 `digest_invalid` for malformed JSON or for the first broken rule, which the message names.

The rules, checked in field order:

- `epoch`, when present, is 16 lowercase hex characters.
- `subjects` is present and holds no more items than the cap.
- Each `kind` is 1 to 32 bytes and matches `[a-z][a-z0-9_]*`.
- Each `ref` is at most 512 bytes, with no control, line separator or bidi characters.
- Each `version` is 1 to 64 bytes of printable ASCII.
- No `(kind, ref)` pair appears twice.
