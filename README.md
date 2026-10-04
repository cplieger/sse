# sse

[![Go Reference](https://pkg.go.dev/badge/github.com/cplieger/sse.svg)](https://pkg.go.dev/github.com/cplieger/sse) [![npm](https://img.shields.io/npm/v/@cplieger/sse)](https://www.npmjs.com/package/@cplieger/sse) [![JSR](https://jsr.io/badges/@cplieger/sse)](https://jsr.io/@cplieger/sse) [![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/sse/badges/mutation.json)](https://github.com/cplieger/sse/issues?q=label%3Agremlins-tracker) [![Mutation (TS)](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/sse/badges/mutation-ts.json)](https://github.com/cplieger/sse/issues?q=label%3Astryker-tracker)

sse keeps browser tabs in step with your Go server's Server-Sent Events through sleep, reconnects and restarts, with a Go hub and a matching TypeScript browser client.

A native `EventSource` cannot tell a silent stream from a dead one, so a tab that slept can silently stop updating. The sse hub and client replace the cursors, heartbeats and resync code you would write around that. The Go module needs Go 1.27.1 or later and one dependency, [webhttp](https://github.com/cplieger/webhttp). The TypeScript package has no runtime dependencies. Both are Apache-2.0.

## Why use it

sse is built for a Go backend that streams live state to browser tabs, where a missed event must never go unnoticed.

- Every published frame carries an `<epoch>:<offset>` cursor. A reconnecting client gets every frame it missed, in order, or none and is told to reconcile.
- `DigestHandler` tells a returning client which items changed or were removed, so it refetches only those, or everything after a server restart or a resolver failure.
- The browser client checks liveness on received bytes and closes a hidden tab's stream until it returns.
- One `SharedWorker` can carry one stream for all of a browser profile's tabs.
- `New` returns an error for unsafe settings, such as a replay buffer with no age limit.

Consider [Centrifugo](https://github.com/centrifugal/centrifugo) if you want a separate real-time server that any backend publishes to. It speaks WebSocket, SSE and gRPC, recovers channel history on reconnect, and scales across nodes with Redis, PostgreSQL or Nats.

## Install

```sh
go get github.com/cplieger/sse@latest
npx jsr add @cplieger/sse  # or: npm i @cplieger/sse
```

## Usage

```go
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/cplieger/sse"
)

func main() {
	hub := sse.MustNew(
		sse.WithReplay(1024),
		sse.WithReplayTTL(10*time.Minute),
		sse.WithReplyMaxEvents(256),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		hub.Serve(w, r, sse.WithTopic(r.URL.Query().Get("room")))
	})

	go func() {
		for t := range time.Tick(time.Second) {
			data := []byte(`{"at":"` + t.Format(time.RFC3339) + `"}`)
			if _, err := hub.Publish(sse.Event{Name: "tick", Data: data}); err != nil {
				log.Print(err)
			}
		}
	}()

	log.Fatal(http.ListenAndServe(":8080", mux))
}
```

This hub keeps the last 1024 frames for up to 10 minutes and sends at most 256 of them to one reconnecting client. `MustNew` panics on a refused setting. `New` takes the same options and returns an error that wraps `ErrConfig` instead. The browser side of the same endpoint is the `createStream` example in [web/README.md](web/README.md).

Three compiled examples in `example_test.go` cover the next steps, and `go test` keeps them true:

- `ExampleHub_Serve` writes initial state from an `OnConnect` hook, resumes a client from its cursor and shuts the hub down.
- `ExampleHub_DigestHandler` mounts `DigestHandler` behind `webhttp.RouteTimeout`, which bounds your resolver.
- `ExampleNew` shows a refused setting.

`Publish` refuses a frame over 1 MiB with `ErrFrameTooLarge` and invalid UTF-8 with `ErrInvalidUTF8`, before any state changes. For a large payload, publish a small frame that tells the client to fetch it. With webhttp, call `Shutdown(ctx)` from the `WithPreDrain` hook of `webhttp.Run`, so streams end before the HTTP server drains.

## API

- `New`, `MustNew` and twelve `With*` options build a hub. `ErrConfig` wraps every refusal.
- `Serve`, with `WithTopic`, `OnConnect` and `WithClientTag`, streams one request. `Writer.Event` writes initial state from the hook.
- `Publish` sends a frame and returns its offset, or `ErrFrameTooLarge` or `ErrInvalidUTF8`.
- `DigestHandler`, a `Resolver` and two `WithDigest*` options answer a client that reconciles.
- `Position`, `Snapshot`, `ClientCount`, `QueuedFrames`, `SetMaxClients` and `Shutdown` inspect and stop the hub.
- `Hello`, `Verdict`, `Cursor`, `ParseCursor`, `Wire`, `MaxOffset` and `MaxFrameBytes` describe the wire. `PresenceEvent` is what the presence hook receives.
- `ssetest` holds `Serve`, `ReadFrames`, `FrameReader`, `Recorder` and `Fixture` for your tests.
- The TypeScript package exports `createStream`, `createVersionMap`, `createDigestClient`, `createWorkerHost` and `attachToWorker`, plus its parser and state machine.

The full reference is on [pkg.go.dev](https://pkg.go.dev/github.com/cplieger/sse) and [JSR](https://jsr.io/@cplieger/sse/doc). [Running the hub](docs/hub.md) covers every option and method.

## A client resumes exactly or reconciles

Every connection opens with an `sse:hello` frame. Its `resumed` field is true only when the hub still holds every frame the client missed and their number is within the `WithReplyMaxEvents` cap. Those frames then follow the hello in order. Otherwise the hub replays nothing, and the client asks your server what changed. The hello's `verdict` says why, for logs and counters.

The hub runs inside one Go process and keeps its replay buffer in memory. Two processes never share a buffer. A client that falls too far behind gets an `sse:reset` frame and is dropped, so `Publish` never waits for it. The epoch is minted when the hub is created. A client that reconnects after a restart, or to another process, presents a cursor from another epoch and reconciles.

To reconcile, the client posts the versions it holds to `DigestHandler`. Your `Resolver` answers each item's current version, and the handler replies with what changed or was removed. Mount it and the `Serve` route behind your own authentication and cross-origin checks, because the hub performs neither.

The TypeScript client treats a stream without a valid hello as a failed connection. A native `EventSource` can read the stream and send `Last-Event-ID`. Your code must then read the `sse:hello` frame and reconcile when `resumed` is false, and it gets none of the client's liveness checks.

[The wire contract](docs/wire.md) lists the hello's fields, every verdict, the digest JSON and its refusals.

## The browser client follows the tab

`createStream` owns the connection over `fetch` and `ReadableStream`, so it can send headers and see the response status. It presents the cursor it holds and treats the stream as dead after `max(3 × keepalive, 15s)` with no bytes, 45 seconds at the default keepalive. It closes a hidden tab's stream after 60 seconds and reopens it when the tab is shown. It reconnects with full-jitter backoff. While your `revalidate` callback runs, it holds incoming frames and then delivers them in order.

The client needs Chrome 98, Firefox 97 or Safari 15.4 or later. `SharedWorker` is optional, and each tab falls back to its own stream where it is missing. The DOM readers are injectable, so the same client runs in Node. [web/README.md](web/README.md) documents it in full.

## Documentation

- [Running the hub](docs/hub.md) covers every option, serving, publishing, presence and the test helpers, for anyone wiring the hub into a server.
- [The wire contract](docs/wire.md) describes the cursor, the stream, the hello and the digest, for anyone writing a client or reading a stream by hand.
- [web/README.md](web/README.md) documents the TypeScript client.

## Credits

- The cursor, the hello's verdict and the rule that only `resumed: true` resumes follow the recovery handshake of [Centrifugo](https://github.com/centrifugal/centrifugo).
- The browser client's parse loop follows [eventsource-parser](https://github.com/rexxars/eventsource-parser).
- The silence watchdog's `max(3 × keepalive, 15s)` follows [Yaffle/EventSource](https://github.com/Yaffle/EventSource).
- The client's version map follows IMAP CONDSTORE, [RFC 7162](https://www.rfc-editor.org/rfc/rfc7162). A new epoch discards every cached version, as a `UIDVALIDITY` change does.
- The Go module's JSON error responses come from [webhttp](https://github.com/cplieger/webhttp), a module by the same author that uses only the standard library.

## Contributing

Issues and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) explains how the Go and TypeScript halves are kept in step.

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude](https://claude.com), [GPT](https://openai.com), and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

Apache-2.0. See [LICENSE](LICENSE). Third-party attributions are in [web/THIRD_PARTY_NOTICES.md](web/THIRD_PARTY_NOTICES.md).
