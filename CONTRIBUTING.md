# Contributing to sse

`sse` is one wire with two implementations, a Go hub and a TypeScript client, that must agree on every frame, cursor and timing constant, so most of what follows is about keeping them in step.

## Architecture

The repository is one Go module at the root and one TypeScript package under `web/`:

- Root package `sse`: `hub.go` (the ring, the subscriber set, `Publish`, `Shutdown`), `serve.go` (one HTTP request into one subscriber: headers, hello, replay, keepalives, reset), `ring.go`, `frame.go` (the encoder), `cursor.go` and `verdict.go` (the `<epoch>:<offset>` grammar and the hello's verdict), `digest.go` (the wake-time digest handler), `presence.go`, `config.go` (options and their refusals), `doc.go`.
- `ssetest/`: the test seam consumers reuse (`Serve`, `ReadFrames`, `Recorder`) and the `Fixture` the TypeScript suites drive; `ssetest/cmd` is the fixture binary.
- `timing.json`: every timing constant both halves share, written once. `timing_test.go` pins the Go constants and derived defaults to it; `web/src/timing.contract.node.test.ts` pins the TypeScript ones.
- `testdata/framing.golden.json`: the encoder's bytes, read by the TypeScript parser suite as well.
- `web/src/`: the client. `stream.ts` is the runtime (`createStream`), `transport.ts` the `fetch` connection, `parser.ts` the owned WHATWG parser, `reducer.ts` the state machine (its transitions are pinned by `reducer.transitions.golden.json`), `wire.ts` the hello and reset shapes, `versions.ts` and `digest.ts` the version map and digest client, `worker.ts` and `tab.ts` the `SharedWorker` host and the tab attachment with `port-protocol.ts` between them, `online.ts` and `visibility.ts` the platform readers, `timing.ts` the constants, `lifecycle.ts` the record union, `index.ts` the export list. `test-helpers/` holds the fixture client and harnesses; nothing in it is published.
- `web/go.mod`: a sentinel module (`module web-ignore`), not a real one. It stops the root module's `./...` walk at the `web/` boundary so `go test ./...` never descends into `web/node_modules`, which vendors Go files inside npm packages.

### The wire is a contract, not an implementation detail

A few properties are essential. Keep them when you change the code.

- **One lock acquisition per subscription, and `head` is defined once.** `subscribe` computes the verdict, copies the replay slice, builds the `Hello` and registers the client inside one critical section, so the head the hello promises is the head the replay ends at and the live channel starts after. `wg.Add(1)` sits inside that section, after the `closed` check, because `Shutdown` reads `closed` and calls `Wait` under the same lock; an `Add` outside it could race a `Wait` that has already started. Never read the ring or the head a second time on the connect path.
- **A client's `reset` channel is closed at most once, under the lock, by one of exactly two closers.** `Publish` closes it for a slow client and deletes the record in the same critical section; `Shutdown` closes it for every remaining client and sets `closed` in the same critical section, so no later `Publish` reaches a record and no later `subscribe` creates one. A third closer, or either closer outside the lock, is a double close.
- **Every write to a `ResponseWriter` goes through `writeAndFlush`**: set the write deadline, write, flush, clear the deadline. A left-armed deadline fires inside an idle gap (an `OnConnect` hook doing real work, an HTTP/2 stream between frames); a write outside the helper is unbounded, so a peer that stopped reading holds the goroutine until its context dies. The read deadline is cleared once at connect and stays cleared: it is what keeps a bodyless stream alive on a server built with `ReadTimeout`.
- **The frame size is exact and counts the terminating LF, on both sides.** `MaxFrameBytes` is the encoded frame including the blank line that ends it, and the `id:` line's width is part of it, so `Publish` screens at the minimal width before the lock and checks the exact width under it. `testdata/framing.golden.json` pins the encoder's bytes and the TypeScript parser reads the same file, so the two caps cannot drift apart.
- **`Publish` validates before it checks `closed`.** The name panics, the UTF-8 check and the size check run before the frame is accepted, so an invalid frame is refused after `Shutdown` exactly as before it, and a valid one is dropped with `(0, nil)`. Moving the `closed` check first would make the contract depend on timing.
- **The replay TTL floor is `max(3 × keepalive, 15s) + 30s`.** That is the client's watchdog window (`watchdogMs`) plus its backoff cap (`cap_ms`), the longest a healthy client can be away before it reconnects; a shorter TTL would evict frames a resuming client is entitled to. `validate` refuses a TTL below it and a ring without one.
- **Every shared timing constant is written once, in `timing.json`.** Change the JSON and both pinning tests go red until both sides follow; never change a constant in one language alone.
- **The keepalive is a named frame by default, and it carries `data: {}`.** `Serve` writes `event: sse:keepalive` from the stream goroutine with no `id:`, so a beat consumes no offset and leaves the client's cursor on the last real frame; routing it through `Publish` would undo both. The `data:` line is load-bearing (a frame with no `data:` field is dropped before dispatch) and `{}` keeps a JSON-parsing consumer on a valid object. The empty name selects the `: keepalive` comment form, and a name holding CR or LF is refused at `New`. The client classifies keepalives by the name the hello announces, which is why `Publish` panics on an application frame under that name.
- **`retry:` is always written and is never `0`.** Every stream opens with `retry: <ms>` (default 1500) ahead of the hello, because the client's post-EOF backoff floor reads it and a legacy `EventSource` falls back to browser defaults without it. `WithReconnectDelay` refuses anything below one millisecond, so no path leads to `retry: 0`, which would mean reconnect at once.

### Both halves or neither

A change to a frame, the cursor grammar, the hello's fields, a reset reason or a timing constant lands in Go and TypeScript in the same commit, with the affected golden regenerated. A single-sided change passes its own suite and breaks every deployment where the server and the running tabs disagree. The hello's `wire` field is the version a client checks at runtime; bump it when the change is not additive.

## Local development

```sh
# Go half
go build ./... && go vet ./...
go test -count=1 -race ./...
gofmt -l .                     # must print nothing
golangci-lint run ./...        # must report 0 issues; golangci-lint fmt applies gofumpt + gci
```

### TypeScript half

The client lives in `web/` with its own `package.json`, `jsr.json` and the sentinel `go.mod`. Every command runs from `web/`:

```sh
cd web
npm ci
npm run typecheck      # tsc -p tsconfig.json, tsconfig.worker.json, tsconfig.tests.json
npm test               # vitest, both projects
npm run lint:eslint
npm run lint:prettier
```

Test placement is by suffix: a `*.node.test.ts` file runs in the `node` project, and every other `*.test.ts` file runs in the `browser` project, in headless Chromium through Playwright. The browser project is included only when a Chromium binary is installed; without one, `npm test` prints the missing path and the install command and runs the node project alone:

```sh
npx --no-install playwright install chromium
```

The integration suites (`runtime.integration.node.test.ts`, `runtime.integration.test.ts`, `worker.integration.test.ts`) drive the Go fixture binary and skip with a printed reason until `SSE_FIXTURE` names it. Build it from the repository root, then run the suite from `web/`:

```sh
go build -o /tmp/ssetest ./ssetest/cmd
cd web && SSE_FIXTURE=/tmp/ssetest npm test
```

The fixture binds `127.0.0.1:45781` so the browser project can reach it through the dev server's proxy; `SSE_FIXTURE_PORT` moves it.

Two golden files pin the wire and the state machine. Regenerate each behind its gate and review the diff like production code:

```sh
UPDATE_GOLDEN=1 go test . -run TestFramingCorpus_golden              # testdata/framing.golden.json
cd web && UPDATE_GOLDEN=1 npx vitest run src/reducer.node.test.ts    # web/src/reducer.transitions.golden.json
```

The framing golden is read by the TypeScript parser suite as well, so a regenerated file lands with both halves in one commit.

## Conventions and gotchas

- **One runtime dependency in Go, none in TypeScript.** The module requires `github.com/cplieger/webhttp/v3` for the JSON error envelope and `pgregory.net/rapid` for the property tests only; `timing_test.go` fails on any other require. The TypeScript package's one test-only dependency beside the toolchain is `eventsource-parser`, the reference the owned parser is differentially fuzzed against. Neither package ships a test dependency.
- **Go tests use `testing` plus `net/http/httptest`**, with rapid for the property tests in `*_prop_test.go` (the verdict properties and the digest race). Plain `if got != want { t.Errorf(...) }`, table-driven subtests, no assertion library. Parser, validator and encoder surfaces carry fuzz targets in `*_fuzz_test.go`; add one when you introduce a new input-parsing surface.
- **`example_test.go` is the README's usage block.** The `Example` functions there are the code the README example is checked against; change both together.
- **Tests that capture `slog` output by swapping `slog.Default()` run serially** (no `t.Parallel()`); prefer injecting a logger with `WithLogger` where the API allows it. Timing-dependent tests run under `testing/synctest`; the few that need a real socket (a wedged peer, an HTTP/2 idle gap) poll against a deadline and never sleep to wait for a goroutine.
- **The published surface is `web/src/index.ts`.** `package.json`'s `files` and `jsr.json`'s `publish` exclude the same three classes (`*.test.ts`, `*-setup.ts`, `test-helpers/`); a new test-only file goes in one of those shapes so no allowlist needs a new line.

## Publishing model

Releases are automated through `.github/workflows/release.yaml`. One tag publishes the Go module as `github.com/cplieger/sse` and, when `web/` changed, the TypeScript package to npm and JSR as `@cplieger/sse`, at the same version. The `version` in `web/package.json` and `web/jsr.json` is a placeholder the pipeline stamps from the tag at publish time; keep the two manifests equal and do not bump them by hand. Nobody publishes manually.

## Commits and PRs

Branch from `main`, keep changes focused with tests, and open a PR. Commits follow [Conventional Commits](https://www.conventionalcommits.org/) (parsed by git-cliff for release notes), so the type drives the version bump: `feat:`, `fix:`, `sec:`, and the non-releasing `chore:`/`ci:`/`docs:`/`refactor:`/`test:` types. Write the subject as the changelog line a consumer would read. A breaking change to either half is a repository major (`feat!:` or a `BREAKING CHANGE:` footer): the npm package and the Go module share the tag, so a TypeScript break moves the Go module path too. Design the TypeScript surface additively and batch a genuine break with the next Go major.

## Conduct and security

By participating you agree to the [Code of Conduct](https://github.com/cplieger/.github/blob/main/CODE_OF_CONDUCT.md). Report security vulnerabilities through the [security policy](https://github.com/cplieger/.github/blob/main/SECURITY.md), never in a public issue.
