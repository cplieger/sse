# Contributing to sse

The [shared rules](https://github.com/cplieger/.github/blob/main/CONTRIBUTING.md) for commits, releases, synced files and checks apply here.

## Rules

- A change to a frame, the cursor grammar, the hello, a reset reason or a timing constant lands in Go and TypeScript in one commit. A one-sided change passes its own suite and breaks tabs talking to the other half.
- When such a change is not additive, raise `wire` in `timing.json`, then `Wire` in `frame.go` and `WIRE` in `web/src/timing.ts` to match. Without the bump, tabs still running the old client accept the new frames and misread them.
- Keep `web/go.mod`, the placeholder module `web-ignore`. It stops `go test ./...` at `web/`, where `node_modules` holds Go files, and keeps the TypeScript tree out of the published Go module.

## Checks

CI skips the three `*.integration*` suites in `web/src/`, because it does not build the Go fixture they drive. After any change to what the hub sends or how the client handles it, run them:

```sh
go build -o /tmp/ssetest ./ssetest/cmd
cd web && SSE_FIXTURE=/tmp/ssetest npm test
```

`npx stryker run` from `web/` mutates the working tree in place, because two suites read files outside `web/`. Start it on a clean tree and edit nothing until it ends.

Stryker leaves `web/stryker-setup-*.js` files behind. Delete them before you run the lint checks.

## Releases

The Go module and `@cplieger/sse` on npm and JSR share one version tag. A breaking change in the TypeScript half is a major release of both.

With such a break, add the new `/vN` to the `go.mod` module path and the module's own imports, or the release fails.

Keep TypeScript changes additive until the next Go major.
