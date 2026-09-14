/** The loopback port the globalSetup binds the fixture to; SSE_FIXTURE_PORT overrides it. */
export function fixturePort(): number {
  const raw = process.env["SSE_FIXTURE_PORT"];
  const port = raw === undefined || raw === "" ? 45781 : Number(raw);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new RangeError(`SSE_FIXTURE_PORT must be a port number, got ${String(raw)}`);
  }
  return port;
}

export function fixtureOrigin(): string {
  return `http://127.0.0.1:${String(fixturePort())}`;
}
