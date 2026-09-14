import { describe, inject } from "vitest";
import { runtimeCases } from "./test-helpers/runtime-cases.js";

const SKIP_REASON = "ssetest fixture not started: set SSE_FIXTURE to the built ssetest/cmd binary";
const fixture = import.meta.env["SSE_FIXTURE"];
if (!fixture) {
  console.warn(`[vitest] ${SKIP_REASON}`);
}

describe.skipIf(!fixture)("runtime against the ssetest fixture, node fetch", () => {
  runtimeCases(() => ({
    url: inject("sseFixtureUrl"),
    fetch: (input, init) => globalThis.fetch(input, init),
  }));
});
