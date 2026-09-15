// Vitest config for Stryker mutation runs ONLY (stryker.config.json points
// here; plain `npx vitest --run` keeps using vitest.config.ts).
//
// Why a separate config: Stryker instruments every statement with a coverage
// counter, which pushes the suite's byte-moving tests past the 5s testTimeout
// the normal config sets. `held bytes reaching 64 MiB end the connection with
// hold_overflow before the count` runs in 234ms uninstrumented and times out
// under instrumentation, failing the initial test run before any mutant is
// tested. Raising the cap here keeps the overflow path mutated rather than
// excluding the test that covers it.
import { defineConfig, mergeConfig } from "vitest/config";
import base from "./vitest.config.js";

export default mergeConfig(
  base,
  defineConfig({
    test: { testTimeout: 30_000 },
  }),
);
