import { type ChildProcess, spawn } from "node:child_process";
import type { TestProject } from "vitest/node";
import { fixtureOrigin, fixturePort } from "./fixture-port.js";

declare module "vitest" {
  export interface ProvidedContext {
    sseFixtureUrl: string;
  }
}

const LISTEN_TIMEOUT_MS = 10_000;

function waitForListen(child: ChildProcess): Promise<string> {
  return new Promise((resolve, reject) => {
    let buffer = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        child.kill("SIGTERM");
        reject(
          new Error(
            `ssetest fixture printed no LISTEN line within ${String(LISTEN_TIMEOUT_MS)} ms`,
          ),
        );
      }
    }, LISTEN_TIMEOUT_MS);
    child.stdout?.on("data", (chunk: Buffer) => {
      buffer += chunk.toString();
      const match = /^LISTEN (\S+)$/m.exec(buffer);
      if (match?.[1] !== undefined && !settled) {
        settled = true;
        clearTimeout(timer);
        resolve(match[1]);
      }
    });
    child.on("exit", (code) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(new Error(`ssetest fixture exited with ${String(code)} before printing LISTEN`));
      }
    });
    child.on("error", (error) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(error);
      }
    });
  });
}

/**
 * Spawns the Go fixture named by SSE_FIXTURE and hands its URL to the tests; a no-op when
 * unset. The port is fixed (fixturePort) because the browser projects reach the fixture through
 * a dev-server proxy whose target is decided when the config loads.
 */
export default async function setup(project: TestProject): Promise<(() => void) | undefined> {
  const binary = process.env["SSE_FIXTURE"];
  if (binary === undefined || binary === "") {
    return undefined;
  }
  const child = spawn(binary, ["-addr", `127.0.0.1:${String(fixturePort())}`], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  const url = await waitForListen(child);
  if (url !== fixtureOrigin()) {
    child.kill("SIGTERM");
    throw new Error(`ssetest fixture listens on ${url}, expected ${fixtureOrigin()}`);
  }
  project.provide("sseFixtureUrl", url);
  return () => {
    child.kill("SIGTERM");
  };
}
