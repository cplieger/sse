import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

interface Manifest {
  readonly version: string;
  readonly exports: Record<string, string>;
  readonly dependencies?: Record<string, string>;
}

const root = join(import.meta.dirname, "..");
const readManifest = (name: string): Manifest =>
  JSON.parse(readFileSync(join(root, name), "utf8")) as Manifest;

describe("package manifests", () => {
  it("declares no runtime dependencies", () => {
    expect(readManifest("package.json").dependencies).toBeUndefined();
  });

  it("exports only .", () => {
    expect(Object.keys(readManifest("package.json").exports)).toEqual(["."]);
    expect(Object.keys(readManifest("jsr.json").exports)).toEqual(["."]);
  });

  it("package.json and jsr.json agree on the placeholder version", () => {
    expect(readManifest("package.json").version).toBe(readManifest("jsr.json").version);
  });
});
