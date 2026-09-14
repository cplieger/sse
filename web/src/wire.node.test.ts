import { describe, expect, it } from "vitest";
import { MAX_OFFSET_STRING } from "./timing.js";
import {
  compareOffset,
  cursorToString,
  parseCursor,
  parseResetReason,
  validateHello,
} from "./wire.js";

const EPOCH = "3f9a1c0e7b2d4a58";

function hello(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    wire: 1,
    epoch: EPOCH,
    floor: "4010",
    head: "4132",
    resumed: true,
    verdict: "resumed",
    keepalive_ms: 15000,
    keepalive_event: "sse:keepalive",
    ...overrides,
  };
}

describe("parseCursor", () => {
  it("parseCursor accepts <16hex>:<decimal> and offset 0", () => {
    expect(parseCursor(`${EPOCH}:4132`)).toEqual({ epoch: EPOCH, offset: "4132" });
    expect(parseCursor(`${EPOCH}:0`)).toEqual({ epoch: EPOCH, offset: "0" });
    expect(parseCursor(`${EPOCH}:${MAX_OFFSET_STRING}`)).toEqual({
      epoch: EPOCH,
      offset: MAX_OFFSET_STRING,
    });
    expect(cursorToString({ epoch: EPOCH, offset: "7" })).toBe(`${EPOCH}:7`);
  });

  it("parseCursor refuses bare integer, uppercase hex, leading zero, 34 bytes, offset above MAX_OFFSET", () => {
    expect(parseCursor("")).toBeNull();
    expect(parseCursor("4132")).toBeNull();
    expect(parseCursor(`${EPOCH.toUpperCase()}:1`)).toBeNull();
    expect(parseCursor(`${EPOCH}:01`)).toBeNull();
    expect(parseCursor(`${EPOCH}:`)).toBeNull();
    expect(parseCursor(`${EPOCH}:-1`)).toBeNull();
    expect(parseCursor(`${EPOCH}0:1`)).toBeNull();
    expect(parseCursor(`${EPOCH}:12345678901234567`)).toBeNull();
    expect(parseCursor(`${EPOCH}:9007199254740992`)).toBeNull();
    expect(parseCursor(`${EPOCH}:1:2`)).toBeNull();
  });
});

describe("compareOffset", () => {
  it("compareOffset orders above 2^53 exactly", () => {
    expect(compareOffset("9007199254740992", "9007199254740993")).toBe(-1);
    expect(compareOffset("9007199254740993", "9007199254740992")).toBe(1);
    expect(compareOffset("9007199254740993", "9007199254740993")).toBe(0);
    expect(compareOffset("0", "1")).toBe(-1);
    expect(compareOffset("10", "9")).toBe(1);
  });
});

describe("validateHello", () => {
  it("validateHello refuses absent or non-integer wire", () => {
    const { wire: _omitted, ...withoutWire } = hello();
    expect(validateHello(withoutWire, 1, 1)).toEqual({ ok: false, reason: { kind: "bad_hello" } });
    expect(validateHello(hello({ wire: 1.5 }), 1, 1)).toEqual({
      ok: false,
      reason: { kind: "bad_hello" },
    });
    expect(validateHello(hello({ wire: "1" }), 1, 1)).toEqual({
      ok: false,
      reason: { kind: "bad_hello" },
    });
    expect(validateHello(null, 1, 1)).toEqual({ ok: false, reason: { kind: "bad_hello" } });
    expect(validateHello("hello", 1, 1)).toEqual({ ok: false, reason: { kind: "bad_hello" } });
  });

  it("validateHello refuses wire below minWire and above maxWire with the offending value", () => {
    expect(validateHello(hello({ wire: 0 }), 1, 1)).toEqual({
      ok: false,
      reason: { kind: "wire_unsupported", wire: 0 },
    });
    expect(validateHello(hello({ wire: 2 }), 1, 1)).toEqual({
      ok: false,
      reason: { kind: "wire_unsupported", wire: 2 },
    });
    expect(validateHello(hello({ wire: 2 }), 1, 2).ok).toBe(true);
    expect(validateHello(hello({ wire: 1 }), 1, 1).ok).toBe(true);
  });

  it("validateHello refuses epoch, floor, head, keepalive_ms, keepalive_event, resumed shape violations one at a time", () => {
    const violations: Record<string, unknown>[] = [
      { epoch: EPOCH.toUpperCase() },
      { epoch: EPOCH.slice(1) },
      { epoch: 42 },
      { floor: "01" },
      { floor: 4010 },
      { floor: "9007199254740992" },
      { head: "" },
      { head: "-1" },
      { keepalive_ms: 0 },
      { keepalive_ms: -1 },
      { keepalive_ms: 1.5 },
      { keepalive_ms: "15000" },
      { keepalive_event: 7 },
      { keepalive_event: null },
      { resumed: "true" },
      { resumed: 1 },
      { resumed: undefined },
    ];
    for (const violation of violations) {
      expect(validateHello(hello(violation), 1, 1), JSON.stringify(violation)).toEqual({
        ok: false,
        reason: { kind: "bad_hello" },
      });
    }
    const ok = validateHello(hello(), 1, 1);
    expect(ok).toEqual({
      ok: true,
      hello: {
        wire: 1,
        epoch: EPOCH,
        floor: "4010",
        head: "4132",
        resumed: true,
        verdict: "resumed",
        keepalive_ms: 15000,
        keepalive_event: "sse:keepalive",
      },
    });
    expect(validateHello(hello({ keepalive_event: "" }), 1, 1).ok).toBe(true);
    expect(validateHello(hello({ floor: "0", head: "0" }), 1, 1).ok).toBe(true);
  });

  it("validateHello reads resumed strictly as true", () => {
    const resumed = validateHello(hello({ resumed: true }), 1, 1);
    const fresh = validateHello(hello({ resumed: false, verdict: "fresh" }), 1, 1);
    expect(resumed.ok && resumed.hello.resumed).toBe(true);
    expect(fresh.ok && fresh.hello.resumed).toBe(false);
    expect(fresh.ok && fresh.hello.verdict).toBe("fresh");
    expect(validateHello(hello({ resumed: "true" }), 1, 1).ok).toBe(false);
  });
});

describe("parseResetReason", () => {
  it("parseResetReason maps slow and shutdown and refuses others", () => {
    expect(parseResetReason('{"reason":"slow"}')).toBe("reset:slow");
    expect(parseResetReason('{"reason":"shutdown"}')).toBe("reset:shutdown");
    expect(parseResetReason('{"reason":"other"}')).toBeNull();
    expect(parseResetReason('{"reason":1}')).toBeNull();
    expect(parseResetReason("{}")).toBeNull();
    expect(parseResetReason("[]")).toBeNull();
    expect(parseResetReason("slow")).toBeNull();
    expect(parseResetReason("")).toBeNull();
  });
});
