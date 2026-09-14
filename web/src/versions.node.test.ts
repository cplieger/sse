import { describe, expect, it } from "vitest";
import { createDigestClient } from "./digest.js";
import { bindListener, createVersionMap, type Subject } from "./versions.js";

const A = "aaaaaaaaaaaaaaaa";
const B = "bbbbbbbbbbbbbbbb";
const chat: Subject = { kind: "chat", ref: "c_1" };
const tabs: Subject = { kind: "tabs", ref: "" };

describe("version map", () => {
  it("bind with a new epoch empties the map and returns the dropped count", () => {
    const map = createVersionMap();
    expect(map.bind(A)).toBe(0);
    map.observe(chat, "41");
    map.observe(tabs, "12");
    expect(map.bind(B)).toBe(2);
    expect(map.epoch()).toBe(B);
    expect(map.has(chat)).toBe(false);
    expect(map.snapshot()).toEqual({ epoch: B, held: [] });
  });

  it("bind with the current epoch returns 0 and fires onBind", () => {
    const map = createVersionMap();
    const seen: [string, number][] = [];
    const off = bindListener(map, (epoch, dropped) => seen.push([epoch, dropped]));
    map.bind(A);
    map.observe(chat, "41");
    expect(map.bind(A)).toBe(0);
    expect(map.has(chat)).toBe(true);
    expect(seen).toEqual([
      [A, 0],
      [A, 0],
    ]);
    expect(map.bind(B)).toBe(1);
    expect(seen).toEqual([
      [A, 0],
      [A, 0],
      [B, 1],
    ]);
    off();
    map.bind(A);
    expect(seen).toHaveLength(3);
    expect(() => bindListener({ ...map }, () => undefined)).toThrow(TypeError);
  });

  it("observe after bind retags", () => {
    const map = createVersionMap();
    map.bind(A);
    expect(map.observe(chat, "41")).toBe(true);
    expect(map.observe(chat, "41")).toBe(false);
    expect(map.observe(chat, "44")).toBe(true);
    map.bind(B);
    expect(map.observe(chat, "3")).toBe(true);
    expect(map.snapshot()).toEqual({
      epoch: B,
      held: [{ kind: "chat", ref: "c_1", version: "3" }],
    });
    map.forget(chat);
    expect(map.has(chat)).toBe(false);
  });

  it("observe with a foreign epoch on a bound map is ignored and reported stale_stamp", () => {
    const stale: [Subject, string][] = [];
    const map = createVersionMap({ onStale: (subject, epoch) => stale.push([subject, epoch]) });
    map.bind(B);
    map.observe(chat, "3", B);
    expect(map.observe(chat, "41", A)).toBe(false);
    expect(map.snapshot().held).toEqual([{ kind: "chat", ref: "c_1", version: "3" }]);
    expect(stale).toEqual([[chat, A]]);
  });

  it("observe with an epoch on an unbound map binds and records", () => {
    const map = createVersionMap();
    expect(map.epoch()).toBeNull();
    expect(map.observe(chat, "41", A)).toBe(true);
    expect(map.epoch()).toBe(A);
    expect(map.has(chat)).toBe(true);
    expect(map.bind(A)).toBe(0);
    expect(map.has(chat)).toBe(true);
  });

  it("a digest changed entry is never written by the client", async () => {
    const map = createVersionMap();
    map.bind(A);
    map.observe(chat, "41");
    const client = createDigestClient({
      url: "/digest",
      fetch: () =>
        Promise.resolve(
          Response.json({
            epoch: A,
            head: "50",
            checked: 1,
            must_refetch: false,
            changed: [{ kind: "chat", ref: "c_1", version: "44" }],
            removed: [],
          }),
        ),
    });
    const result = await client.check(map.snapshot());
    expect(result.kind).toBe("ok");
    expect(result.kind === "ok" && result.changed[0]?.version).toBe("44");
    expect(map.snapshot().held).toEqual([{ kind: "chat", ref: "c_1", version: "41" }]);
  });

  it("snapshot().epoch is what the digest request carries", async () => {
    const map = createVersionMap();
    map.bind(B);
    map.observe(tabs, "12");
    const bodies: unknown[] = [];
    const client = createDigestClient({
      url: "/digest",
      fetch: (_url, init) => {
        bodies.push(JSON.parse(String(init?.body)));
        return Promise.resolve(
          Response.json({ epoch: B, head: "7", checked: 1, must_refetch: false }),
        );
      },
    });
    await client.check(map.snapshot());
    expect(bodies).toEqual([{ epoch: B, subjects: [{ kind: "tabs", ref: "", version: "12" }] }]);
    expect(map.snapshot().epoch).toBe(B);
  });
});
