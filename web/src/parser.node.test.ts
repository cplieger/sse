import { describe, expect, it } from "vitest";
import { createParser, type ParsedEvent } from "./parser.js";
import { MAX_FRAME_BYTES } from "./timing.js";

const encoder = new TextEncoder();

interface Run {
  readonly events: ParsedEvent[];
  readonly retries: number[];
  readonly comments: number;
  readonly overflows: number[];
  readonly feed: (text: string) => void;
  readonly feedBytes: (bytes: Uint8Array) => void;
  readonly reset: () => void;
}

function run(maxBufferBytes = MAX_FRAME_BYTES): Run {
  const events: ParsedEvent[] = [];
  const retries: number[] = [];
  const overflows: number[] = [];
  let comments = 0;
  const parser = createParser(
    {
      onEvent: (ev) => events.push(ev),
      onRetry: (ms) => retries.push(ms),
      onComment: () => {
        comments++;
      },
      onOverflow: (bytes) => overflows.push(bytes),
    },
    { maxBufferBytes },
  );
  return {
    events,
    retries,
    get comments() {
      return comments;
    },
    overflows,
    feed: (text) => parser.feed(encoder.encode(text)),
    feedBytes: (bytes) => parser.feed(bytes),
    reset: () => parser.reset(),
  };
}

function shape(ev: ParsedEvent): [string, string, string | null, string] {
  return [ev.type, ev.data, ev.id, ev.lastEventId];
}

describe("parser checklist", () => {
  it("item 01: strips exactly one BOM, at stream start only", () => {
    const one = run();
    one.feedBytes(new Uint8Array([0xef, 0xbb, 0xbf]));
    one.feed("data: a\n\n\uFEFFdata: b\n\ndata: c\n\n");
    expect(one.events.map((e) => e.data)).toEqual(["a", "c"]);
    const two = run();
    two.feedBytes(new Uint8Array([0xef, 0xbb, 0xbf, 0xef, 0xbb, 0xbf]));
    two.feed("data: a\n\ndata: b\n\n");
    expect(two.events.map((e) => e.data)).toEqual(["b"]);
  });

  it("item 02: accepts LF, CRLF and CR as line terminators", () => {
    const r = run();
    r.feed("data: lf\n\ndata: crlf\r\n\r\ndata: cr\r\r");
    expect(r.events.map((e) => e.data)).toEqual(["lf", "crlf", "cr"]);
  });

  it("item 03: a CRLF split across two reads is one terminator", () => {
    const r = run();
    r.feed("data: one\r");
    r.feed("\ndata: two\r\n\r\n");
    expect(r.events.map((e) => e.data)).toEqual(["one\ntwo"]);
    r.feed("data: x\r\n\r");
    expect(r.events).toHaveLength(2);
    r.feed("\ndata: after\n\n");
    expect(r.events.map((e) => e.data)).toEqual(["one\ntwo", "x", "after"]);
  });

  it("item 04: multiple data lines join with LF", () => {
    const r = run();
    r.feed("data: a\ndata: b\ndata: c\n\n");
    expect(r.events.map((e) => e.data)).toEqual(["a\nb\nc"]);
  });

  it("item 05: the trailing LF of joined data is removed", () => {
    const r = run();
    r.feed("data: a\ndata:\n\ndata:\n\n");
    expect(r.events.map((e) => e.data)).toEqual(["a\n", ""]);
  });

  it("item 06: exactly one space after the colon is stripped", () => {
    const r = run();
    r.feed("data:  two\n\ndata:none\n\nevent:  x\ndata: y\n\n");
    expect(r.events.map((e) => e.data)).toEqual([" two", "none", "y"]);
    expect(r.events[2]?.type).toBe(" x");
  });

  it("item 07: comment lines reach onComment and dispatch nothing", () => {
    const r = run();
    r.feed(": keepalive\n\n: another\ndata: x\n\n");
    expect(r.comments).toBe(2);
    expect(r.events.map((e) => e.data)).toEqual(["x"]);
  });

  it("item 08: a line with no colon is a field name with empty value", () => {
    const r = run();
    r.feed("event: named\ndata\nevent\n\n");
    expect(r.events.map(shape)).toEqual([["message", "", null, ""]]);
  });

  it("item 09: retry accepts ASCII digits only", () => {
    const r = run();
    r.feed("retry: 1500\nretry: 1500ms\nretry: 1e4\nretry: ١٢\nretry:\nretry: 007\n\n");
    expect(r.retries).toEqual([1500, 7]);
    expect(r.events).toHaveLength(0);
  });

  it("item 10: an id containing U+0000 is ignored", () => {
    const r = run();
    r.feed("id: good\ndata: a\n\nid: bad\0id\ndata: b\n\n");
    expect(r.events.map(shape)).toEqual([
      ["message", "a", "good", "good"],
      ["message", "b", null, "good"],
    ]);
  });

  it("item 11: a frame without id leaves lastEventId unchanged", () => {
    const r = run();
    r.feed("id: 7\ndata: a\n\ndata: b\n\nevent: sse:keepalive\ndata: {}\n\n");
    expect(r.events.map((e) => e.lastEventId)).toEqual(["7", "7", "7"]);
  });

  it("item 12: an id-only frame advances lastEventId and dispatches nothing", () => {
    const r = run();
    r.feed("id: 9\n\ndata: a\n\n");
    expect(r.events.map(shape)).toEqual([["message", "a", null, "9"]]);
  });

  it("item 13: an empty event field resets the type to message", () => {
    const r = run();
    r.feed("event: notify\nevent:\ndata: a\n\nevent: notify\ndata: b\n\ndata: c\n\n");
    expect(r.events.map((e) => e.type)).toEqual(["message", "notify", "message"]);
  });

  it("item 14: field names match literally", () => {
    const r = run();
    r.feed("Data: a\nDATA: b\nevent : c\ndata: d\n\n");
    expect(r.events.map(shape)).toEqual([["message", "d", null, ""]]);
  });

  it("item 15: EOF mid-event discards the partial event", () => {
    const r = run();
    r.feed("data: complete\n\ndata: partial\nid: 3\n");
    r.reset();
    r.feed("data: next\n\n");
    expect(r.events.map(shape)).toEqual([
      ["message", "complete", null, ""],
      ["message", "next", null, ""],
    ]);
  });

  it("item 16: UTF-8 split across chunks decodes correctly", () => {
    const r = run();
    const bytes = encoder.encode("data: héllo €\n\n");
    const euro = bytes.indexOf(0xe2);
    r.feedBytes(bytes.subarray(0, 7));
    r.feedBytes(bytes.subarray(7, euro + 1));
    r.feedBytes(bytes.subarray(euro + 1, euro + 2));
    r.feedBytes(bytes.subarray(euro + 2));
    expect(r.events.map((e) => e.data)).toEqual(["héllo €"]);
    expect(r.events[0]?.bytes).toBe(bytes.length);
  });

  it("item 17: a frame above maxBufferBytes calls onOverflow with the encoded byte count", () => {
    const cap = 64;
    const r = run(cap);
    const payload = "x".repeat(cap - "data: ".length - 2);
    r.feed(`data: ${payload}\n\n`);
    expect(r.events).toHaveLength(1);
    expect(r.events[0]?.bytes).toBe(cap);
    r.feed(`data: ${payload}x\n\n`);
    expect(r.overflows).toEqual([cap + 1]);
    expect(r.events).toHaveLength(1);
    r.feed("data: ignored\n\n");
    expect(r.events).toHaveLength(1);
    expect(r.overflows).toEqual([cap + 1]);
    r.reset();
    r.feed("data: ok\n\n");
    expect(r.events.map((e) => e.data)).toEqual([payload, "ok"]);
  });

  it("item 18: a near-cap frame fed one byte at a time parses in linear time", () => {
    const r = run();
    const total = MAX_FRAME_BYTES - 1;
    const frame = new Uint8Array(total);
    frame.set(encoder.encode("data: "), 0);
    frame.fill(0x61, 6, total - 2);
    frame[total - 2] = 0x0a;
    frame[total - 1] = 0x0a;
    const started = performance.now();
    for (let i = 0; i < total; i++) {
      r.feedBytes(frame.subarray(i, i + 1));
    }
    const elapsed = performance.now() - started;
    expect(r.events).toHaveLength(1);
    expect(r.events[0]?.bytes).toBe(total);
    expect(r.events[0]?.data.length).toBe(total - 8);
    expect(elapsed).toBeLessThan(5000);
  }, 10_000);

  it("item 19: id is per frame and lastEventId is sticky", () => {
    const r = run();
    r.feed("id: a\ndata: 1\n\ndata: 2\n\nid: b\ndata: 3\n\n");
    expect(r.events.map(shape)).toEqual([
      ["message", "1", "a", "a"],
      ["message", "2", null, "a"],
      ["message", "3", "b", "b"],
    ]);
  });

  it("item 20: an empty id field sets lastEventId to the empty string", () => {
    const r = run();
    r.feed("id: a\ndata: 1\n\nid:\ndata: 2\n\ndata: 3\n\n");
    expect(r.events.map(shape)).toEqual([
      ["message", "1", "a", "a"],
      ["message", "2", "", ""],
      ["message", "3", null, ""],
    ]);
  });
});
