import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createParser as createOracle } from "eventsource-parser";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createParser, type ParsedEvent } from "./parser.js";
import { MAX_FRAME_BYTES } from "./timing.js";

const encoder = new TextEncoder();

type Dispatch = readonly [type: string, data: string, id: string | null];

function parseAll(chunks: readonly Uint8Array[]): Dispatch[] {
  const out: Dispatch[] = [];
  const parser = createParser(
    { onEvent: (ev: ParsedEvent) => out.push([ev.type, ev.data, ev.id]) },
    { maxBufferBytes: MAX_FRAME_BYTES },
  );
  for (const chunk of chunks) {
    parser.feed(chunk);
  }
  return out;
}

function oracleAll(text: string): Dispatch[] {
  const out: Dispatch[] = [];
  const parser = createOracle({
    onEvent: (ev) => out.push([ev.event ?? "message", ev.data, ev.id ?? null]),
    onComment: () => undefined,
  });
  parser.feed(text);
  return out;
}

function split(bytes: Uint8Array, cuts: readonly number[]): Uint8Array[] {
  const points = [...new Set(cuts.map((c) => c % (bytes.length + 1)))].sort((a, b) => a - b);
  const chunks: Uint8Array[] = [];
  let start = 0;
  for (const p of points) {
    chunks.push(bytes.subarray(start, p));
    start = p;
  }
  chunks.push(bytes.subarray(start));
  return chunks;
}

const terminator = fc.constantFrom("\n", "\r\n", "\r");
const fieldValue = fc.string({
  unit: fc.constantFrom("a", "b", " ", ":", "é", "€", "𐍈", "\0", "1", "x"),
  maxLength: 12,
});
const line = fc.oneof(
  fc.tuple(fc.constant("data"), fieldValue).map(([f, v]) => `${f}: ${v}`),
  fc.tuple(fc.constant("data"), fieldValue).map(([f, v]) => `${f}:${v}`),
  fc.tuple(fc.constant("event"), fieldValue).map(([f, v]) => `${f}: ${v}`),
  fc.tuple(fc.constant("id"), fieldValue).map(([f, v]) => `${f}: ${v}`),
  fc.tuple(fc.constant("retry"), fieldValue).map(([f, v]) => `${f}: ${v}`),
  fieldValue.map((v) => `: ${v}`),
  fieldValue.map((v) => `Data: ${v}`),
  fc.constantFrom("data", "event", "id", "unknown"),
  fc.constant(""),
);
const stream = fc
  .tuple(fc.boolean(), fc.array(fc.tuple(line, terminator), { maxLength: 24 }))
  .map(([bom, lines]) => (bom ? "\uFEFF" : "") + lines.map(([l, t]) => l + t).join(""));

const corpus = [
  "data: a\n\n",
  "\uFEFFdata: a\n\ndata: b\n\n",
  "data: one\r\ndata: two\r\n\r\n",
  "data: cr\rdata: two\r\r",
  'id: 3f9a1c0e7b2d4a58:12\nevent: notify\ndata: {"a":1}\n\n',
  "id: x\n\ndata: y\n\n",
  "id:\ndata: y\n\n",
  "id: a\0b\ndata: y\n\n",
  "event: notify\nevent:\ndata: y\n\n",
  "data:  two spaces\n\n",
  "data\n\n",
  "Data: not data\ndata: real\n\n",
  "retry: 1500\n\n",
  ": comment\n\n",
  "data: partial",
  "data: héllo €\n\n",
];

describe("parser fuzz", () => {
  it("random chunk boundaries equal the unchunked parse", () => {
    fc.assert(
      fc.property(stream, fc.array(fc.nat(), { maxLength: 16 }), (text, cuts) => {
        const bytes = encoder.encode(text);
        expect(parseAll(split(bytes, cuts))).toEqual(parseAll([bytes]));
      }),
    );
  });

  it("differential against eventsource-parser over the corpus and fuzz inputs", () => {
    for (const text of corpus) {
      expect(parseAll([encoder.encode(text)]), JSON.stringify(text)).toEqual(oracleAll(text));
    }
    fc.assert(
      fc.property(stream, (text) => {
        expect(parseAll([encoder.encode(text)])).toEqual(oracleAll(text));
      }),
    );
  });

  it("the Go framing golden decodes to the LF-normalised input", () => {
    const path = join(import.meta.dirname, "..", "..", "testdata", "framing.golden.json");
    const rows = JSON.parse(readFileSync(path, "utf8")) as {
      name: string;
      id: string;
      event: string;
      encodedHex: string;
      decoded: string;
      invalidUTF8: boolean;
    }[];
    expect(rows.length).toBeGreaterThan(0);
    let checked = 0;
    for (const row of rows) {
      if (row.invalidUTF8) {
        continue;
      }
      checked++;
      const bytes = Uint8Array.from(Buffer.from(row.encodedHex, "hex"));
      const got = parseAll([bytes]);
      const want: Dispatch = [
        row.event === "" ? "message" : row.event,
        row.decoded,
        row.id === "" ? null : row.id,
      ];
      expect(got, row.name).toEqual([want]);
    }
    expect(checked).toBe(rows.length - 1);
  });
});
