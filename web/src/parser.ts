// Parse loop follows rexxars/eventsource-parser (MIT): https://github.com/rexxars/eventsource-parser

/** One dispatched frame. `id` is this frame's field or null; `lastEventId` is the sticky WHATWG value. */
export interface ParsedEvent {
  readonly type: string;
  readonly data: string;
  readonly id: string | null;
  readonly lastEventId: string;
  /** Encoded bytes of the frame as received, terminating blank line included. */
  readonly bytes: number;
}

/**
 * What the parser reports. `onEvent` fires once per dispatched frame, which per WHATWG means a
 * frame carrying no `data` field dispatches nothing at all — its `id` is still remembered as
 * the sticky last event id. The other three are optional and each fires for one wire feature:
 * a well-formed `retry:` value, a comment line, and the buffer cap being passed.
 */
export interface ParserHandlers {
  readonly onEvent: (event: ParsedEvent) => void;
  readonly onRetry?: (ms: number) => void;
  readonly onComment?: () => void;
  /** Called once when a frame exceeds maxBufferBytes; the parser then ignores every feed until reset(). */
  readonly onOverflow?: (bytes: number) => void;
}

/**
 * The parser's only bound: the encoded bytes one frame may occupy, counted from the end of the
 * previous frame and including its own terminating blank line. It must be at least
 * MAX_FRAME_BYTES, or a frame the server is entitled to send would abort every connection.
 */
export interface ParserOptions {
  readonly maxBufferBytes: number;
}

/**
 * A byte-fed event-stream parser. Feeding is incremental: a chunk may split a line, a CRLF pair
 * or a multi-byte character, and the parse resumes from the next chunk. Once a frame passes
 * maxBufferBytes the parser latches off — every further feed is ignored until reset().
 */
export interface Parser {
  feed(chunk: Uint8Array): void;
  /** Returns the parser to its construction state; a partial event is discarded. */
  reset(): void;
}

const LF = 0x0a;
const CR = 0x0d;
const DIGITS_RE = /^[0-9]+$/;

/** Creates a WHATWG event-stream parser over raw bytes. */
export function createParser(handlers: ParserHandlers, opts: ParserOptions): Parser {
  const { onEvent, onRetry, onComment, onOverflow } = handlers;
  const maxBufferBytes = opts.maxBufferBytes;

  let decoder = new TextDecoder("utf-8", { ignoreBOM: true });
  let lineParts: string[] = [];
  let pendingCR = false;
  let atStart = true;
  let frameBytes = 0;
  let overflowed = false;

  let dataLines: string[] = [];
  let typeBuffer = "";
  let frameId: string | null = null;
  let lastEventId = "";

  function dispatch(): void {
    if (frameId !== null) {
      lastEventId = frameId;
    }
    if (dataLines.length > 0) {
      const type = typeBuffer === "" ? "message" : typeBuffer;
      onEvent({ type, data: dataLines.join("\n"), id: frameId, lastEventId, bytes: frameBytes });
    }
    dataLines = [];
    typeBuffer = "";
    frameId = null;
    frameBytes = 0;
  }

  function processField(field: string, value: string): void {
    switch (field) {
      case "event":
        typeBuffer = value;
        break;
      case "data":
        dataLines.push(value);
        break;
      case "id":
        if (!value.includes("\0")) {
          frameId = value;
        }
        break;
      case "retry":
        if (DIGITS_RE.test(value)) {
          onRetry?.(parseInt(value, 10));
        }
        break;
      default:
        break;
    }
  }

  function processLine(line: string): void {
    if (line === "") {
      dispatch();
      return;
    }
    if (line.charCodeAt(0) === 0x3a) {
      onComment?.();
      return;
    }
    const colon = line.indexOf(":");
    if (colon === -1) {
      processField(line, "");
      return;
    }
    const valueStart = line.charCodeAt(colon + 1) === 0x20 ? colon + 2 : colon + 1;
    processField(line.slice(0, colon), line.slice(valueStart));
  }

  function endLine(bytes: Uint8Array): void {
    let text: string;
    if (lineParts.length === 0) {
      text = decoder.decode(bytes);
    } else {
      lineParts.push(decoder.decode(bytes));
      text = lineParts.join("");
      lineParts = [];
    }
    if (atStart) {
      atStart = false;
      if (text.charCodeAt(0) === 0xfeff) {
        text = text.slice(1);
      }
    }
    processLine(text);
  }

  function overflow(): void {
    overflowed = true;
    lineParts = [];
    dataLines = [];
    onOverflow?.(frameBytes);
  }

  function feed(chunk: Uint8Array): void {
    if (overflowed || chunk.length === 0) {
      return;
    }
    let pos = 0;
    if (pendingCR) {
      pendingCR = false;
      if (chunk[0] === LF) {
        pos = 1;
      }
    }
    let lineStart = pos;
    while (pos < chunk.length) {
      const b = chunk[pos];
      if (b !== LF && b !== CR) {
        pos++;
        continue;
      }
      let terminator = 1;
      if (b === CR) {
        if (pos + 1 < chunk.length) {
          if (chunk[pos + 1] === LF) {
            terminator = 2;
          }
        } else {
          pendingCR = true;
        }
      }
      frameBytes += pos - lineStart + terminator;
      if (frameBytes > maxBufferBytes) {
        overflow();
        return;
      }
      endLine(chunk.subarray(lineStart, pos));
      pos += terminator;
      lineStart = pos;
    }
    if (lineStart < chunk.length) {
      frameBytes += chunk.length - lineStart;
      if (frameBytes > maxBufferBytes) {
        overflow();
        return;
      }
      lineParts.push(decoder.decode(chunk.subarray(lineStart), { stream: true }));
    }
  }

  function reset(): void {
    decoder = new TextDecoder("utf-8", { ignoreBOM: true });
    lineParts = [];
    pendingCR = false;
    atStart = true;
    frameBytes = 0;
    overflowed = false;
    dataLines = [];
    typeBuffer = "";
    frameId = null;
    lastEventId = "";
  }

  return { feed, reset };
}
