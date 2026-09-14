import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  DEFAULT_KEEPALIVE_MS,
  DEFAULT_RETRY_MS,
  DEFAULT_TIMING,
  DIGEST_MAX_SUBJECTS,
  KEEPALIVE_EVENT,
  MAX_FRAME_BYTES,
  MAX_OFFSET,
  MAX_OFFSET_STRING,
  WIRE,
  resolveTiming,
  watchdogMs,
} from "./timing.js";

interface TimingContract {
  readonly wire: number;
  readonly keepalive_ms: number;
  readonly max_frame_bytes: number;
  readonly watchdog_beats: number;
  readonly watchdog_floor_ms: number;
  readonly connect_timeout_ms: number;
  readonly hello_timeout_ms: number;
  readonly revalidate_timeout_ms: number;
  readonly digest_timeout_ms: number;
  readonly held_max_frames: number;
  readonly held_max_bytes_frames: number;
  readonly retry_ms: number;
  readonly base_ms: number;
  readonly cap_ms: number;
  readonly stable_ms: number;
  readonly hidden_close_after_ms: number;
  readonly heartbeat_ms: number;
  readonly alive_every_beats: number;
  readonly wake_throttle_ms: number;
  readonly digest_max_subjects: number;
  readonly max_offset: string;
  readonly keepalive_event: string;
}

const contractPath = join(import.meta.dirname, "..", "..", "timing.json");
const contract = JSON.parse(readFileSync(contractPath, "utf8")) as TimingContract;

describe("timing contract", () => {
  it("pins every client constant to timing.json", () => {
    expect(WIRE).toBe(contract.wire);
    expect(MAX_FRAME_BYTES).toBe(contract.max_frame_bytes);
    expect(MAX_OFFSET_STRING).toBe(contract.max_offset);
    expect(MAX_OFFSET).toBe(BigInt(contract.max_offset));
    expect(DEFAULT_KEEPALIVE_MS).toBe(contract.keepalive_ms);
    expect(DEFAULT_RETRY_MS).toBe(contract.retry_ms);
    expect(KEEPALIVE_EVENT).toBe(contract.keepalive_event);
    expect(DIGEST_MAX_SUBJECTS).toBe(contract.digest_max_subjects);
    expect(DEFAULT_TIMING).toEqual({
      connectTimeoutMs: contract.connect_timeout_ms,
      helloTimeoutMs: contract.hello_timeout_ms,
      revalidateTimeoutMs: contract.revalidate_timeout_ms,
      digestTimeoutMs: contract.digest_timeout_ms,
      heldMaxFrames: contract.held_max_frames,
      heldMaxBytes: contract.held_max_bytes_frames * contract.max_frame_bytes,
      baseMs: contract.base_ms,
      capMs: contract.cap_ms,
      stableMs: contract.stable_ms,
      hiddenCloseAfterMs: contract.hidden_close_after_ms,
      wakeThrottleMs: contract.wake_throttle_ms,
      heartbeatMs: contract.heartbeat_ms,
      aliveEveryBeats: contract.alive_every_beats,
      watchdogBeats: contract.watchdog_beats,
      watchdogFloorMs: contract.watchdog_floor_ms,
      maxBufferBytes: contract.max_frame_bytes,
    });
  });

  it("watchdogMs derives max(3k, floor)", () => {
    expect(watchdogMs(15_000)).toBe(45_000);
    expect(watchdogMs(1_000)).toBe(15_000);
    expect(watchdogMs(5_000)).toBe(15_000);
    expect(watchdogMs(5_001)).toBe(15_003);
  });

  it("resolveTiming refuses maxBufferBytes below MAX_FRAME_BYTES", () => {
    expect(() => resolveTiming({ maxBufferBytes: MAX_FRAME_BYTES - 1 })).toThrow(RangeError);
    expect(resolveTiming({ maxBufferBytes: MAX_FRAME_BYTES }).maxBufferBytes).toBe(MAX_FRAME_BYTES);
    expect(resolveTiming({ capMs: 1 })).toEqual({ ...DEFAULT_TIMING, capMs: 1 });
    expect(resolveTiming()).toEqual(DEFAULT_TIMING);
  });
});
