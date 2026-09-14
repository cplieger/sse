import { describe, expect, it } from "vitest";
import { createDOMOnlineSource, createOnlineManager, type OnlineSource } from "./online.js";

interface FakeSource extends OnlineSource {
  listens: number;
  unlistens: number;
  onlineValue: boolean;
  fire(online: boolean): void;
}

function fakeSource(online = true): FakeSource {
  let cb: ((online: boolean) => void) | null = null;
  const source: FakeSource = {
    listens: 0,
    unlistens: 0,
    onlineValue: online,
    online: () => source.onlineValue,
    listen(next) {
      source.listens++;
      cb = next;
      return () => {
        source.unlistens++;
        cb = null;
      };
    },
    fire(value) {
      cb?.(value);
    },
  };
  return source;
}

describe("online manager", () => {
  it("adds the DOM listener on the first subscriber and removes it on the last", () => {
    const source = fakeSource();
    const manager = createOnlineManager(source);
    expect(source.listens).toBe(0);
    const seenA: boolean[] = [];
    const seenB: boolean[] = [];
    const offA = manager.subscribe((v) => seenA.push(v));
    const offB = manager.subscribe((v) => seenB.push(v));
    expect(source.listens).toBe(1);
    source.fire(false);
    expect(seenA).toEqual([false]);
    expect(seenB).toEqual([false]);
    offA();
    expect(source.unlistens).toBe(0);
    offB();
    expect(source.unlistens).toBe(1);
    source.fire(true);
    expect(seenB).toEqual([false]);
  });

  it("setOnline pins the value and emits once", () => {
    const source = fakeSource(true);
    const manager = createOnlineManager(source);
    const seen: boolean[] = [];
    manager.subscribe((v) => seen.push(v));
    manager.setOnline(false);
    expect(manager.isOnline()).toBe(false);
    expect(seen).toEqual([false]);
    expect(source.unlistens).toBe(1);
    source.fire(true);
    expect(seen).toEqual([false]);
    manager.setOnline(undefined);
    expect(manager.isOnline()).toBe(true);
    expect(source.listens).toBe(2);
    source.fire(false);
    expect(seen).toEqual([false, false]);
  });

  it("reads the source while unpinned", () => {
    const source = fakeSource(false);
    const manager = createOnlineManager(source);
    expect(manager.isOnline()).toBe(false);
    source.onlineValue = true;
    expect(manager.isOnline()).toBe(true);
  });

  it("the DOM source reads online and listens to nothing where window is absent and navigator has no onLine", () => {
    expect(typeof window).toBe("undefined");
    expect((navigator as { onLine?: boolean }).onLine).toBeUndefined();
    const source = createDOMOnlineSource();
    expect(source.online()).toBe(true);
    const seen: boolean[] = [];
    const off = source.listen((v) => seen.push(v));
    off();
    expect(seen).toEqual([]);
  });
});
