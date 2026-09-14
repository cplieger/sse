import { describe, expect, it } from "vitest";
import {
  adaptVisibility,
  createDOMVisibilitySource,
  createVisibilityManager,
  type VisibilityEvent,
  type VisibilityInput,
  type VisibilitySource,
} from "./visibility.js";

interface FakeSource extends VisibilitySource {
  listens: number;
  unlistens: number;
  visibleValue: boolean;
  fire(ev: VisibilityEvent): void;
}

function fakeSource(visible = true): FakeSource {
  let cb: ((ev: VisibilityEvent) => void) | null = null;
  const source: FakeSource = {
    listens: 0,
    unlistens: 0,
    visibleValue: visible,
    visible: () => source.visibleValue,
    listen(next) {
      source.listens++;
      cb = next;
      return () => {
        source.unlistens++;
        cb = null;
      };
    },
    fire(ev) {
      cb?.(ev);
    },
  };
  return source;
}

describe("visibility manager", () => {
  it("adds the DOM listener on the first subscriber and removes it on the last", () => {
    const source = fakeSource();
    const manager = createVisibilityManager(source);
    expect(source.listens).toBe(0);
    const seenA: VisibilityEvent[] = [];
    const seenB: VisibilityEvent[] = [];
    const offA = manager.subscribe((ev) => seenA.push(ev));
    const offB = manager.subscribe((ev) => seenB.push(ev));
    expect(source.listens).toBe(1);
    source.fire("hidden");
    expect(seenA).toEqual(["hidden"]);
    expect(seenB).toEqual(["hidden"]);
    offA();
    expect(source.unlistens).toBe(0);
    offB();
    expect(source.unlistens).toBe(1);
    source.fire("visible");
    expect(seenB).toEqual(["hidden"]);
    manager.subscribe(() => undefined);
    expect(source.listens).toBe(2);
  });

  it("setVisible pins the value and emits once", () => {
    const source = fakeSource(true);
    const manager = createVisibilityManager(source);
    const seen: VisibilityEvent[] = [];
    manager.subscribe((ev) => seen.push(ev));
    manager.setVisible(false);
    expect(manager.isVisible()).toBe(false);
    expect(seen).toEqual(["hidden"]);
    expect(source.unlistens).toBe(1);
    source.fire("visible");
    expect(seen).toEqual(["hidden"]);
    manager.setVisible(undefined);
    expect(manager.isVisible()).toBe(true);
    expect(source.listens).toBe(2);
    source.fire("pageshow");
    expect(seen).toEqual(["hidden", "pageshow"]);
  });

  it("resume while hidden emits nothing", () => {
    const source = fakeSource(false);
    const manager = createVisibilityManager(source);
    const emitted: VisibilityInput[] = [];
    adaptVisibility(manager, source, (ev) => emitted.push(ev));
    source.fire("resume");
    expect(emitted).toEqual([]);
  });

  it("pageshow while hidden emits nothing", () => {
    const source = fakeSource(false);
    const manager = createVisibilityManager(source);
    const emitted: VisibilityInput[] = [];
    adaptVisibility(manager, source, (ev) => emitted.push(ev));
    source.fire("pageshow");
    expect(emitted).toEqual([]);
  });

  it("resume and pageshow while visible emit visible and pageshow", () => {
    const source = fakeSource(true);
    const manager = createVisibilityManager(source);
    const emitted: VisibilityInput[] = [];
    const off = adaptVisibility(manager, source, (ev) => emitted.push(ev));
    source.fire("resume");
    source.fire("pageshow");
    source.fire("freeze");
    source.fire("hidden");
    source.fire("pagehide");
    source.fire("visible");
    expect(emitted).toEqual(["visible", "pageshow", "hidden", "hidden", "pagehide", "visible"]);
    off();
    expect(source.unlistens).toBe(1);
  });

  it("the DOM source reads visible and listens to nothing where document and window are absent", () => {
    expect(typeof document).toBe("undefined");
    const source = createDOMVisibilitySource();
    expect(source.visible()).toBe(true);
    const seen: VisibilityEvent[] = [];
    const off = source.listen((ev) => seen.push(ev));
    off();
    expect(seen).toEqual([]);
  });
});
