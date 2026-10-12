import { afterEach, describe, expect, it } from "vitest";
import { type VisibilityEvent, createDOMVisibilitySource } from "./visibility.js";

// createDOMVisibilitySource listens on the real document and window, so every test releases
// its own registration, and any visibilityState pin, before the next one dispatches.
const releases: (() => void)[] = [];

function listen(cb: (ev: VisibilityEvent) => void): () => void {
  const off = createDOMVisibilitySource().listen(cb);
  releases.push(off);
  return off;
}

function pinVisibility(state: "visible" | "hidden"): void {
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    get: () => state,
  });
}

afterEach(() => {
  for (const off of releases.splice(0)) {
    off();
  }
  Reflect.deleteProperty(document, "visibilityState");
});

describe("createDOMVisibilitySource", () => {
  it("maps a visibilitychange to hidden while the document reads hidden", () => {
    const seen: VisibilityEvent[] = [];
    listen((ev) => seen.push(ev));
    pinVisibility("hidden");
    document.dispatchEvent(new Event("visibilitychange"));
    expect(seen).toEqual(["hidden"]);
  });

  it("maps a visibilitychange to visible while the document reads visible", () => {
    const seen: VisibilityEvent[] = [];
    listen((ev) => seen.push(ev));
    pinVisibility("visible");
    document.dispatchEvent(new Event("visibilitychange"));
    expect(seen).toEqual(["visible"]);
  });

  it("reports freeze and resume under their own names", () => {
    const seen: VisibilityEvent[] = [];
    listen((ev) => seen.push(ev));
    document.dispatchEvent(new Event("freeze"));
    document.dispatchEvent(new Event("resume"));
    expect(seen).toEqual(["freeze", "resume"]);
  });

  it("reports the window's pagehide and pageshow", () => {
    const seen: VisibilityEvent[] = [];
    listen((ev) => seen.push(ev));
    window.dispatchEvent(new Event("pagehide"));
    window.dispatchEvent(new Event("pageshow"));
    expect(seen).toEqual(["pagehide", "pageshow"]);
  });

  it("hears nothing from the document or the window once the unsubscribe has run", () => {
    const seen: VisibilityEvent[] = [];
    const off = listen((ev) => seen.push(ev));
    document.dispatchEvent(new Event("freeze"));
    expect(seen).toEqual(["freeze"]);
    off();
    document.dispatchEvent(new Event("freeze"));
    document.dispatchEvent(new Event("resume"));
    document.dispatchEvent(new Event("visibilitychange"));
    window.dispatchEvent(new Event("pagehide"));
    window.dispatchEvent(new Event("pageshow"));
    expect(seen).toEqual(["freeze"]);
  });

  it("reads the document's visibilityState as the on-demand value", () => {
    const source = createDOMVisibilitySource();
    pinVisibility("hidden");
    expect(source.visible()).toBe(false);
    pinVisibility("visible");
    expect(source.visible()).toBe(true);
  });
});
