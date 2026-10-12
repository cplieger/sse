import { afterEach, describe, expect, it } from "vitest";
import { createDOMOnlineSource } from "./online.js";

// createDOMOnlineSource listens on the real window, so every test releases its own
// registration before the next one dispatches.
const releases: (() => void)[] = [];

function listen(cb: (online: boolean) => void): () => void {
  const off = createDOMOnlineSource().listen(cb);
  releases.push(off);
  return off;
}

afterEach(() => {
  for (const off of releases.splice(0)) {
    off();
  }
});

describe("createDOMOnlineSource", () => {
  it("reports true when the window fires online", () => {
    const seen: boolean[] = [];
    listen((online) => seen.push(online));
    window.dispatchEvent(new Event("online"));
    expect(seen).toEqual([true]);
  });

  it("reports false when the window fires offline", () => {
    const seen: boolean[] = [];
    listen((online) => seen.push(online));
    window.dispatchEvent(new Event("offline"));
    expect(seen).toEqual([false]);
  });

  it("reports each transition in the order the window fired them", () => {
    const seen: boolean[] = [];
    listen((online) => seen.push(online));
    window.dispatchEvent(new Event("offline"));
    window.dispatchEvent(new Event("online"));
    window.dispatchEvent(new Event("offline"));
    expect(seen).toEqual([false, true, false]);
  });

  it("hears nothing on either event once the unsubscribe has run", () => {
    const seen: boolean[] = [];
    const off = listen((online) => seen.push(online));
    window.dispatchEvent(new Event("online"));
    expect(seen).toEqual([true]);
    off();
    window.dispatchEvent(new Event("online"));
    window.dispatchEvent(new Event("offline"));
    expect(seen).toEqual([true]);
  });

  it("reads navigator.onLine as the on-demand value", () => {
    expect(createDOMOnlineSource().online()).toBe(navigator.onLine);
  });
});
