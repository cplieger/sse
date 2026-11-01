import { describe, expect, it } from "vitest";
import { createProfileOnlineSource, createProfileVisibilitySource } from "./fold.js";
import type { VisibilityEvent } from "./visibility.js";

describe("profile visibility source", () => {
  it("reports visible while any attached tab is, and not after the last one goes hidden", () => {
    const source = createProfileVisibilitySource();
    source.attach("a", false);
    expect(source.visible()).toBe(false);
    source.attach("b", true);
    expect(source.visible()).toBe(true);
    source.report("b", "hidden");
    expect(source.visible()).toBe(false);
  });

  it("emits one visible for a set of tabs arriving and one hidden once none is left showing", () => {
    const source = createProfileVisibilitySource();
    const seen: VisibilityEvent[] = [];
    source.listen((ev) => seen.push(ev));
    source.attach("a", true);
    source.attach("b", true);
    expect(seen).toEqual(["visible"]);
    source.report("a", "hidden");
    expect(seen).toEqual(["visible"]);
    source.report("b", "hidden");
    expect(seen).toEqual(["visible", "hidden"]);
  });

  it("emits hidden when the last visible tab leaves by detach rather than by reporting", () => {
    const source = createProfileVisibilitySource();
    const seen: VisibilityEvent[] = [];
    source.listen((ev) => seen.push(ev));
    source.attach("a", true);
    expect(seen).toEqual(["visible"]);
    source.detach("a");
    expect(seen).toEqual(["visible", "hidden"]);
    expect(source.visible()).toBe(false);
  });

  it("emits pageshow once for a restored visible tab", () => {
    const source = createProfileVisibilitySource();
    const seen: VisibilityEvent[] = [];
    source.listen((ev) => seen.push(ev));
    source.attach("a", true);
    source.report("a", "pageshow");
    expect(seen).toEqual(["visible", "pageshow"]);
    source.report("a", "pageshow");
    expect(seen).toEqual(["visible", "pageshow"]);
  });

  it("stops telling a listener once its unsubscribe has run", () => {
    const source = createProfileVisibilitySource();
    const seen: VisibilityEvent[] = [];
    const off = source.listen((ev) => seen.push(ev));
    source.attach("a", true);
    expect(seen).toEqual(["visible"]);
    off();
    source.report("a", "hidden");
    expect(seen).toEqual(["visible"]);
  });
});

describe("profile online source", () => {
  it("reads the host's seed until a tab reports otherwise", () => {
    const source = createProfileOnlineSource(false);
    expect(source.online()).toBe(false);
    source.attach("a", false);
    expect(source.online()).toBe(false);
    source.report("a", true);
    expect(source.online()).toBe(true);
  });

  it("emits true when a tab's report brings the profile back onto the network", () => {
    const source = createProfileOnlineSource(false);
    const seen: boolean[] = [];
    source.listen((value) => seen.push(value));
    source.attach("a", false);
    expect(seen).toEqual([]);
    source.report("a", true);
    expect(seen).toEqual([true]);
  });

  it("emits false when a tab's report takes the profile off the network", () => {
    const source = createProfileOnlineSource(true);
    const seen: boolean[] = [];
    source.listen((value) => seen.push(value));
    source.attach("a", true);
    source.report("a", false);
    expect(seen).toEqual([false]);
    expect(source.online()).toBe(false);
  });

  it("emits nothing for a report that repeats the profile's current reading", () => {
    const source = createProfileOnlineSource(true);
    const seen: boolean[] = [];
    source.listen((value) => seen.push(value));
    source.attach("a", true);
    source.report("a", true);
    expect(seen).toEqual([]);
  });

  // The network is a property of the machine, so a departure never recomputes the reading.
  it("keeps the last reported reading when the tab that reported it detaches", () => {
    const source = createProfileOnlineSource(true);
    source.attach("a", true);
    source.report("a", false);
    expect(source.online()).toBe(false);
    source.detach("a");
    expect(source.online()).toBe(false);
  });

  it("stops telling a listener once its unsubscribe has run", () => {
    const source = createProfileOnlineSource(true);
    const seen: boolean[] = [];
    const off = source.listen((value) => seen.push(value));
    source.attach("a", true);
    source.report("a", false);
    expect(seen).toEqual([false]);
    off();
    source.report("a", true);
    expect(seen).toEqual([false]);
  });
});
