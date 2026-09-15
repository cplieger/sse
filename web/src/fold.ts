import type { OnlineSource } from "./online.js";
import type { VisibilityEvent, VisibilityInput, VisibilitySource } from "./visibility.js";

/**
 * One attached tab's contribution to the profile: the two readings it forwards. A tab is in the fold
 * from its attach until it is removed or reports `pagehide`. Its visibility stops counting the moment
 * it leaves; the profile's network reading is not recomputed on a departure and keeps the last value
 * reported, by whichever tab.
 */
export interface TabState {
  readonly visible: boolean;
  readonly online: boolean;
  /** A freshly attached tab may earn one pageshow; cleared by its first. */
  readonly pageshowPending: boolean;
}

/**
 * One tab's report into the fold, always keyed by `tabId`. An `attach` seeds that tab's two readings,
 * and `visibility` and `network` update them afterwards. A `visibility` report for a tab the fold does
 * not know is dropped; a `network` report from one still counts as the profile's latest reading, since
 * the network is a property of the machine rather than of the tab that noticed.
 */
export type FoldInput =
  | {
      readonly type: "attach";
      readonly tabId: string;
      readonly visible: boolean;
      readonly online: boolean;
    }
  | { readonly type: "visibility"; readonly tabId: string; readonly ev: VisibilityInput }
  | { readonly type: "network"; readonly tabId: string; readonly online: boolean }
  /** Detach, port close, expiry: the tab leaves as if by pagehide. */
  | { readonly type: "remove"; readonly tabId: string };

/**
 * What a fold step emits for the profile's one stream: a transition, never a tab's report. A step that
 * changes nothing the stream can act on emits nothing — a tab going hidden while another stays visible
 * is silent — and `pageshow` is emitted at most once per visible period however many tabs are restored.
 */
export type FoldOutput = "visible" | "hidden" | "pageshow" | "online" | "offline";

/**
 * The profile's folded view of its tabs. Every step returns a new value rather than mutating this one,
 * so a caller may hold an earlier state and compare against it.
 */
export interface FoldState {
  readonly tabs: ReadonlyMap<string, TabState>;
  /** The latest network report from any tab, seeded by the host. */
  readonly online: boolean;
  /** Whether a pageshow was emitted since the fold last became visible. */
  readonly pageshown: boolean;
}

/**
 * The fold before any tab has attached. `online` is the host's own reading, which stands as the
 * profile's until a tab reports one, so a host that seeds it wrong holds the stream off the network
 * until the first tab corrects it.
 */
export function createFoldState(online: boolean): FoldState {
  return { tabs: new Map(), online, pageshown: false };
}

/** The profile reads visible while any attached tab does. */
export function anyVisible(state: FoldState): boolean {
  for (const tab of state.tabs.values()) {
    if (tab.visible) {
      return true;
    }
  }
  return false;
}

interface Folded {
  readonly state: FoldState;
  readonly events: FoldOutput[];
}

/**
 * Folds one tab's platform event into the profile's, pure over the tab set. Visibility events
 * precede network events within one step, the order a single tab would report them in.
 */
export function foldTabs(state: FoldState, input: FoldInput): Folded {
  const tabs = new Map(state.tabs);
  const visibilityEvents: FoldOutput[] = [];
  const networkEvents: FoldOutput[] = [];
  const before = anyVisible(state);
  let online = state.online;
  let pageshown = state.pageshown;

  function report(value: boolean): void {
    if (value !== online) {
      online = value;
      networkEvents.push(value ? "online" : "offline");
    }
  }

  switch (input.type) {
    case "attach":
      tabs.set(input.tabId, {
        visible: input.visible,
        online: input.online,
        pageshowPending: true,
      });
      report(input.online);
      break;
    case "network": {
      const tab = tabs.get(input.tabId);
      if (tab !== undefined) {
        tabs.set(input.tabId, { ...tab, online: input.online });
      }
      report(input.online);
      break;
    }
    case "remove":
      tabs.delete(input.tabId);
      break;
    case "visibility": {
      const tab = tabs.get(input.tabId);
      if (tab === undefined) {
        break;
      }
      switch (input.ev) {
        case "visible":
          tabs.set(input.tabId, { ...tab, visible: true });
          break;
        case "hidden":
          tabs.set(input.tabId, { ...tab, visible: false });
          break;
        case "pagehide":
          tabs.delete(input.tabId);
          break;
        case "pageshow":
          if (tab.visible && tab.pageshowPending && !pageshown) {
            pageshown = true;
            visibilityEvents.push("pageshow");
          }
          tabs.set(input.tabId, { ...tab, pageshowPending: false });
          break;
      }
      break;
    }
  }

  const after = anyVisible({ tabs, online, pageshown });
  if (after && !before) {
    visibilityEvents.unshift("visible");
    pageshown = visibilityEvents.includes("pageshow");
  } else if (!after && before) {
    visibilityEvents.push("hidden");
    pageshown = false;
  }
  return { state: { tabs, online, pageshown }, events: [...visibilityEvents, ...networkEvents] };
}

type Listener<T> = (value: T) => void;

function listenable<T>(): { readonly listeners: Set<Listener<T>>; emit(value: T): void } {
  const listeners = new Set<Listener<T>>();
  return {
    listeners,
    emit(value) {
      for (const cb of [...listeners]) {
        cb(value);
      }
    },
  };
}

/** A VisibilitySource over attached tabs, fed by the worker host. */
export interface ProfileVisibilitySource extends VisibilitySource {
  attach(tabId: string, visible: boolean): void;
  detach(tabId: string): void;
  report(tabId: string, ev: VisibilityInput): void;
}

/**
 * Creates the visibility reading the worker host gives its stream: the fold over the tabs it has, fed
 * by attach, detach and report rather than by any platform event. It emits only the profile's own
 * transitions, so the stream sees one `visible` for a whole set of tabs coming back and one `hidden`
 * only once none is left showing.
 */
export function createProfileVisibilitySource(): ProfileVisibilitySource {
  let state = createFoldState(true);
  const out = listenable<VisibilityEvent>();

  function apply(input: FoldInput): void {
    const folded = foldTabs(state, input);
    state = folded.state;
    for (const ev of folded.events) {
      if (ev === "visible" || ev === "hidden" || ev === "pageshow") {
        out.emit(ev);
      }
    }
  }

  return {
    visible: () => anyVisible(state),
    listen(cb) {
      out.listeners.add(cb);
      return () => {
        out.listeners.delete(cb);
      };
    },
    attach(tabId, visible) {
      apply({ type: "attach", tabId, visible, online: true });
    },
    detach(tabId) {
      apply({ type: "remove", tabId });
    },
    report(tabId, ev) {
      apply({ type: "visibility", tabId, ev });
    },
  };
}

/** An OnlineSource over the tabs' forwarded network events, seeded from the worker's own reading. */
export interface ProfileOnlineSource extends OnlineSource {
  attach(tabId: string, online: boolean): void;
  detach(tabId: string): void;
  report(tabId: string, online: boolean): void;
}

/**
 * Creates the network reading the worker host gives its stream: the last value any tab reported,
 * starting at `seed`. The tabs are the source because createDOMOnlineSource registers nothing inside a
 * worker — there is no `window` there to hear `online` and `offline` on — so a host reading the DOM
 * directly would never be handed a transition to act on.
 */
export function createProfileOnlineSource(seed: boolean): ProfileOnlineSource {
  let state = createFoldState(seed);
  const out = listenable<boolean>();

  function apply(input: FoldInput): void {
    const folded = foldTabs(state, input);
    state = folded.state;
    for (const ev of folded.events) {
      if (ev === "online") {
        out.emit(true);
      } else if (ev === "offline") {
        out.emit(false);
      }
    }
  }

  return {
    online: () => state.online,
    listen(cb) {
      out.listeners.add(cb);
      return () => {
        out.listeners.delete(cb);
      };
    },
    attach(tabId, online) {
      apply({ type: "attach", tabId, visible: false, online });
    },
    detach(tabId) {
      apply({ type: "remove", tabId });
    },
    report(tabId, online) {
      apply({ type: "network", tabId, online });
    },
  };
}
