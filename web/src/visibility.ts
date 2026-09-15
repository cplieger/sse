/**
 * The six page events a source reports. Two of them never reach the reducer: adaptVisibility folds
 * `freeze` onto `hidden` and `resume` onto `visible`, the latter only while the source still reads
 * visible, because a resumed page may have been resumed in the background.
 */
export type VisibilityEvent = "visible" | "hidden" | "pagehide" | "pageshow" | "freeze" | "resume";

/** The four visibility inputs the reducer accepts; the adapter folds freeze and resume into them. */
export type VisibilityInput = "visible" | "hidden" | "pagehide" | "pageshow";

/**
 * The page-visibility reading, injected rather than assumed: `visible()` is asked on demand and
 * `listen` returns the unsubscribe for the stream of events. createDOMVisibilitySource is the
 * implementation over `document` and `window`; the worker host passes the fold over its tabs
 * instead, and a test passes whatever it wants to drive.
 */
export interface VisibilitySource {
  visible(): boolean;
  listen(cb: (ev: VisibilityEvent) => void): () => void;
}

/**
 * One visibility reading shared by every subscriber. The source is listened to only while at least
 * one subscriber is attached, so a manager nobody uses registers no listener, and a pin set through
 * setVisible replaces the source's answer for readers and emitters alike.
 */
export interface VisibilityManager {
  isVisible(): boolean;
  /** The source is listened to from the first subscriber to the last. */
  subscribe(listener: (ev: VisibilityEvent) => void): () => void;
  /** Pins the value and stops listening: emits visible or hidden once, then nothing. undefined clears the pin. */
  setVisible(value: boolean | undefined): void;
}

/** Creates the manager over `source`, defaulting to the DOM. */
export function createVisibilityManager(
  source: VisibilitySource = createDOMVisibilitySource(),
): VisibilityManager {
  const listeners = new Set<(ev: VisibilityEvent) => void>();
  let pinned: boolean | undefined;
  let unlisten: (() => void) | null = null;

  function forward(ev: VisibilityEvent): void {
    for (const listener of [...listeners]) {
      listener(ev);
    }
  }

  function listenIfNeeded(): void {
    if (unlisten === null && listeners.size > 0 && pinned === undefined) {
      unlisten = source.listen(forward);
    }
  }

  function stopListening(): void {
    unlisten?.();
    unlisten = null;
  }

  return {
    isVisible: () => pinned ?? source.visible(),
    subscribe(listener) {
      listeners.add(listener);
      listenIfNeeded();
      return () => {
        listeners.delete(listener);
        if (listeners.size === 0) {
          stopListening();
        }
      };
    },
    setVisible(value) {
      pinned = value;
      if (value === undefined) {
        listenIfNeeded();
        return;
      }
      stopListening();
      forward(value ? "visible" : "hidden");
    },
  };
}

// Read through globalThis so the module typechecks under a WebWorker lib, where neither exists.
interface DOMVisibilityGlobals {
  readonly document?: EventTarget & { readonly visibilityState?: string };
  readonly window?: EventTarget;
}

/** The only reader of document and window visibility in the package. */
export function createDOMVisibilitySource(): VisibilitySource {
  const dom = globalThis as DOMVisibilityGlobals;
  const visible = (): boolean => dom.document?.visibilityState !== "hidden";
  return {
    visible,
    listen(cb) {
      const doc = dom.document;
      const win = dom.window;
      if (doc === undefined || win === undefined) {
        return () => undefined;
      }
      const controller = new AbortController();
      const opts = { signal: controller.signal };
      const on = (target: EventTarget, type: string, ev: () => VisibilityEvent): void => {
        target.addEventListener(
          type,
          () => {
            cb(ev());
          },
          opts,
        );
      };
      on(doc, "visibilitychange", () => (visible() ? "visible" : "hidden"));
      on(doc, "freeze", () => "freeze");
      on(doc, "resume", () => "resume");
      on(win, "pagehide", () => "pagehide");
      on(win, "pageshow", () => "pageshow");
      return () => {
        controller.abort();
      };
    },
  };
}

/**
 * Maps manager events onto reducer inputs: freeze is hidden; resume and pageshow are emitted
 * only while the source reads visible, because a restored or resumed page may still be hidden.
 */
export function adaptVisibility(
  manager: VisibilityManager,
  source: VisibilitySource,
  emit: (ev: VisibilityInput) => void,
): () => void {
  return manager.subscribe((ev) => {
    switch (ev) {
      case "freeze":
        emit("hidden");
        return;
      case "resume":
        if (source.visible()) {
          emit("visible");
        }
        return;
      case "pageshow":
        if (source.visible()) {
          emit("pageshow");
        }
        return;
      default:
        emit(ev);
    }
  });
}
