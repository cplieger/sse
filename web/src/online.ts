export interface OnlineSource {
  online(): boolean;
  listen(cb: (online: boolean) => void): () => void;
}

export interface OnlineManager {
  isOnline(): boolean;
  /** The source is listened to from the first subscriber to the last. */
  subscribe(listener: (online: boolean) => void): () => void;
  /** Pins the value and stops listening: emits it once, then nothing. undefined clears the pin. */
  setOnline(value: boolean | undefined): void;
}

/** Creates the manager over `source`, defaulting to navigator.onLine and the window events. */
export function createOnlineManager(source: OnlineSource = createDOMOnlineSource()): OnlineManager {
  const listeners = new Set<(online: boolean) => void>();
  let pinned: boolean | undefined;
  let unlisten: (() => void) | null = null;

  function forward(online: boolean): void {
    for (const listener of [...listeners]) {
      listener(online);
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
    isOnline: () => pinned ?? source.online(),
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
    setOnline(value) {
      pinned = value;
      if (value === undefined) {
        listenIfNeeded();
        return;
      }
      stopListening();
      forward(value);
    },
  };
}

// Read through globalThis so the module typechecks under a WebWorker lib, where window is absent.
interface DOMOnlineGlobals {
  readonly navigator?: { readonly onLine?: boolean };
  readonly window?: EventTarget;
}

/** navigator.onLine is a hint: true can mean no route, so the value is only ever a reason to try. */
export function createDOMOnlineSource(): OnlineSource {
  const dom = globalThis as DOMOnlineGlobals;
  return {
    online: () => dom.navigator?.onLine !== false,
    listen(cb) {
      const win = dom.window;
      if (win === undefined) {
        return () => undefined;
      }
      const controller = new AbortController();
      const opts = { signal: controller.signal };
      win.addEventListener(
        "online",
        () => {
          cb(true);
        },
        opts,
      );
      win.addEventListener(
        "offline",
        () => {
          cb(false);
        },
        opts,
      );
      return () => {
        controller.abort();
      };
    },
  };
}
