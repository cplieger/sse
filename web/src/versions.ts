/** A digest subject key: the application's kind and reference. */
export interface Subject {
  readonly kind: string;
  readonly ref: string;
}

/** A subject with the version the client holds for it. */
export interface Held extends Subject {
  readonly version: string;
}

/**
 * The epoch-bound version map. Versions are opaque strings recorded after the state they
 * stamp has been applied, never from a digest answer.
 */
export interface VersionMap {
  /** The epoch every held version was minted under; null until bound. */
  epoch(): string | null;
  /** Binds the map to `epoch`; a different epoch clears every entry. Returns the dropped count. */
  bind(epoch: string): number;
  /**
   * Records `version` for `subject` and returns whether the held version changed. With `epoch`:
   * an unbound map binds to it first; a map bound to a different epoch ignores the call and
   * reports it through onStale.
   */
  observe(subject: Subject, version: string, epoch?: string): boolean;
  forget(subject: Subject): void;
  has(subject: Subject): boolean;
  snapshot(): { epoch: string | null; held: Held[] };
}

/**
 * The map's one hook. A stamp from a foreign epoch is not an error the application has to handle:
 * observe() ignores it and answers false, and this is how the application learns it happened.
 * staleListener reports the same events, which is the path the runtime uses for its `stale_stamp`
 * records, so a consumer that wants both gets both.
 */
export interface VersionMapOptions {
  /** Called for every stamp ignored because its epoch differs from the bound epoch. */
  readonly onStale?: (subject: Subject, epoch: string) => void;
}

type BindListener = (epoch: string, dropped: number) => void;
type StaleListener = (subject: Subject, epoch: string) => void;

interface Listeners {
  readonly bound: Set<BindListener>;
  readonly stale: Set<StaleListener>;
}

const listeners = new WeakMap<VersionMap, Listeners>();

function key(subject: Subject): string {
  return `${subject.kind}\0${subject.ref}`;
}

// Epoch binding follows IMAP CONDSTORE (RFC 7162): a UIDVALIDITY change discards cached mod-sequences.
/** Creates an empty, unbound version map. */
export function createVersionMap(opts: VersionMapOptions = {}): VersionMap {
  let epoch: string | null = null;
  const held = new Map<string, Held>();
  const bound = new Set<BindListener>();
  const stale = new Set<StaleListener>();

  const map: VersionMap = {
    epoch: () => epoch,
    bind(next) {
      let dropped = 0;
      if (epoch !== next) {
        dropped = held.size;
        held.clear();
        epoch = next;
      }
      for (const listener of bound) {
        listener(next, dropped);
      }
      return dropped;
    },
    observe(subject, version, stampEpoch) {
      if (stampEpoch !== undefined) {
        if (epoch === null) {
          epoch = stampEpoch;
        } else if (epoch !== stampEpoch) {
          opts.onStale?.(subject, stampEpoch);
          for (const listener of stale) {
            listener(subject, stampEpoch);
          }
          return false;
        }
      }
      const k = key(subject);
      const previous = held.get(k);
      held.set(k, { kind: subject.kind, ref: subject.ref, version });
      return previous?.version !== version;
    },
    forget(subject) {
      held.delete(key(subject));
    },
    has(subject) {
      return held.has(key(subject));
    },
    snapshot() {
      return { epoch, held: [...held.values()] };
    },
  };
  listeners.set(map, { bound, stale });
  return map;
}

function listenersOf(map: VersionMap, caller: string): Listeners {
  const set = listeners.get(map);
  if (set === undefined) {
    throw new TypeError(`${caller}: map was not created by createVersionMap`);
  }
  return set;
}

/**
 * Subscribes to every bind() on a map created by createVersionMap; the runtime uses it to
 * discharge a queued full revalidation. Returns the unsubscribe. Throws on a foreign map.
 */
export function bindListener(map: VersionMap, fn: BindListener): () => void {
  const set = listenersOf(map, "bindListener").bound;
  set.add(fn);
  return () => {
    set.delete(fn);
  };
}

/**
 * Subscribes to every stamp the map ignores for a foreign epoch; the runtime reports each as
 * stale_stamp. Returns the unsubscribe. Throws on a foreign map.
 */
export function staleListener(map: VersionMap, fn: StaleListener): () => void {
  const set = listenersOf(map, "staleListener").stale;
  set.add(fn);
  return () => {
    set.delete(fn);
  };
}
