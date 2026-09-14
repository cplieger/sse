package sse

import "time"

// PresenceCause says why a client left, on a disconnected PresenceEvent.
type PresenceCause string

// The causes a disconnected event carries. Closed and dead are the socket's
// account; evicted and shutdown are the two sse:reset reasons; hook_failed is
// the application ending its own connection from OnConnect.
const (
	PresenceClosed     PresenceCause = "closed"
	PresenceDead       PresenceCause = "dead"
	PresenceEvicted    PresenceCause = "evicted"
	PresenceShutdown   PresenceCause = "shutdown"
	PresenceHookFailed PresenceCause = "hook_failed"
)

// PresenceKind says whether a PresenceEvent is an arrival or a departure.
type PresenceKind string

// The two kinds a PresenceEvent carries.
const (
	PresenceConnected    PresenceKind = "connected"
	PresenceDisconnected PresenceKind = "disconnected"
)

// PresenceEvent is one arrival or departure as WithPresence reports it. A
// connected event is delivered once per Serve that flushed its hello, and
// exactly one disconnected event follows it; a Serve that failed before the
// hello produces neither. Dead means a write or flush failed with the socket
// still open, which the kernel may take minutes to notice: it is observability
// of socket death, not a liveness bound.
type PresenceEvent struct {
	At    time.Time
	Kind  PresenceKind
	Topic string
	// Verdict is the hello's verdict on connected; empty on disconnected.
	Verdict Verdict
	// Cause is set on disconnected; empty on connected.
	Cause PresenceCause
	// Write names the failed write: "keepalive", "frame" or "hook" on a dead
	// event, "reset" on an evicted or shutdown event whose reset write failed.
	Write string
	Epoch string
	// Tag is the WithClientTag value, or "" when none was passed or it failed
	// the grammar. A presence table folds clients by it.
	Tag string
	// ClientID is process-unique and minted at subscribe, so two connects from
	// one browser are two clients.
	ClientID uint64
}

// departure is a stream goroutine's account of why it returned.
type departure struct {
	cause PresenceCause
	write string
}

// presence delivers ev to the hook on the calling goroutine, never under the
// hub lock; a panicking hook is recovered and logged, and the stream's own
// exit is unaffected.
func (h *Hub) presence(ev *PresenceEvent) {
	fn := h.cfg.presence
	if fn == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("sse: presence hook panicked", "panic", r,
				"kind", ev.Kind, "cause", ev.Cause, "write", ev.Write, "verdict", ev.Verdict,
				"epoch", ev.Epoch, "client_id", ev.ClientID, "tag", ev.Tag, "topic", ev.Topic)
		}
	}()
	fn(*ev)
}

func (h *Hub) presenceEvent(c *client, kind PresenceKind) PresenceEvent {
	return PresenceEvent{
		At:       h.cfg.now(),
		Kind:     kind,
		Topic:    c.topic,
		Epoch:    h.epoch,
		Tag:      c.tag,
		ClientID: c.id,
	}
}

// resetDeparture names the reset reason as the cause; the reset write's own
// failure is the Write field, never a second cause.
func resetDeparture(c *client, resetErr error) departure {
	d := departure{cause: PresenceEvicted}
	if c.reason == reasonShutdown {
		d.cause = PresenceShutdown
	}
	if resetErr != nil {
		d.write = "reset"
	}
	return d
}
