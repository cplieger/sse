package sse

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// ErrConfig is wrapped by every construction-time refusal New returns; the
// message names the offending option or pair.
var ErrConfig = errors.New("sse: incoherent configuration")

const (
	defaultKeepalive      = 15 * time.Second
	defaultReconnectDelay = 1500 * time.Millisecond
	defaultReplayMaxBytes = 64 * MaxFrameBytes
	writeTimeoutBeats     = 2
	replyMaxCeiling       = 256
	clientBufferFloor     = 256
	resetWriteTimeout     = 250 * time.Millisecond

	// The client's watchdog window plus its backoff cap: the longest a wake-less
	// reconnect can take to present its cursor, and so the floor for a TTL.
	watchdogBeats      = 3
	watchdogFloor      = 15 * time.Second
	clientBackoffCap   = 30 * time.Second
	replayTTLFloorBase = clientBackoffCap
)

type config struct {
	logger          *slog.Logger
	now             func() time.Time
	presence        func(PresenceEvent)
	keepaliveEvent  string
	epoch           string
	keepalive       time.Duration
	ttl             time.Duration
	writeTimeout    time.Duration
	reconnectDelay  time.Duration
	ringSize        int
	clientBuffer    int
	maxClients      int
	replyMax        int
	maxBytes        int
	clientBufferSet bool
	replyMaxSet     bool
	writeTimeoutSet bool
}

func defaultConfig() config {
	return config{
		logger:         slog.Default(),
		now:            time.Now,
		keepaliveEvent: defaultKeepaliveEvent,
		keepalive:      defaultKeepalive,
		reconnectDelay: defaultReconnectDelay,
		maxBytes:       defaultReplayMaxBytes,
	}
}

// Option configures a Hub at construction. An option refuses its own value on
// the spot; rules that compare two options run after every option has been
// applied, so option order never changes the verdict.
type Option func(*config) error

// WithReplay sets the ring capacity in events (default 0, no replay). A
// non-zero capacity requires WithReplayTTL. Negative is refused.
func WithReplay(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("%w: WithReplay(%d) is negative", ErrConfig, n)
		}
		c.ringSize = n
		return nil
	}
}

// WithReplayTTL sets the maximum age of a ring entry (no default; required when
// WithReplay is non-zero). Refused at or below zero, and refused below
// max(3 × keepalive, 15s) + 30s, the window a client needs to reconnect after a
// half-open stream.
func WithReplayTTL(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithReplayTTL(%v) is not positive", ErrConfig, d)
		}
		c.ttl = d
		return nil
	}
}

// WithReplayMaxBytes caps the sum of encoded frame bytes the ring holds
// (default 64 × MaxFrameBytes); the oldest entries are evicted when an append
// would exceed it. Refused below MaxFrameBytes.
func WithReplayMaxBytes(n int) Option {
	return func(c *config) error {
		if n < MaxFrameBytes {
			return fmt.Errorf("%w: WithReplayMaxBytes(%d) is below MaxFrameBytes (%d)", ErrConfig, n, MaxFrameBytes)
		}
		c.maxBytes = n
		return nil
	}
}

// WithReplyMaxEvents caps the replay one connection may receive (default
// min(ring capacity, 256)); a resume needing more is gap_budget. Refused when
// negative or above the ring capacity.
func WithReplyMaxEvents(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("%w: WithReplyMaxEvents(%d) is negative", ErrConfig, n)
		}
		c.replyMax = n
		c.replyMaxSet = true
		return nil
	}
}

// WithClientBuffer sets the per-client channel capacity (default
// max(ring capacity, 256)). A client that falls this many frames behind is
// reset as slow. Refused at or below zero.
func WithClientBuffer(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("%w: WithClientBuffer(%d) is not positive", ErrConfig, n)
		}
		c.clientBuffer = n
		c.clientBufferSet = true
		return nil
	}
}

// WithMaxClients caps concurrent clients; Serve answers 503 beyond it. Zero or
// negative means unlimited.
func WithMaxClients(n int) Option {
	return func(c *config) error {
		c.maxClients = max(n, 0)
		return nil
	}
}

// WithKeepalive sets the keepalive interval (default 15s), announced to the
// client as hello.keepalive_ms. Refused below one millisecond.
func WithKeepalive(d time.Duration) Option {
	return func(c *config) error {
		if d < time.Millisecond {
			return fmt.Errorf("%w: WithKeepalive(%v) is below 1ms", ErrConfig, d)
		}
		c.keepalive = d
		return nil
	}
}

// WithKeepaliveEvent names the keepalive frame's event (default
// "sse:keepalive"). The empty string selects the comment form ": keepalive",
// invisible to EventSource consumers. A name holding CR or LF is refused.
func WithKeepaliveEvent(name string) Option {
	return func(c *config) error {
		if strings.ContainsAny(name, "\r\n") {
			return fmt.Errorf("%w: WithKeepaliveEvent name spans lines", ErrConfig)
		}
		c.keepaliveEvent = name
		return nil
	}
}

// WithReconnectDelay sets the stream's retry: field (default 1500ms), the
// wait a client observes before reconnecting. Refused below one millisecond,
// since 0 on the wire means reconnect at once.
func WithReconnectDelay(d time.Duration) Option {
	return func(c *config) error {
		if d < time.Millisecond {
			return fmt.Errorf("%w: WithReconnectDelay(%v) is below 1ms", ErrConfig, d)
		}
		c.reconnectDelay = d
		return nil
	}
}

// WithWriteTimeout bounds every write to a client (default 2 × keepalive).
// Refused at or below the keepalive interval.
func WithWriteTimeout(d time.Duration) Option {
	return func(c *config) error {
		c.writeTimeout = d
		c.writeTimeoutSet = true
		return nil
	}
}

// WithLogger sets the logger for connection and eviction diagnostics (default
// slog.Default()). A nil logger keeps the default.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		if l != nil {
			c.logger = l
		}
		return nil
	}
}

// WithPresence installs the one hook that receives every PresenceEvent (nil
// removes it). It runs on the arriving or departing stream's own goroutine,
// after the hello flush or after the client is unregistered, never under the
// hub lock, so a slow hook delays only that stream. A panic in it is recovered
// and logged at Error.
func WithPresence(fn func(PresenceEvent)) Option {
	return func(c *config) error {
		c.presence = fn
		return nil
	}
}

func replayTTLFloor(keepalive time.Duration) time.Duration {
	return max(watchdogBeats*keepalive, watchdogFloor) + replayTTLFloorBase
}

// validate applies the cross-field rules and derives the defaults that depend
// on another option, after every option has run.
func validate(c *config) error {
	if c.ringSize > 0 && c.ttl == 0 {
		return fmt.Errorf("%w: WithReplay(%d) requires WithReplayTTL", ErrConfig, c.ringSize)
	}
	if floor := replayTTLFloor(c.keepalive); c.ttl != 0 && c.ttl < floor {
		return fmt.Errorf("%w: WithReplayTTL(%v) is below %v, the client's watchdog window plus backoff cap at WithKeepalive(%v)", ErrConfig, c.ttl, floor, c.keepalive)
	}
	if !c.writeTimeoutSet {
		c.writeTimeout = writeTimeoutBeats * c.keepalive
	} else if c.writeTimeout <= c.keepalive {
		return fmt.Errorf("%w: WithWriteTimeout(%v) must exceed WithKeepalive(%v)", ErrConfig, c.writeTimeout, c.keepalive)
	}
	if !c.replyMaxSet {
		c.replyMax = min(c.ringSize, replyMaxCeiling)
	} else if c.replyMax > c.ringSize {
		return fmt.Errorf("%w: WithReplyMaxEvents(%d) exceeds WithReplay(%d)", ErrConfig, c.replyMax, c.ringSize)
	}
	if !c.clientBufferSet {
		c.clientBuffer = max(c.ringSize, clientBufferFloor)
	}
	return nil
}
