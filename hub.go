package sse

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Event is one server-sent event to publish.
type Event struct {
	// Topic scopes delivery: "" reaches every client, any other value only the
	// clients subscribed to that topic.
	Topic string
	// Name is the event: field. It may be empty (no event: line; a browser
	// dispatches the frame as "message"), must not start with "sse:", and must
	// not contain CR or LF.
	Name string
	// Data is the data: payload, UTF-8 text split on CRLF, CR and LF into one
	// data: line each. Publish takes ownership of the slice: the ring retains
	// it until eviction and replay reads it, so a caller that writes to it
	// afterwards changes replay bytes after the UTF-8 and size checks passed.
	// Marshal fresh per publish.
	Data []byte
}

// Position is the hub's replay window at one instant: the epoch, the oldest
// offset the ring can replay (0 when empty) and the newest offset published.
type Position struct {
	Epoch string
	Floor uint64
	Head  uint64
}

// ReplayEvent is one ring entry as Snapshot returns it.
type ReplayEvent struct {
	At     time.Time
	Event  Event
	Offset uint64
}

const (
	reasonSlow     = "slow"
	reasonShutdown = "shutdown"
)

var (
	errShutdown  = errors.New("sse: hub is shut down")
	errClientCap = errors.New("sse: client cap reached")
)

// client is one connected stream. reset is closed at most once, under the hub
// lock, by Publish (which also deletes the record) or by Shutdown (which sets
// closed so no later Publish reaches the record); reason is written before
// the close and read after it.
type client struct {
	ch     chan entry
	reset  chan struct{}
	reason string
	topic  string
	tag    string
	id     uint64
}

type subscription struct {
	c      *client
	replay []entry
	hello  Hello
}

func topicMatches(subTopic, eventTopic string) bool {
	return eventTopic == "" || subTopic == "" || subTopic == eventTopic
}

// Hub is a broadcast fan-out for Server-Sent Events with a replay ring. The
// zero value is not usable; construct with New. Safe for concurrent use by
// any number of publishers and subscribers.
type Hub struct {
	logger       *slog.Logger
	ring         *ring
	clients      map[*client]struct{}
	epoch        string
	cfg          config
	wg           sync.WaitGroup
	mu           sync.Mutex
	seq          uint64
	nextClientID uint64
	flusherOnce  sync.Once
	deadlineOnce sync.Once
	closed       bool
}

// New returns a Hub configured by opts, or an error wrapping ErrConfig for an
// incoherent option set. A nil option is skipped.
func New(opts ...Option) (*Hub, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	if cfg.epoch == "" {
		cfg.epoch = newEpoch()
	}
	return &Hub{
		logger:  cfg.logger,
		ring:    newRing(cfg.ringSize),
		clients: make(map[*client]struct{}),
		epoch:   cfg.epoch,
		cfg:     cfg,
	}, nil
}

// MustNew is New for package-level construction in main; it panics with the
// wrapped ErrConfig message on an incoherent option set.
func MustNew(opts ...Option) *Hub {
	h, err := New(opts...)
	if err != nil {
		panic(err.Error())
	}
	return h
}

// Publish assigns the event the next offset, appends it to the ring and fans
// it out to every matching client without blocking; a client whose buffer is
// full is reset as slow and dropped. It returns the offset, or (0, nil) on a
// nil *Hub and for a valid frame after Shutdown (dropped). A Name outside
// Event's grammar panics; Data that is not UTF-8 returns ErrInvalidUTF8 and an
// encoded frame above MaxFrameBytes returns ErrFrameTooLarge, and neither
// refusal consumes an offset or touches the ring, before or after Shutdown.
func (h *Hub) Publish(ev Event) (uint64, error) {
	if h == nil {
		return 0, nil
	}
	checkEventName(ev.Name, h.cfg.keepaliveEvent)
	if err := checkData(ev.Data); err != nil {
		return 0, err
	}
	lines := splitDataLines(ev.Data)
	base := frameSize(len(ev.Name), lines, 0)
	if screen := base + idLineBytes(1); screen > MaxFrameBytes {
		return 0, tooLarge(screen)
	}

	h.mu.Lock()
	size := base + idLineBytes(digits(h.seq+1))
	if size > MaxFrameBytes {
		h.mu.Unlock()
		return 0, tooLarge(size)
	}
	if h.closed {
		h.mu.Unlock()
		return 0, nil
	}
	now := h.cfg.now()
	h.ring.evict(now, h.cfg.ttl, h.cfg.maxBytes)
	if h.seq == MaxOffset {
		h.mu.Unlock()
		panic("sse: offset would exceed MaxOffset")
	}
	h.seq++
	e := entry{at: now, event: ev, offset: h.seq, size: size}
	h.ring.append(&e)
	h.ring.evict(now, h.cfg.ttl, h.cfg.maxBytes)
	var evicted []*client
	for c := range h.clients {
		if !topicMatches(c.topic, ev.Topic) {
			continue
		}
		select {
		case c.ch <- e:
		default:
			c.reason = reasonSlow
			close(c.reset)
			delete(h.clients, c)
			evicted = append(evicted, c)
		}
	}
	head := h.seq
	h.mu.Unlock()

	for _, c := range evicted {
		h.logger.Warn("sse: evicting slow client", "topic", c.topic, "queued", len(c.ch), "head", head)
	}
	return head, nil
}

// Position returns the replay window after evicting expired entries, so its
// Floor is the one a subscription at this instant would be judged against.
func (h *Hub) Position() Position {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ring.evict(h.cfg.now(), h.cfg.ttl, h.cfg.maxBytes)
	return Position{Epoch: h.epoch, Floor: h.ring.floor(), Head: h.seq}
}

// Snapshot returns a copy of the ring, oldest first, with each entry's Data
// cloned so the caller cannot change replay bytes. A diagnostic surface.
func (h *Hub) Snapshot() []ReplayEvent {
	h.mu.Lock()
	entries := h.ring.since(0, "")
	h.mu.Unlock()
	out := make([]ReplayEvent, len(entries))
	for i, e := range entries {
		ev := e.event
		ev.Data = bytes.Clone(ev.Data)
		out[i] = ReplayEvent{At: e.at, Event: ev, Offset: e.offset}
	}
	return out
}

// ClientCount returns the number of subscribed clients: registered and not yet
// reset as slow. A connection inside its OnConnect hook is counted; one that
// Publish evicted is not, even while its goroutine is still writing the reset.
// After Shutdown it counts the goroutines still draining.
func (h *Hub) ClientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// QueuedFrames returns the sum of frames queued across client channels.
func (h *Hub) QueuedFrames() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for c := range h.clients {
		n += len(c.ch)
	}
	return n
}

// SetMaxClients replaces the client cap at runtime (0 or negative means
// unlimited). Lowering it evicts nobody; admission enforces it.
func (h *Hub) SetMaxClients(n int) {
	h.mu.Lock()
	h.cfg.maxClients = max(n, 0)
	h.mu.Unlock()
}

// Shutdown refuses new subscriptions, signals every client to write its
// sse:reset and waits for the stream goroutines to return, bounded by ctx.
// Per client the wait is the remainder of a running OnConnect hook plus
// max(write timeout, 250ms). Returns ctx.Err() when ctx expires first.
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		for c := range h.clients {
			c.reason = reasonShutdown
			close(c.reset)
		}
	}
	h.mu.Unlock()

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		h.logger.Warn("sse: shutdown deadline expired", "remaining", h.ClientCount())
		return ctx.Err()
	}
}

// subscribe is the one critical section a connection's position rests on:
// eviction, verdict, replay copy, hello, channel, wg.Add and registration all
// happen under the lock, so no later Publish can allocate an offset at or
// below the hello's head and wg.Add cannot race Shutdown's Wait.
func (h *Hub) subscribe(topic, tag string, cur Cursor, present bool) (*subscription, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, errShutdown
	}
	if h.cfg.maxClients > 0 && len(h.clients) >= h.cfg.maxClients {
		h.mu.Unlock()
		return nil, errClientCap
	}
	h.ring.evict(h.cfg.now(), h.cfg.ttl, h.cfg.maxBytes)
	floor, head := h.ring.floor(), h.seq
	verdict, resumed := resolve(cur, present, h.epoch, floor, head, h.ring.len(), h.cfg.replyMax)
	var replay []entry
	if resumed && cur.Offset < head {
		replay = h.ring.since(cur.Offset, topic)
	}
	sub := &subscription{
		c: &client{
			ch:    make(chan entry, h.cfg.clientBuffer),
			reset: make(chan struct{}),
			topic: topic,
			tag:   tag,
			id:    h.nextClientID + 1,
		},
		replay: replay,
		hello: Hello{
			Wire:           Wire,
			Epoch:          h.epoch,
			Floor:          floor,
			Head:           head,
			Resumed:        resumed,
			Verdict:        verdict,
			KeepaliveMS:    int(h.cfg.keepalive.Milliseconds()),
			KeepaliveEvent: h.cfg.keepaliveEvent,
		},
	}
	h.nextClientID++
	h.wg.Add(1)
	h.clients[sub.c] = struct{}{}
	n := len(h.clients)
	h.mu.Unlock()

	h.logger.Debug("sse: client connected",
		"epoch", h.epoch, "verdict", verdict, "head", head, "floor", floor,
		"replayed", len(replay), "topic", topic, "clients", n)
	return sub, nil
}

func (h *Hub) unsubscribe(c *client) {
	defer h.wg.Done()
	h.mu.Lock()
	delete(h.clients, c)
	n := len(h.clients)
	h.mu.Unlock()
	h.logger.Debug("sse: client disconnected", "topic", c.topic, "clients", n)
}
