package sse

import "time"

// entry is one published event with its offset, publish time and encoded size.
type entry struct {
	at     time.Time
	event  Event
	offset uint64
	size   int
}

// ring is a fixed-capacity replay buffer bounded by count, age and bytes. Not
// concurrency-safe on its own; the Hub guards it with its mutex.
type ring struct {
	buf   []entry
	head  int // next write position
	n     int // occupied count
	bytes int // sum of the occupied entries' sizes
}

func newRing(capacity int) *ring {
	return &ring{buf: make([]entry, capacity)}
}

// append stores e, dropping the oldest entry when the ring is full.
func (r *ring) append(e *entry) {
	if len(r.buf) == 0 {
		return
	}
	if r.n == len(r.buf) {
		r.dropOldest()
	}
	r.buf[r.head] = *e
	r.head = (r.head + 1) % len(r.buf)
	r.n++
	r.bytes += e.size
}

// evict drops entries older than ttl (when ttl > 0), then the oldest entries
// while the byte sum exceeds maxBytes.
func (r *ring) evict(now time.Time, ttl time.Duration, maxBytes int) {
	for r.n > 0 && ttl > 0 && now.Sub(r.buf[r.oldest()].at) > ttl {
		r.dropOldest()
	}
	for r.n > 0 && r.bytes > maxBytes {
		r.dropOldest()
	}
}

func (r *ring) dropOldest() {
	i := r.oldest()
	r.bytes -= r.buf[i].size
	r.buf[i] = entry{}
	r.n--
}

func (r *ring) oldest() int {
	return (r.head - r.n + len(r.buf)) % len(r.buf)
}

// floor returns the oldest surviving offset, or 0 when empty.
func (r *ring) floor() uint64 {
	if r.n == 0 {
		return 0
	}
	return r.buf[r.oldest()].offset
}

func (r *ring) len() int {
	return r.n
}

// since returns a copy of the entries with an offset above since that a client
// filtered to topic would receive, oldest first.
func (r *ring) since(since uint64, topic string) []entry {
	var out []entry
	if r.n == 0 {
		return out
	}
	start := r.oldest()
	for i := range r.n {
		e := r.buf[(start+i)%len(r.buf)]
		if e.offset > since && topicMatches(topic, e.event.Topic) {
			out = append(out, e)
		}
	}
	return out
}
