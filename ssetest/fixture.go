package ssetest

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cplieger/sse"
	"github.com/cplieger/webhttp/v3"
)

const (
	defaultAliveWindow  = 30 * time.Second
	restartShutdownWait = 2 * time.Second
	controlMaxBody      = 64 << 10
	fixtureReplay       = 1024
	fixtureReplayTTL    = 10 * time.Minute
	fixtureReplyMax     = 256
	// Above the client's held_max_frames (2000), so a hold-overflow burst is
	// queued whole and never evicts the reading client as slow.
	fixtureClientBuffer = 4096
)

var errCloseAfter = errors.New("ssetest: close-after frame count reached")

type storeEntry struct {
	status  sse.Status
	version uint64
}

// presenceRow is one tag's fold of the presence feed and the alive route: gone
// when no client is connected or none acknowledged inside the alive window.
type presenceRow struct {
	lastAliveAt time.Time
	connected   int
	gone        bool
}

// Fixture is the controllable SSE server the integration suites drive: a hub
// that /control/restart replaces, an in-memory subject store with one version
// counter per key, a presence table keyed by client tag, a stamped REST stub,
// and flags that stall, delay, fail or close a stream on demand. Every method
// is safe for concurrent use.
type Fixture struct {
	hub            *sse.Hub
	store          map[sse.Subject]*storeEntry
	presence       map[string]*presenceRow
	hubOpts        []sse.Option
	events         []sse.PresenceEvent
	mu             sync.Mutex
	aliveWindow    time.Duration
	delayHello     time.Duration
	delayDigest    time.Duration
	hookSleep      time.Duration
	closeAfter     int
	passWrites     int
	stallEpoch     int
	aliveCount     int
	expiredCount   int
	legacyConnects int
	v3Connects     int
	stall          bool
	hookFail       bool
	restFailOnce   bool
}

// NewFixture builds a fixture whose hub carries WithReplay(1024),
// WithReplayTTL(10m), WithReplyMaxEvents(256), WithClientBuffer(4096) and the
// fixture's own presence hook, then the caller's options. It panics on an
// incoherent option set, which is a fixture bug rather than a runtime condition.
func NewFixture(opts ...sse.Option) *Fixture {
	f := &Fixture{
		store:       make(map[sse.Subject]*storeEntry),
		presence:    make(map[string]*presenceRow),
		aliveWindow: defaultAliveWindow,
	}
	f.hubOpts = append([]sse.Option{
		sse.WithReplay(fixtureReplay),
		sse.WithReplayTTL(fixtureReplayTTL),
		sse.WithReplyMaxEvents(fixtureReplyMax),
		sse.WithClientBuffer(fixtureClientBuffer),
		sse.WithPresence(func(ev sse.PresenceEvent) { f.recordPresence(&ev) }),
	}, opts...)
	f.hub = sse.MustNew(f.hubOpts...)
	return f
}

// Hub returns the current hub; /control/restart replaces it.
func (f *Fixture) Hub() *sse.Hub {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hub
}

// Shutdown shuts the current hub down, ending every stream with sse:reset.
func (f *Fixture) Shutdown(ctx context.Context) error {
	return f.Hub().Shutdown(ctx)
}

// Handler mounts the fixture's routes on a fresh ServeMux.
func (f *Fixture) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", f.handleEvents)
	mux.HandleFunc("POST /digest", f.handleDigest)
	mux.HandleFunc("POST /alive", f.handleAlive)
	mux.HandleFunc("GET /rest/{kind}/{ref}", f.handleRest)
	mux.HandleFunc("POST /control/publish", f.handlePublish)
	mux.HandleFunc("POST /control/stall", f.handleStall)
	mux.HandleFunc("POST /control/restart", f.handleRestart)
	mux.HandleFunc("POST /control/delay-hello", f.durationSetter(&f.delayHello))
	mux.HandleFunc("POST /control/delay-digest", f.durationSetter(&f.delayDigest))
	mux.HandleFunc("POST /control/hook-sleep", f.handleHookSleep)
	mux.HandleFunc("POST /control/mutate", f.handleMutate)
	mux.HandleFunc("POST /control/rest-fail-once", f.handleRestFailOnce)
	mux.HandleFunc("POST /control/close-after", f.handleCloseAfter)
	mux.HandleFunc("POST /control/alive-window", f.durationSetter(&f.aliveWindow))
	mux.HandleFunc("GET /control/state", f.handleState)
	return mux
}

func (f *Fixture) recordPresence(ev *sse.PresenceEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, *ev)
	if ev.Tag == "" {
		return
	}
	row := f.row(ev.Tag)
	switch ev.Kind {
	case sse.PresenceConnected:
		row.connected++
		row.lastAliveAt = ev.At
	case sse.PresenceDisconnected:
		row.connected--
	}
	f.refreshLocked(time.Now())
}

// row returns the tag's row, creating it as gone so its first refresh counts
// an alive transition.
func (f *Fixture) row(tag string) *presenceRow {
	row, ok := f.presence[tag]
	if !ok {
		row = &presenceRow{gone: true}
		f.presence[tag] = row
	}
	return row
}

func (f *Fixture) goneLocked(row *presenceRow, now time.Time) bool {
	return row.connected <= 0 || now.Sub(row.lastAliveAt) > f.aliveWindow
}

// refreshLocked re-evaluates every row's gone predicate and counts the flips.
func (f *Fixture) refreshLocked(now time.Time) {
	for _, row := range f.presence {
		gone := f.goneLocked(row, now)
		if gone == row.gone {
			continue
		}
		row.gone = gone
		if gone {
			f.expiredCount++
		} else {
			f.aliveCount++
		}
	}
}

func (f *Fixture) handleEvents(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	hub := f.hub
	if r.Header.Get("SSE-Wire") != "" {
		f.v3Connects++
	} else {
		f.legacyConnects++
	}
	delay := f.delayHello
	f.mu.Unlock()

	opts := []sse.ServeOption{sse.OnConnect(func(sw *sse.Writer, _ sse.Hello) error { return f.connectHook(sw) })}
	if tag := r.Header.Get("SSE-Client"); tag != "" {
		opts = append(opts, sse.WithClientTag(tag))
	}
	if topic := r.URL.Query().Get("topic"); topic != "" {
		opts = append(opts, sse.WithTopic(topic))
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	hub.Serve(&streamWriter{ResponseWriter: w, f: f, stallEpoch: -1}, r, opts...)
}

// connectHook applies the hook-sleep and hook-fail flags, then writes the
// id-less connected frame.
func (f *Fixture) connectHook(sw *sse.Writer) error {
	f.mu.Lock()
	sleep, fail := f.hookSleep, f.hookFail
	f.mu.Unlock()
	if sleep > 0 {
		time.Sleep(sleep)
	}
	if fail {
		return errors.New("ssetest: hook failure requested")
	}
	return sw.Event("", []byte(`{"type":"connected"}`))
}

// streamWriter applies the stall and close-after flags to one stream. A stall
// discards writes after the configured pass count while flushes still reach
// the socket, so headers and any passed bytes leave and nothing else does;
// close-after hijacks and closes the connection once N frames have been sent.
type streamWriter struct {
	http.ResponseWriter
	f          *Fixture
	stallEpoch int
	passed     int
	frames     int
	closed     bool
}

func (s *streamWriter) Write(b []byte) (int, error) {
	if s.closed {
		return 0, errCloseAfter
	}
	f := s.f
	f.mu.Lock()
	stall, pass, epoch, closeAfter := f.stall, f.passWrites, f.stallEpoch, f.closeAfter
	f.mu.Unlock()
	if stall {
		if epoch != s.stallEpoch {
			s.stallEpoch, s.passed = epoch, 0
		}
		if s.passed >= pass {
			return len(b), nil
		}
		s.passed++
	}
	n, err := s.ResponseWriter.Write(b)
	if err != nil {
		return n, err
	}
	if isFrame(b) {
		s.frames++
		if closeAfter > 0 && s.frames >= closeAfter {
			s.Flush()
			s.closed = true
			if conn, _, hijackErr := http.NewResponseController(s.ResponseWriter).Hijack(); hijackErr == nil {
				_ = conn.Close()
			}
			return n, errCloseAfter
		}
	}
	return n, nil
}

// Flush forwards to the underlying writer; after a hijack the flush is a
// no-op error the stream loop already treats as terminal.
func (s *streamWriter) Flush() {
	if s.closed {
		return
	}
	_ = http.NewResponseController(s.ResponseWriter).Flush()
}

func (s *streamWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// isFrame reports whether one write carries an event: the retry field and
// comments do not count.
func isFrame(b []byte) bool {
	return len(b) > 0 && (strings.HasPrefix(string(b), "id: ") || strings.HasPrefix(string(b), "event: ") || strings.HasPrefix(string(b), "data: "))
}

func (f *Fixture) handleDigest(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	hub, delay := f.hub, f.delayDigest
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	hub.DigestHandler(f.resolve).ServeHTTP(w, r)
}

func (f *Fixture) resolve(_ context.Context, held []sse.Held) ([]sse.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]sse.State, 0, len(held))
	for _, hd := range held {
		st := sse.State{Subject: hd.Subject, Status: sse.StatusGone}
		if e, ok := f.store[hd.Subject]; ok {
			st.Status = e.status
			if e.status == sse.StatusCurrent {
				st.Version = strconv.FormatUint(e.version, 10)
			}
		}
		out = append(out, st)
	}
	return out, nil
}

func (f *Fixture) handleAlive(w http.ResponseWriter, r *http.Request) {
	tag := r.Header.Get("SSE-Client")
	if !webhttp.ValidRequestID(tag) {
		webhttp.WriteError(w, r, http.StatusBadRequest, "alive_invalid", "SSE-Client header missing or malformed")
		return
	}
	f.mu.Lock()
	now := time.Now()
	f.row(tag).lastAliveAt = now
	f.refreshLocked(now)
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type restResponse struct {
	Kind    string `json:"kind"`
	Ref     string `json:"ref"`
	Version string `json:"version"`
	Epoch   string `json:"epoch"`
	Payload string `json:"payload"`
}

// handleRest answers the stamped GET, creating an unknown subject at version 1
// so a client can load a subject before anything mutated it.
func (f *Fixture) handleRest(w http.ResponseWriter, r *http.Request) {
	key := sse.Subject{Kind: r.PathValue("kind"), Ref: r.PathValue("ref")}
	f.mu.Lock()
	if f.restFailOnce {
		f.restFailOnce = false
		f.mu.Unlock()
		webhttp.WriteError(w, r, http.StatusInternalServerError, "rest_fail_once", "failure requested by /control/rest-fail-once")
		return
	}
	e, ok := f.store[key]
	if !ok {
		e = &storeEntry{version: 1}
		f.store[key] = e
	}
	entry := *e
	epoch := f.hub.Position().Epoch
	f.mu.Unlock()
	switch entry.status {
	case sse.StatusGone:
		webhttp.WriteError(w, r, http.StatusNotFound, "rest_gone", "subject is gone")
		return
	case sse.StatusForbidden:
		webhttp.WriteError(w, r, http.StatusForbidden, "rest_forbidden", "subject is forbidden")
		return
	}
	version := strconv.FormatUint(entry.version, 10)
	webhttp.WriteJSON(w, restResponse{Kind: key.Kind, Ref: key.Ref, Version: version, Epoch: epoch, Payload: "v" + version})
}

// decodeControl decodes a control body; an empty body is the zero request.
func decodeControl(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.ContentLength == 0 {
		return true
	}
	if err := webhttp.DecodeJSONInto(w, r, v, controlMaxBody); err != nil {
		webhttp.WriteError(w, r, http.StatusBadRequest, "control_invalid", err.Error())
		return false
	}
	return true
}

type publishRequest struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Data  string `json:"data"`
	Count int    `json:"count"`
	Size  int    `json:"size"`
}

type publishResponse struct {
	Head    string   `json:"head"`
	Offsets []string `json:"offsets"`
}

func (f *Fixture) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req publishRequest
	if !decodeControl(w, r, &req) {
		return
	}
	hub := f.Hub()
	count := max(req.Count, 1)
	offsets := make([]string, 0, count)
	for range count {
		data := []byte(req.Data)
		if req.Size > 0 {
			data = dataOfFrameSize(req.Size, req.Name, hub.Position())
		}
		off, err := hub.Publish(sse.Event{Topic: req.Topic, Name: req.Name, Data: data})
		if err != nil {
			webhttp.WriteError(w, r, http.StatusUnprocessableEntity, "publish_refused", err.Error())
			return
		}
		offsets = append(offsets, strconv.FormatUint(off, 10))
	}
	webhttp.WriteJSON(w, publishResponse{Offsets: offsets, Head: strconv.FormatUint(hub.Position().Head, 10)})
}

// dataOfFrameSize builds a one-line payload whose encoded frame is exactly
// size bytes at the next offset: the id line is "id: <epoch>:<offset>\n", the
// optional event line, "data: " plus the payload plus LF, and the blank line.
// A size below the overhead yields an empty payload, never a negative count.
func dataOfFrameSize(size int, name string, pos sse.Position) []byte {
	next := strconv.FormatUint(pos.Head+1, 10)
	overhead := len("id: ") + len(pos.Epoch) + 1 + len(next) + 1
	if name != "" {
		overhead += len("event: ") + len(name) + 1
	}
	overhead += len("data: ") + 1 + 1
	return []byte(strings.Repeat("x", max(size-overhead, 0)))
}

type stallRequest struct {
	On         bool `json:"on"`
	PassWrites int  `json:"pass_writes"`
}

func (f *Fixture) handleStall(w http.ResponseWriter, r *http.Request) {
	var req stallRequest
	if !decodeControl(w, r, &req) {
		return
	}
	f.mu.Lock()
	f.stall, f.passWrites = req.On, max(req.PassWrites, 0)
	f.stallEpoch++
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type restartResponse struct {
	Epoch string `json:"epoch"`
}

// handleRestart replaces the hub, so the epoch changes and every open stream
// receives sse:reset shutdown from the old one.
func (f *Fixture) handleRestart(w http.ResponseWriter, r *http.Request) {
	next := sse.MustNew(f.hubOpts...)
	f.mu.Lock()
	old := f.hub
	f.hub = next
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), restartShutdownWait)
	defer cancel()
	_ = old.Shutdown(ctx)
	webhttp.WriteJSON(w, restartResponse{Epoch: next.Position().Epoch})
}

type durationRequest struct {
	MS int64 `json:"ms"`
}

// durationSetter answers a {ms} body by storing the duration under the mutex.
func (f *Fixture) durationSetter(field *time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req durationRequest
		if !decodeControl(w, r, &req) {
			return
		}
		f.mu.Lock()
		*field = time.Duration(max(req.MS, 0)) * time.Millisecond
		f.refreshLocked(time.Now())
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
}

type hookSleepRequest struct {
	MS   int64 `json:"ms"`
	Fail bool  `json:"fail"`
}

func (f *Fixture) handleHookSleep(w http.ResponseWriter, r *http.Request) {
	var req hookSleepRequest
	if !decodeControl(w, r, &req) {
		return
	}
	f.mu.Lock()
	f.hookSleep = time.Duration(max(req.MS, 0)) * time.Millisecond
	f.hookFail = req.Fail
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type mutateRequest struct {
	Kind   string     `json:"kind"`
	Ref    string     `json:"ref"`
	Status sse.Status `json:"status"`
}

type mutateResponse struct {
	Kind    string     `json:"kind"`
	Ref     string     `json:"ref"`
	Version string     `json:"version"`
	Status  sse.Status `json:"status"`
}

// handleMutate bumps the subject's version, creating it at 1, and sets its
// status to the request's (empty restores current). It publishes nothing; a
// test that wants the frame calls /control/publish.
func (f *Fixture) handleMutate(w http.ResponseWriter, r *http.Request) {
	var req mutateRequest
	if !decodeControl(w, r, &req) {
		return
	}
	if req.Kind == "" {
		webhttp.WriteError(w, r, http.StatusBadRequest, "control_invalid", "kind is required")
		return
	}
	key := sse.Subject{Kind: req.Kind, Ref: req.Ref}
	f.mu.Lock()
	e, ok := f.store[key]
	if !ok {
		e = &storeEntry{}
		f.store[key] = e
	}
	e.version++
	e.status = req.Status
	resp := mutateResponse{Kind: key.Kind, Ref: key.Ref, Version: strconv.FormatUint(e.version, 10), Status: e.status}
	f.mu.Unlock()
	webhttp.WriteJSON(w, resp)
}

func (f *Fixture) handleRestFailOnce(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	f.restFailOnce = true
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type closeAfterRequest struct {
	Frames int `json:"frames"`
}

func (f *Fixture) handleCloseAfter(w http.ResponseWriter, r *http.Request) {
	var req closeAfterRequest
	if !decodeControl(w, r, &req) {
		return
	}
	f.mu.Lock()
	f.closeAfter = max(req.Frames, 0)
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type stateResponse struct {
	Position       positionJSON    `json:"position"`
	Presence       []presenceJSON  `json:"presence"`
	Events         []presenceEvent `json:"events"`
	Transitions    transitionsJSON `json:"transitions"`
	Clients        int             `json:"clients"`
	Queued         int             `json:"queued"`
	LegacyConnects int             `json:"legacy_connects"`
	V3Connects     int             `json:"v3_connects"`
}

type positionJSON struct {
	Epoch string `json:"epoch"`
	Floor string `json:"floor"`
	Head  string `json:"head"`
}

type presenceJSON struct {
	LastAliveAt time.Time `json:"last_alive_at"`
	Tag         string    `json:"tag"`
	Connected   int       `json:"connected"`
	Gone        bool      `json:"gone"`
}

type transitionsJSON struct {
	Alive   int `json:"alive"`
	Expired int `json:"expired"`
}

type presenceEvent struct {
	At       time.Time         `json:"at"`
	Kind     sse.PresenceKind  `json:"kind"`
	Topic    string            `json:"topic"`
	Verdict  sse.Verdict       `json:"verdict"`
	Cause    sse.PresenceCause `json:"cause"`
	Write    string            `json:"write"`
	Epoch    string            `json:"epoch"`
	Tag      string            `json:"tag"`
	ClientID uint64            `json:"client_id"`
}

func (f *Fixture) handleState(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	now := time.Now()
	f.refreshLocked(now)
	hub := f.hub
	pos := hub.Position()
	resp := stateResponse{
		Clients:  hub.ClientCount(),
		Queued:   hub.QueuedFrames(),
		Position: positionJSON{Epoch: pos.Epoch, Floor: strconv.FormatUint(pos.Floor, 10), Head: strconv.FormatUint(pos.Head, 10)},
		Presence: make([]presenceJSON, 0, len(f.presence)),
		Events:   make([]presenceEvent, 0, len(f.events)),
		Transitions: transitionsJSON{
			Alive:   f.aliveCount,
			Expired: f.expiredCount,
		},
		LegacyConnects: f.legacyConnects,
		V3Connects:     f.v3Connects,
	}
	for tag, row := range f.presence {
		resp.Presence = append(resp.Presence, presenceJSON{Tag: tag, Connected: row.connected, LastAliveAt: row.lastAliveAt, Gone: row.gone})
	}
	for i := range f.events {
		ev := &f.events[i]
		resp.Events = append(resp.Events, presenceEvent{
			At: ev.At, Kind: ev.Kind, Topic: ev.Topic, Verdict: ev.Verdict, Cause: ev.Cause,
			Write: ev.Write, Epoch: ev.Epoch, Tag: ev.Tag, ClientID: ev.ClientID,
		})
	}
	f.mu.Unlock()
	slices.SortFunc(resp.Presence, func(a, b presenceJSON) int { return strings.Compare(a.Tag, b.Tag) })
	webhttp.WriteJSON(w, resp)
}
