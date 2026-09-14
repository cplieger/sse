package sse

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// presenceLog records every PresenceEvent a hub delivers.
type presenceLog struct {
	mu     sync.Mutex
	events []PresenceEvent
}

func (p *presenceLog) record(ev PresenceEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
}

func (p *presenceLog) all() []PresenceEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PresenceEvent(nil), p.events...)
}

func (p *presenceLog) ofKind(kind PresenceKind) []PresenceEvent {
	var out []PresenceEvent
	for _, ev := range p.all() {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

// awaitDisconnected polls on the bubble's fake clock until n disconnected
// events have been delivered.
func awaitDisconnected(t *testing.T, p *presenceLog, n int) []PresenceEvent {
	t.Helper()
	for range 100 {
		synctest.Wait()
		if got := p.ofKind(PresenceDisconnected); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("disconnected events = %d after 1s, want %d; all = %+v", len(p.ofKind(PresenceDisconnected)), n, p.all())
	return nil
}

// failingWriter fails every Write once armed, so a stream sees a write error
// on a socket that never closed.
type failingWriter struct {
	http.ResponseWriter
	fail *atomic.Bool
}

var errInjectedWrite = errors.New("injected write failure")

func (f *failingWriter) Write(b []byte) (int, error) {
	if f.fail.Load() {
		return 0, errInjectedWrite
	}
	return f.ResponseWriter.Write(b)
}

func (f *failingWriter) Unwrap() http.ResponseWriter { return f.ResponseWriter }

func startFailingServer(t *testing.T, h *Hub, fail *atomic.Bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.Serve(&failingWriter{ResponseWriter: w, fail: fail}, r)
	}))
	_ = srv.Client()
	return srv
}

func TestPresence_connectedCarriesVerdict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p presenceLog
		h := mustNew(t, withEpoch(testEpoch), WithPresence(p.record), WithReplay(8), WithReplayTTL(replayTTL))
		if _, err := h.Publish(Event{Data: []byte("e1")}); err != nil {
			t.Fatal(err)
		}
		srv := startServer(t, h, WithTopic("chat"))
		resp, sc := openStream(t, srv, srv.URL, http.Header{"Last-Event-ID": {testEpoch + ":1"}})
		defer resp.Body.Close()
		readHello(t, sc)
		synctest.Wait()

		got := p.all()
		if len(got) != 1 {
			t.Fatalf("events after the hello = %+v, want exactly one connected", got)
		}
		ev := got[0]
		if ev.Kind != PresenceConnected || ev.Verdict != VerdictResumed || ev.Cause != "" || ev.Write != "" {
			t.Errorf("connected = %+v, want Kind connected, Verdict resumed, no cause", ev)
		}
		if ev.Epoch != testEpoch || ev.Topic != "chat" || ev.ClientID == 0 || ev.At.IsZero() {
			t.Errorf("connected = %+v, want the epoch, the topic, a non-zero ClientID and At", ev)
		}
	})
}

func TestPresence_closedOnContextEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p presenceLog
		h := mustNew(t, WithPresence(p.record))
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		readHello(t, sc)
		resp.Body.Close()

		got := awaitDisconnected(t, &p, 1)
		if got[0].Cause != PresenceClosed || got[0].Write != "" || got[0].Verdict != "" {
			t.Errorf("disconnected = %+v, want Cause closed with no Write and no Verdict", got[0])
		}
		all := p.all()
		if len(all) != 2 || all[0].ClientID != all[1].ClientID {
			t.Errorf("events = %+v, want one connected and one disconnected sharing a ClientID", all)
		}
	})
}

func TestPresence_deadNamesTheFailingWrite(t *testing.T) {
	tests := []struct {
		name      string
		provoke   func(t *testing.T, h *Hub)
		wantCause PresenceCause
		wantWrite string
	}{
		{
			name: "keepalive",
			provoke: func(t *testing.T, h *Hub) {
				t.Helper()
				time.Sleep(h.cfg.keepalive)
			},
			wantCause: PresenceDead,
			wantWrite: "keepalive",
		},
		{
			name: "frame",
			provoke: func(t *testing.T, h *Hub) {
				t.Helper()
				if _, err := h.Publish(Event{Data: []byte("live")}); err != nil {
					t.Fatal(err)
				}
			},
			wantCause: PresenceDead,
			wantWrite: "frame",
		},
		{
			name: "reset",
			provoke: func(t *testing.T, h *Hub) {
				t.Helper()
				if err := h.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			},
			wantCause: PresenceShutdown,
			wantWrite: "reset",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var p presenceLog
				var fail atomic.Bool
				h := mustNew(t, WithPresence(p.record), WithKeepalive(time.Second))
				srv := startFailingServer(t, h, &fail)
				resp, sc := openStream(t, srv, srv.URL, nil)
				defer resp.Body.Close()
				readHello(t, sc)
				requireClients(t, h, 1)
				fail.Store(true)
				tc.provoke(t, h)

				got := awaitDisconnected(t, &p, 1)
				if got[0].Cause != tc.wantCause || got[0].Write != tc.wantWrite {
					t.Errorf("disconnected after a failed %s write = %+v, want Cause %q Write %q", tc.name, got[0], tc.wantCause, tc.wantWrite)
				}
			})
		})
	}
}

func TestPresence_evictedAndShutdownCauses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := captureLog()
		var p presenceLog
		h := mustNew(t, WithLogger(logger), WithPresence(p.record), WithClientBuffer(2))
		srv := startServer(t, h, OnConnect(func(*Writer, Hello) error {
			for range 3 {
				if _, err := h.Publish(Event{Data: []byte("burst")}); err != nil {
					return err
				}
			}
			return nil
		}))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		readUntil(t, sc, func(l string) bool { return l == `data: {"reason":"slow"}` })
		got := awaitDisconnected(t, &p, 1)
		if got[0].Cause != PresenceEvicted || got[0].Write != "" {
			t.Errorf("disconnected after a slow reset = %+v, want Cause evicted with no Write", got[0])
		}

		var p2 presenceLog
		h2 := mustNew(t, WithPresence(p2.record))
		srv2 := startServer(t, h2)
		resp2, sc2 := openStream(t, srv2, srv2.URL, nil)
		defer resp2.Body.Close()
		readHello(t, sc2)
		requireClients(t, h2, 1)
		if err := h2.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		readUntil(t, sc2, func(l string) bool { return l == `data: {"reason":"shutdown"}` })
		got2 := awaitDisconnected(t, &p2, 1)
		if got2[0].Cause != PresenceShutdown || got2[0].Write != "" {
			t.Errorf("disconnected after Shutdown = %+v, want Cause shutdown with no Write", got2[0])
		}
	})
}

func TestPresence_exactlyOneDisconnectedPerConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p presenceLog
		h := mustNew(t, WithPresence(p.record))
		srv := startServer(t, h)
		const n = 32
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, http.NoBody)
				if err != nil {
					t.Error(err)
					return
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Errorf("client %d: %v", i, err)
					return
				}
				defer resp.Body.Close()
				sc := bufio.NewScanner(resp.Body)
				readHello(t, sc)
				if i%2 == 0 {
					return
				}
				for sc.Scan() {
					if sc.Text() == `data: {"reason":"shutdown"}` {
						return
					}
				}
			})
		}
		for range 50 {
			synctest.Wait()
			if len(p.ofKind(PresenceConnected)) == n {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := h.Publish(Event{Data: []byte("mid-stream")}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if err := h.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		wg.Wait()
		awaitDisconnected(t, &p, n)

		connected := map[uint64]int{}
		disconnected := map[uint64]int{}
		for _, ev := range p.all() {
			switch ev.Kind {
			case PresenceConnected:
				connected[ev.ClientID]++
			case PresenceDisconnected:
				disconnected[ev.ClientID]++
			}
		}
		if len(connected) != n {
			t.Errorf("distinct connected ClientIDs = %d, want %d", len(connected), n)
		}
		for id, c := range connected {
			if c != 1 || disconnected[id] != 1 {
				t.Errorf("client %d: connected %d, disconnected %d, want 1 and 1", id, c, disconnected[id])
			}
		}
		for id := range disconnected {
			if connected[id] == 0 {
				t.Errorf("client %d: disconnected without a connected", id)
			}
		}
	})
}

func TestPresence_slowHookDelaysOnlyItsStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p presenceLog
		h := mustNew(t, WithPresence(func(ev PresenceEvent) {
			if ev.Kind == PresenceDisconnected {
				time.Sleep(time.Second)
			}
			p.record(ev)
		}))
		srv := startServer(t, h)
		respA, scA := openStream(t, srv, srv.URL, nil)
		readHello(t, scA)
		respB, scB := openStream(t, srv, srv.URL, nil)
		defer respB.Body.Close()
		readHello(t, scB)
		requireClients(t, h, 2)

		start := time.Now()
		respA.Body.Close()
		awaitClients(t, h, 1)
		if _, err := h.Publish(Event{Data: []byte("to-b")}); err != nil {
			t.Fatal(err)
		}
		readUntil(t, scB, func(l string) bool { return l == "data: to-b" })
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Errorf("B received its frame after %v, want before A's 1s hook returned", elapsed)
		}
		if got := len(p.ofKind(PresenceDisconnected)); got != 0 {
			t.Errorf("disconnected events recorded while A's hook still sleeps = %d, want 0", got)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if got := p.ofKind(PresenceDisconnected); len(got) != 1 || got[0].Cause != PresenceClosed {
			t.Errorf("disconnected events once the hook returned = %+v, want one closed", got)
		}
	})
}

func TestPresence_panicIsRecoveredAndLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithLogger(logger), WithPresence(func(ev PresenceEvent) {
			panic("hook exploded on " + ev.Kind)
		}))
		srv := startServer(t, h, WithTopic("t1"))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		requireClients(t, h, 1)
		if _, err := h.Publish(Event{Data: []byte("still-live")}); err != nil {
			t.Fatal(err)
		}
		readUntil(t, sc, func(l string) bool { return l == "data: still-live" })
		if err := h.Shutdown(t.Context()); err != nil {
			t.Errorf("Shutdown after a panicking hook = %v, want nil", err)
		}
		awaitClients(t, h, 0)
		log := logged.String()
		if got := strings.Count(log, "level=ERROR msg=\"sse: presence hook panicked\""); got != 2 {
			t.Errorf("Error lines for the panicking hook = %d, want 2 (connected and disconnected); log = %s", got, log)
		}
		if !strings.Contains(log, "kind=connected") || !strings.Contains(log, "kind=disconnected") || !strings.Contains(log, "cause=shutdown") || !strings.Contains(log, "topic=t1") {
			t.Errorf("log = %q, want the event's kind, cause and topic on the Error lines", log)
		}
	})
}

// noFlushWriter is a ResponseWriter without Flush, the deployment defect Serve
// answers with 500.
type noFlushWriter struct {
	hdr  http.Header
	code int
}

func (w *noFlushWriter) Header() http.Header         { return w.hdr }
func (w *noFlushWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *noFlushWriter) WriteHeader(code int)        { w.code = code }

func TestPresence_noEventBeforeHello(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := captureLog()
		var p presenceLog
		h := mustNew(t, WithLogger(logger), WithPresence(p.record), WithMaxClients(1))
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		requireClients(t, h, 1)

		refused, _ := openStream(t, srv, srv.URL, nil)
		refused.Body.Close()
		if refused.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("second connection status = %d, want 503", refused.StatusCode)
		}
		nf := &noFlushWriter{hdr: http.Header{}}
		h.Serve(nf, httptest.NewRequest(http.MethodGet, "/events", http.NoBody))
		if nf.code != http.StatusInternalServerError {
			t.Fatalf("non-flushing writer status = %d, want 500", nf.code)
		}
		synctest.Wait()
		if got := p.all(); len(got) != 1 || got[0].Kind != PresenceConnected {
			t.Errorf("events = %+v, want only the first client's connected (a 503 and a 500 emit nothing)", got)
		}
	})
}

func TestPresence_tagIsCarried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		var p presenceLog
		h := mustNew(t, WithLogger(logger), WithPresence(p.record))
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h.Serve(w, r, WithClientTag(r.Header.Get("SSE-Client")))
		}))
		_ = srv.Client()

		resp, sc := openStream(t, srv, srv.URL, http.Header{"SSE-Client": {"browser-1"}})
		readHello(t, sc)
		resp.Body.Close()
		awaitDisconnected(t, &p, 1)
		for _, ev := range p.all() {
			if ev.Tag != "browser-1" {
				t.Errorf("%s event Tag = %q, want browser-1", ev.Kind, ev.Tag)
			}
		}

		bad, scBad := openStream(t, srv, srv.URL, http.Header{"SSE-Client": {"not valid!"}})
		readHello(t, scBad)
		bad.Body.Close()
		awaitDisconnected(t, &p, 2)
		for _, ev := range p.all()[2:] {
			if ev.Tag != "" {
				t.Errorf("%s event Tag with an invalid header = %q, want empty", ev.Kind, ev.Tag)
			}
		}
		if !strings.Contains(logged.String(), "sse: client tag rejected") || !strings.Contains(logged.String(), "class=char") || strings.Contains(logged.String(), "not valid!") {
			t.Errorf("log = %q, want the tag Warn naming class=char and never the bytes", logged)
		}
	})
}
