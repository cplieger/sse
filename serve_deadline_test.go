package sse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// The wedged-peer tests run on a real loopback server and a real clock: the
// in-memory network's connection buffer is unbounded, so a write to it can
// never block, and TLS plus HTTP/2 need a real listener. Every wait below is a
// deadline-bounded poll or a write the kernel bounds.

const (
	realKeepalive    = 100 * time.Millisecond
	realWriteTimeout = 200 * time.Millisecond
	// The wedge tests publish wedgeFrames MiB before they act on the blocked
	// write, and under the race detector on a loaded runner that loop alone can
	// outlast 200ms, after which the peer is already gone and there is nothing
	// left to reset. The deadline they run under is wide enough to still be
	// pending when Shutdown reaches the stream.
	wedgeWriteTimeout = 1500 * time.Millisecond
	// wedgeFrames of one MiB each exceeds the loopback send and receive buffers
	// together (measured: the writer blocks after three), so a peer that stops
	// reading wedges the writer well inside the write timeout.
	wedgeFrames = 24
)

// startRealServer serves h on a loopback listener and tears the streams down
// before the server closes, since Close waits for active handlers.
func startRealServer(t *testing.T, srv *httptest.Server, h *Hub) *httptest.Server {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
		srv.CloseClientConnections()
		srv.Close()
	})
	return srv
}

func newRealServer(t *testing.T, h *Hub, opts ...ServeOption) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.Serve(w, r, opts...)
	}))
	return startRealServer(t, srv, h)
}

// pollUntil retries cond every 10ms and fails closed at the deadline.
func pollUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForWedge polls until the stream goroutine has stopped pulling frames
// off its channel, which on a peer that is not reading means its write blocked.
func waitForWedge(t *testing.T, h *Hub, queued int) {
	t.Helper()
	last := -1
	pollUntil(t, 5*time.Second, "the stream write to wedge", func() bool {
		n := h.QueuedFrames()
		stable := n == last && n < queued
		last = n
		return stable
	})
}

func TestServe_slowClientReceivesReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithLogger(logger), WithClientBuffer(4))
		srv := startServer(t, h, OnConnect(func(*Writer, Hello) error {
			for i := range 5 {
				if _, err := h.Publish(Event{Data: []byte("burst")}); err != nil {
					t.Errorf("Publish #%d in the hook: %v", i+1, err)
				}
			}
			return nil
		}))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		lines := readUntil(t, sc, func(l string) bool { return l == `data: {"reason":"slow"}` })
		if lines[len(lines)-2] != "event: sse:reset" {
			t.Errorf("reset lines = %q, want event: sse:reset then the reason", lines)
		}
		if got := dataLines(lines); len(got) != 1 {
			t.Errorf("frames between the hello and the reset = %v, want none (the queue is dropped, not drained)", got)
		}
		synctest.Wait()
		if sc.Scan() && sc.Text() != "" || sc.Scan() {
			t.Errorf("stream carried %q after the reset, want EOF", sc.Text())
		}
		if got := h.ClientCount(); got != 0 {
			t.Errorf("ClientCount() = %d, want 0", got)
		}
		if !strings.Contains(logged.String(), "sse: evicting slow client") || !strings.Contains(logged.String(), "reason=slow") {
			t.Errorf("log = %q, want the eviction Warn and the reset Debug with reason=slow", logged)
		}
	})
}

func TestServe_wedgedPeerWriteTimeout(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger), WithKeepalive(realKeepalive), WithWriteTimeout(wedgeWriteTimeout), WithClientBuffer(wedgeFrames))
	srv := newRealServer(t, h)
	resp, sc := openStream(t, srv, srv.URL, nil)
	defer resp.Body.Close()
	readHello(t, sc)
	pollUntil(t, 5*time.Second, "the client to register", func() bool { return h.ClientCount() == 1 })

	payload := dataOfFrameSize(MaxFrameBytes, 2)
	for i := range wedgeFrames {
		if _, err := h.Publish(Event{Data: payload}); err != nil {
			t.Fatalf("Publish #%d error = %v", i+1, err)
		}
	}
	waitForWedge(t, h, wedgeFrames)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err := h.Shutdown(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Shutdown against a wedged peer = %v, want nil", err)
	}
	if bound := max(wedgeWriteTimeout, resetWriteTimeout) + time.Second; elapsed > bound {
		t.Errorf("Shutdown took %v, want within max(writeTimeout, resetWriteTimeout) plus scheduling (%v)", elapsed, bound)
	}
	log := logged.String()
	if !strings.Contains(log, "write_timeout=true") || !strings.Contains(log, "reset_unwritten=true") || !strings.Contains(log, "reason=shutdown") {
		t.Errorf("log = %q, want write_timeout=true reset_unwritten=true reason=shutdown", log)
	}
}

func TestServe_h2IdleGapDoesNotExpireDeadline(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger), WithKeepalive(realKeepalive), WithWriteTimeout(realWriteTimeout))
	hookErr := make(chan error, 1)
	// Keepalives beat faster than the write timeout by construction, so the one
	// place a stream can idle for 2 × writeTimeout with no write is inside the
	// hook; a deadline left armed by the handshake flush fires there.
	unstarted := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.Serve(w, r, OnConnect(func(sw *Writer, _ Hello) error {
			time.Sleep(2 * realWriteTimeout)
			err := sw.Event("after-gap", []byte("ok"))
			hookErr <- err
			return err
		}))
	}))
	unstarted.EnableHTTP2 = true
	unstarted.StartTLS()
	srv := startRealServer(t, unstarted, h)

	resp, sc := openStream(t, srv, srv.URL, nil)
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("Setup: response protocol = %s, want HTTP/2", resp.Proto)
	}
	readHello(t, sc)
	readUntil(t, sc, func(l string) bool { return l == "data: ok" })
	if err := <-hookErr; err != nil {
		t.Fatalf("Writer.Event after a %v idle gap = %v, want nil", 2*realWriteTimeout, err)
	}
	pollUntil(t, 5*time.Second, "the client to register", func() bool { return h.ClientCount() == 1 })
	if _, err := h.Publish(Event{Data: []byte("live")}); err != nil {
		t.Fatal(err)
	}
	readUntil(t, sc, func(l string) bool { return l == "data: live" })
	if strings.Contains(logged.String(), "does not support write deadlines") {
		t.Errorf("log = %q, the h2 writer must support write deadlines for this test to mean anything", logged)
	}
}

func TestServe_survivesServerReadTimeout(t *testing.T) {
	h := mustNew(t, WithKeepalive(realKeepalive))
	unstarted := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.Serve(w, r)
	}))
	unstarted.Config.ReadTimeout = 200 * time.Millisecond
	unstarted.Start()
	srv := startRealServer(t, unstarted, h)

	resp, sc := openStream(t, srv, srv.URL, nil)
	defer resp.Body.Close()
	readHello(t, sc)
	start := time.Now()
	beats := 0
	for time.Since(start) < 1100*time.Millisecond {
		readUntil(t, sc, func(l string) bool { return l == "data: {}" })
		beats++
	}
	if beats < 8 {
		t.Errorf("read %d keepalives in over a second at a %v cadence, want at least 8", beats, realKeepalive)
	}
	if got := h.ClientCount(); got != 1 {
		t.Errorf("ClientCount() after 1s under ReadTimeout 200ms = %d, want 1", got)
	}
}

// resetOnWrite publishes through the hub from inside the first frame write, so
// the client's reset closes mid-batch with frames still queued, and counts the
// frames written after that moment.
type resetOnWrite struct {
	*httptest.ResponseRecorder
	trigger   func()
	triggered bool
	after     int
}

func (r *resetOnWrite) Write(b []byte) (int, error) {
	if strings.HasPrefix(string(b), "id: ") {
		if r.triggered {
			r.after++
		} else {
			r.triggered = true
			r.trigger()
		}
	}
	return r.ResponseRecorder.Write(b)
}

func TestServe_atMostOneFrameAfterReset(t *testing.T) {
	for run := range 200 {
		logger, _ := captureLog()
		h := mustNew(t, WithLogger(logger), WithClientBuffer(4))
		sub, err := h.subscribe("", "", Cursor{}, false)
		if err != nil {
			t.Fatal(err)
		}
		for range 4 {
			if _, err := h.Publish(Event{Data: []byte("queued")}); err != nil {
				t.Fatal(err)
			}
		}
		rec := &resetOnWrite{ResponseRecorder: httptest.NewRecorder()}
		rec.trigger = func() {
			// One publish fills the slot the drain just freed; the next overflows
			// and evicts this client under the hub lock.
			for range 2 {
				if _, err := h.Publish(Event{Data: []byte("overflow")}); err != nil {
					t.Errorf("run %d: Publish in trigger: %v", run, err)
				}
			}
		}
		sw := &Writer{w: rec, rc: http.NewResponseController(rec), keepalive: h.cfg.keepaliveEvent, timeout: h.cfg.writeTimeout}
		h.stream(t.Context(), sw, sub.c)
		h.unsubscribe(sub.c)

		if !rec.triggered {
			t.Fatalf("run %d: the drain never wrote a frame", run)
		}
		if !isClosed(sub.c.reset) || sub.c.reason != reasonSlow {
			t.Fatalf("run %d: reset closed = %v reason = %q, want closed as slow", run, isClosed(sub.c.reset), sub.c.reason)
		}
		if rec.after > 1 {
			t.Fatalf("run %d: %d frames written after reset closed, want at most 1", run, rec.after)
		}
		if body := rec.Body.String(); !strings.HasSuffix(body, "event: sse:reset\ndata: {\"reason\":\"slow\"}\n\n") {
			t.Fatalf("run %d: body = %q, want it to end with the slow reset", run, body)
		}
	}
}

func TestServe_writeDeadlineUnsupportedWarnsOnce(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 2 {
		rec := httptest.NewRecorder()
		h.Serve(rec, httptest.NewRequest(http.MethodGet, "/events", http.NoBody).WithContext(ctx))
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200 (a writer without deadlines still streams)", rec.Code)
		}
	}
	if got := strings.Count(logged.String(), "sse: response writer does not support write deadlines"); got != 1 {
		t.Errorf("deadline Warn logged %d times over two connections, want 1; log = %s", got, logged)
	}
	if !strings.Contains(logged.String(), "writer=*httptest.ResponseRecorder") {
		t.Errorf("log = %q, want the writer type named", logged)
	}
}

func TestServe_hookWriteTimeoutEndsConnection(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger), WithKeepalive(realKeepalive), WithWriteTimeout(realWriteTimeout))
	hookErr := make(chan error, 1)
	payload := dataOfFrameSize(MaxFrameBytes, 2)
	srv := newRealServer(t, h, OnConnect(func(sw *Writer, _ Hello) error {
		for range wedgeFrames {
			if err := sw.Event("", payload); err != nil {
				hookErr <- err
				return err
			}
		}
		hookErr <- nil
		return nil
	}))
	resp, sc := openStream(t, srv, srv.URL, nil)
	defer resp.Body.Close()
	readHello(t, sc)

	select {
	case err := <-hookErr:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Writer.Event against a wedged peer = %v, want a deadline error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the hook did not return within 5s; the write never timed out")
	}
	pollUntil(t, 5*time.Second, "Serve to return", func() bool { return h.ClientCount() == 0 })
	if !strings.Contains(logged.String(), "write=hook") || !strings.Contains(logged.String(), "write_timeout=true") {
		t.Errorf("log = %q, want the hook write failure with write_timeout=true", logged)
	}
}

func TestServe_peerCloseEndsStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithLogger(logger))
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		readHello(t, sc)
		requireClients(t, h, 1)
		resp.Body.Close()
		awaitClients(t, h, 0)
		if !strings.Contains(logged.String(), "sse: peer disconnected") {
			t.Errorf("log = %q, want the peer-disconnected Debug", logged)
		}
	})
}

func TestServe_resetUnwrittenOnWedgedPeer(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger), WithKeepalive(realKeepalive), WithWriteTimeout(wedgeWriteTimeout), WithClientBuffer(4))
	srv := newRealServer(t, h)
	resp, sc := openStream(t, srv, srv.URL, nil)
	defer resp.Body.Close()
	readHello(t, sc)
	pollUntil(t, 5*time.Second, "the client to register", func() bool { return h.ClientCount() == 1 })

	payload := dataOfFrameSize(MaxFrameBytes, 2)
	for i := range wedgeFrames {
		if _, err := h.Publish(Event{Data: payload}); err != nil {
			t.Fatalf("Publish #%d error = %v", i+1, err)
		}
	}
	if got := h.ClientCount(); got != 0 {
		t.Fatalf("ClientCount() after %d publishes into a 4-frame buffer = %d, want 0 (evicted as slow)", wedgeFrames, got)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown waiting on the evicted goroutine = %v, want nil", err)
	}
	log := logged.String()
	if !strings.Contains(log, "reason=slow") || !strings.Contains(log, "reset_unwritten=true") || !strings.Contains(log, "write_timeout=true") {
		t.Errorf("log = %q, want reason=slow reset_unwritten=true write_timeout=true", log)
	}
}
