package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// startServer wraps a hub in an httptest server on Go 1.27's in-memory
// network, which keeps every goroutine inside the synctest bubble. Client() is
// called eagerly because srv.URL stays "" until the first Client call.
func startServer(t *testing.T, h *Hub, opts ...ServeOption) *httptest.Server {
	t.Helper()
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.Serve(w, r, opts...)
	}))
	_ = srv.Client()
	return srv
}

// openStream connects to the SSE endpoint and returns the response plus a
// line scanner. Callers close the response body.
func openStream(t *testing.T, srv *httptest.Server, url string, header http.Header) (*http.Response, *bufio.Scanner) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 2*MaxFrameBytes)
	return resp, sc
}

// readUntil scans lines until pred returns true, failing after a bounded
// number of lines so a hung stream cannot hang the test.
func readUntil(t *testing.T, sc *bufio.Scanner, pred func(line string) bool) []string {
	t.Helper()
	var lines []string
	for range 400 {
		if !sc.Scan() {
			t.Fatalf("stream ended early (%v); lines so far: %q", sc.Err(), lines)
		}
		line := sc.Text()
		lines = append(lines, line)
		if pred(line) {
			return lines
		}
	}
	t.Fatalf("predicate never satisfied; lines: %q", lines)
	return nil
}

// readHello scans through the handshake and returns the decoded hello plus
// every line read up to and including its data: line.
func readHello(t *testing.T, sc *bufio.Scanner) (Hello, []string) {
	t.Helper()
	lines := readUntil(t, sc, func(l string) bool { return l == "event: "+helloEvent })
	if !sc.Scan() {
		t.Fatalf("stream ended after the hello event line: %v", sc.Err())
	}
	data, ok := strings.CutPrefix(sc.Text(), "data: ")
	if !ok {
		t.Fatalf("line after the hello event = %q, want a data: line", sc.Text())
	}
	lines = append(lines, sc.Text())
	var hello Hello
	if err := json.Unmarshal([]byte(data), &hello); err != nil {
		t.Fatalf("hello data %q does not decode: %v", data, err)
	}
	return hello, lines
}

// requireClients asserts the registered client count exactly, inside a
// synctest bubble: Wait returns once every other goroutine is durably blocked,
// so registration has provably completed.
func requireClients(t *testing.T, h *Hub, want int) {
	t.Helper()
	synctest.Wait()
	if got := h.ClientCount(); got != want {
		t.Fatalf("ClientCount() = %d, want %d", got, want)
	}
}

// awaitClients polls the client count on the bubble's fake clock, for a
// departure the transport signals a moment after the body is closed.
func awaitClients(t *testing.T, h *Hub, want int) {
	t.Helper()
	for range 100 {
		synctest.Wait()
		if h.ClientCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("ClientCount() = %d after 1s, want %d", h.ClientCount(), want)
}

// dataLines returns the data: payloads among lines.
func dataLines(lines []string) []string {
	var out []string
	for _, l := range lines {
		if d, ok := strings.CutPrefix(l, "data: "); ok {
			out = append(out, d)
		}
	}
	return out
}

func TestServe_headers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t)
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
			t.Errorf("Content-Type = %q, want text/event-stream", got)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-cache, no-transform" {
			t.Errorf("Cache-Control = %q, want %q", got, "no-cache, no-transform")
		}
		if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
			t.Errorf("X-Accel-Buffering = %q, want no", got)
		}
		readHello(t, sc)
	})
}

// flushCounter counts flushes so a test can see how many the handshake took.
type flushCounter struct {
	http.ResponseWriter
	flushes atomic.Int32
}

func (f *flushCounter) Flush() {
	f.flushes.Add(1)
	http.NewResponseController(f.ResponseWriter).Flush()
}

func (f *flushCounter) Unwrap() http.ResponseWriter { return f.ResponseWriter }

func TestServe_retryThenHelloThenReplayInOneFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t, withEpoch(testEpoch), WithReplay(8), WithReplayTTL(replayTTL))
		for i := 1; i <= 3; i++ {
			if _, err := h.Publish(Event{Data: fmt.Appendf(nil, "e%d", i)}); err != nil {
				t.Fatal(err)
			}
		}
		var flushesAtHook atomic.Int32
		counter := &flushCounter{}
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			counter.ResponseWriter = w
			h.Serve(counter, r, OnConnect(func(*Writer, Hello) error {
				flushesAtHook.Store(counter.flushes.Load())
				return nil
			}))
		}))
		_ = srv.Client()
		resp, sc := openStream(t, srv, srv.URL, http.Header{"Last-Event-ID": {testEpoch + ":1"}})
		defer resp.Body.Close()

		hello, lines := readHello(t, sc)
		if lines[0] != "retry: 1500" || lines[1] != "" || lines[2] != "event: sse:hello" {
			t.Errorf("stream opened with %q, want retry: 1500, a blank line, then the hello", lines[:3])
		}
		if !hello.Resumed || hello.Verdict != VerdictResumed {
			t.Fatalf("hello = %+v, want resumed", hello)
		}
		replay := readUntil(t, sc, func(l string) bool { return l == "data: e3" })
		if got := dataLines(replay); len(got) != 2 || got[0] != "e2" || got[1] != "e3" {
			t.Errorf("replay after the hello = %v, want [e2 e3]", got)
		}
		requireClients(t, h, 1)
		if got := flushesAtHook.Load(); got != 1 {
			t.Errorf("flushes before OnConnect ran = %d, want 1 (retry, hello and replay leave together)", got)
		}
	})
}

func TestServe_helloJSON(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t, withEpoch(testEpoch), WithReplay(8), WithReplayTTL(replayTTL), WithKeepalive(20*time.Second))
		if _, err := h.Publish(Event{Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		_, lines := readHello(t, sc)
		data, _ := strings.CutPrefix(lines[len(lines)-1], "data: ")
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{
			"wire":            "1",
			"epoch":           `"` + testEpoch + `"`,
			"floor":           `"1"`,
			"head":            `"1"`,
			"resumed":         "false",
			"verdict":         `"fresh"`,
			"keepalive_ms":    "20000",
			"keepalive_event": `"sse:keepalive"`,
		}
		for k, v := range want {
			if got := string(raw[k]); got != v {
				t.Errorf("hello[%q] = %s, want %s", k, got, v)
			}
		}
		if len(raw) != len(want) {
			t.Errorf("hello has %d fields %v, want exactly %d", len(raw), raw, len(want))
		}
	})
}

func TestServe_freshHelloHasNoReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t, WithReplay(8), WithReplayTTL(replayTTL))
		for range 3 {
			if _, err := h.Publish(Event{Data: []byte("old")}); err != nil {
				t.Fatal(err)
			}
		}
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		hello, _ := readHello(t, sc)
		if hello.Verdict != VerdictFresh || hello.Resumed || hello.Head != 3 || hello.Floor != 1 {
			t.Errorf("hello = %+v, want fresh, not resumed, head 3, floor 1", hello)
		}
		requireClients(t, h, 1)
		if _, err := h.Publish(Event{Data: []byte("live")}); err != nil {
			t.Fatal(err)
		}
		lines := readUntil(t, sc, func(l string) bool { return l == "data: live" })
		if got := dataLines(lines); len(got) != 1 {
			t.Errorf("frames after a fresh hello = %v, want only the live one", got)
		}
	})
}

func TestServe_resumedReplaysAfterCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t, withEpoch(testEpoch), WithReplay(8), WithReplayTTL(replayTTL))
		for i := 1; i <= 4; i++ {
			if _, err := h.Publish(Event{Name: "n", Data: fmt.Appendf(nil, "e%d", i)}); err != nil {
				t.Fatal(err)
			}
		}
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, http.Header{"Last-Event-ID": {testEpoch + ":2"}})
		defer resp.Body.Close()
		hello, _ := readHello(t, sc)
		if !hello.Resumed || hello.Head != 4 {
			t.Fatalf("hello = %+v, want resumed at head 4", hello)
		}
		lines := readUntil(t, sc, func(l string) bool { return l == "data: e4" })
		want := []string{"", "id: " + testEpoch + ":3", "event: n", "data: e3", "", "id: " + testEpoch + ":4", "event: n", "data: e4"}
		if strings.Join(lines, "\n") != strings.Join(want, "\n") {
			t.Errorf("replay lines = %q, want %q", lines, want)
		}
	})
}

func TestServe_epochChangedReplaysNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t, withEpoch(testEpoch), WithReplay(8), WithReplayTTL(replayTTL))
		for range 3 {
			if _, err := h.Publish(Event{Data: []byte("old")}); err != nil {
				t.Fatal(err)
			}
		}
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, http.Header{"Last-Event-ID": {"00000000000000ff:1"}})
		defer resp.Body.Close()
		hello, _ := readHello(t, sc)
		if hello.Verdict != VerdictEpochChanged || hello.Resumed || hello.Epoch != testEpoch {
			t.Errorf("hello = %+v, want epoch_changed carrying the hub's epoch", hello)
		}
		requireClients(t, h, 1)
		if _, err := h.Publish(Event{Data: []byte("live")}); err != nil {
			t.Fatal(err)
		}
		lines := readUntil(t, sc, func(l string) bool { return l == "data: live" })
		if got := dataLines(lines); len(got) != 1 {
			t.Errorf("frames after an epoch_changed hello = %v, want only the live one", got)
		}
	})
}

func TestServe_invalidCursorHello(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithLogger(logger), WithReplay(8), WithReplayTTL(replayTTL))
		if _, err := h.Publish(Event{Data: []byte("old")}); err != nil {
			t.Fatal(err)
		}
		srv := startServer(t, h)
		const raw = "SECRET-LOOKING-VALUE-42"
		resp, sc := openStream(t, srv, srv.URL, http.Header{"Last-Event-ID": {raw}})
		defer resp.Body.Close()
		hello, _ := readHello(t, sc)
		if hello.Verdict != VerdictCursorInvalid || hello.Resumed {
			t.Errorf("hello = %+v, want cursor_invalid", hello)
		}
		requireClients(t, h, 1)
		log := logged.String()
		if !strings.Contains(log, "sse: Last-Event-ID rejected") || !strings.Contains(log, fmt.Sprintf("length=%d", len(raw))) {
			t.Errorf("log = %q, want the rejection with length=%d", log, len(raw))
		}
		if strings.Contains(log, raw) {
			t.Errorf("log = %q, carries the raw header value", log)
		}
	})
}

func TestServe_topicFilter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t)
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h.Serve(w, r, WithTopic(r.URL.Query().Get("topic")))
		}))
		_ = srv.Client()
		resp, sc := openStream(t, srv, srv.URL+"?topic=a", nil)
		defer resp.Body.Close()
		readHello(t, sc)
		requireClients(t, h, 1)
		for _, ev := range []Event{{Topic: "b", Data: []byte("skip")}, {Topic: "a", Data: []byte("take")}, {Data: []byte("broadcast")}} {
			if _, err := h.Publish(ev); err != nil {
				t.Fatal(err)
			}
		}
		lines := readUntil(t, sc, func(l string) bool { return l == "data: broadcast" })
		if got := dataLines(lines); len(got) != 2 || got[0] != "take" {
			t.Errorf("delivered = %v, want [take broadcast]", got)
		}
	})
}

func TestServe_keepaliveNamedByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = 20 * time.Millisecond
		h := mustNew(t, WithKeepalive(interval))
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		hello, _ := readHello(t, sc)
		if hello.KeepaliveEvent != "sse:keepalive" || hello.KeepaliveMS != 20 {
			t.Errorf("hello = %+v, want keepalive_event sse:keepalive and keepalive_ms 20", hello)
		}
		start := time.Now()
		lines := readUntil(t, sc, func(l string) bool { return l == "data: {}" })
		if got := time.Since(start); got != interval {
			t.Errorf("first keepalive after %v, want exactly %v", got, interval)
		}
		if lines[len(lines)-2] != "event: sse:keepalive" {
			t.Errorf("keepalive lines = %q, want event: sse:keepalive then data: {}", lines)
		}
		for _, l := range lines {
			if strings.HasPrefix(l, "id:") {
				t.Errorf("keepalive carried %q, want no id: line", l)
			}
		}
		readUntil(t, sc, func(l string) bool { return l == "data: {}" })
		if got := time.Since(start); got != 2*interval {
			t.Errorf("second keepalive after %v, want exactly %v", got, 2*interval)
		}
	})
}

func TestServe_keepaliveCommentForm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = 20 * time.Millisecond
		h := mustNew(t, WithKeepalive(interval), WithKeepaliveEvent(""))
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		hello, _ := readHello(t, sc)
		if hello.KeepaliveEvent != "" {
			t.Errorf("hello.KeepaliveEvent = %q, want the empty string for the comment form", hello.KeepaliveEvent)
		}
		start := time.Now()
		lines := readUntil(t, sc, func(l string) bool { return l == ": keepalive" })
		if got := time.Since(start); got != interval {
			t.Errorf("first keepalive after %v, want exactly %v", got, interval)
		}
		for _, l := range lines {
			if strings.HasPrefix(l, "event:") && l != "event: sse:hello" {
				t.Errorf("comment-form stream carried %q, want no keepalive event line", l)
			}
		}
	})
}

func TestServe_keepaliveNeverEntersRing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = 20 * time.Millisecond
		h := mustNew(t, withEpoch(testEpoch), WithReplay(2), WithReplayTTL(replayTTL), WithKeepalive(interval))
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		requireClients(t, h, 1)
		for range 3 {
			readUntil(t, sc, func(l string) bool { return l == "data: {}" })
		}
		if got := h.Snapshot(); len(got) != 0 {
			t.Errorf("Snapshot() after 3 keepalives = %+v, want the ring untouched", got)
		}
		if got := h.Position(); got.Floor != 0 || got.Head != 0 {
			t.Errorf("Position() after 3 keepalives = %+v, want Floor 0 Head 0", got)
		}
		if _, err := h.Publish(Event{Data: []byte("real")}); err != nil {
			t.Fatal(err)
		}
		lines := readUntil(t, sc, func(l string) bool { return l == "data: real" })
		if lines[len(lines)-2] != "id: "+testEpoch+":1" {
			t.Errorf("first real frame lines = %q, want id: %s:1", lines, testEpoch)
		}
	})
}

func TestServe_onConnectWritesIdlessFrames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t, withEpoch(testEpoch))
		var hookHello Hello
		srv := startServer(t, h, OnConnect(func(w *Writer, hello Hello) error {
			hookHello = hello
			if err := w.Event("", []byte("unnamed")); err != nil {
				return err
			}
			if _, err := h.Publish(Event{Data: []byte("during-hook")}); err != nil {
				return err
			}
			return w.Event("state", []byte("named"))
		}))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		hello, _ := readHello(t, sc)
		lines := readUntil(t, sc, func(l string) bool { return l == "data: during-hook" })
		want := []string{"", "data: unnamed", "", "event: state", "data: named", "", "id: " + testEpoch + ":1", "data: during-hook"}
		if strings.Join(lines, "\n") != strings.Join(want, "\n") {
			t.Errorf("lines after the hello = %q, want %q (idless hook frames, then the publish made during the hook)", lines, want)
		}
		requireClients(t, h, 1)
		if hookHello != hello {
			t.Errorf("hook received %+v, client received %+v, want the same Hello", hookHello, hello)
		}
	})
}

func TestServe_hookErrorEndsAfterHello(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithLogger(logger))
		srv := startServer(t, h, OnConnect(func(*Writer, Hello) error { return errors.New("boom") }))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		synctest.Wait()
		var tail []string
		for sc.Scan() {
			tail = append(tail, sc.Text())
		}
		if len(tail) > 1 {
			t.Errorf("stream carried %q after the hello, want it to end", tail)
		}
		if got := h.ClientCount(); got != 0 {
			t.Errorf("ClientCount() after the hook failed = %d, want 0", got)
		}
		if !strings.Contains(logged.String(), "sse: OnConnect failed") || !strings.Contains(logged.String(), "error=boom") {
			t.Errorf("log = %q, want the OnConnect failure with error=boom", logged)
		}
	})
}

func TestServe_clientTagGrammar(t *testing.T) {
	tests := []struct {
		name      string
		tag       string
		wantTag   string
		wantClass string
	}{
		{name: "Valid", tag: "profile_A-1", wantTag: "profile_A-1"},
		{name: "SixtyFour", tag: strings.Repeat("a", 64), wantTag: strings.Repeat("a", 64)},
		{name: "SixtyFive", tag: strings.Repeat("a", 65), wantClass: "too_long"},
		{name: "Slash", tag: "a/b", wantClass: "char"},
		{name: "Empty", tag: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger, logged := captureLog()
				h := mustNew(t, WithLogger(logger))
				srv := startServer(t, h, WithClientTag(tt.tag))
				resp, sc := openStream(t, srv, srv.URL, nil)
				defer resp.Body.Close()
				readHello(t, sc)
				requireClients(t, h, 1)
				h.mu.Lock()
				var got string
				for c := range h.clients {
					got = c.tag
				}
				h.mu.Unlock()
				if got != tt.wantTag {
					t.Errorf("WithClientTag(%q) stored %q, want %q", tt.tag, got, tt.wantTag)
				}
				warns := strings.Count(logged.String(), "sse: client tag rejected")
				if tt.wantClass == "" {
					if warns != 0 {
						t.Errorf("WithClientTag(%q) logged %d rejections, want 0", tt.tag, warns)
					}
					return
				}
				if warns != 1 {
					t.Fatalf("WithClientTag(%q) logged %d rejections, want 1; log = %s", tt.tag, warns, logged)
				}
				if !strings.Contains(logged.String(), fmt.Sprintf("length=%d", len(tt.tag))) || !strings.Contains(logged.String(), "class="+tt.wantClass) {
					t.Errorf("rejection log = %q, want length=%d class=%s", logged, len(tt.tag), tt.wantClass)
				}
				if tt.tag != "" && strings.Contains(logged.String(), tt.tag) {
					t.Errorf("rejection log = %q, carries the tag bytes", logged)
				}
			})
		})
	}
}

func TestServe_maxClients503(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithMaxClients(1), WithLogger(logger))
		srv := startServer(t, h)
		resp1, sc := openStream(t, srv, srv.URL, nil)
		defer resp1.Body.Close()
		readHello(t, sc)
		requireClients(t, h, 1)

		resp2, sc2 := openStream(t, srv, srv.URL, nil)
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("second client status = %d, want 503", resp2.StatusCode)
		}
		if sc2.Scan(); !strings.Contains(sc2.Text(), "sse_unavailable") {
			t.Errorf("body = %q, want the sse_unavailable envelope", sc2.Text())
		}
		if !strings.Contains(logged.String(), "sse: client cap reached") || !strings.Contains(logged.String(), "clients=1") || !strings.Contains(logged.String(), "max=1") {
			t.Errorf("log = %q, want the cap Info with clients=1 max=1", logged)
		}
	})
}

// noFlushRecorder implements http.ResponseWriter without http.Flusher.
type noFlushRecorder struct {
	header http.Header
	status int
}

func (r *noFlushRecorder) Header() http.Header         { return r.header }
func (r *noFlushRecorder) Write(b []byte) (int, error) { return len(b), nil }
func (r *noFlushRecorder) WriteHeader(code int)        { r.status = code }

func TestServe_noFlusher500LogsOnce(t *testing.T) {
	logger, logged := captureLog()
	h1 := mustNew(t, WithLogger(logger))
	h2 := mustNew(t, WithLogger(logger))
	for _, h := range []*Hub{h1, h1, h2} {
		rec := &noFlushRecorder{header: make(http.Header)}
		h.Serve(rec, httptest.NewRequest(http.MethodGet, "/events", http.NoBody))
		if rec.status != http.StatusInternalServerError {
			t.Errorf("status = %d, want 500", rec.status)
		}
	}
	if got := strings.Count(logged.String(), "sse: response writer cannot flush"); got != 2 {
		t.Errorf("flusher Error logged %d times across two hubs (one served twice), want 2; log = %s", got, logged)
	}
	if !strings.Contains(logged.String(), "*sse.noFlushRecorder") {
		t.Errorf("log = %q, want the writer type named", logged)
	}
}

// unwrapOnlyWriter exposes streaming only via Unwrap, never implementing
// http.Flusher itself.
type unwrapOnlyWriter struct{ http.ResponseWriter }

func (u *unwrapOnlyWriter) Unwrap() http.ResponseWriter { return u.ResponseWriter }

func TestServe_flushesThroughUnwrapChain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := mustNew(t)
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h.Serve(&unwrapOnlyWriter{ResponseWriter: w}, r)
		}))
		_ = srv.Client()
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (a flusher reachable via Unwrap must stream)", resp.StatusCode)
		}
		readHello(t, sc)
		requireClients(t, h, 1)
		if _, err := h.Publish(Event{Data: []byte("via-unwrap")}); err != nil {
			t.Fatal(err)
		}
		readUntil(t, sc, func(l string) bool { return l == "data: via-unwrap" })
	})
}

func TestServe_readDeadlineUnsupportedDebug(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := httptest.NewRecorder()
	h.Serve(rec, httptest.NewRequest(http.MethodGet, "/events", http.NoBody).WithContext(ctx))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (an unclearable read deadline must not refuse the stream)", rec.Code)
	}
	if body := rec.Body.String(); !strings.HasPrefix(body, "retry: 1500\n\nevent: sse:hello\n") {
		t.Errorf("body = %q, want the retry field and the hello", body)
	}
	if !strings.Contains(logged.String(), "level=DEBUG msg=\"sse: clear read deadline\"") {
		t.Errorf("log = %q, want the read-deadline Debug line", logged)
	}
	if got := h.ClientCount(); got != 0 {
		t.Errorf("ClientCount() after the pre-cancelled request = %d, want 0", got)
	}
}
