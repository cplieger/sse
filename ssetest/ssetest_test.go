package ssetest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cplieger/sse"
)

// testClient has no timeout because it reads streams; every request carries
// the test's context.
var testClient = &http.Client{}

// logBuffer collects slog output from stream goroutines that may still be
// logging when the test reads it.
type logBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func captureLog() (*slog.Logger, *logBuffer) {
	buf := &logBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

func mustHub(t *testing.T, opts ...sse.Option) *sse.Hub {
	t.Helper()
	h, err := sse.New(opts...)
	if err != nil {
		t.Fatalf("Setup: sse.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})
	return h
}

// get opens a stream with the given headers. The caller closes the body.
func get(t *testing.T, url string, header http.Header) *http.Response {
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
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestReadFrames_parsesEncoderOutput(t *testing.T) {
	h := mustHub(t, sse.WithReplay(8), sse.WithReplayTTL(10*time.Minute))
	if _, err := h.Publish(sse.Event{Data: []byte("line one\r\nline two\nline three")}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Publish(sse.Event{Name: "named", Data: []byte(`{"n":2}`)}); err != nil {
		t.Fatal(err)
	}
	epoch := h.Position().Epoch
	url := Serve(t, h)
	resp := get(t, url, http.Header{"Last-Event-ID": {epoch + ":0"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	frames, err := ReadFrames(resp.Body, 4)
	if err != nil {
		t.Fatalf("ReadFrames = %v, want nil; frames so far %+v", err, frames)
	}
	want := []Frame{
		{Event: "retry", Data: "1500"},
		{Event: "sse:hello"},
		{ID: epoch + ":1", Data: "line one\nline two\nline three"},
		{ID: epoch + ":2", Event: "named", Data: `{"n":2}`},
	}
	for i, w := range want {
		got := frames[i]
		if w.Event == "sse:hello" {
			var hello sse.Hello
			if err := json.Unmarshal([]byte(got.Data), &hello); err != nil || hello.Epoch != epoch || !hello.Resumed {
				t.Errorf("frame %d = %+v, want a resumed hello for %s (decode error %v)", i, got, epoch, err)
			}
			continue
		}
		if got != w {
			t.Errorf("frame %d = %+v, want %+v", i, got, w)
		}
	}
}

func TestReadFrames_skipsCommentsAndEmptyDispatch(t *testing.T) {
	input := ": keepalive\n\n\n\nevent: ping\n\ndata: a\ndata:b\n\nid: x\ndata: last"
	frames, err := ReadFrames(strings.NewReader(input), 0)
	if err != nil {
		t.Fatalf("ReadFrames = %v, want nil", err)
	}
	want := []Frame{{Data: "a\nb"}}
	if len(frames) != len(want) || frames[0] != want[0] {
		t.Errorf("ReadFrames(%q) = %+v, want %+v (comments, blank lines, a data-less event and an unterminated frame dispatch nothing)", input, frames, want)
	}
	if _, err := ReadFrames(strings.NewReader("data: a\n\n"), 2); err == nil {
		t.Error("ReadFrames asked for 2 frames of a 1-frame stream = nil error, want io.ErrUnexpectedEOF")
	}
}

func TestRecorder_hidesDeadlineSetters(t *testing.T) {
	logger, logged := captureLog()
	h := mustHub(t, sse.WithLogger(logger))
	rec := NewRecorder()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	h.Serve(rec, httptest.NewRequest(http.MethodGet, "/events", http.NoBody).WithContext(ctx))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(logged.String(), "sse: response writer does not support write deadlines") || !strings.Contains(logged.String(), "sse: clear read deadline") {
		t.Errorf("log = %q, want both deadline setters reported unsupported", logged)
	}
	frames, err := rec.Frames()
	if err != nil {
		t.Fatalf("Frames = %v, want nil", err)
	}
	if len(frames) != 2 || frames[0].Event != "retry" || frames[1].Event != "sse:hello" {
		t.Errorf("Frames = %+v, want the retry field then the hello", frames)
	}
	if err := http.NewResponseController(rec).SetWriteDeadline(time.Now()); err == nil {
		t.Error("SetWriteDeadline through a ResponseController = nil, want http.ErrNotSupported")
	}
}
