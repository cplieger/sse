package ssetest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/sse"
)

const pollTimeout = 5 * time.Second

// fixtureServer serves a fresh Fixture over loopback and tears it down after
// the test.
func fixtureServer(t *testing.T, opts ...sse.Option) (*Fixture, string) {
	t.Helper()
	logger, _ := captureLog()
	f := NewFixture(append([]sse.Option{sse.WithLogger(logger)}, opts...)...)
	srv := httptest.NewServer(f.Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = f.Shutdown(ctx)
		srv.CloseClientConnections()
		srv.Close()
	})
	return f, srv.URL
}

// post sends a JSON body, decodes the JSON answer, if any, into out, and
// returns the status code.
func post(t *testing.T, url, body string, header http.Header, out any) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Set(k, v)
		}
	}
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("POST %s answered %d with non-JSON %q: %v", url, resp.StatusCode, raw, err)
		}
	}
	return resp.StatusCode
}

func control(t *testing.T, base, route, body string) {
	t.Helper()
	status := post(t, base+"/control/"+route, body, nil, nil)
	if status != http.StatusNoContent && status != http.StatusOK {
		t.Fatalf("POST /control/%s %s = %d, want 204 or 200", route, body, status)
	}
}

func state(t *testing.T, base string) stateResponse {
	t.Helper()
	resp := get(t, base+"/control/state", nil)
	defer resp.Body.Close()
	var st stateResponse
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("GET /control/state: %v", err)
	}
	return st
}

// restStatus performs a GET whose body the test does not read.
func restStatus(t *testing.T, url string) int {
	t.Helper()
	resp := get(t, url, nil)
	defer resp.Body.Close()
	return resp.StatusCode
}

// openEvents connects to /events and consumes the retry field and the hello.
// It returns the reader that consumed them so the caller reads the rest of the
// same stream through it; the caller closes the returned body.
func openEvents(t *testing.T, base string, header http.Header) (*http.Response, *FrameReader, sse.Hello) {
	t.Helper()
	resp := get(t, base+"/events", header)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /events = %d, want 200", resp.StatusCode)
	}
	stream := NewFrameReader(resp.Body)
	frames, err := stream.Read(2)
	if err != nil {
		t.Fatalf("reading the handshake: %v (frames %+v)", err, frames)
	}
	var hello sse.Hello
	if frames[1].Event != "sse:hello" {
		t.Fatalf("second frame = %+v, want the hello", frames[1])
	}
	if err := json.Unmarshal([]byte(frames[1].Data), &hello); err != nil {
		t.Fatalf("hello %q: %v", frames[1].Data, err)
	}
	return resp, stream, hello
}

func pollUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, pollTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// frameFeed reads a stream on one goroutine and hands out frames one at a
// time, so a bounded wait for the next frame never leaves a second reader on
// the body.
type frameFeed struct {
	frames chan Frame
	err    chan error
}

func feedFrames(stream *FrameReader) *frameFeed {
	f := &frameFeed{frames: make(chan Frame), err: make(chan error, 1)}
	go func() {
		defer close(f.frames)
		for {
			frames, err := stream.Read(1)
			if err != nil {
				f.err <- err
				return
			}
			f.frames <- frames[0]
		}
	}()
	return f
}

// next returns the next frame or context.DeadlineExceeded when none arrives
// within d.
func (f *frameFeed) next(d time.Duration) (Frame, error) {
	select {
	case fr, ok := <-f.frames:
		if !ok {
			return Frame{}, <-f.err
		}
		return fr, nil
	case <-time.After(d):
		return Frame{}, context.DeadlineExceeded
	}
}

func TestFixture_controlRoutes(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		f, base := fixtureServer(t)
		var resp publishResponse
		if status := post(t, base+"/control/publish", `{"data":"hi","count":2,"name":"n","topic":"tp"}`, nil, &resp); status != http.StatusOK {
			t.Fatalf("publish status = %d, want 200", status)
		}
		if len(resp.Offsets) != 2 || resp.Offsets[0] != "1" || resp.Offsets[1] != "2" || resp.Head != "2" {
			t.Errorf("publish response = %+v, want offsets [1 2] head 2", resp)
		}
		if got := f.Hub().Snapshot(); len(got) != 2 || got[1].Event.Topic != "tp" || got[1].Event.Name != "n" || string(got[1].Event.Data) != "hi" {
			t.Errorf("ring = %+v, want two frames on topic tp named n carrying hi", got)
		}
		if status := post(t, base+"/control/publish", `{"size":1048576}`, nil, &resp); status != http.StatusOK {
			t.Errorf("publish of an exactly MaxFrameBytes frame = %d, want 200", status)
		}
		var refused map[string]any
		if status := post(t, base+"/control/publish", `{"size":1048577}`, nil, &refused); status != http.StatusUnprocessableEntity || refused["code"] != "publish_refused" {
			t.Errorf("publish of MaxFrameBytes+1 = %d %v, want 422 publish_refused", status, refused)
		}
		if got := f.Hub().Position().Head; got != 3 {
			t.Errorf("head after the refused publish = %d, want 3", got)
		}
	})

	t.Run("stall", func(t *testing.T) {
		_, base := fixtureServer(t)
		resp, stream, _ := openEvents(t, base, nil)
		defer resp.Body.Close()
		if _, err := stream.Read(1); err != nil {
			t.Fatalf("reading the connected frame: %v", err)
		}
		feed := feedFrames(stream)
		control(t, base, "stall", `{"on":true}`)
		control(t, base, "publish", `{"data":"hidden"}`)
		if fr, err := feed.next(300 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("frame during a stall = %+v (%v), want none", fr, err)
		}
		control(t, base, "stall", `{"on":false}`)
		control(t, base, "publish", `{"data":"visible"}`)
		fr, err := feed.next(pollTimeout)
		if err != nil || fr.Data != "visible" {
			t.Errorf("frame after the stall lifted = %+v (%v), want the visible frame and not the hidden one", fr, err)
		}
	})

	t.Run("stall passes the first writes", func(t *testing.T) {
		_, base := fixtureServer(t)
		control(t, base, "stall", `{"on":true,"pass_writes":1}`)
		resp := get(t, base+"/events", nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (headers leave even under a stall)", resp.StatusCode)
		}
		feed := feedFrames(NewFrameReader(resp.Body))
		if fr, err := feed.next(pollTimeout); err != nil || fr.Event != "retry" {
			t.Fatalf("first frame = %+v (%v), want the retry field to pass", fr, err)
		}
		if fr, err := feed.next(300 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second frame = %+v (%v), want the hello held back by the stall", fr, err)
		}
	})

	t.Run("restart", func(t *testing.T) {
		_, base := fixtureServer(t)
		resp, stream, hello := openEvents(t, base, nil)
		defer resp.Body.Close()
		var restarted restartResponse
		post(t, base+"/control/restart", ``, nil, &restarted)
		if restarted.Epoch == "" || restarted.Epoch == hello.Epoch {
			t.Errorf("restart epoch = %q, want a new epoch (old %q)", restarted.Epoch, hello.Epoch)
		}
		frames, err := stream.Read(2)
		if err != nil || frames[1].Event != "sse:reset" || frames[1].Data != `{"reason":"shutdown"}` {
			t.Errorf("old stream after restart = %+v (%v), want the connected frame then sse:reset shutdown", frames, err)
		}
		if st := state(t, base); st.Position.Epoch != restarted.Epoch || st.Position.Head != "0" {
			t.Errorf("state after restart = %+v, want the new epoch at head 0", st.Position)
		}
	})

	t.Run("delay-hello", func(t *testing.T) {
		_, base := fixtureServer(t)
		control(t, base, "delay-hello", `{"ms":300}`)
		start := time.Now()
		resp, _, _ := openEvents(t, base, nil)
		defer resp.Body.Close()
		if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
			t.Errorf("hello arrived after %v, want at least 300ms", elapsed)
		}
	})

	t.Run("delay-digest", func(t *testing.T) {
		_, base := fixtureServer(t)
		control(t, base, "delay-digest", `{"ms":300}`)
		start := time.Now()
		var resp map[string]any
		if status := post(t, base+"/digest", `{"subjects":[]}`, nil, &resp); status != http.StatusOK {
			t.Fatalf("digest = %d %v, want 200", status, resp)
		}
		if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
			t.Errorf("digest answered after %v, want at least 300ms", elapsed)
		}
	})

	t.Run("hook-sleep", func(t *testing.T) {
		_, base := fixtureServer(t)
		control(t, base, "hook-sleep", `{"ms":200}`)
		start := time.Now()
		resp, stream, _ := openEvents(t, base, nil)
		defer resp.Body.Close()
		helloAt := time.Since(start)
		frames, err := stream.Read(1)
		if err != nil || frames[0].Data != `{"type":"connected"}` || frames[0].ID != "" {
			t.Fatalf("hook frame = %+v (%v), want the id-less connected frame", frames, err)
		}
		if connectedAt := time.Since(start); connectedAt-helloAt < 150*time.Millisecond {
			t.Errorf("connected frame arrived %v after the hello, want about 200ms (the hello is not delayed by the hook)", connectedAt-helloAt)
		}
		control(t, base, "hook-sleep", `{"ms":0,"fail":true}`)
		failing, failingStream, _ := openEvents(t, base, nil)
		defer failing.Body.Close()
		if frames, err := failingStream.Read(1); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("stream with a failing hook = %+v (%v), want EOF right after the hello", frames, err)
		}
	})

	t.Run("mutate", func(t *testing.T) {
		_, base := fixtureServer(t)
		var m mutateResponse
		post(t, base+"/control/mutate", `{"kind":"chat","ref":"c1"}`, nil, &m)
		if m.Version != "1" || m.Status != "" {
			t.Errorf("first mutate = %+v, want version 1 current", m)
		}
		post(t, base+"/control/mutate", `{"kind":"chat","ref":"c1"}`, nil, &m)
		if m.Version != "2" {
			t.Errorf("second mutate = %+v, want version 2", m)
		}
		post(t, base+"/control/mutate", `{"kind":"chat","ref":"c2","status":"gone"}`, nil, &m)
		epoch := state(t, base).Position.Epoch
		var digest map[string]any
		post(t, base+"/digest", `{"epoch":"`+epoch+`","subjects":[{"kind":"chat","ref":"c1","version":"1"},{"kind":"chat","ref":"c2","version":"1"},{"kind":"chat","ref":"c3","version":"1"}]}`, nil, &digest)
		changed, _ := json.Marshal(digest["changed"])
		removed, _ := json.Marshal(digest["removed"])
		if string(changed) != `[{"kind":"chat","ref":"c1","version":"2"}]` {
			t.Errorf("changed = %s, want c1 at version 2", changed)
		}
		if string(removed) != `[{"kind":"chat","reason":"gone","ref":"c2"},{"kind":"chat","reason":"gone","ref":"c3"}]` {
			t.Errorf("removed = %s, want c2 (flagged) and c3 (unknown) gone", removed)
		}
		if status := post(t, base+"/control/mutate", `{"ref":"x"}`, nil, nil); status != http.StatusBadRequest {
			t.Errorf("mutate without kind = %d, want 400", status)
		}
	})

	t.Run("rest and rest-fail-once", func(t *testing.T) {
		_, base := fixtureServer(t)
		control(t, base, "rest-fail-once", ``)
		if status := restStatus(t, base+"/rest/chat/c1"); status != http.StatusInternalServerError {
			t.Errorf("first GET after rest-fail-once = %d, want 500", status)
		}
		resp := get(t, base+"/rest/chat/c1", nil)
		defer resp.Body.Close()
		var body restResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("second GET = %d (%v), want 200", resp.StatusCode, err)
		}
		epoch := state(t, base).Position.Epoch
		want := restResponse{Kind: "chat", Ref: "c1", Version: "1", Epoch: epoch, Payload: "v1"}
		if body != want {
			t.Errorf("GET /rest/chat/c1 = %+v, want %+v", body, want)
		}
		control(t, base, "mutate", `{"kind":"chat","ref":"c1","status":"forbidden"}`)
		if status := restStatus(t, base+"/rest/chat/c1"); status != http.StatusForbidden {
			t.Errorf("GET of a forbidden subject = %d, want 403", status)
		}
	})

	t.Run("close-after", func(t *testing.T) {
		f, base := fixtureServer(t)
		control(t, base, "close-after", `{"frames":3}`)
		resp, stream, _ := openEvents(t, base, nil)
		defer resp.Body.Close()
		control(t, base, "publish", `{"data":"third"}`)
		control(t, base, "publish", `{"data":"never sent"}`)
		frames, err := stream.Read(0)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("ReadFrames to the hard close = %v, want io.ErrUnexpectedEOF (the connection was closed mid-response)", err)
		}
		if len(frames) != 2 || frames[0].Data != `{"type":"connected"}` || frames[1].Data != "third" {
			t.Errorf("frames after the hello = %+v, want connected, third and nothing after", frames)
		}
		pollUntil(t, "the closed stream to unregister", func() bool { return f.Hub().ClientCount() == 0 })
		st := state(t, base)
		if len(st.Events) != 2 || st.Events[1].Cause != sse.PresenceDead || st.Events[1].Write != "frame" {
			t.Errorf("events = %+v, want connected then disconnected dead on a frame write", st.Events)
		}
	})

	t.Run("state counts wire declarations", func(t *testing.T) {
		_, base := fixtureServer(t)
		v3, _, _ := openEvents(t, base, http.Header{"SSE-Wire": {"1"}})
		defer v3.Body.Close()
		legacy, _, _ := openEvents(t, base, nil)
		defer legacy.Body.Close()
		tagged, _, _ := openEvents(t, base, http.Header{"SSE-Wire": {"anything"}, "SSE-Client": {"tab-a"}})
		defer tagged.Body.Close()
		st := state(t, base)
		if st.V3Connects != 2 || st.LegacyConnects != 1 || st.Clients != 3 {
			t.Errorf("state = v3 %d legacy %d clients %d, want 2, 1, 3", st.V3Connects, st.LegacyConnects, st.Clients)
		}
		if len(st.Presence) != 1 || st.Presence[0].Tag != "tab-a" || st.Presence[0].Connected != 1 || st.Presence[0].Gone {
			t.Errorf("presence = %+v, want one present row for tab-a", st.Presence)
		}
		if len(st.Events) != 3 || st.Events[2].Tag != "tab-a" || st.Events[2].Kind != "connected" {
			t.Errorf("events = %+v, want three connected events, the last tagged tab-a", st.Events)
		}
	})
}

func TestFixture_aliveRouteGrammar(t *testing.T) {
	_, base := fixtureServer(t)
	for _, tag := range []string{"", "has space", "a/b", strings.Repeat("x", 65)} {
		var envelope map[string]any
		status := post(t, base+"/alive", ``, http.Header{"SSE-Client": {tag}}, &envelope)
		if status != http.StatusBadRequest || envelope["code"] != "alive_invalid" {
			t.Errorf("POST /alive with tag %q = %d %v, want 400 alive_invalid", tag, status, envelope)
		}
	}
	if st := state(t, base); len(st.Presence) != 0 {
		t.Errorf("presence after refused acknowledgements = %+v, want no rows", st.Presence)
	}
	if status := post(t, base+"/alive", ``, http.Header{"SSE-Client": {"tab-1_A"}}, nil); status != http.StatusNoContent {
		t.Errorf("POST /alive with a valid tag = %d, want 204", status)
	}
	st := state(t, base)
	if len(st.Presence) != 1 || st.Presence[0].Tag != "tab-1_A" || !st.Presence[0].Gone || st.Presence[0].LastAliveAt.IsZero() {
		t.Errorf("presence = %+v, want one gone row (acknowledged, nothing connected) with last_alive_at set", st.Presence)
	}
}

func TestFixture_presenceGoneAtAliveWindow(t *testing.T) {
	_, base := fixtureServer(t)
	control(t, base, "alive-window", `{"ms":200}`)
	resp, _, _ := openEvents(t, base, http.Header{"SSE-Client": {"tab-1"}})
	defer resp.Body.Close()
	st := state(t, base)
	if len(st.Presence) != 1 || st.Presence[0].Gone || st.Presence[0].Connected != 1 || st.Transitions.Alive != 1 {
		t.Fatalf("state right after connect = %+v %+v, want tab-1 present with one alive transition", st.Presence, st.Transitions)
	}
	pollUntil(t, "the row to read gone at the alive window", func() bool { return state(t, base).Presence[0].Gone })
	st = state(t, base)
	if st.Clients != 1 || st.Transitions.Expired != 1 {
		t.Errorf("state after the window = clients %d transitions %+v, want the socket still counted and one expired transition", st.Clients, st.Transitions)
	}
	if status := post(t, base+"/alive", ``, http.Header{"SSE-Client": {"tab-1"}}, nil); status != http.StatusNoContent {
		t.Fatalf("POST /alive = %d, want 204", status)
	}
	st = state(t, base)
	if st.Presence[0].Gone || st.Transitions.Alive != 2 || st.Transitions.Expired != 1 {
		t.Errorf("state after the acknowledgement = %+v %+v, want present with two alive and one expired transitions", st.Presence, st.Transitions)
	}
}

func TestFixture_aliveWindowMatchesTimingContract(t *testing.T) {
	raw, err := os.ReadFile("../timing.json")
	if err != nil {
		t.Fatalf("read timing.json: %v", err)
	}
	var c struct {
		KeepaliveMS      int `json:"keepalive_ms"`
		AliveWindowBeats int `json:"alive_window_beats"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("decode timing.json: %v", err)
	}
	if want := time.Duration(c.AliveWindowBeats*c.KeepaliveMS) * time.Millisecond; defaultAliveWindow != want {
		t.Errorf("defaultAliveWindow = %v, want alive_window_beats × keepalive_ms = %v from timing.json", defaultAliveWindow, want)
	}
}

func TestFixture_restartMintsNewEpoch(t *testing.T) {
	f, base := fixtureServer(t)
	control(t, base, "publish", `{"data":"before"}`)
	first := state(t, base).Position
	resp, stream, hello := openEvents(t, base, http.Header{"SSE-Client": {"tab-1"}})
	defer resp.Body.Close()
	if hello.Epoch != first.Epoch {
		t.Fatalf("hello epoch = %q, want %q", hello.Epoch, first.Epoch)
	}
	var restarted restartResponse
	post(t, base+"/control/restart", ``, nil, &restarted)
	if restarted.Epoch == first.Epoch || len(restarted.Epoch) != 16 {
		t.Errorf("restart epoch = %q, want a new 16-hex epoch (old %q)", restarted.Epoch, first.Epoch)
	}
	if _, err := stream.Read(2); err != nil {
		t.Fatalf("old stream: %v", err)
	}
	if _, err := stream.Read(1); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("old stream after its reset = %v, want EOF", err)
	}
	pollUntil(t, "the old stream to report its shutdown", func() bool {
		st := state(t, base)
		return len(st.Events) == 2 && st.Events[1].Cause == sse.PresenceShutdown
	})
	if f.Hub().Position().Epoch != restarted.Epoch {
		t.Errorf("Hub() epoch = %q, want the restarted %q", f.Hub().Position().Epoch, restarted.Epoch)
	}
	reconnected, _, again := openEvents(t, base, http.Header{"Last-Event-ID": {first.Epoch + ":1"}})
	defer reconnected.Body.Close()
	if again.Epoch != restarted.Epoch || again.Verdict != sse.VerdictEpochChanged {
		t.Errorf("hello after restart = %+v, want the new epoch with verdict epoch_changed", again)
	}
	var digest map[string]any
	post(t, base+"/digest", `{"epoch":"`+first.Epoch+`","subjects":[{"kind":"chat","ref":"c1","version":"1"}]}`, nil, &digest)
	if digest["must_refetch"] != true || digest["epoch"] != restarted.Epoch {
		t.Errorf("digest under the old epoch = %v, want must_refetch with the new epoch", digest)
	}
}
