package sse

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteFrame_table(t *testing.T) {
	id := testEpoch + ":7"
	tests := []struct {
		name  string
		id    string
		event string
		data  string
		want  string
	}{
		{name: "IdAndData", id: id, data: `{"a":1}`, want: "id: " + id + "\ndata: {\"a\":1}\n\n"},
		{name: "NamedEvent", id: id, event: "notify", data: "x", want: "id: " + id + "\nevent: notify\ndata: x\n\n"},
		{name: "EmptyNameOmitsEventLine", id: id, event: "", data: "x", want: "id: " + id + "\ndata: x\n\n"},
		{name: "Idless", data: "x", want: "data: x\n\n"},
		{name: "LFSplits", id: id, data: "a\nb", want: "id: " + id + "\ndata: a\ndata: b\n\n"},
		{name: "CRLFSplits", id: id, data: "a\r\nb", want: "id: " + id + "\ndata: a\ndata: b\n\n"},
		{name: "CRSplits", id: id, data: "a\rb", want: "id: " + id + "\ndata: a\ndata: b\n\n"},
		{name: "MixedTerminators", data: "a\r\nb\rc\nd", want: "data: a\ndata: b\ndata: c\ndata: d\n\n"},
		{name: "TrailingTerminatorYieldsEmptyLine", data: "a\n", want: "data: a\ndata: \n\n"},
		{name: "EmptyDataOneLine", id: id, data: "", want: "id: " + id + "\ndata: \n\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			lines := splitDataLines([]byte(tt.data))
			if err := writeFrame(&buf, tt.id, tt.event, lines); err != nil {
				t.Fatalf("writeFrame(%q, %q, %q) error = %v", tt.id, tt.event, tt.data, err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("writeFrame(%q, %q, %q) = %q, want %q", tt.id, tt.event, tt.data, got, tt.want)
			}
			if got, want := frameSize(len(tt.event), lines, len(tt.id)), buf.Len(); got != want {
				t.Errorf("frameSize(%q, %q, %q) = %d, want the %d bytes written", tt.id, tt.event, tt.data, got, want)
			}
		})
	}
}

func TestFrameSize_exactAtOffsetBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		seq    uint64
		digits int
	}{
		{name: "Offset1", seq: 0, digits: 1},
		{name: "Offset9", seq: 8, digits: 1},
		{name: "Offset10", seq: 9, digits: 2},
		{name: "MaxOffset", seq: MaxOffset - 1, digits: 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := mustNew(t, withEpoch(testEpoch))
			h.seq = tt.seq
			exact := dataOfFrameSize(MaxFrameBytes, tt.digits)
			var buf bytes.Buffer
			if err := writeFrame(&buf, Cursor{Epoch: testEpoch, Offset: tt.seq + 1}.String(), "", splitDataLines(exact)); err != nil {
				t.Fatalf("Setup: writeFrame: %v", err)
			}
			if buf.Len() != MaxFrameBytes {
				t.Fatalf("Setup: encoded frame at offset %d is %d bytes, want %d", tt.seq+1, buf.Len(), MaxFrameBytes)
			}
			if _, err := h.Publish(Event{Data: exact}); err != nil {
				t.Errorf("Publish(%d-byte frame at offset %d) error = %v, want nil", MaxFrameBytes, tt.seq+1, err)
			}
			h.seq = tt.seq
			if _, err := h.Publish(Event{Data: dataOfFrameSize(MaxFrameBytes+1, tt.digits)}); !errors.Is(err, ErrFrameTooLarge) {
				t.Errorf("Publish(%d-byte frame at offset %d) error = %v, want ErrFrameTooLarge", MaxFrameBytes+1, tt.seq+1, err)
			}
		})
	}
}

func TestPublish_frameTooLargeLeavesNoTrace(t *testing.T) {
	h := mustNew(t, WithReplay(8), WithReplayTTL(replayTTL))
	_, err := h.Publish(Event{Data: dataOfFrameSize(MaxFrameBytes+1, 1)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Publish(oversize) error = %v, want ErrFrameTooLarge", err)
	}
	if !strings.Contains(err.Error(), "1048577") || !strings.Contains(err.Error(), "1048576") {
		t.Errorf("Publish(oversize) error = %q, want the size and the cap", err)
	}
	if got := h.Position().Head; got != 0 {
		t.Errorf("Position().Head after a refused frame = %d, want 0", got)
	}
	if got := h.Snapshot(); len(got) != 0 {
		t.Errorf("Snapshot() after a refused frame has %d entries, want 0", len(got))
	}
}

func TestWriterEvent_frameTooLargeBoundary(t *testing.T) {
	h := mustNew(t)
	rec := httptest.NewRecorder()
	sw := &Writer{w: rec, rc: http.NewResponseController(rec), keepalive: h.cfg.keepaliveEvent, timeout: h.cfg.writeTimeout}
	exact := bytes.Repeat([]byte{'x'}, MaxFrameBytes-frameSize(0, [][]byte{nil}, 0))
	if err := sw.Event("", exact); err != nil {
		t.Errorf("Writer.Event(%d-byte frame) error = %v, want nil", MaxFrameBytes, err)
	}
	if rec.Body.Len() != MaxFrameBytes {
		t.Errorf("Writer.Event wrote %d bytes, want exactly %d", rec.Body.Len(), MaxFrameBytes)
	}
	if err := sw.Event("", bytes.Repeat([]byte{'x'}, len(exact)+1)); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("Writer.Event(%d-byte frame) error = %v, want ErrFrameTooLarge", MaxFrameBytes+1, err)
	}
}

func TestPublish_invalidUTF8Refused(t *testing.T) {
	h := mustNew(t, WithReplay(8), WithReplayTTL(replayTTL))
	_, err := h.Publish(Event{Data: []byte("ok\xffbad")})
	if !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("Publish(invalid UTF-8) error = %v, want ErrInvalidUTF8", err)
	}
	if !strings.Contains(err.Error(), "offset 2") {
		t.Errorf("Publish(invalid UTF-8) error = %q, want the byte offset 2", err)
	}
	if got := h.Position().Head; got != 0 {
		t.Errorf("Position().Head after a refused frame = %d, want 0", got)
	}
	if got := h.Snapshot(); len(got) != 0 {
		t.Errorf("Snapshot() after a refused frame has %d entries, want 0", len(got))
	}
}

func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("no panic, want one containing %q", want)
		}
		if s, ok := r.(string); !ok || !strings.Contains(s, want) {
			t.Errorf("panic = %v, want it to contain %q", r, want)
		}
	}()
	fn()
}

func TestPublish_reservedNamePanics(t *testing.T) {
	h := mustNew(t)
	for _, name := range []string{"sse:hello", "sse:reset", "sse:anything", "sse:"} {
		mustPanic(t, "sse: event name reserved", func() { _, _ = h.Publish(Event{Name: name}) })
	}
	if got := h.Position().Head; got != 0 {
		t.Errorf("Position().Head after reserved-name panics = %d, want 0", got)
	}
}

func TestPublish_crlfNamePanics(t *testing.T) {
	h := mustNew(t)
	for _, name := range []string{"beat\nid: 9", "beat\rid: 9", "\n", "\r\n"} {
		mustPanic(t, "sse: event name reserved", func() { _, _ = h.Publish(Event{Name: name}) })
	}
}

func TestPublish_keepaliveNameCollisionPanics(t *testing.T) {
	h := mustNew(t, WithKeepaliveEvent("heartbeat"))
	mustPanic(t, "heartbeat", func() { _, _ = h.Publish(Event{Name: "heartbeat"}) })

	rec := httptest.NewRecorder()
	sw := &Writer{w: rec, rc: http.NewResponseController(rec), keepalive: "heartbeat", timeout: h.cfg.writeTimeout}
	mustPanic(t, "heartbeat", func() { _ = sw.Event("heartbeat", nil) })

	comment := mustNew(t, WithKeepaliveEvent(""))
	if _, err := comment.Publish(Event{Name: "heartbeat"}); err != nil {
		t.Errorf("Publish(heartbeat) under WithKeepaliveEvent(\"\") error = %v, want nil", err)
	}
	if _, err := comment.Publish(Event{Name: ""}); err != nil {
		t.Errorf("Publish(empty name) under WithKeepaliveEvent(\"\") error = %v, want nil", err)
	}
}
