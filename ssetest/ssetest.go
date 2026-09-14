// Package ssetest is the test seam between the sse hub and its clients: an
// httptest server for a hub, a frame reader for the wire, a recorder that
// answers http.ErrNotSupported to the deadline setters, and Fixture, the
// controllable server the TypeScript integration suites drive through the
// ssetest/cmd binary.
package ssetest

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/sse"
)

// Serve starts an httptest server whose every request is h.Serve with opts and
// returns its URL. The server closes at test cleanup; the caller still owns the
// hub and shuts it down.
func Serve(t testing.TB, h *sse.Hub, opts ...sse.ServeOption) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.Serve(w, r, opts...)
	}))
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
	})
	return srv.URL
}

// Frame is one dispatched event: the id: field, the event: name and the data:
// lines joined by LF. A retry: field is reported as a Frame with Event "retry"
// and the milliseconds in Data.
type Frame struct {
	ID    string
	Event string
	Data  string
}

// ReadFrames parses the WHATWG event-stream line grammar from r and returns
// the first n dispatched frames, or every frame up to EOF when n <= 0. Comment
// lines are skipped, and a blank line that closes no data or retry field
// dispatches nothing, exactly as a browser's parser behaves.
func ReadFrames(r io.Reader, n int) ([]Frame, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 2*sse.MaxFrameBytes)
	var frames []Frame
	var b frameBuilder
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if fr, ok := b.dispatch(); ok {
				frames = append(frames, fr)
			}
			if n > 0 && len(frames) >= n {
				return frames, nil
			}
		case strings.HasPrefix(line, ":"):
		default:
			b.field(line)
		}
	}
	if err := sc.Err(); err != nil {
		return frames, err
	}
	if n > 0 && len(frames) < n {
		return frames, io.ErrUnexpectedEOF
	}
	return frames, nil
}

// frameBuilder accumulates one frame's fields until a blank line dispatches it.
type frameBuilder struct {
	cur       Frame
	data      []string
	haveData  bool
	haveRetry bool
}

func (b *frameBuilder) field(line string) {
	name, value, _ := strings.Cut(line, ":")
	value = strings.TrimPrefix(value, " ")
	switch name {
	case "id":
		b.cur.ID = value
	case "event":
		b.cur.Event = value
	case "data":
		b.data = append(b.data, value)
		b.haveData = true
	case "retry":
		b.cur.Data = value
		b.haveRetry = true
	}
}

// dispatch returns the accumulated frame and starts the next one; ok is false
// when the blank line closed neither a data nor a retry field.
func (b *frameBuilder) dispatch() (fr Frame, ok bool) {
	switch {
	case b.haveRetry:
		fr, ok = Frame{Event: "retry", Data: b.cur.Data}, true
	case b.haveData:
		b.cur.Data = strings.Join(b.data, "\n")
		fr, ok = b.cur, true
	}
	*b = frameBuilder{}
	return fr, ok
}

// Recorder is an httptest.ResponseRecorder that flushes but answers
// http.ErrNotSupported to both deadline setters, the shape of a wrapper that
// exposes Flush without Unwrap, so a Serve against it runs the tolerated
// unbounded path.
type Recorder struct {
	*httptest.ResponseRecorder
}

// NewRecorder returns a Recorder over a fresh httptest.ResponseRecorder.
func NewRecorder() *Recorder {
	return &Recorder{ResponseRecorder: httptest.NewRecorder()}
}

// SetWriteDeadline always answers http.ErrNotSupported.
func (r *Recorder) SetWriteDeadline(time.Time) error { return http.ErrNotSupported }

// SetReadDeadline always answers http.ErrNotSupported.
func (r *Recorder) SetReadDeadline(time.Time) error { return http.ErrNotSupported }

// Frames parses everything written so far.
func (r *Recorder) Frames() ([]Frame, error) {
	frames, err := ReadFrames(bytes.NewReader(r.Body.Bytes()), 0)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return frames, nil
	}
	return frames, err
}
