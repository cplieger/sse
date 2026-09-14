package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/cplieger/webhttp/v3"
)

// ServeOption configures one Serve call.
type ServeOption func(*serveConfig)

type serveConfig struct {
	onConnect func(w *Writer, h Hello) error
	topic     string
	tag       string
}

// WithTopic subscribes the connection to broadcasts plus events scoped to
// topic. The empty topic (the default) receives everything.
func WithTopic(topic string) ServeOption {
	return func(c *serveConfig) { c.topic = topic }
}

// OnConnect installs a hook that runs after the retry: field, the hello and
// the replay have been flushed, before live delivery. It receives the Hello
// the client received and writes initial-state frames through the Writer; an
// error ends the connection. The hook runs on the stream goroutine, holds no
// hub lock, and may call Publish and Position. Keep it short: a hook that
// runs past the client's hello timeout loses the tab to a reconnect.
func OnConnect(fn func(w *Writer, h Hello) error) ServeOption {
	return func(c *serveConfig) { c.onConnect = fn }
}

// WithClientTag attaches an application-chosen presence key to the connection.
// The empty string means no tag, so a handler may pass the SSE-Client header
// through unconditionally; any other tag outside [A-Za-z0-9_-]{1,64} is
// treated as absent with one Warn naming its length and the class of failure,
// never the bytes.
func WithClientTag(tag string) ServeOption {
	return func(c *serveConfig) { c.tag = tag }
}

// Writer is handed to OnConnect only; frames it writes carry no id. Serve
// always supplies it, so there is no constructor and no useful zero value.
type Writer struct {
	w         http.ResponseWriter
	rc        *http.ResponseController
	keepalive string
	timeout   time.Duration
	failed    bool
	deadlines bool
}

// Event writes one frame as its own bounded write and flush; name may be
// empty. It returns ErrFrameTooLarge or ErrInvalidUTF8 exactly as Publish
// does (the size check has no id line here), panics on the same names, and
// returns the deadline error when the peer stopped reading, which the hook
// propagates.
func (sw *Writer) Event(name string, data []byte) error {
	checkEventName(name, sw.keepalive)
	if err := checkData(data); err != nil {
		return err
	}
	lines := splitDataLines(data)
	if size := frameSize(len(name), lines, 0); size > MaxFrameBytes {
		return tooLarge(size)
	}
	return sw.writeAndFlush(sw.timeout, func(w io.Writer) error {
		return writeFrame(w, "", name, lines)
	})
}

// writeAndFlush is the one path every byte to the client takes: a deadline,
// the write, a flush, then the deadline cleared. The clear is load-bearing: a
// deadline left armed across an idle gap kills a healthy stream, and on
// HTTP/2 no later write can revive it. A failure marks the Writer failed, which
// is how Serve tells a hook's write error from the hook's own error.
func (sw *Writer) writeAndFlush(timeout time.Duration, write func(w io.Writer) error) error {
	err := sw.writeAndFlushOnce(timeout, write)
	sw.failed = err != nil
	return err
}

func (sw *Writer) writeAndFlushOnce(timeout time.Duration, write func(w io.Writer) error) error {
	if sw.deadlines {
		if err := sw.rc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	}
	if err := write(sw.w); err != nil {
		return err
	}
	if err := sw.rc.Flush(); err != nil {
		return err
	}
	if sw.deadlines {
		return sw.rc.SetWriteDeadline(time.Time{})
	}
	return nil
}

// Serve subscribes the request to the hub and streams until the peer leaves,
// the request context ends, the client is reset as slow, a write fails, or
// the hub shuts down. It owns the response headers, the retry: field, the
// hello, the Last-Event-ID replay, keepalives and the sse:reset frame.
//
// It answers 503 sse_unavailable over the client cap or after Shutdown, and
// 500 streaming_unsupported when no http.Flusher is reachable through the
// ResponseWriter or its Unwrap chain.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, opts ...ServeOption) {
	var sc serveConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	tag := h.clientTag(sc)
	raw := r.Header.Get("Last-Event-ID")
	present := raw != ""
	var cur Cursor
	if present {
		c, err := ParseCursor(raw)
		if err != nil {
			h.logger.Debug("sse: Last-Event-ID rejected", "length", len(raw), "error", err)
		}
		cur = c
	}

	if !canFlush(w) {
		h.flusherOnce.Do(func() {
			h.logger.Error("sse: response writer cannot flush; streaming refused", "writer", fmt.Sprintf("%T", w))
		})
		webhttp.WriteError(w, r, http.StatusInternalServerError, "streaming_unsupported", "streaming not supported")
		return
	}
	sub, err := h.subscribe(sc.topic, tag, cur, present)
	if err != nil {
		if errors.Is(err, errClientCap) {
			h.logger.Info("sse: client cap reached", "clients", h.ClientCount(), "max", h.maxClients())
		}
		webhttp.WriteError(w, r, http.StatusServiceUnavailable, "sse_unavailable", "sse unavailable")
		return
	}
	c := sub.c
	writeStreamHeaders(w)
	rc := http.NewResponseController(w)
	sw := &Writer{w: w, rc: rc, keepalive: h.cfg.keepaliveEvent, timeout: h.cfg.writeTimeout, deadlines: true}
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		sw.deadlines = false
		h.deadlineOnce.Do(func() {
			h.logger.Warn("sse: response writer does not support write deadlines; streams run unbounded", "writer", fmt.Sprintf("%T", w), "error", err)
		})
	}
	if err := rc.SetReadDeadline(time.Time{}); err != nil {
		h.logger.Debug("sse: clear read deadline", "error", err)
	}

	if err := sw.writeAndFlush(sw.timeout, func(w io.Writer) error { return h.writeHandshake(w, sub) }); err != nil {
		h.endOnWriteError(sw, c, "handshake", err)
		h.unsubscribe(c)
		return
	}
	connected := h.presenceEvent(c, PresenceConnected)
	connected.Verdict = sub.hello.Verdict
	h.presence(&connected)
	// Deferred so a panicking OnConnect still unregisters the client and still
	// pairs its connected event with one disconnected; the hook is then the
	// cause by default.
	dep := departure{cause: PresenceHookFailed}
	defer func() {
		h.unsubscribe(c)
		disconnected := h.presenceEvent(c, PresenceDisconnected)
		disconnected.Cause, disconnected.Write = dep.cause, dep.write
		h.presence(&disconnected)
	}()
	dep = h.serveConnected(r.Context(), sw, sub, sc)
}

// serveConnected runs the hook and the live loop once the hello has left, and
// reports why the connection ended.
func (h *Hub) serveConnected(ctx context.Context, sw *Writer, sub *subscription, sc serveConfig) departure {
	c := sub.c
	if isClosed(c.reset) {
		return h.writeReset(sw, c)
	}
	if sc.onConnect != nil {
		if err := sc.onConnect(sw, sub.hello); err != nil {
			if sw.failed {
				return h.endOnWriteError(sw, c, "hook", err)
			}
			h.logger.Warn("sse: OnConnect failed", "error", err, "epoch", h.epoch, "verdict", sub.hello.Verdict, "topic", c.topic)
			if isClosed(c.reset) {
				return resetDeparture(c, nil)
			}
			return departure{cause: PresenceHookFailed}
		}
	}
	return h.stream(ctx, sw, c)
}

// clientTag applies the tag grammar; a failing tag is absent, with one Warn.
func (h *Hub) clientTag(sc serveConfig) string {
	if sc.tag == "" || webhttp.ValidRequestID(sc.tag) {
		return sc.tag
	}
	class := "char"
	if len(sc.tag) > 64 {
		class = "too_long"
	}
	h.logger.Warn("sse: client tag rejected", "length", len(sc.tag), "class", class)
	return ""
}

func (h *Hub) maxClients() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cfg.maxClients
}

// canFlush reports whether w or something in its Unwrap chain implements
// http.Flusher, the discovery http.ResponseController performs, without
// writing anything so the refusal can still be a JSON 500.
func canFlush(w http.ResponseWriter) bool {
	for {
		switch t := w.(type) {
		case http.Flusher:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			w = t.Unwrap()
		default:
			return false
		}
	}
}

// stream is the live loop. reset is checked before every frame written: a
// non-blocking receive at the top of each iteration, as a case of the blocking
// select, and inside the drain before each frame.
func (h *Hub) stream(ctx context.Context, sw *Writer, c *client) departure {
	ticker := time.NewTicker(h.cfg.keepalive)
	defer ticker.Stop()
	for {
		if isClosed(c.reset) {
			return h.writeReset(sw, c)
		}
		select {
		case <-c.reset:
			return h.writeReset(sw, c)
		case e := <-c.ch:
			if err := h.drain(sw, c, &e); err != nil {
				return h.endOnWriteError(sw, c, "frame", err)
			}
		case <-ticker.C:
			err := sw.writeAndFlush(sw.timeout, func(w io.Writer) error {
				return writeKeepalive(w, h.cfg.keepaliveEvent)
			})
			if err != nil {
				return h.endOnWriteError(sw, c, "keepalive", err)
			}
		case <-ctx.Done():
			h.logger.Debug("sse: peer disconnected", "topic", c.topic)
			return departure{cause: PresenceClosed}
		}
	}
}

// drain writes first plus everything queued behind it as one bounded write
// and flush, stopping at the first frame whose reset check fails; the loop
// top then writes the reset. Whatever is still queued is dropped, which is
// safe because the client's cursor never passed a frame it did not receive.
func (h *Hub) drain(sw *Writer, c *client, first *entry) error {
	return sw.writeAndFlush(sw.timeout, func(w io.Writer) error {
		e := *first
		for {
			select {
			case <-c.reset:
				return nil
			default:
			}
			if err := h.writeEntry(w, &e); err != nil {
				return err
			}
			select {
			case e = <-c.ch:
			case <-c.reset:
				return nil
			default:
				return nil
			}
		}
	})
}

// writeHandshake writes the retry: field, the hello and the replay slice; the
// caller flushes them together so they leave before any application code runs.
func (h *Hub) writeHandshake(w io.Writer, sub *subscription) error {
	if err := writeRetry(w, h.cfg.reconnectDelay); err != nil {
		return err
	}
	hello, err := json.Marshal(sub.hello)
	if err != nil {
		return err
	}
	if err := writeFrame(w, "", helloEvent, [][]byte{hello}); err != nil {
		return err
	}
	for i := range sub.replay {
		if err := h.writeEntry(w, &sub.replay[i]); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hub) writeEntry(w io.Writer, e *entry) error {
	id := Cursor{Epoch: h.epoch, Offset: e.offset}.String()
	return writeFrame(w, id, e.event.Name, splitDataLines(e.event.Data))
}

// writeReset writes the sse:reset frame under its own short deadline and
// returns; a failure there means the peer is gone and costs one Debug line.
func (h *Hub) writeReset(sw *Writer, c *client) departure {
	err := writeResetFrame(sw, c)
	h.logger.Debug("sse: client reset", "reason", c.reason, "reset_unwritten", err != nil, "topic", c.topic)
	return resetDeparture(c, err)
}

func writeResetFrame(sw *Writer, c *client) error {
	return sw.writeAndFlush(resetWriteTimeout, func(w io.Writer) error {
		return writeFrame(w, "", resetEvent, [][]byte{fmt.Appendf(nil, `{"reason":%q}`, c.reason)})
	})
}

// endOnWriteError logs a failed write, then attempts the reset write if the
// client was already signalled; on a timed-out socket that attempt fails at
// once and is counted reset_unwritten.
func (h *Hub) endOnWriteError(sw *Writer, c *client, write string, err error) departure {
	attrs := []any{"write", write, "write_timeout", errors.Is(err, os.ErrDeadlineExceeded), "error", err, "topic", c.topic}
	dep := departure{cause: PresenceDead, write: write}
	if isClosed(c.reset) {
		resetErr := writeResetFrame(sw, c)
		attrs = append(attrs, "reason", c.reason, "reset_unwritten", resetErr != nil)
		dep = resetDeparture(c, resetErr)
	}
	h.logger.Debug("sse: stream write failed", attrs...)
	return dep
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// writeKeepalive writes the named form "event: <name>\ndata: {}\n\n", or the
// comment ": keepalive\n\n" when no name is configured. The data: line is what
// makes the named form dispatch: a frame with no data: field is discarded by
// the receiver before it becomes an event.
func writeKeepalive(w io.Writer, name string) error {
	if name == "" {
		_, err := io.WriteString(w, ": keepalive\n\n")
		return err
	}
	return writeFrame(w, "", name, [][]byte{[]byte("{}")})
}

func writeRetry(w io.Writer, d time.Duration) error {
	_, err := fmt.Fprintf(w, "retry: %d\n\n", d.Milliseconds())
	return err
}

// writeStreamHeaders sets the proxy-defensive headers: no-transform stops an
// intermediary from gzip-wrapping the stream and buffering per-event flushes,
// X-Accel-Buffering disables nginx-style response buffering.
func writeStreamHeaders(w http.ResponseWriter) {
	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache, no-transform")
	hdr.Set("X-Accel-Buffering", "no")
}
