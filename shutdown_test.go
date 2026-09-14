package sse

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestShutdown_thenPublishNeverDoubleCloses(t *testing.T) {
	logger, _ := captureLog()
	const buffer = 4
	h := mustNew(t, WithLogger(logger), WithClientBuffer(buffer))
	sub, err := h.subscribe("", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	for range buffer {
		if _, err := h.Publish(Event{Data: []byte("fill")}); err != nil {
			t.Fatal(err)
		}
	}
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- h.Shutdown(context.Background()) }()
	<-sub.c.reset
	for i := range 2 * buffer {
		got, err := h.Publish(Event{Data: []byte("after")})
		if got != 0 || err != nil {
			t.Errorf("Publish #%d after Shutdown = (%d, %v), want (0, nil)", i+1, got, err)
		}
	}
	if sub.c.reason != reasonShutdown {
		t.Errorf("reason = %q, want %q", sub.c.reason, reasonShutdown)
	}
	h.unsubscribe(sub.c)
	if err := <-shutdownErr; err != nil {
		t.Errorf("Shutdown = %v, want nil", err)
	}
}

func TestShutdown_connectStorm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := captureLog()
		h := mustNew(t, WithLogger(logger))
		srv := startServer(t, h)
		const n = 64
		var hellos, refusals, other atomic.Int32
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, http.NoBody)
				if err != nil {
					t.Error(err)
					return
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					other.Add(1)
					t.Errorf("Serve during Shutdown: %v", err)
					return
				}
				defer resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusServiceUnavailable:
					refusals.Add(1)
				case http.StatusOK:
					sc := bufio.NewScanner(resp.Body)
					hello, _ := readHello(t, sc)
					if hello.Wire != Wire {
						t.Errorf("hello = %+v, want wire %d", hello, Wire)
					}
					for sc.Scan() {
						if sc.Text() == `data: {"reason":"shutdown"}` {
							hellos.Add(1)
							return
						}
					}
					t.Errorf("stream ended without a shutdown reset: %v", sc.Err())
				default:
					other.Add(1)
					t.Errorf("status = %d, want 200 or 503", resp.StatusCode)
				}
			})
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if err := h.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown under a connect storm = %v, want nil", err)
		}
		wg.Wait()
		if got := hellos.Load() + refusals.Load() + other.Load(); got != n {
			t.Errorf("%d connections accounted for, want %d", got, n)
		}
		if got := h.ClientCount(); got != 0 {
			t.Errorf("ClientCount() after Shutdown returned and every client finished = %d, want 0", got)
		}
	})
}

func TestShutdown_blocksUntilGoroutinesExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := captureLog()
		h := mustNew(t, WithLogger(logger))
		release := make(chan struct{})
		srv := startServer(t, h, OnConnect(func(*Writer, Hello) error {
			<-release
			return nil
		}))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		requireClients(t, h, 1)

		done := make(chan error, 1)
		go func() { done <- h.Shutdown(t.Context()) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned %v while a stream goroutine was still inside its hook", err)
		default:
		}
		if got := h.ClientCount(); got != 1 {
			t.Errorf("ClientCount() during Shutdown with the goroutine still draining = %d, want 1", got)
		}
		close(release)
		if err := <-done; err != nil {
			t.Errorf("Shutdown = %v, want nil once the goroutine returned", err)
		}
		readUntil(t, sc, func(l string) bool { return l == `data: {"reason":"shutdown"}` })
		if got := h.ClientCount(); got != 0 {
			t.Errorf("ClientCount() after Shutdown = %d, want 0", got)
		}
	})
}

func TestServe_afterShutdown503(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithLogger(logger))
		if err := h.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		srv := startServer(t, h)
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503 after Shutdown", resp.StatusCode)
		}
		if sc.Scan(); !strings.Contains(sc.Text(), "sse_unavailable") {
			t.Errorf("body = %q, want the sse_unavailable envelope", sc.Text())
		}
		if strings.Contains(logged.String(), "client cap") {
			t.Errorf("log = %q, want no cap line for a refusal after Shutdown", logged)
		}
	})
}

func TestShutdown_ctxExpiryReturnsErr(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logged := captureLog()
		h := mustNew(t, WithLogger(logger))
		release := make(chan struct{})
		srv := startServer(t, h, OnConnect(func(*Writer, Hello) error {
			<-release
			return nil
		}))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		requireClients(t, h, 1)

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := h.Shutdown(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown with a hook still running = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != 100*time.Millisecond {
			t.Errorf("Shutdown returned after %v, want exactly the ctx deadline", got)
		}
		if !strings.Contains(logged.String(), "sse: shutdown deadline expired") || !strings.Contains(logged.String(), "remaining=1") {
			t.Errorf("log = %q, want the Warn with remaining=1", logged)
		}
		close(release)
		if err := h.Shutdown(t.Context()); err != nil {
			t.Errorf("Shutdown after the hook returned = %v, want nil", err)
		}
	})
}

func TestShutdown_evictedClientNotCountedWhileDraining(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := captureLog()
		h := mustNew(t, WithLogger(logger), WithClientBuffer(2))
		var countInsideHook atomic.Int32
		srv := startServer(t, h, OnConnect(func(*Writer, Hello) error {
			for range 3 {
				if _, err := h.Publish(Event{Data: []byte("x")}); err != nil {
					return err
				}
			}
			countInsideHook.Store(int32(h.ClientCount()))
			return nil
		}))
		resp, sc := openStream(t, srv, srv.URL, nil)
		defer resp.Body.Close()
		readHello(t, sc)
		readUntil(t, sc, func(l string) bool { return l == `data: {"reason":"slow"}` })
		if got := countInsideHook.Load(); got != 0 {
			t.Errorf("ClientCount() inside the hook after eviction = %d, want 0 while the goroutine still drains", got)
		}
		if err := h.Shutdown(t.Context()); err != nil {
			t.Errorf("Shutdown collecting the evicted goroutine = %v, want nil", err)
		}
	})
}
