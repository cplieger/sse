package sse

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestPublish_assignsSequentialOffsets(t *testing.T) {
	h := mustNew(t, WithReplay(8), WithReplayTTL(replayTTL))
	for want := uint64(1); want <= 5; want++ {
		got, err := h.Publish(Event{Data: []byte("e")})
		if err != nil || got != want {
			t.Errorf("Publish #%d = (%d, %v), want (%d, nil)", want, got, err, want)
		}
	}
	if got := h.Position(); got.Floor != 1 || got.Head != 5 || got.Epoch != h.epoch {
		t.Errorf("Position() = %+v, want Floor 1 Head 5 Epoch %q", got, h.epoch)
	}
}

func TestPublish_nilHubReturnsZero(t *testing.T) {
	var h *Hub
	got, err := h.Publish(Event{Data: []byte("x")})
	if got != 0 || err != nil {
		t.Errorf("(*Hub)(nil).Publish = (%d, %v), want (0, nil)", got, err)
	}
}

func TestPublish_afterShutdownDropsValidFrame(t *testing.T) {
	h := mustNew(t, WithReplay(8), WithReplayTTL(replayTTL))
	if _, err := h.Publish(Event{Data: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	if err := h.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown with no clients error = %v, want nil", err)
	}
	got, err := h.Publish(Event{Data: []byte("after")})
	if got != 0 || err != nil {
		t.Errorf("Publish after Shutdown = (%d, %v), want (0, nil)", got, err)
	}
	if head := h.Position().Head; head != 1 {
		t.Errorf("Position().Head after a dropped publish = %d, want 1", head)
	}
	if _, err := h.Publish(Event{Data: dataOfFrameSize(MaxFrameBytes+1, 1)}); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("Publish(oversize) after Shutdown error = %v, want ErrFrameTooLarge", err)
	}
	if _, err := h.Publish(Event{Data: []byte("\xff")}); !errors.Is(err, ErrInvalidUTF8) {
		t.Errorf("Publish(invalid UTF-8) after Shutdown error = %v, want ErrInvalidUTF8", err)
	}
	mustPanic(t, "sse: event name reserved", func() { _, _ = h.Publish(Event{Name: "sse:hello"}) })
}

func TestPublish_maxOffsetPanics(t *testing.T) {
	h := mustNew(t)
	h.seq = MaxOffset
	mustPanic(t, "MaxOffset", func() { _, _ = h.Publish(Event{Data: []byte("x")}) })
	if got := h.Position().Head; got != MaxOffset {
		t.Errorf("Position().Head after the panic = %d, want MaxOffset unchanged", got)
	}
}

func TestPublish_fanOutRespectsTopic(t *testing.T) {
	h := mustNew(t)
	subA, err := h.subscribe("a", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.unsubscribe(subA.c)
	subAll, err := h.subscribe("", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.unsubscribe(subAll.c)

	for _, ev := range []Event{{Topic: "a", Data: []byte("x")}, {Topic: "b", Data: []byte("y")}, {Data: []byte("z")}} {
		if _, err := h.Publish(ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(subA.c.ch); got != 2 {
		t.Errorf("topic-a client queued %d frames, want 2 (its topic plus the broadcast)", got)
	}
	if got := len(subAll.c.ch); got != 3 {
		t.Errorf("unfiltered client queued %d frames, want 3", got)
	}
	first := <-subA.c.ch
	if first.offset != 1 || string(first.event.Data) != "x" {
		t.Errorf("topic-a client's first frame = offset %d data %q, want offset 1 data x", first.offset, first.event.Data)
	}
	last := <-subA.c.ch
	if last.offset != 3 {
		t.Errorf("topic-a client's second frame = offset %d, want 3 (the broadcast, not topic b)", last.offset)
	}
}

func TestPublish_slowClientClosesResetOnce(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithClientBuffer(1), WithLogger(logger))
	sub, err := h.subscribe("t", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.unsubscribe(sub.c)
	for i := range 3 {
		if _, err := h.Publish(Event{Topic: "t", Data: []byte("x")}); err != nil {
			t.Fatalf("Publish #%d error = %v", i+1, err)
		}
	}
	if !isClosed(sub.c.reset) {
		t.Fatal("reset is open after the channel overflowed, want it closed")
	}
	if sub.c.reason != reasonSlow {
		t.Errorf("reason = %q, want %q", sub.c.reason, reasonSlow)
	}
	if n := h.ClientCount(); n != 0 {
		t.Errorf("ClientCount() after eviction = %d, want 0", n)
	}
	if got := len(sub.c.ch); got != 1 {
		t.Errorf("channel holds %d frames, want the 1 that fit", got)
	}
	if got := strings.Count(logged.String(), "sse: evicting slow client"); got != 1 {
		t.Errorf("eviction logged %d times, want once (the record is gone after the first); log = %s", got, logged)
	}
	if !strings.Contains(logged.String(), "topic=t") || !strings.Contains(logged.String(), "queued=1") || !strings.Contains(logged.String(), "head=2") {
		t.Errorf("eviction log = %q, want topic=t queued=1 head=2", logged)
	}
}

func TestSnapshot_clonesData(t *testing.T) {
	h := mustNew(t, WithReplay(4), WithReplayTTL(replayTTL))
	if _, err := h.Publish(Event{Topic: "a", Name: "n", Data: []byte("original")}); err != nil {
		t.Fatal(err)
	}
	first := h.Snapshot()
	if len(first) != 1 || first[0].Offset != 1 || first[0].Event.Topic != "a" || first[0].Event.Name != "n" || first[0].At.IsZero() {
		t.Fatalf("Snapshot() = %+v, want one entry at offset 1 carrying topic, name and time", first)
	}
	copy(first[0].Event.Data, "mutated!")
	second := h.Snapshot()
	if got := string(second[0].Event.Data); got != "original" {
		t.Errorf("Snapshot() after mutating a previous result's Data = %q, want %q", got, "original")
	}
}

func TestQueuedFrames_sumsChannels(t *testing.T) {
	h := mustNew(t)
	subA, err := h.subscribe("a", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.unsubscribe(subA.c)
	subAll, err := h.subscribe("", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.unsubscribe(subAll.c)
	if got := h.QueuedFrames(); got != 0 {
		t.Errorf("QueuedFrames() before any publish = %d, want 0", got)
	}
	for _, ev := range []Event{{Topic: "a"}, {Topic: "b"}, {}} {
		if _, err := h.Publish(ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := h.QueuedFrames(); got != 5 {
		t.Errorf("QueuedFrames() = %d, want 5 (2 on topic a, 3 unfiltered)", got)
	}
	<-subAll.c.ch
	if got := h.QueuedFrames(); got != 4 {
		t.Errorf("QueuedFrames() after one drain = %d, want 4", got)
	}
}

func TestPosition_evictsBeforeReading(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch), WithReplay(1024), WithReplayTTL(replayTTL), WithReplayMaxBytes(MaxFrameBytes))
	if _, err := h.Publish(Event{Data: dataOfFrameSize(MaxFrameBytes, 1)}); err != nil {
		t.Fatal(err)
	}
	// Lower the cap under the ring's back so the next reader, not a Publish,
	// is what performs the eviction.
	h.mu.Lock()
	h.cfg.maxBytes = 64
	h.mu.Unlock()
	if got := h.Position(); got.Floor != 0 || got.Head != 1 || got.Epoch != testEpoch {
		t.Errorf("Position() = %+v, want Floor 0 (evicted by the reader) Head 1 Epoch %q", got, testEpoch)
	}
}

func TestSubscribe_registersHeadOnce(t *testing.T) {
	h := mustNew(t, WithClientBuffer(4096))
	stop := make(chan struct{})
	var publishers sync.WaitGroup
	for _, topic := range []string{"a", "b", ""} {
		publishers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					if _, err := h.Publish(Event{Topic: topic}); err != nil {
						return
					}
				}
			}
		})
	}
	for range 200 {
		sub, err := h.subscribe("a", "", Cursor{}, false)
		if err != nil {
			t.Fatal(err)
		}
		for range 3 {
			e := <-sub.c.ch
			if e.offset <= sub.hello.Head {
				t.Errorf("live frame offset %d <= hello.Head %d", e.offset, sub.hello.Head)
			}
			if e.event.Topic != "" && e.event.Topic != "a" {
				t.Errorf("topic-a client received topic %q", e.event.Topic)
			}
		}
		h.unsubscribe(sub.c)
	}
	close(stop)
	publishers.Wait()
}

func TestSetMaxClients_admissionOnly(t *testing.T) {
	h := mustNew(t, WithMaxClients(1))
	first, err := h.subscribe("", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.unsubscribe(first.c)
	if _, err := h.subscribe("", "", Cursor{}, false); !errors.Is(err, errClientCap) {
		t.Fatalf("second subscribe error = %v, want errClientCap", err)
	}
	h.SetMaxClients(2)
	second, err := h.subscribe("", "", Cursor{}, false)
	if err != nil {
		t.Fatalf("subscribe after raising the cap error = %v", err)
	}
	defer h.unsubscribe(second.c)
	h.SetMaxClients(1)
	if got := h.ClientCount(); got != 2 {
		t.Errorf("ClientCount() after lowering the cap = %d, want 2 (no eviction)", got)
	}
	if _, err := h.subscribe("", "", Cursor{}, false); !errors.Is(err, errClientCap) {
		t.Errorf("subscribe past the lowered cap error = %v, want errClientCap", err)
	}
	h.SetMaxClients(-3)
	third, err := h.subscribe("", "", Cursor{}, false)
	if err != nil {
		t.Errorf("subscribe with a negative (unlimited) cap error = %v", err)
	} else {
		h.unsubscribe(third.c)
	}
}

func TestShutdown_twiceIsIdempotent(t *testing.T) {
	logger, _ := captureLog()
	h := mustNew(t, WithLogger(logger))
	sub, err := h.subscribe("", "", Cursor{}, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Shutdown with an expired ctx and a live client = %v, want context.Canceled", err)
	}
	if err := h.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("second Shutdown = %v, want context.Canceled and no double close", err)
	}
	if sub.c.reason != reasonShutdown {
		t.Errorf("reason = %q, want %q", sub.c.reason, reasonShutdown)
	}
	h.unsubscribe(sub.c)
	if err := h.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown after the last goroutine returned = %v, want nil", err)
	}
}
