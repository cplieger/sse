package sse

import (
	"testing"
	"time"
)

func TestRing_evictsByAge(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	h := mustNew(t, WithReplay(8), WithReplayTTL(replayTTL), withNow(func() time.Time { return now }))
	for range 3 {
		if _, err := h.Publish(Event{Data: []byte("e")}); err != nil {
			t.Fatalf("Setup: Publish: %v", err)
		}
	}
	if got := h.Position(); got.Floor != 1 || got.Head != 3 {
		t.Fatalf("Position() before expiry = %+v, want Floor 1 Head 3", got)
	}
	now = now.Add(replayTTL)
	if got := h.Position().Floor; got != 1 {
		t.Errorf("Position().Floor at exactly the TTL = %d, want 1 (an entry at max age survives)", got)
	}
	now = now.Add(time.Nanosecond)
	if got := h.Position(); got.Floor != 0 || got.Head != 3 {
		t.Errorf("Position() past the TTL without a Publish = %+v, want Floor 0 Head 3", got)
	}
	sub, err := h.subscribe("", "", Cursor{Epoch: h.epoch, Offset: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.unsubscribe(sub.c)
	if sub.hello.Verdict != VerdictGapFloor {
		t.Errorf("subscribe(offset 1) after expiry verdict = %s, want gap_floor", sub.hello.Verdict)
	}
}

func TestRing_evictsByBytes(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch), WithReplay(1024), WithReplayTTL(replayTTL), WithReplayMaxBytes(4*MaxFrameBytes))
	for i := range 5 {
		if _, err := h.Publish(Event{Data: dataOfFrameSize(MaxFrameBytes, 1)}); err != nil {
			t.Fatalf("Publish(frame %d) error = %v", i+1, err)
		}
	}
	if got := h.Position(); got.Floor != 2 || got.Head != 5 {
		t.Fatalf("Position() after five %d-byte frames under a 4-frame cap = %+v, want Floor 2 Head 5", MaxFrameBytes, got)
	}
	if got := h.ring.bytes; got != 4*MaxFrameBytes {
		t.Errorf("ring bytes = %d, want %d", got, 4*MaxFrameBytes)
	}

	// A cursor names the last offset the client holds, so offset 1 needs 2..5,
	// which the ring has; only offset 0 is below the floor.
	for _, tt := range []struct {
		offset      uint64
		wantVerdict Verdict
		wantReplay  int
	}{
		{offset: 0, wantVerdict: VerdictGapFloor, wantReplay: 0},
		{offset: 1, wantVerdict: VerdictResumed, wantReplay: 4},
		{offset: 2, wantVerdict: VerdictResumed, wantReplay: 3},
	} {
		sub, err := h.subscribe("", "", Cursor{Epoch: testEpoch, Offset: tt.offset}, true)
		if err != nil {
			t.Fatal(err)
		}
		if sub.hello.Verdict != tt.wantVerdict || len(sub.replay) != tt.wantReplay {
			t.Errorf("subscribe(offset %d) = (%s, %d frames), want (%s, %d)", tt.offset, sub.hello.Verdict, len(sub.replay), tt.wantVerdict, tt.wantReplay)
		}
		h.unsubscribe(sub.c)
	}

	// The ring sits exactly at the cap, so one more byte over it evicts the
	// oldest entry: the sum is the bound that binds, not the count or the age.
	if _, err := h.Publish(Event{Data: make([]byte, 32)}); err != nil {
		t.Fatalf("Publish(32 bytes) error = %v", err)
	}
	if got := h.Position(); got.Floor != 3 || got.Head != 6 {
		t.Errorf("Position() after a 32-byte sixth frame = %+v, want Floor 3 Head 6", got)
	}
	if got := h.ring.len(); got != 4 {
		t.Errorf("ring holds %d entries after the sixth publish, want 4", got)
	}
}

func TestRing_sinceCopies(t *testing.T) {
	r := newRing(4)
	for i := uint64(1); i <= 3; i++ {
		r.append(&entry{offset: i, event: Event{Topic: "a"}})
	}
	got := r.since(0, "")
	if len(got) != 3 {
		t.Fatalf("since(0) returned %d entries, want 3", len(got))
	}
	got[0].offset = 99
	got[1].event.Topic = "mutated"
	again := r.since(0, "")
	if again[0].offset != 1 || again[1].event.Topic != "a" {
		t.Errorf("since(0) after mutating a previous result = %+v, want the ring unchanged", again)
	}
	filtered := r.since(1, "a")
	if len(filtered) != 2 || filtered[0].offset != 2 || filtered[1].offset != 3 {
		t.Errorf("since(1, a) = %+v, want offsets [2 3]", filtered)
	}
	if got := r.since(3, ""); len(got) != 0 {
		t.Errorf("since(head) = %+v, want empty", got)
	}
}

func TestRing_zeroCapacityIsInert(t *testing.T) {
	r := newRing(0)
	r.append(&entry{offset: 1, size: 40})
	r.evict(time.Now(), time.Minute, 1)
	if got := r.since(0, ""); len(got) != 0 {
		t.Errorf("since on a zero-capacity ring = %+v, want empty", got)
	}
	if r.floor() != 0 || r.len() != 0 || r.bytes != 0 {
		t.Errorf("zero-capacity ring floor/len/bytes = %d/%d/%d, want 0/0/0", r.floor(), r.len(), r.bytes)
	}

	h := mustNew(t)
	if _, err := h.Publish(Event{Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if got := h.Position(); got.Floor != 0 || got.Head != 1 {
		t.Errorf("Position() with no ring = %+v, want Floor 0 Head 1", got)
	}
}

func TestRing_countEvictionKeepsBytesConsistent(t *testing.T) {
	r := newRing(2)
	for i := uint64(1); i <= 5; i++ {
		r.append(&entry{offset: i, size: int(i)})
	}
	if r.floor() != 4 || r.len() != 2 || r.bytes != 9 {
		t.Errorf("ring after 5 appends into capacity 2: floor/len/bytes = %d/%d/%d, want 4/2/9", r.floor(), r.len(), r.bytes)
	}
	r.evict(time.Time{}, 0, 5)
	if r.floor() != 5 || r.bytes != 5 {
		t.Errorf("ring after byte eviction to 5: floor/bytes = %d/%d, want 5/5", r.floor(), r.bytes)
	}
}
