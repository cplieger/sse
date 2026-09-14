package sse

import (
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"
)

var topics = []string{"", "a", "b", "c"}

// ringModel builds a ring the way Publish does and keeps the test's own record
// of every appended (offset, topic), which the properties check against.
type ringModel struct {
	r        *ring
	appended []entry
	head     uint64
	capacity int
}

// survivors is the model's own view of what the ring holds: the newest
// capacity entries appended.
func (m *ringModel) survivors() []entry {
	return m.appended[len(m.appended)-min(len(m.appended), m.capacity):]
}

func drawRing(t *rapid.T) *ringModel {
	capacity := rapid.IntRange(0, 12).Draw(t, "capacity")
	count := rapid.IntRange(0, 20).Draw(t, "count")
	m := &ringModel{r: newRing(capacity), capacity: capacity}
	now := time.Unix(0, 0)
	for range count {
		m.head++
		e := entry{at: now, event: Event{Topic: rapid.SampledFrom(topics).Draw(t, "topic")}, offset: m.head, size: 32}
		m.r.append(&e)
		m.appended = append(m.appended, e)
	}
	return m
}

func (m *ringModel) resolveAndReplay(t *rapid.T, since uint64, topic string, replyCap int) (Verdict, bool, []entry) {
	verdict, resumed := resolve(Cursor{Epoch: testEpoch, Offset: since}, true, testEpoch, m.r.floor(), m.head, m.r.len(), replyCap)
	var replay []entry
	if resumed && since < m.head {
		replay = m.r.since(since, topic)
	}
	return verdict, resumed, replay
}

func TestResolve_replayIsFilteredOrderedSubset(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := drawRing(t)
		since := rapid.Uint64Range(0, m.head).Draw(t, "since")
		topic := rapid.SampledFrom(topics).Draw(t, "topic")
		replyCap := rapid.IntRange(0, m.r.len()).Draw(t, "replyCap")
		_, _, replay := m.resolveAndReplay(t, since, topic, replyCap)

		var want []uint64
		for _, e := range m.survivors() {
			if e.offset > since && topicMatches(topic, e.event.Topic) {
				want = append(want, e.offset)
			}
		}
		var got []uint64
		for _, e := range replay {
			got = append(got, e.offset)
		}
		if !slices.IsSortedFunc(got, func(a, b uint64) int { return int(a) - int(b) }) {
			t.Fatalf("replay offsets %v are not in order", got)
		}
		for _, o := range got {
			if !slices.Contains(want, o) {
				t.Fatalf("replay(since=%d, topic=%q) = %v, contains offset %d outside the eligible set %v", since, topic, got, o, want)
			}
		}
	})
}

func TestResolve_lengthBoundedByHeadMinusSince(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := drawRing(t)
		since := rapid.Uint64Range(0, m.head).Draw(t, "since")
		topic := rapid.SampledFrom(topics).Draw(t, "topic")
		replyCap := int(m.head)
		verdict, resumed, replay := m.resolveAndReplay(t, since, topic, replyCap)
		if uint64(len(replay)) > m.head-since {
			t.Fatalf("len(replay) = %d, above head-since = %d (verdict %s)", len(replay), m.head-since, verdict)
		}
		fullRing := m.r.len() > 0 && m.r.floor() <= since+1
		if topic == "" && resumed && fullRing && uint64(len(replay)) != m.head-since {
			t.Fatalf("unfiltered replay over a ring holding (%d, %d] has %d frames, want %d", since, m.head, len(replay), m.head-since)
		}
	})
}

func TestResolve_capAppliedToUnfilteredSpan(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := drawRing(t)
		if m.head == 0 {
			t.Skip("empty stream")
		}
		since := rapid.Uint64Range(max(m.r.floor(), 1)-1, m.head-1).Draw(t, "since")
		topic := rapid.SampledFrom(topics[1:]).Draw(t, "topic")
		span := int(m.head - since)
		replyCap := rapid.IntRange(0, span-1).Draw(t, "replyCap")
		verdict, _, replay := m.resolveAndReplay(t, since, topic, replyCap)
		if m.r.len() == 0 || since+1 < m.r.floor() {
			if verdict != VerdictGapFloor {
				t.Fatalf("resolve(since=%d below floor %d) = %s, want gap_floor", since, m.r.floor(), verdict)
			}
			return
		}
		if verdict != VerdictGapBudget {
			t.Fatalf("resolve(since=%d, head=%d, replyCap=%d) = %s, want gap_budget on the unfiltered span %d", since, m.head, replyCap, verdict, span)
		}
		if len(replay) != 0 {
			t.Fatalf("gap_budget replayed %d frames, want 0", len(replay))
		}
	})
}

func TestResolve_eachGapVerdictHasOneCause(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		head := rapid.Uint64Range(1, 1000).Draw(t, "head")
		n := rapid.IntRange(1, int(head)).Draw(t, "n")
		floor := head - uint64(n) + 1
		replyCap := rapid.IntRange(1, n).Draw(t, "replyCap")
		var since uint64
		var want Verdict
		switch rapid.SampledFrom([]Verdict{VerdictGapAhead, VerdictGapFloor, VerdictGapBudget}).Draw(t, "cause") {
		case VerdictGapAhead:
			since = rapid.Uint64Range(head+1, MaxOffset).Draw(t, "since")
			want = VerdictGapAhead
		case VerdictGapFloor:
			if floor < 2 {
				t.Skip("ring holds every offset; no floor gap exists")
			}
			since = rapid.Uint64Range(0, floor-2).Draw(t, "since")
			want = VerdictGapFloor
		default:
			if uint64(replyCap) >= head-(floor-1) {
				t.Skip("replyCap covers the whole ring; no budget gap exists")
			}
			since = rapid.Uint64Range(floor-1, head-uint64(replyCap)-1).Draw(t, "since")
			want = VerdictGapBudget
		}
		got, resumed := resolve(Cursor{Epoch: testEpoch, Offset: since}, true, testEpoch, floor, head, n, replyCap)
		if got != want || resumed {
			t.Fatalf("resolve(since=%d, floor=%d, head=%d, n=%d, replyCap=%d) = (%s, %v), want (%s, false)", since, floor, head, n, replyCap, got, resumed, want)
		}
	})
}

func TestResolve_sinceEqualsHeadIsResumedOnEmptyRing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		head := rapid.Uint64Range(0, MaxOffset).Draw(t, "head")
		replyCap := rapid.IntRange(0, 256).Draw(t, "replyCap")
		got, resumed := resolve(Cursor{Epoch: testEpoch, Offset: head}, true, testEpoch, 0, head, 0, replyCap)
		if got != VerdictResumed || !resumed {
			t.Fatalf("resolve(since=head=%d, n=0) = (%s, %v), want (resumed, true)", head, got, resumed)
		}
	})
}

func TestResolve_offsetZeroWithHeadZeroIsResumed(t *testing.T) {
	got, resumed := resolve(Cursor{Epoch: testEpoch, Offset: 0}, true, testEpoch, 0, 0, 0, 0)
	if got != VerdictResumed || !resumed {
		t.Errorf("resolve(since=0, head=0) = (%s, %v), want (resumed, true)", got, resumed)
	}
	for _, tt := range []struct {
		name    string
		cur     Cursor
		present bool
		want    Verdict
	}{
		{name: "Absent", present: false, want: VerdictFresh},
		{name: "Invalid", cur: Cursor{}, present: true, want: VerdictCursorInvalid},
		{name: "OtherEpoch", cur: Cursor{Epoch: "00000000000000ff"}, present: true, want: VerdictEpochChanged},
	} {
		got, resumed := resolve(tt.cur, tt.present, testEpoch, 0, 0, 0, 0)
		if got != tt.want || resumed {
			t.Errorf("resolve(%s) = (%s, %v), want (%s, false)", tt.name, got, resumed, tt.want)
		}
	}
}
