package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// stampedFrame is what the client receives on the stream for one mutation:
// the frame's offset and the version its payload carries.
type stampedFrame struct {
	version string
	offset  uint64
}

// digestOutcome is one interleaving of a digest against a concurrent publish:
// the response the client read and the frame it received.
type digestOutcome struct {
	response map[string]any
	frame    stampedFrame
}

// versionMapModel is the reference client of design section 8.4: it applies
// every frame and observes the stamp on commit, and takes a changed entry as a
// fetch instruction rather than a version.
type versionMapModel struct {
	held map[Subject]string
}

func (m *versionMapModel) applyFrame(s Subject, f stampedFrame) {
	m.held[s] = f.version
}

func (m *versionMapModel) applyDigest(resp map[string]any, refetch func(Subject) string) {
	changed, _ := resp["changed"].([]any)
	for _, c := range changed {
		entry, _ := c.(map[string]any)
		s := Subject{Kind: fmt.Sprint(entry["kind"]), Ref: fmt.Sprint(entry["ref"])}
		m.held[s] = refetch(s)
	}
}

// runRace drives one interleaving: the resolver answers 41 while a publish
// carrying 42 lands either before the resolver returns (so the response head
// covers the frame) or after the response left (so the frame is ahead of head).
func runRace(t testing.TB, publishBefore bool, framesAhead int) digestOutcome {
	t.Helper()
	subject := Subject{Kind: "chat", Ref: "c1"}
	h := mustNew(t, withEpoch(testEpoch), WithReplay(64), WithReplayTTL(replayTTL))
	for range framesAhead {
		if _, err := h.Publish(Event{Name: "other", Data: []byte("{}")}); err != nil {
			t.Fatal(err)
		}
	}
	var frame stampedFrame
	publish42 := func() {
		off, err := h.Publish(Event{Name: "chat", Data: []byte(`{"ref":"c1","version":"42"}`)})
		if err != nil {
			t.Fatal(err)
		}
		frame = stampedFrame{version: "42", offset: off}
	}
	handler := h.DigestHandler(func(_ context.Context, held []Held) ([]State, error) {
		states := []State{{Subject: subject, Version: "41"}}
		if publishBefore {
			publish42()
		}
		return states, nil
	})
	req := httptest.NewRequest(http.MethodPost, "/digest", strings.NewReader(`{"epoch":"`+testEpoch+`","subjects":[{"kind":"chat","ref":"c1","version":"41"}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if !publishBefore {
		publish42()
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("digest response %q: %v", rec.Body.String(), err)
	}
	return digestOutcome{response: resp, frame: frame}
}

func headOf(t testing.TB, resp map[string]any) uint64 {
	t.Helper()
	head, err := strconv.ParseUint(fmt.Sprint(resp["head"]), 10, 64)
	if err != nil {
		t.Fatalf("head %v is not a decimal string: %v", resp["head"], err)
	}
	return head
}

func TestDigest_version41vs42Race(t *testing.T) {
	subject := Subject{Kind: "chat", Ref: "c1"}
	rapid.Check(t, func(rt *rapid.T) {
		publishBefore := rapid.Bool().Draw(rt, "publishBefore")
		framesAhead := rapid.IntRange(0, 5).Draw(rt, "framesAhead")
		digestFirst := rapid.Bool().Draw(rt, "digestFirst")
		out := runRace(t, publishBefore, framesAhead)
		if out.response["must_refetch"] != false {
			rt.Fatalf("response = %v, want a comparable answer", out.response)
		}
		head := headOf(t, out.response)
		if publishBefore && out.frame.offset > head {
			rt.Fatalf("publish before the resolver returned landed at %d, above head %d", out.frame.offset, head)
		}
		if !publishBefore && out.frame.offset <= head {
			rt.Fatalf("publish after the response landed at %d, not above head %d", out.frame.offset, head)
		}

		model := &versionMapModel{held: map[Subject]string{subject: "41"}}
		if digestFirst {
			model.applyDigest(out.response, func(Subject) string { return "42" })
			model.applyFrame(subject, out.frame)
		} else {
			model.applyFrame(subject, out.frame)
			model.applyDigest(out.response, func(Subject) string { return "42" })
		}
		if got := model.held[subject]; got != "42" {
			rt.Fatalf("reference client holds %q after publishBefore=%v digestFirst=%v head=%d offset=%d, want 42", got, publishBefore, digestFirst, head, out.frame.offset)
		}
	})
}

// A client that drops frames at or below the digest's head misses the race:
// the resolver read 41 before the mutation, the mutation's frame landed at
// head, and nothing else ever carries 42.
func TestDigest_droppingFramesBelowHeadLosesTheRace(t *testing.T) {
	subject := Subject{Kind: "chat", Ref: "c1"}
	out := runRace(t, true, 0)
	head := headOf(t, out.response)
	if out.frame.offset > head {
		t.Fatalf("Setup: frame offset %d is above head %d; the race needs it covered", out.frame.offset, head)
	}
	dropping := &versionMapModel{held: map[Subject]string{subject: "41"}}
	if out.frame.offset > head {
		dropping.applyFrame(subject, out.frame)
	}
	dropping.applyDigest(out.response, func(Subject) string { return "42" })
	if got := dropping.held[subject]; got != "41" {
		t.Fatalf("dropping client holds %q, want 41: the property must be able to fail for a client that trusts head", got)
	}
}
