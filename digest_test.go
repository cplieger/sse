package sse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// staticResolver answers every held subject from a fixed table; a subject
// missing from the table is answered gone.
func staticResolver(states map[Subject]State) Resolver {
	return func(_ context.Context, held []Held) ([]State, error) {
		out := make([]State, 0, len(held))
		for _, hd := range held {
			s, ok := states[hd.Subject]
			if !ok {
				s = State{Subject: hd.Subject, Status: StatusGone}
			}
			out = append(out, s)
		}
		return out, nil
	}
}

func current(kind, ref, version string) State {
	return State{Kind: kind, Ref: ref, Version: version}
}

// postDigest sends body to the handler as application/json and decodes the
// envelope or the response into a generic map.
func postDigest(t *testing.T, handler http.Handler, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/digest", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("response %q is not JSON: %v", rec.Body.String(), err)
	}
	return rec, got
}

func digestBody(t *testing.T, epoch string, subjects ...Held) string {
	t.Helper()
	wire := make([]digestSubject, len(subjects))
	for i, s := range subjects {
		wire[i] = digestSubject{Kind: s.Kind, Ref: s.Ref, Version: s.Version}
	}
	req := map[string]any{"subjects": wire}
	if epoch != "" {
		req["epoch"] = epoch
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDigest_methodNotAllowed(t *testing.T) {
	h := mustNew(t)
	handler := h.DigestHandler(staticResolver(nil))
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, "/digest", http.NoBody))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s status = %d, want 405", method, rec.Code)
		}
		if got := rec.Header().Get("Allow"); got != "POST" {
			t.Errorf("%s Allow = %q, want POST", method, got)
		}
		if !strings.Contains(rec.Body.String(), `"code":"digest_method"`) {
			t.Errorf("%s body = %q, want code digest_method", method, rec.Body.String())
		}
	}
}

func TestDigest_contentType(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch))
	handler := h.DigestHandler(staticResolver(nil))
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded", "application/json-patch+json"} {
		req := httptest.NewRequest(http.MethodPost, "/digest", strings.NewReader(`{"subjects":[]}`))
		req.Header.Set("Content-Type", ct)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnsupportedMediaType || !strings.Contains(rec.Body.String(), `"code":"digest_content_type"`) {
			t.Errorf("Content-Type %q: status %d body %q, want 415 digest_content_type", ct, rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/digest", strings.NewReader(`{"subjects":[]}`))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Content-Type with a charset parameter: status %d body %q, want 200", rec.Code, rec.Body.String())
	}
}

func TestDigest_bodyOverCap413(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch))
	handler := h.DigestHandler(staticResolver(nil), WithDigestMaxBody(64))
	rec, got := postDigest(t, handler, `{"subjects":[],"padding":"`+strings.Repeat("x", 100)+`"}`)
	if rec.Code != http.StatusRequestEntityTooLarge || got["code"] != "digest_too_large" {
		t.Errorf("status %d body %v, want 413 digest_too_large", rec.Code, got)
	}
}

func TestDigest_validationTable(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch))
	handler := h.DigestHandler(staticResolver(nil))
	sub := func(kind, ref, version string) string {
		b, _ := json.Marshal(digestSubject{Kind: kind, Ref: ref, Version: version})
		return string(b)
	}
	many := make([]string, 257)
	for i := range many {
		many[i] = sub("chat", fmt.Sprintf("c%d", i), "1")
	}
	tests := []struct {
		name     string
		body     string
		wantRule string
	}{
		{"bad epoch", `{"epoch":"ZZ","subjects":[]}`, "epoch"},
		{"subjects absent", `{"epoch":"` + testEpoch + `"}`, "subjects is required"},
		{"subjects null", `{"subjects":null}`, "subjects is required"},
		{"bad kind uppercase", `{"subjects":[` + sub("Chat", "", "1") + `]}`, "kind"},
		{"bad kind leading digit", `{"subjects":[` + sub("1chat", "", "1") + `]}`, "kind"},
		{"kind empty", `{"subjects":[` + sub("", "", "1") + `]}`, "kind"},
		{"kind 33 bytes", `{"subjects":[` + sub(strings.Repeat("k", 33), "", "1") + `]}`, "kind"},
		{"ref over 512 bytes", `{"subjects":[` + sub("chat", strings.Repeat("r", 513), "1") + `]}`, "ref"},
		{"ref with U+202E", `{"subjects":[` + sub("chat", "a\u202eb", "1") + `]}`, "ref"},
		{"ref with U+0085", `{"subjects":[` + sub("chat", "a\u0085b", "1") + `]}`, "ref"},
		{"ref with LF", `{"subjects":[` + sub("chat", "a\nb", "1") + `]}`, "ref"},
		{"ref with U+2028", `{"subjects":[` + sub("chat", "a\u2028b", "1") + `]}`, "ref"},
		{"version empty", `{"subjects":[` + sub("chat", "", "") + `]}`, "version"},
		{"version 65 bytes", `{"subjects":[` + sub("chat", "", strings.Repeat("v", 65)) + `]}`, "version"},
		{"version non-ASCII", `{"subjects":[` + sub("chat", "", "v\u00e9") + `]}`, "version"},
		{"version control", `{"subjects":[` + sub("chat", "", "v\t1") + `]}`, "version"},
		{"duplicate key", `{"subjects":[` + sub("chat", "c1", "1") + `,` + sub("chat", "c1", "2") + `]}`, "duplicates"},
		{"257 subjects", `{"subjects":[` + strings.Join(many, ",") + `]}`, "subjects holds 257"},
		{"trailing data", `{"subjects":[]} {"subjects":[]}`, "malformed"},
		{"not an object", `[1,2]`, "malformed"},
		{"empty body", ``, "malformed"},
	}
	for _, tc := range tests {
		t.Run(strings.ReplaceAll(tc.name, " ", "_"), func(t *testing.T) {
			rec, got := postDigest(t, handler, tc.body)
			if rec.Code != http.StatusBadRequest || got["code"] != "digest_invalid" {
				t.Fatalf("body %q: status %d response %v, want 400 digest_invalid", tc.body, rec.Code, got)
			}
			msg, _ := got["error"].(string)
			if !strings.Contains(msg, tc.wantRule) {
				t.Errorf("body %q: message %q, want it to name %q", tc.body, msg, tc.wantRule)
			}
		})
	}
}

func TestDigest_unknownFieldsIgnored(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch))
	handler := h.DigestHandler(staticResolver(map[Subject]State{{Kind: "chat", Ref: "c1"}: current("chat", "c1", "1")}))
	rec, got := postDigest(t, handler, `{"epoch":"`+testEpoch+`","future":true,"subjects":[{"kind":"chat","ref":"c1","version":"1","extra":1}]}`)
	if rec.Code != http.StatusOK || got["must_refetch"] != false || got["checked"] != float64(1) {
		t.Errorf("status %d response %v, want 200 checked 1 with unknown fields ignored", rec.Code, got)
	}
}

func TestDigest_emptySubjectsAnswersPosition(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch), WithReplay(8), WithReplayTTL(replayTTL))
	for range 3 {
		if _, err := h.Publish(Event{Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	called := false
	handler := h.DigestHandler(func(context.Context, []Held) ([]State, error) {
		called = true
		return nil, nil
	})
	rec, got := postDigest(t, handler, `{"subjects":[]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	want := map[string]any{
		"epoch": testEpoch, "head": "3", "floor": "1", "checked": float64(0), "must_refetch": false,
		"changed": []any{}, "removed": []any{},
	}
	for k, v := range want {
		if fmt.Sprint(got[k]) != fmt.Sprint(v) {
			t.Errorf("response[%q] = %v, want %v (full: %v)", k, got[k], v, got)
		}
	}
	if called {
		t.Error("the resolver ran for an empty subjects array, want it skipped")
	}
}

func TestDigest_epochMismatchMustRefetch(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger), withEpoch(testEpoch))
	called := false
	handler := h.DigestHandler(func(context.Context, []Held) ([]State, error) {
		called = true
		return nil, nil
	})
	const other = "0000000000000000"
	for _, epoch := range []string{"", other} {
		rec, got := postDigest(t, handler, digestBody(t, epoch, Held{Subject{"chat", "c1"}, "1"}))
		if rec.Code != http.StatusOK || got["must_refetch"] != true || got["epoch"] != testEpoch || got["checked"] != float64(0) {
			t.Errorf("epoch %q: status %d response %v, want 200 must_refetch with the hub's epoch and checked 0", epoch, rec.Code, got)
		}
		if changed, _ := got["changed"].([]any); len(changed) != 0 {
			t.Errorf("epoch %q: changed = %v, want empty on must_refetch", epoch, changed)
		}
	}
	if called {
		t.Error("the resolver ran on an epoch mismatch, want it skipped")
	}
	if !strings.Contains(logged.String(), "request_epoch="+other) || !strings.Contains(logged.String(), "epoch="+testEpoch) {
		t.Errorf("log = %q, want a Debug line with both epochs", logged)
	}
}

func TestDigest_resolverErrorMustRefetch(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger), withEpoch(testEpoch))
	handler := h.DigestHandler(func(context.Context, []Held) ([]State, error) {
		return nil, errors.New("store locked")
	})
	rec, got := postDigest(t, handler, digestBody(t, testEpoch, Held{Subject{"chat", "c1"}, "1"}, Held{Subject{"chat", "c2"}, "1"}))
	if rec.Code != http.StatusOK || got["must_refetch"] != true {
		t.Errorf("status %d response %v, want 200 must_refetch", rec.Code, got)
	}
	if !strings.Contains(logged.String(), "level=WARN") || !strings.Contains(logged.String(), "store locked") || !strings.Contains(logged.String(), "subjects=2") {
		t.Errorf("log = %q, want a Warn with the error and subjects=2", logged)
	}
}

func TestDigest_keyMismatchClasses(t *testing.T) {
	c1 := Subject{Kind: "chat", Ref: "c1"}
	c2 := Subject{Kind: "chat", Ref: "c2"}
	tests := []struct {
		name      string
		states    []State
		wantClass string
		wantRef   string
		wantCount int
	}{
		{"missing", []State{current("chat", "c1", "1")}, "missing", "c2", 1},
		{"duplicate", []State{current("chat", "c1", "1"), current("chat", "c1", "1"), current("chat", "c2", "1")}, "duplicate", "c1", 3},
		{"unrequested", []State{current("chat", "c1", "1"), current("chat", "c2", "1"), current("chat", "c9", "1")}, "unrequested", "c9", 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, logged := captureLog()
			h := mustNew(t, WithLogger(logger), withEpoch(testEpoch))
			handler := h.DigestHandler(func(context.Context, []Held) ([]State, error) { return tc.states, nil })
			rec, got := postDigest(t, handler, digestBody(t, testEpoch, Held{c1, "1"}, Held{c2, "1"}))
			if rec.Code != http.StatusOK || got["must_refetch"] != true {
				t.Errorf("status %d response %v, want 200 must_refetch", rec.Code, got)
			}
			log := logged.String()
			for _, want := range []string{"level=ERROR", "class=" + tc.wantClass, "request_subjects=2", fmt.Sprintf("resolver_states=%d", tc.wantCount), "kind=chat", "ref=" + tc.wantRef} {
				if !strings.Contains(log, want) {
					t.Errorf("log = %q, want %q", log, want)
				}
			}
		})
	}
}

func TestDigest_unrequestedKeyIsSanitisedInLog(t *testing.T) {
	logger, logged := captureLog()
	h := mustNew(t, WithLogger(logger), withEpoch(testEpoch))
	hostile := "evil\r\n\u202e" + strings.Repeat("A", 200)
	handler := h.DigestHandler(func(context.Context, []Held) ([]State, error) {
		return []State{current("chat", "c1", "1"), current("chat", hostile, "1")}, nil
	})
	rec, got := postDigest(t, handler, digestBody(t, testEpoch, Held{Subject{"chat", "c1"}, "1"}))
	if rec.Code != http.StatusOK || got["must_refetch"] != true {
		t.Errorf("status %d response %v, want 200 must_refetch", rec.Code, got)
	}
	log := logged.String()
	if strings.Contains(log, "\u202e") || strings.Contains(log, "evil\r") || strings.Contains(log, "evil\n") {
		t.Errorf("log = %q, want CR, LF and U+202E stripped", log)
	}
	if strings.Contains(log, strings.Repeat("A", 125)) || !strings.Contains(log, "ref=evil"+strings.Repeat("A", 100)) {
		t.Errorf("log = %q, want the ref truncated at the %d-byte cap", log, maxLogValueBytes)
	}
}

func TestDigest_forbiddenAndGonePassThrough(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch))
	handler := h.DigestHandler(staticResolver(map[Subject]State{
		{Kind: "chat", Ref: "c1"}:      current("chat", "c1", "44"),
		{Kind: "chat", Ref: "c2"}:      current("chat", "c2", "7"),
		{Kind: "live_turn", Ref: "c1"}: {Subject: Subject{Kind: "live_turn", Ref: "c1"}, Status: StatusGone},
		{Kind: "chat", Ref: "secret"}:  {Subject: Subject{Kind: "chat", Ref: "secret"}, Status: StatusForbidden},
	}))
	rec, got := postDigest(t, handler, digestBody(t, testEpoch,
		Held{Subject{"chat", "c1"}, "41"}, Held{Subject{"chat", "c2"}, "7"},
		Held{Subject{"live_turn", "c1"}, "17:12"}, Held{Subject{"chat", "secret"}, "1"}))
	if rec.Code != http.StatusOK || got["must_refetch"] != false || got["checked"] != float64(4) {
		t.Fatalf("status %d response %v, want 200 checked 4", rec.Code, got)
	}
	wantChanged := `[{"kind":"chat","ref":"c1","version":"44"}]`
	wantRemoved := `[{"kind":"live_turn","reason":"gone","ref":"c1"},{"kind":"chat","reason":"forbidden","ref":"secret"}]`
	if changed, _ := json.Marshal(got["changed"]); string(changed) != wantChanged {
		t.Errorf("changed = %s, want %s", changed, wantChanged)
	}
	if removed, _ := json.Marshal(got["removed"]); string(removed) != wantRemoved {
		t.Errorf("removed = %s, want %s", removed, wantRemoved)
	}
}

func TestDigest_headReadAfterResolver(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch), WithReplay(8), WithReplayTTL(replayTTL))
	var published uint64
	handler := h.DigestHandler(func(_ context.Context, held []Held) ([]State, error) {
		off, err := h.Publish(Event{Data: []byte("during-resolve")})
		if err != nil {
			return nil, err
		}
		published = off
		return []State{current("chat", "c1", "1")}, nil
	})
	rec, got := postDigest(t, handler, digestBody(t, testEpoch, Held{Subject{"chat", "c1"}, "1"}))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got["head"] != fmt.Sprint(published) {
		t.Errorf("head = %v, want %d (the offset the resolver published)", got["head"], published)
	}
}

func TestDigest_stringOffsets(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch), WithReplay(8), WithReplayTTL(replayTTL))
	for range 12 {
		if _, err := h.Publish(Event{Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	handler := h.DigestHandler(staticResolver(nil))
	req := httptest.NewRequest(http.MethodPost, "/digest", strings.NewReader(`{"subjects":[]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, `"head":"12"`) || !strings.Contains(body, `"floor":"5"`) {
		t.Errorf("body = %s, want head and floor as decimal strings \"12\" and \"5\"", body)
	}
}

func TestDigest_worstCaseBatchFitsDefaultCap(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch))
	states := make(map[Subject]State, defaultDigestMaxSubjects)
	subjects := make([]Held, defaultDigestMaxSubjects)
	for i := range subjects {
		ref := strings.Repeat(`\`, maxRefBytes-3) + fmt.Sprintf("%03d", i)
		subjects[i] = Held{Kind: strings.Repeat("k", maxKindBytes), Ref: ref, Version: strings.Repeat(`"`, maxVersionBytes)}
		states[subjects[i].Subject] = State{Subject: subjects[i].Subject, Version: subjects[i].Version}
	}
	body := digestBody(t, testEpoch, subjects...)
	if len(body) >= defaultDigestMaxBody {
		t.Fatalf("worst-case body is %d bytes, want under the %d-byte default cap", len(body), defaultDigestMaxBody)
	}
	rec, got := postDigest(t, h.DigestHandler(staticResolver(states)), body)
	if rec.Code != http.StatusOK || got["must_refetch"] != false || got["checked"] != float64(defaultDigestMaxSubjects) {
		t.Errorf("status %d response %v, want 200 checked %d for a %d-byte body", rec.Code, got, defaultDigestMaxSubjects, len(body))
	}
	t.Logf("worst-case batch: %d bytes of %d", len(body), defaultDigestMaxBody)
}

func TestSanitizeLogValue_behavior(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "chat/c1", "chat/c1"},
		{"controls stripped", "a\x00b\x1fc\x7fd\u0085e", "abcde"},
		{"newlines stripped", "one\r\ntwo", "onetwo"},
		{"bidi stripped", "a\u202eb\u2066c\u061cd\u200ee", "abcde"},
		{"line separators stripped", "a\u2028b\u2029c", "abc"},
		{"invalid utf8 dropped", "a\xffb", "ab"},
		{"cap on rune boundary", strings.Repeat("x", 126) + "\u00e9\u00e9", strings.Repeat("x", 126) + "\u00e9"},
		{"cap exact", strings.Repeat("y", 128), strings.Repeat("y", 128)},
		{"cap over", strings.Repeat("z", 129), strings.Repeat("z", 128)},
	}
	for _, tc := range tests {
		t.Run(strings.ReplaceAll(tc.name, " ", "_"), func(t *testing.T) {
			if got := sanitizeLogValue(tc.in); got != tc.want {
				t.Errorf("sanitizeLogValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestDigest_nilOptionAndDefaults(t *testing.T) {
	h := mustNew(t, withEpoch(testEpoch))
	handler := h.DigestHandler(staticResolver(nil), nil, WithDigestMaxSubjects(0), WithDigestMaxBody(-1))
	body := bytes.Repeat([]byte(" "), 4096)
	body = append(body, `{"subjects":[]}`...)
	rec, got := postDigest(t, handler, string(body))
	if rec.Code != http.StatusOK || got["checked"] != float64(0) {
		t.Errorf("status %d response %v, want 200 (non-positive caps keep the defaults, a nil option is skipped)", rec.Code, got)
	}
}
