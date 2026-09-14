package sse

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// framingCase is one row of testdata/framing.golden.json, shared with the
// TypeScript parser suite. InputHex carries the bytes a JSON string cannot
// (the invalid-UTF-8 row); Decoded is the LF-joined data a conforming
// receiver dispatches.
type framingCase struct {
	Name        string `json:"name"`
	ID          string `json:"id"`
	Event       string `json:"event"`
	Input       string `json:"input"`
	InputHex    string `json:"inputHex"`
	EncodedHex  string `json:"encodedHex"`
	Decoded     string `json:"decoded"`
	InvalidUTF8 bool   `json:"invalidUTF8"`
}

const goldenRegen = "UPDATE_GOLDEN=1 go test . -run TestFramingCorpus_golden"

func TestFramingCorpus_golden(t *testing.T) {
	id := testEpoch + ":12"
	inputs := []struct {
		name, id, event, input string
	}{
		{name: "lf", id: id, event: "notify", input: "line one\nline two"},
		{name: "crlf", id: id, event: "notify", input: "line one\r\nline two"},
		{name: "cr", id: id, event: "notify", input: "line one\rline two"},
		{name: "mixed", id: id, event: "", input: "a\r\nb\rc\nd\n"},
		{name: "bom", id: id, event: "notify", input: "\ufeff{\"k\":1}"},
		{name: "utf8-multibyte", id: id, event: "notify", input: "héllo wörld ✓ 日本語 🎉"},
		{name: "empty-data", id: id, event: "notify", input: ""},
		{name: "idless", id: "", event: "", input: "{\"x\":1}"},
		{name: "colon-in-data", id: id, event: "", input: "key: value: more"},
		{name: "leading-space", id: id, event: "", input: "  indented\n\ttabbed"},
		{name: "invalid-utf8", id: id, event: "notify", input: "ok\xffbad"},
	}
	var got []framingCase
	for _, in := range inputs {
		c := framingCase{Name: in.name, ID: in.id, Event: in.event, Input: in.input, InputHex: hex.EncodeToString([]byte(in.input))}
		h := mustNew(t, withEpoch(testEpoch))
		if _, err := h.Publish(Event{Name: in.event, Data: []byte(in.input)}); errors.Is(err, ErrInvalidUTF8) {
			c.InvalidUTF8 = true
			if head := h.Position().Head; head != 0 {
				t.Errorf("Position().Head after refused %s = %d, want 0", in.name, head)
			}
			got = append(got, c)
			continue
		} else if err != nil {
			t.Fatalf("Publish(%s) error = %v", in.name, err)
		}
		lines := splitDataLines([]byte(in.input))
		var buf bytes.Buffer
		if err := writeFrame(&buf, in.id, in.event, lines); err != nil {
			t.Fatalf("writeFrame(%s) error = %v", in.name, err)
		}
		c.EncodedHex = hex.EncodeToString(buf.Bytes())
		c.Decoded = string(bytes.Join(lines, []byte("\n")))
		got = append(got, c)
	}

	path := filepath.Join("testdata", "framing.golden.json")
	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	gotJSON = append(gotJSON, '\n')
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, gotJSON, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run %s): %v", path, goldenRegen, err)
	}
	if !bytes.Equal(gotJSON, want) {
		t.Errorf("framing corpus differs from %s; regenerate with %s and re-run web/src/parser.fuzz.node.test.ts\n--- want\n%s\n+++ got\n%s",
			path, goldenRegen, strings.TrimSpace(string(want)), strings.TrimSpace(string(gotJSON)))
	}
}
