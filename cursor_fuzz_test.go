package sse

import (
	"errors"
	"testing"
)

func FuzzParseCursor_roundTripOrRefuse(f *testing.F) {
	for _, seed := range []string{
		"", testEpoch + ":0", testEpoch + ":1", testEpoch + ":9007199254740991",
		testEpoch + ":9007199254740992", testEpoch + ":01", "42", ":1", testEpoch + ":",
		testEpoch + ":1:2", "3F9A1C0E7B2D4A58:1", testEpoch + ":18446744073709551615",
		testEpoch + ":184467440737095516150", "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00:1",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		c, err := ParseCursor(s)
		if err != nil {
			if !errors.Is(err, ErrCursor) {
				t.Fatalf("ParseCursor(%q) error = %v, want it to wrap ErrCursor", s, err)
			}
			if c != (Cursor{}) {
				t.Fatalf("ParseCursor(%q) = %+v with an error, want the zero Cursor", s, c)
			}
			return
		}
		if s == "" {
			if c != (Cursor{}) {
				t.Fatalf("ParseCursor(%q) = %+v, want the zero Cursor", s, c)
			}
			return
		}
		if c.Offset > MaxOffset {
			t.Fatalf("ParseCursor(%q).Offset = %d, above MaxOffset", s, c.Offset)
		}
		if !isEpoch(c.Epoch) {
			t.Fatalf("ParseCursor(%q).Epoch = %q, not 16 lowercase hex", s, c.Epoch)
		}
		if got := c.String(); got != s {
			t.Fatalf("ParseCursor(%q).String() = %q, want the input", s, got)
		}
	})
}
