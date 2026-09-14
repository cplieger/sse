package sse

import (
	"errors"
	"strings"
	"testing"
)

func TestParseCursor_table(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Cursor
		wantErr bool
	}{
		{name: "OffsetZero", in: testEpoch + ":0", want: Cursor{Epoch: testEpoch, Offset: 0}},
		{name: "Ordinary", in: testEpoch + ":4132", want: Cursor{Epoch: testEpoch, Offset: 4132}},
		{name: "MaxOffset", in: testEpoch + ":9007199254740991", want: Cursor{Epoch: testEpoch, Offset: MaxOffset}},
		{name: "MaxOffsetPlusOne", in: testEpoch + ":9007199254740992", wantErr: true},
		{name: "LeadingZero", in: testEpoch + ":01", wantErr: true},
		{name: "ThirtyFourBytes", in: testEpoch + ":90071992547409910", wantErr: true},
		{name: "UppercaseHex", in: strings.ToUpper(testEpoch) + ":1", wantErr: true},
		{name: "BareInteger", in: "42", wantErr: true},
		{name: "Empty", in: "", want: Cursor{}},
		{name: "MissingOffset", in: testEpoch + ":", wantErr: true},
		{name: "MissingEpoch", in: ":42", wantErr: true},
		{name: "ShortEpoch", in: "3f9a1c0e:42", wantErr: true},
		{name: "NegativeOffset", in: testEpoch + ":-1", wantErr: true},
		{name: "TwoColons", in: testEpoch + ":1:2", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCursor(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseCursor(%q) error = %v, want error %v", tt.in, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrCursor) {
				t.Errorf("ParseCursor(%q) error = %v, want it to wrap ErrCursor", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseCursor(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestCursor_stringRoundTrip(t *testing.T) {
	for _, offset := range []uint64{0, 1, 9, 10, 4132, MaxOffset} {
		c := Cursor{Epoch: testEpoch, Offset: offset}
		s := c.String()
		got, err := ParseCursor(s)
		if err != nil {
			t.Errorf("ParseCursor(%q) error = %v, want nil", s, err)
			continue
		}
		if got != c {
			t.Errorf("ParseCursor(Cursor%+v.String()) = %+v, want the original", c, got)
		}
	}
}
