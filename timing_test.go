package sse

import (
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// timingContract is the machine-readable half of the shared timing table;
// the TypeScript client pins its constants to the same file.
type timingContract struct {
	MaxOffset             string `json:"max_offset"`
	KeepaliveEvent        string `json:"keepalive_event"`
	Wire                  int    `json:"wire"`
	KeepaliveMS           int    `json:"keepalive_ms"`
	WriteTimeoutBeats     int    `json:"write_timeout_beats"`
	MaxFrameBytes         int    `json:"max_frame_bytes"`
	ReplayMaxBytesFrames  int    `json:"replay_max_bytes_frames"`
	ResetWriteTimeoutMS   int    `json:"reset_write_timeout_ms"`
	WatchdogBeats         int    `json:"watchdog_beats"`
	WatchdogFloorMS       int    `json:"watchdog_floor_ms"`
	RetryMS               int    `json:"retry_ms"`
	CapMS                 int    `json:"cap_ms"`
	ReplyMaxEventsCeiling int    `json:"reply_max_events_ceiling"`
	ClientBufferFloor     int    `json:"client_buffer_floor"`
	ReplayTTLFloorExtraMS int    `json:"replay_ttl_floor_extra_ms"`
	HeldMaxBytesFrames    int    `json:"held_max_bytes_frames"`
	DigestMaxSubjects     int    `json:"digest_max_subjects"`
	DigestMaxBodyBytes    int    `json:"digest_max_body_bytes"`
}

func TestTiming_goConstantsMatchContract(t *testing.T) {
	raw, err := os.ReadFile("timing.json")
	if err != nil {
		t.Fatalf("read timing.json: %v", err)
	}
	var c timingContract
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("decode timing.json: %v", err)
	}
	h := mustNew(t)
	ms := func(d time.Duration) int { return int(d.Milliseconds()) }

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Wire", Wire, c.Wire},
		{"MaxFrameBytes", MaxFrameBytes, c.MaxFrameBytes},
		{"MaxOffset", strconv.FormatUint(MaxOffset, 10), c.MaxOffset},
		{"defaultKeepaliveEvent", defaultKeepaliveEvent, c.KeepaliveEvent},
		{"defaultKeepalive", ms(h.cfg.keepalive), c.KeepaliveMS},
		{"writeTimeout default", ms(h.cfg.writeTimeout), c.WriteTimeoutBeats * c.KeepaliveMS},
		{"writeTimeoutBeats", writeTimeoutBeats, c.WriteTimeoutBeats},
		{"defaultReplayMaxBytes", h.cfg.maxBytes, c.ReplayMaxBytesFrames * c.MaxFrameBytes},
		{"resetWriteTimeout", ms(resetWriteTimeout), c.ResetWriteTimeoutMS},
		{"defaultReconnectDelay", ms(h.cfg.reconnectDelay), c.RetryMS},
		{"replyMaxCeiling", replyMaxCeiling, c.ReplyMaxEventsCeiling},
		{"clientBufferFloor", clientBufferFloor, c.ClientBufferFloor},
		{"watchdogBeats", watchdogBeats, c.WatchdogBeats},
		{"watchdogFloor", ms(watchdogFloor), c.WatchdogFloorMS},
		{"replayTTLFloorBase", ms(replayTTLFloorBase), c.ReplayTTLFloorExtraMS},
		{"replayTTLFloorBase is cap_ms", ms(replayTTLFloorBase), c.CapMS},
		{"replayTTLFloor at default keepalive", ms(replayTTLFloor(h.cfg.keepalive)), max(c.WatchdogBeats*c.KeepaliveMS, c.WatchdogFloorMS) + c.ReplayTTLFloorExtraMS},
		{"replayTTLFloor at 1s keepalive", ms(replayTTLFloor(time.Second)), c.WatchdogFloorMS + c.ReplayTTLFloorExtraMS},
		{"held_max_bytes_frames equals replay_max_bytes_frames", c.HeldMaxBytesFrames, c.ReplayMaxBytesFrames},
		{"defaultDigestMaxSubjects", defaultDigestMaxSubjects, c.DigestMaxSubjects},
		{"defaultDigestMaxBody", defaultDigestMaxBody, c.DigestMaxBodyBytes},
	}
	for _, chk := range checks {
		if chk.got != chk.want {
			t.Errorf("%s = %v, want %v from timing.json", chk.name, chk.got, chk.want)
		}
	}
	if got := mustNew(t, WithReplay(1024), WithReplayTTL(replayTTL)); got.cfg.replyMax != c.ReplyMaxEventsCeiling || got.cfg.clientBuffer != 1024 {
		t.Errorf("New(WithReplay(1024)) replyMax/clientBuffer = %d/%d, want %d/1024", got.cfg.replyMax, got.cfg.clientBuffer, c.ReplyMaxEventsCeiling)
	}
}

// TestTiming_goModRequires pins the module's dependency footprint: the webhttp
// root package at runtime and rapid for the property tests, nothing else.
func TestTiming_goModRequires(t *testing.T) {
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	inBlock := false
	for line := range strings.SplitSeq(string(raw), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "require (":
			inBlock = true
		case line == ")":
			inBlock = false
		case strings.HasPrefix(line, "require "):
			got = append(got, strings.Fields(line)[1])
		case inBlock && line != "":
			got = append(got, strings.Fields(line)[0])
		}
	}
	want := []string{"github.com/cplieger/webhttp/v3", "pgregory.net/rapid"}
	if !slices.Equal(got, want) {
		t.Errorf("go.mod requires = %q, want exactly %q", got, want)
	}
}
