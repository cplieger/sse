package sse

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNew_refusalMatrix(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{name: "SubMillisecondKeepalive", opts: []Option{WithKeepalive(500 * time.Microsecond)}, wantErr: true},
		{name: "ZeroClientBuffer", opts: []Option{WithClientBuffer(0)}, wantErr: true},
		{name: "WriteTimeoutEqualsKeepalive", opts: []Option{WithKeepalive(time.Second), WithWriteTimeout(time.Second)}, wantErr: true},
		{name: "WriteTimeoutOneMillisecondAbove", opts: []Option{WithKeepalive(time.Second), WithWriteTimeout(time.Second + time.Millisecond)}},
		{name: "ReplyCapAboveRing", opts: []Option{WithReplay(16), WithReplayTTL(replayTTL), WithReplyMaxEvents(17)}, wantErr: true},
		{name: "ReplyCapEqualsRing", opts: []Option{WithReplay(16), WithReplayTTL(replayTTL), WithReplyMaxEvents(16)}},
		{name: "NegativeReplyCap", opts: []Option{WithReplyMaxEvents(-1)}, wantErr: true},
		{name: "RingWithoutTTL", opts: []Option{WithReplay(16)}, wantErr: true},
		{name: "ZeroTTL", opts: []Option{WithReplayTTL(0)}, wantErr: true},
		{name: "LineFeedKeepaliveName", opts: []Option{WithKeepaliveEvent("beat\nid: 9")}, wantErr: true},
		{name: "CarriageReturnKeepaliveName", opts: []Option{WithKeepaliveEvent("beat\rid: 9")}, wantErr: true},
		{name: "EmptyKeepaliveName", opts: []Option{WithKeepaliveEvent("")}},
		{name: "NegativeRing", opts: []Option{WithReplay(-1)}, wantErr: true},
		{name: "ByteCapBelowOneFrame", opts: []Option{WithReplayMaxBytes(MaxFrameBytes - 1)}, wantErr: true},
		{name: "ByteCapOfOneFrame", opts: []Option{WithReplayMaxBytes(MaxFrameBytes)}},
		{name: "ZeroReconnectDelay", opts: []Option{WithReconnectDelay(0)}, wantErr: true},
		{name: "NegativeMaxClientsClamps", opts: []Option{WithMaxClients(-4)}},
		{name: "NilOptionSkipped", opts: []Option{nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := New(tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New(%s) error = %v, want error %v", tt.name, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrConfig) {
				t.Errorf("New(%s) error = %v, want it to wrap ErrConfig", tt.name, err)
			}
			if err == nil && h == nil {
				t.Errorf("New(%s) = nil hub with nil error", tt.name)
			}
		})
	}
}

func TestNew_crossFieldRulesInBothOrders(t *testing.T) {
	tests := []struct {
		name    string
		a, b    Option
		wantErr bool
	}{
		{name: "WriteTimeoutAboveKeepalive", a: WithWriteTimeout(20 * time.Second), b: WithKeepalive(15 * time.Second)},
		{name: "ReplyCapAboveRing", a: WithReplyMaxEvents(512), b: WithReplay(256), wantErr: true},
		{name: "RingWithTTL", a: WithReplay(256), b: WithReplayTTL(replayTTL)},
		{name: "TTLBelowFloorForKeepalive", a: WithReplayTTL(50 * time.Second), b: WithKeepalive(10 * time.Second), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A ring without a TTL is refused for its own reason, so the cap case
			// carries one to isolate the rule under test.
			extra := []Option{}
			if tt.name == "ReplyCapAboveRing" {
				extra = append(extra, WithReplayTTL(replayTTL))
			}
			_, errAB := New(append([]Option{tt.a, tt.b}, extra...)...)
			_, errBA := New(append([]Option{tt.b, tt.a}, extra...)...)
			if (errAB != nil) != tt.wantErr {
				t.Errorf("New(a, b) error = %v, want error %v", errAB, tt.wantErr)
			}
			if (errBA != nil) != tt.wantErr {
				t.Errorf("New(b, a) error = %v, want error %v", errBA, tt.wantErr)
			}
		})
	}
}

func TestNew_noOptionsIsConstructible(t *testing.T) {
	h := mustNew(t)
	if got := h.cfg.clientBuffer; got != 256 {
		t.Errorf("New() clientBuffer = %d, want 256", got)
	}
	if got := h.cfg.ringSize; got != 0 {
		t.Errorf("New() ringSize = %d, want 0", got)
	}
	if got := h.cfg.replyMax; got != 0 {
		t.Errorf("New() replyMax = %d, want 0", got)
	}
	if got := len(h.epoch); got != 16 || !isEpoch(h.epoch) {
		t.Errorf("New() epoch = %q, want 16 lowercase hex characters", h.epoch)
	}
}

func TestNew_ttlFloorAtBothArms(t *testing.T) {
	tests := []struct {
		name      string
		keepalive time.Duration
		ttl       time.Duration
		wantErr   bool
	}{
		{name: "Default15sKeepaliveBelow", keepalive: 15 * time.Second, ttl: 75*time.Second - time.Millisecond, wantErr: true},
		{name: "Default15sKeepaliveAt", keepalive: 15 * time.Second, ttl: 75 * time.Second},
		{name: "OneSecondKeepaliveBelow", keepalive: time.Second, ttl: 45*time.Second - time.Millisecond, wantErr: true},
		{name: "OneSecondKeepaliveAt", keepalive: time.Second, ttl: 45 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(WithReplay(8), WithKeepalive(tt.keepalive), WithReplayTTL(tt.ttl))
			if (err != nil) != tt.wantErr {
				t.Errorf("New(WithKeepalive(%v), WithReplayTTL(%v)) error = %v, want error %v", tt.keepalive, tt.ttl, err, tt.wantErr)
			}
		})
	}
}

func TestNew_derivedDefaults(t *testing.T) {
	tests := []struct {
		name             string
		opts             []Option
		wantWriteTimeout time.Duration
		wantReplyMax     int
		wantBuffer       int
	}{
		{name: "Defaults", wantWriteTimeout: 30 * time.Second, wantReplyMax: 0, wantBuffer: 256},
		{name: "SmallRing", opts: []Option{WithReplay(16), WithReplayTTL(replayTTL)}, wantWriteTimeout: 30 * time.Second, wantReplyMax: 16, wantBuffer: 256},
		{name: "LargeRing", opts: []Option{WithReplay(1024), WithReplayTTL(replayTTL)}, wantWriteTimeout: 30 * time.Second, wantReplyMax: 256, wantBuffer: 1024},
		{name: "KeepaliveDrivesWriteTimeout", opts: []Option{WithKeepalive(5 * time.Second)}, wantWriteTimeout: 10 * time.Second, wantReplyMax: 0, wantBuffer: 256},
		{name: "ExplicitBufferWins", opts: []Option{WithClientBuffer(32), WithReplay(1024), WithReplayTTL(replayTTL)}, wantWriteTimeout: 30 * time.Second, wantReplyMax: 256, wantBuffer: 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := mustNew(t, tt.opts...)
			if got := h.cfg.writeTimeout; got != tt.wantWriteTimeout {
				t.Errorf("New(%s) writeTimeout = %v, want %v", tt.name, got, tt.wantWriteTimeout)
			}
			if got := h.cfg.replyMax; got != tt.wantReplyMax {
				t.Errorf("New(%s) replyMax = %d, want %d", tt.name, got, tt.wantReplyMax)
			}
			if got := h.cfg.clientBuffer; got != tt.wantBuffer {
				t.Errorf("New(%s) clientBuffer = %d, want %d", tt.name, got, tt.wantBuffer)
			}
		})
	}
}

func TestMustNew_panicsWithConfigMessage(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustNew(WithReplay(-1)) did not panic")
		}
		if s, ok := r.(string); !ok || !strings.Contains(s, ErrConfig.Error()) {
			t.Errorf("MustNew panic = %v, want the ErrConfig message", r)
		}
	}()
	MustNew(WithReplay(-1))
}
