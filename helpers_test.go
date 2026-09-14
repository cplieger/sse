package sse

import (
	"bytes"
	"log/slog"
	"sync"
	"testing"
	"time"
)

const testEpoch = "3f9a1c0e7b2d4a58"

// withEpoch pins the epoch through the unexported seam.
func withEpoch(epoch string) Option {
	return func(c *config) error {
		c.epoch = epoch
		return nil
	}
}

// withNow pins the ring's clock through the unexported seam.
func withNow(now func() time.Time) Option {
	return func(c *config) error {
		c.now = now
		return nil
	}
}

// mustNew is New for a test's own fixture; a refusal is a setup failure.
func mustNew(t testing.TB, opts ...Option) *Hub {
	t.Helper()
	h, err := New(opts...)
	if err != nil {
		t.Fatalf("Setup: New: %v", err)
	}
	return h
}

// logBuffer is a bytes.Buffer safe to read while stream goroutines still log.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLog returns a Debug-level text logger writing into the returned buffer.
func captureLog() (*slog.Logger, *logBuffer) {
	buf := &logBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// replayTTL is a TTL above the floor at every keepalive the tests use.
const replayTTL = 10 * time.Minute

// dataOfFrameSize returns Data such that an unnamed frame with an id of the
// given digit count encodes to exactly size bytes.
func dataOfFrameSize(size, offsetDigits int) []byte {
	return bytes.Repeat([]byte{'x'}, size-frameSize(0, [][]byte{nil}, 0)-idLineBytes(offsetDigits))
}
