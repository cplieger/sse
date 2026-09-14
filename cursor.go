package sse

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// MaxOffset is the largest offset ever emitted or accepted: the JavaScript
// exact-integer bound, so a peer holding offsets as Number never rounds one.
const MaxOffset uint64 = 1<<53 - 1

// epochLen is the epoch's width: 8 random bytes as lowercase hex.
const epochLen = 16

// maxCursorLen is the epoch, a colon and MaxOffset's 16 decimal digits.
const maxCursorLen = epochLen + 1 + 16

// ErrCursor is returned by ParseCursor, wrapped with the reason, for any
// non-empty string that is not a well-formed "<epoch>:<offset>".
var ErrCursor = errors.New("sse: invalid cursor")

// Cursor is a stream position: the hub epoch that issued Offset, and Offset
// itself. The zero Cursor means no position.
type Cursor struct {
	Epoch  string
	Offset uint64
}

// ParseCursor parses a Last-Event-ID value. The empty string is no cursor and
// yields the zero Cursor with a nil error. Anything else must be exactly 16
// lowercase hex characters, a colon and a decimal offset with no leading zero
// at most MaxOffset; a bare integer is invalid.
func ParseCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	if len(s) > maxCursorLen {
		return Cursor{}, fmt.Errorf("%w: %d bytes, max %d", ErrCursor, len(s), maxCursorLen)
	}
	epoch, offset, ok := strings.Cut(s, ":")
	if !ok {
		return Cursor{}, fmt.Errorf("%w: no epoch", ErrCursor)
	}
	if !isEpoch(epoch) {
		return Cursor{}, fmt.Errorf("%w: malformed epoch", ErrCursor)
	}
	if !isOffset(offset) {
		return Cursor{}, fmt.Errorf("%w: malformed offset", ErrCursor)
	}
	n, err := strconv.ParseUint(offset, 10, 64)
	if err != nil || n > MaxOffset {
		return Cursor{}, fmt.Errorf("%w: offset above MaxOffset", ErrCursor)
	}
	return Cursor{Epoch: epoch, Offset: n}, nil
}

// String renders the cursor as "<epoch>:<offset>", the id: field's value.
func (c Cursor) String() string {
	return c.Epoch + ":" + strconv.FormatUint(c.Offset, 10)
}

func isEpoch(s string) bool {
	if len(s) != epochLen {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// isOffset matches ^(0|[1-9][0-9]*)$.
func isOffset(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '0' {
		return len(s) == 1
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func newEpoch() string {
	var b [epochLen / 2]byte
	_, _ = rand.Read(b[:]) // never fails; crashes irrecoverably on entropy failure
	return hex.EncodeToString(b[:])
}
