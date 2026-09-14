package sse

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cplieger/webhttp/v3"
)

// Subject is one application-defined unit of state the digest compares: a
// Kind the application chooses and an opaque Ref within it.
type Subject struct {
	Kind string
	Ref  string
}

// Held is a Subject with the version the client presents for it.
type Held struct {
	Subject
	Version string
}

// Status is a resolver's answer for a subject it no longer serves.
type Status string

// StatusCurrent means the subject exists and Version is its current version;
// StatusGone means it no longer exists; StatusForbidden means it exists and
// the principal may not read it. In a multi-principal deployment the resolver
// decides whether the principal may learn the distinction and answers gone for
// both when it may not.
const (
	StatusCurrent   Status = ""
	StatusGone      Status = "gone"
	StatusForbidden Status = "forbidden"
)

// State is a resolver's answer for one requested subject.
type State struct {
	Subject
	Version string
	Status  Status
}

// Resolver answers the current State of every held subject, exactly one per
// requested (Kind, Ref) and none the request did not carry. It runs on the
// request goroutine with the request's context and no library-level
// concurrency bound; the application bounds it in time and in flight. The
// principal comes from ctx however the application's middleware stored it.
type Resolver func(ctx context.Context, held []Held) ([]State, error)

// DigestOption configures one DigestHandler.
type DigestOption func(*digestConfig)

type digestConfig struct {
	maxBody     int64
	maxSubjects int
}

const (
	defaultDigestMaxSubjects = 256
	defaultDigestMaxBody     = 512 << 10
	maxKindBytes             = 32
	maxRefBytes              = 512
	maxVersionBytes          = 64
	maxLogValueBytes         = 128
)

// WithDigestMaxSubjects caps the subjects one request may carry (default 256);
// a larger request is 400. Non-positive keeps the default.
func WithDigestMaxSubjects(n int) DigestOption {
	return func(c *digestConfig) {
		if n > 0 {
			c.maxSubjects = n
		}
	}
}

// WithDigestMaxBody caps the request body in bytes (default 512 KiB, sized so
// a full default batch of worst-case escaped refs and versions cannot 413).
// Non-positive keeps the default.
func WithDigestMaxBody(n int64) DigestOption {
	return func(c *digestConfig) {
		if n > 0 {
			c.maxBody = n
		}
	}
}

type digestSubject struct {
	Kind    string `json:"kind"`
	Ref     string `json:"ref"`
	Version string `json:"version"`
}

// digestRequest is the wire shape; an empty or absent epoch both mean the
// client vouches for nothing, and a nil Subjects means the field was absent.
type digestRequest struct {
	Subjects *[]digestSubject `json:"subjects"`
	Epoch    string           `json:"epoch"`
}

type digestChanged struct {
	Kind    string `json:"kind"`
	Ref     string `json:"ref"`
	Version string `json:"version"`
}

type digestRemoved struct {
	Kind   string `json:"kind"`
	Ref    string `json:"ref"`
	Reason Status `json:"reason"`
}

type digestResponse struct {
	Epoch       string          `json:"epoch"`
	Changed     []digestChanged `json:"changed"`
	Removed     []digestRemoved `json:"removed"`
	Floor       uint64          `json:"floor,string"`
	Head        uint64          `json:"head,string"`
	Checked     int             `json:"checked"`
	MustRefetch bool            `json:"must_refetch"`
}

// DigestHandler answers POST application/json digest requests: which of the
// client's held subjects changed or were removed, pinned to the hub's epoch
// and head. Mount it inside the application's authentication and cross-origin
// middleware; it performs neither check itself. The response is must_refetch
// when the request epoch is absent or not this hub's, when resolve fails, or
// when its output does not match the request by key; every well-formed
// request is answered 200.
func (h *Hub) DigestHandler(resolve Resolver, opts ...DigestOption) http.Handler {
	cfg := digestConfig{maxBody: defaultDigestMaxBody, maxSubjects: defaultDigestMaxSubjects}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			h.logger.Debug("sse: digest method refused", "method", sanitizeLogValue(r.Method))
			w.Header().Set("Allow", http.MethodPost)
			webhttp.WriteError(w, r, http.StatusMethodNotAllowed, "digest_method", "POST required")
			return
		}
		if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
			h.logger.Debug("sse: digest content type refused", "length", len(r.Header.Get("Content-Type")))
			webhttp.WriteError(w, r, http.StatusUnsupportedMediaType, "digest_content_type", "Content-Type must be application/json")
			return
		}
		var req digestRequest
		if err := webhttp.DecodeJSONInto(w, r, &req, cfg.maxBody); err != nil {
			if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
				h.logger.Debug("sse: digest body over cap", "max_body", cfg.maxBody)
				webhttp.WriteError(w, r, http.StatusRequestEntityTooLarge, "digest_too_large", fmt.Sprintf("body exceeds %d bytes", cfg.maxBody))
				return
			}
			h.logger.Debug("sse: digest body malformed", "error", err)
			webhttp.WriteError(w, r, http.StatusBadRequest, "digest_invalid", "malformed JSON body")
			return
		}
		if msg := validateDigestRequest(&req, cfg.maxSubjects); msg != "" {
			h.logger.Debug("sse: digest request invalid", "rule", msg)
			webhttp.WriteError(w, r, http.StatusBadRequest, "digest_invalid", msg)
			return
		}
		h.answerDigest(w, r, &req, resolve)
	})
}

func (h *Hub) answerDigest(w http.ResponseWriter, r *http.Request, req *digestRequest, resolve Resolver) {
	subjects := *req.Subjects
	if len(subjects) == 0 {
		h.writeDigest(w, h.Position(), nil, nil, 0, false)
		return
	}
	if req.Epoch != h.epoch {
		h.logger.Debug("sse: digest epoch mismatch", "request_epoch", req.Epoch, "epoch", h.epoch)
		h.writeDigest(w, h.Position(), nil, nil, 0, true)
		return
	}
	held := make([]Held, len(subjects))
	for i, s := range subjects {
		held[i] = Held{Kind: s.Kind, Ref: s.Ref, Version: s.Version}
	}
	states, err := resolve(r.Context(), held)
	if err != nil {
		h.logger.Warn("sse: digest resolver failed", "error", err, "subjects", len(held))
		h.writeDigest(w, h.Position(), nil, nil, 0, true)
		return
	}
	changed, removed, mismatch := matchByKey(held, states)
	if mismatch.class != "" {
		h.logger.Error("sse: digest resolver output does not match request",
			"class", mismatch.class, "request_subjects", len(held), "resolver_states", len(states),
			"kind", sanitizeLogValue(mismatch.key.Kind), "ref", sanitizeLogValue(mismatch.key.Ref))
		h.writeDigest(w, h.Position(), nil, nil, 0, true)
		return
	}
	h.writeDigest(w, h.Position(), changed, removed, len(held), false)
}

// writeDigest renders the response; Position is read by the caller after the
// resolver returned, so head is at or after every version compared.
func (h *Hub) writeDigest(w http.ResponseWriter, pos Position, changed []digestChanged, removed []digestRemoved, checked int, mustRefetch bool) {
	if changed == nil {
		changed = []digestChanged{}
	}
	if removed == nil {
		removed = []digestRemoved{}
	}
	webhttp.WriteJSON(w, digestResponse{
		Epoch:       pos.Epoch,
		Floor:       pos.Floor,
		Head:        pos.Head,
		Checked:     checked,
		MustRefetch: mustRefetch,
		Changed:     changed,
		Removed:     removed,
	})
}

type keyMismatch struct {
	class string
	key   Subject
}

// matchByKey pairs the resolver's output with the request by (Kind, Ref) and
// reports the first defect: a key the request did not carry, a key answered
// twice, or a requested key with no answer.
func matchByKey(held []Held, states []State) ([]digestChanged, []digestRemoved, keyMismatch) {
	byKey := make(map[Subject]*State, len(held))
	for i := range held {
		byKey[held[i].Subject] = nil
	}
	for i := range states {
		s := &states[i]
		prev, requested := byKey[s.Subject]
		switch {
		case !requested:
			return nil, nil, keyMismatch{class: "unrequested", key: s.Subject}
		case prev != nil:
			return nil, nil, keyMismatch{class: "duplicate", key: s.Subject}
		}
		byKey[s.Subject] = s
	}
	var changed []digestChanged
	var removed []digestRemoved
	for _, hd := range held {
		s := byKey[hd.Subject]
		switch {
		case s == nil:
			return nil, nil, keyMismatch{class: "missing", key: hd.Subject}
		case s.Status != StatusCurrent:
			removed = append(removed, digestRemoved{Kind: hd.Kind, Ref: hd.Ref, Reason: s.Status})
		case s.Version != hd.Version:
			changed = append(changed, digestChanged{Kind: hd.Kind, Ref: hd.Ref, Version: s.Version})
		}
	}
	return changed, removed, keyMismatch{}
}

// validateDigestRequest applies the body rules in field order and returns the
// first violated rule as the envelope message, or "" when the request is valid.
func validateDigestRequest(req *digestRequest, maxSubjects int) string {
	if req.Epoch != "" && !isEpoch(req.Epoch) {
		return "epoch must be 16 lowercase hex characters"
	}
	if req.Subjects == nil {
		return "subjects is required"
	}
	subjects := *req.Subjects
	if len(subjects) > maxSubjects {
		return fmt.Sprintf("subjects holds %d entries, max %d", len(subjects), maxSubjects)
	}
	seen := make(map[Subject]struct{}, len(subjects))
	for i, s := range subjects {
		if msg := validateSubject(s); msg != "" {
			return fmt.Sprintf("subjects[%d].%s", i, msg)
		}
		key := Subject{Kind: s.Kind, Ref: s.Ref}
		if _, dup := seen[key]; dup {
			return fmt.Sprintf("subjects[%d] duplicates an earlier (kind, ref)", i)
		}
		seen[key] = struct{}{}
	}
	return ""
}

func validateSubject(s digestSubject) string {
	switch {
	case s.Kind == "" || len(s.Kind) > maxKindBytes:
		return fmt.Sprintf("kind must be 1 to %d bytes", maxKindBytes)
	case !isKind(s.Kind):
		return "kind must match [a-z][a-z0-9_]*"
	case len(s.Ref) > maxRefBytes:
		return fmt.Sprintf("ref must be at most %d bytes", maxRefBytes)
	case strings.ContainsFunc(s.Ref, isUnsafeRune):
		return "ref must not contain control, line separator or bidi characters"
	case s.Version == "" || len(s.Version) > maxVersionBytes:
		return fmt.Sprintf("version must be 1 to %d bytes", maxVersionBytes)
	case !isPrintableASCII(s.Version):
		return "version must be printable ASCII"
	}
	return ""
}

func isKind(s string) bool {
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
		case i > 0 && (c >= '0' && c <= '9' || c == '_'):
		default:
			return false
		}
	}
	return true
}

func isPrintableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// isUnsafeRune reports a C0 or C1 control, U+2028/U+2029, or an explicit bidi
// formatting character (the Trojan Source set, https://trojansource.codes/).
func isUnsafeRune(r rune) bool {
	switch {
	case unicode.IsControl(r), r == 0x2028, r == 0x2029:
		return true
	case r == 0x061c, r == 0x200e, r == 0x200f:
		return true
	case r >= 0x202a && r <= 0x202e, r >= 0x2066 && r <= 0x2069:
		return true
	}
	return false
}

// sanitizeLogValue bounds a string headed for a log line: at most 128 bytes,
// cut on a rune boundary, with controls, line separators and bidi characters
// removed so the value stays on one line and cannot restyle what follows it.
func sanitizeLogValue(s string) string {
	if len(s) > maxLogValueBytes {
		cut := maxLogValueBytes
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut]
	}
	if !strings.ContainsFunc(s, isUnsafeRune) && utf8.ValidString(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == utf8.RuneError || isUnsafeRune(r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
