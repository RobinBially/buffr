// Package matcher decides which recorded HTTP interaction (if any) corresponds
// to a live incoming request.
//
// The basic strategy is: build a canonical signature for a request (method,
// path, normalized body) and look it up against a precomputed map of
// signatures from the cassette. Normalization is pluggable per path prefix —
// for example chat completions need to ignore client-supplied request_ids in
// the body, while a simpler API might want to match exact bodies.
package matcher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"sync"

	"buffr/internal/cassette"
)

// Ignore "in" targets.
const (
	IgnoreInBody = "request.body"
	IgnoreInPath = "request.path"
)

// IgnoreRule rewrites part of a request before its signature is computed so
// that semantically equivalent requests with shifting per-run noise (UUIDs,
// timestamps, run IDs) still hash the same. Both the recorded request and the
// incoming request pass through the same rules — a recorded body containing
// "/runs/20250101-120000-001/" and a live body containing
// "/runs/20260524-093045-042/" both normalize to "/runs/<RUN_ID>/" and match.
//
// When SyncResponse is true, the literal substring matched in the live
// request is also propagated into the replayed response: whatever the recorded
// response contains at the spot where the original run-time value sat gets
// swapped for the value from the current request. That keeps echoed IDs
// (run IDs, request IDs, idempotency keys) consistent for the caller — they
// see their own value reflected back, not the one frozen at record time.
type IgnoreRule struct {
	In           string         // IgnoreInBody or IgnoreInPath
	Pattern      *regexp.Regexp // regex matching the substring to rewrite
	ReplaceWith  string         // replacement string (may be empty)
	SyncResponse bool           // propagate live request value into replayed response
}

// SyncReplacement is one substitution to apply to a replayed response body:
// every occurrence of From is replaced with To. Returned by
// ComputeSyncReplacements for the proxy to apply at write time.
type SyncReplacement struct {
	From, To string
}

// Normalizer transforms a recorded or live request body into a canonical form
// before hashing. Returning the input unchanged is a valid no-op normalizer.
//
// Normalizers receive the path so they can short-circuit on irrelevant
// requests, but they must be deterministic — same input always produces the
// same output. Non-deterministic transforms (e.g. UUIDs) will defeat matching.
type Normalizer func(method, path, body string) string

// Matcher serves recorded responses for live requests.
//
// Each call to Take returns the response from the next matching interaction
// and removes that interaction from the pool — so a cassette that recorded
// two identical requests still replays them as two distinct responses, in
// the order they were recorded. This matters for retry/loop scenarios where
// the same prompt produces different completions across iterations.
type Matcher struct {
	normalizer Normalizer
	rules      []IgnoreRule
	mu         sync.Mutex
	pool       []*cassette.HTTPExchange
}

// New returns a Matcher seeded from the HTTP exchanges in `c`. Non-HTTP
// interactions are ignored (the WS path uses its own matcher).
//
// If normalizer is nil, ExactBodyNormalizer is used. Optional rules rewrite
// path/body substrings before hashing so non-deterministic noise (run IDs,
// UUIDs) does not defeat matching.
func New(c *cassette.Cassette, normalizer Normalizer, rules ...IgnoreRule) *Matcher {
	if normalizer == nil {
		normalizer = ExactBodyNormalizer
	}
	m := &Matcher{normalizer: normalizer, rules: rules}
	for _, it := range c.Interactions {
		if it.Type == "http" && it.HTTP != nil {
			m.pool = append(m.pool, it.HTTP)
		}
	}
	return m
}

// Take pops and returns the first cassette entry matching the live request,
// or nil if none matches. Subsequent calls will not see the popped entry.
//
// host distinguishes destinations in forward-proxy mode, where one cassette can
// hold several hosts. Pass "" in reverse-proxy mode (the destination is implicit
// in the cassette); an empty host contributes nothing to the signature, so
// pre-existing cassettes hash and match exactly as before.
func (m *Matcher) Take(method, host, path, body string) *cassette.HTTPExchange {
	wantSig := m.signature(method, host, path, body)
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, ex := range m.pool {
		gotSig := m.signature(ex.Request.Method, ex.Request.Host, ex.Request.Path, ex.Request.Body)
		if gotSig == wantSig {
			m.pool = append(m.pool[:i], m.pool[i+1:]...)
			return ex
		}
	}
	return nil
}

// Add inserts a freshly recorded exchange into the replay pool so a subsequent
// identical request in the same session can hit the cassette instead of going
// upstream again. This is what makes `auto` mode self-healing within a single
// run — without it, every duplicate call records a new copy.
func (m *Matcher) Add(ex *cassette.HTTPExchange) {
	if ex == nil {
		return
	}
	m.mu.Lock()
	m.pool = append(m.pool, ex)
	m.mu.Unlock()
}

// Remaining returns how many recorded HTTP exchanges have not been taken yet.
// Useful at the end of a test to assert the cassette was fully consumed.
func (m *Matcher) Remaining() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pool)
}

// Rules exposes the rule list for callers that need to drive record-time
// capture extraction or replay-time response rewriting (the proxy package).
func (m *Matcher) Rules() []IgnoreRule {
	return m.rules
}

// ExtractCaptures runs each SyncResponse rule against the request and
// returns the literal substring each rule actually matched. Called at record
// time so the cassette knows what value was sitting in that spot, which
// replay later swaps for the live request's value.
//
// Rules without SyncResponse, rules whose target field is empty, and rules
// whose pattern does not match are silently skipped — they contribute no
// capture.
func ExtractCaptures(rules []IgnoreRule, method, path, body string) []cassette.Capture {
	var out []cassette.Capture
	for _, r := range rules {
		if !r.SyncResponse {
			continue
		}
		var src string
		switch r.In {
		case IgnoreInBody:
			src = body
		case IgnoreInPath:
			src = path
		default:
			continue
		}
		if found := r.Pattern.FindString(src); found != "" {
			out = append(out, cassette.Capture{
				Pattern:  r.Pattern.String(),
				Captured: found,
			})
		}
	}
	return out
}

// ComputeSyncReplacements pairs each rule's live match with the substring the
// same rule captured at record time, returning the (recorded, live) pairs the
// proxy should apply to the response. A rule contributes nothing when:
//
//   - the rule is not SyncResponse,
//   - the rule's pattern does not match the live request,
//   - the cassette has no capture for the rule's pattern, or
//   - the live and recorded values are identical (replacement would be a no-op).
//
// Capture lookup is by pattern source string, so config-time rule reordering
// stays safe between record and replay.
func ComputeSyncReplacements(rules []IgnoreRule, method, path, liveBody string, ex *cassette.HTTPExchange) []SyncReplacement {
	if ex == nil || ex.Match == nil || len(ex.Match.Captures) == 0 {
		return nil
	}
	var out []SyncReplacement
	for _, r := range rules {
		if !r.SyncResponse {
			continue
		}
		var src string
		switch r.In {
		case IgnoreInBody:
			src = liveBody
		case IgnoreInPath:
			src = path
		default:
			continue
		}
		live := r.Pattern.FindString(src)
		if live == "" {
			continue
		}
		recorded := findCapture(ex.Match.Captures, r.Pattern.String())
		if recorded == "" || recorded == live {
			continue
		}
		out = append(out, SyncReplacement{From: recorded, To: live})
	}
	return out
}

// ApplyReplacements applies sync_response substitutions to a response body
// fragment. Used both for whole bodies and individual SSE chunks; for chunks
// this means a captured value split across chunk boundaries is not rewritten.
// Captured values like UUIDs and timestamps are short enough that this is
// extremely rare in practice and the simplicity is worth it.
func ApplyReplacements(s string, repls []SyncReplacement) string {
	for _, r := range repls {
		s = strings.ReplaceAll(s, r.From, r.To)
	}
	return s
}

func findCapture(caps []cassette.Capture, pattern string) string {
	for _, c := range caps {
		if c.Pattern == pattern {
			return c.Captured
		}
	}
	return ""
}

func (m *Matcher) signature(method, host, path, body string) string {
	for _, r := range m.rules {
		switch r.In {
		case IgnoreInBody:
			body = r.Pattern.ReplaceAllString(body, r.ReplaceWith)
		case IgnoreInPath:
			path = r.Pattern.ReplaceAllString(path, r.ReplaceWith)
		}
	}
	normalized := m.normalizer(method, path, body)
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	// Only fold host into the hash when present so reverse-mode signatures
	// (host == "") stay byte-identical to those of cassettes recorded before
	// host-aware matching existed.
	if host != "" {
		h.Write([]byte(host))
		h.Write([]byte{0})
	}
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write([]byte(normalized))
	return hex.EncodeToString(h.Sum(nil))
}

// ExactBodyNormalizer hashes the request body verbatim. This is the safest
// default — anything that differs (whitespace, key order) yields a different
// signature. Tests with strictly deterministic prompts can use it as-is.
func ExactBodyNormalizer(method, path, body string) string {
	return body
}

// JSONBodyNormalizer parses the body as JSON and re-encodes it in a canonical
// form (sorted keys, no whitespace). Useful when the client may emit JSON
// with shifting key order or formatting but the semantic content is stable.
// Falls back to exact-body matching for non-JSON payloads.
func JSONBodyNormalizer(method, path, body string) string {
	if !strings.HasPrefix(strings.TrimSpace(body), "{") && !strings.HasPrefix(strings.TrimSpace(body), "[") {
		return body
	}
	var raw any
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		return body
	}
	out, err := canonicalJSON(raw)
	if err != nil {
		return body
	}
	return out
}

func canonicalJSON(v any) (string, error) {
	// json.Marshal already sorts object keys deterministically (alphabetical
	// per the encoding/json contract). Re-marshaling parsed input is enough
	// to canonicalize whitespace + key order in one shot.
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
