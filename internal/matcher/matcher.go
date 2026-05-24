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
	"strings"

	"buffr/internal/cassette"
)

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
	pool       []*cassette.HTTPExchange
}

// New returns a Matcher seeded from the HTTP exchanges in `c`. Non-HTTP
// interactions are ignored (the WS path uses its own matcher).
//
// If normalizer is nil, ExactBodyNormalizer is used.
func New(c *cassette.Cassette, normalizer Normalizer) *Matcher {
	if normalizer == nil {
		normalizer = ExactBodyNormalizer
	}
	m := &Matcher{normalizer: normalizer}
	for _, it := range c.Interactions {
		if it.Type == "http" && it.HTTP != nil {
			m.pool = append(m.pool, it.HTTP)
		}
	}
	return m
}

// Take pops and returns the first cassette entry matching the live request,
// or nil if none matches. Subsequent calls will not see the popped entry.
func (m *Matcher) Take(method, path, body string) *cassette.HTTPExchange {
	wantSig := m.signature(method, path, body)
	for i, ex := range m.pool {
		gotSig := m.signature(ex.Request.Method, ex.Request.Path, ex.Request.Body)
		if gotSig == wantSig {
			m.pool = append(m.pool[:i], m.pool[i+1:]...)
			return ex
		}
	}
	return nil
}

// Remaining returns how many recorded HTTP exchanges have not been taken yet.
// Useful at the end of a test to assert the cassette was fully consumed.
func (m *Matcher) Remaining() int {
	return len(m.pool)
}

func (m *Matcher) signature(method, path, body string) string {
	normalized := m.normalizer(method, path, body)
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
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
