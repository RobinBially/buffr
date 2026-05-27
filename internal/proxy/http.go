// Package proxy implements the HTTP + WebSocket record/replay handlers.
package proxy

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"buffr/internal/cassette"
	"buffr/internal/matcher"
)

// replayNoDelay, when BUFFR_REPLAY_NODELAY=1, skips the recorded inter-chunk /
// inter-frame delays on replay. Tests don't need the original streaming cadence,
// and replaying it (per-chunk time.Sleep) dominates e2e runtime.
var replayNoDelay = os.Getenv("BUFFR_REPLAY_NODELAY") == "1"

func fmtDur(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// Recorder is a synchronization wrapper around a Cassette so the HTTP and WS
// handlers can append interactions concurrently. The cassette is flushed to
// disk after every append so a crashed test session still leaves a usable
// (partial) recording behind.
//
// Rules are stored alongside the recorder so the HTTP handler can extract
// sync_response captures at record time without an extra plumbing argument.
type Recorder struct {
	mu          sync.Mutex
	cassette    *cassette.Cassette
	path        string
	rules       []matcher.IgnoreRule
	subscribers []func(cassette.Interaction)
}

func NewRecorder(path string, rules ...matcher.IgnoreRule) *Recorder {
	return &Recorder{
		cassette: &cassette.Cassette{Version: cassette.CurrentVersion},
		path:     path,
		rules:    rules,
	}
}

// Rules exposes the configured ignore rules for the HTTP handler to consult.
func (r *Recorder) Rules() []matcher.IgnoreRule {
	return r.rules
}

// Subscribe registers a callback fired after each successful Append. The
// auto handler uses this to feed fresh recordings back into the matcher pool
// so a second identical request in the same session can replay instead of
// re-recording.
func (r *Recorder) Subscribe(fn func(cassette.Interaction)) {
	r.mu.Lock()
	r.subscribers = append(r.subscribers, fn)
	r.mu.Unlock()
}

// Append adds an interaction and flushes. Errors during flush are returned but
// not propagated to the HTTP client — the client doesn't care that we failed
// to persist its traffic.
func (r *Recorder) Append(it cassette.Interaction) error {
	r.mu.Lock()
	r.cassette.Interactions = append(r.cassette.Interactions, it)
	subs := append([]func(cassette.Interaction){}, r.subscribers...)
	err := cassette.Save(r.path, r.cassette)
	r.mu.Unlock()
	for _, fn := range subs {
		fn(it)
	}
	return err
}

// NewAutoRecorder creates a Recorder pre-seeded from an existing cassette file.
// If the file does not exist yet, an empty cassette is used. The returned
// cassette snapshot can be handed to matcher.New so the auto handler can
// match against already-recorded interactions.
func NewAutoRecorder(path string, rules ...matcher.IgnoreRule) (*Recorder, *cassette.Cassette) {
	c, err := cassette.Load(path)
	if err != nil {
		c = &cassette.Cassette{Version: cassette.CurrentVersion}
	}
	return &Recorder{cassette: c, path: path, rules: rules}, c
}

// AutoHandler is the record-on-miss handler: it serves from the cassette when
// a match exists, and falls back to forwarding + recording when it does not.
// This lets tests run fully offline once every interaction has been captured,
// while still accumulating new cassette entries transparently on first run.
func AutoHandler(target *url.URL, rec *Recorder, m *matcher.Matcher) http.Handler {
	// Feed every fresh recording back into the matcher's replay pool. Without
	// this hook a long-running auto-mode proxy never replays anything recorded
	// after startup, since matcher.New snapshots the cassette only once.
	rec.Subscribe(func(it cassette.Interaction) {
		if it.Type == "http" && it.HTTP != nil {
			m.Add(it.HTTP)
		}
	})
	record := RecordHandler(target, rec)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "buffr: failed to read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()

		ex := m.Take(r.Method, r.URL.Path, string(body))
		if ex == nil {
			// Cache miss — RecordHandler logs this as src=upstream.
			r.Body = io.NopCloser(bytes.NewReader(body))
			record.ServeHTTP(w, r)
			return
		}

		// Re-add so identical follow-up calls keep replaying. Auto mode treats
		// the cassette as a cache; the popping semantic only applies to strict
		// replay where each recorded exchange should serve exactly once.
		m.Add(ex)

		// Cache hit — replay from cassette.
		writeReplay(w, r, ex, m.Rules(), string(body), start)
	})
}

// writeReplay emits a recorded HTTPExchange as the response, applying
// sync_response substitutions so values like run IDs match the live request
// instead of the value that was on the wire at record time.
func writeReplay(w http.ResponseWriter, r *http.Request, ex *cassette.HTTPExchange, rules []matcher.IgnoreRule, liveBody string, start time.Time) {
	repls := matcher.ComputeSyncReplacements(rules, r.Method, r.URL.Path, liveBody, ex)

	for k, vs := range ex.Response.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(ex.Response.Status)
	flusher, _ := w.(http.Flusher)

	if len(ex.Response.BodyChunks) > 0 {
		for _, c := range ex.Response.BodyChunks {
			if c.DelayMs > 0 && !replayNoDelay {
				time.Sleep(time.Duration(c.DelayMs) * time.Millisecond)
			}
			if _, werr := w.Write([]byte(matcher.ApplyReplacements(c.Data, repls))); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	} else {
		_, _ = w.Write([]byte(matcher.ApplyReplacements(ex.Response.Body, repls)))
	}
	slog.Info(r.Method+" "+r.URL.Path,
		"status", ex.Response.Status,
		"dur", fmtDur(time.Since(start)),
		"src", "cassette")
}

// RecordHandler returns an http.Handler that proxies every request to `target`
// and writes the round-trip to the recorder. It supports `text/event-stream`
// responses by streaming chunks through and capturing each chunk with the
// elapsed time since the previous chunk.
//
// Hop-by-hop headers are stripped per RFC 7230 §6.1; this also prevents
// `Host`/`Connection` from leaking the local proxy port back into the
// recording.
func RecordHandler(target *url.URL, rec *Recorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("failed to read request body", "err", err)
			http.Error(w, "buffr: failed to read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()

		upstream := *target
		upstream.Path = singleJoin(upstream.Path, r.URL.Path)
		upstream.RawQuery = r.URL.RawQuery

		upReq, err := http.NewRequestWithContext(r.Context(), r.Method, upstream.String(), bytes.NewReader(body))
		if err != nil {
			slog.Error("failed to build upstream request", "err", err)
			http.Error(w, "buffr: failed to build upstream request: "+err.Error(), http.StatusInternalServerError)
			return
		}
		copyHeadersExceptHopByHop(upReq.Header, r.Header)

		resp, err := http.DefaultTransport.RoundTrip(upReq)
		if err != nil {
			slog.Error(r.Method+" "+r.URL.Path, "err", err, "src", "upstream")
			http.Error(w, "buffr: upstream request failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		recordedResp := cassette.HTTPResponse{
			Status:  resp.StatusCode,
			Headers: filterHeaders(resp.Header),
		}

		isSSE := isEventStream(resp.Header)
		if isSSE {
			copyHeadersExceptHopByHop(w.Header(), resp.Header)
			w.WriteHeader(resp.StatusCode)
			flusher, _ := w.(http.Flusher)
			buf := make([]byte, 4096)
			lastChunkAt := time.Now()
			for {
				n, readErr := resp.Body.Read(buf)
				if n > 0 {
					chunk := append([]byte(nil), buf[:n]...)
					if _, werr := w.Write(chunk); werr != nil {
						break
					}
					if flusher != nil {
						flusher.Flush()
					}
					now := time.Now()
					recordedResp.BodyChunks = append(recordedResp.BodyChunks, cassette.Chunk{
						Data:    string(chunk),
						DelayMs: int(now.Sub(lastChunkAt) / time.Millisecond),
					})
					lastChunkAt = now
				}
				if readErr != nil {
					break
				}
			}
		} else {
			full, readErr := io.ReadAll(resp.Body)
			if readErr != nil && readErr != io.EOF {
				slog.Error("upstream body read failed", "err", readErr)
				http.Error(w, "buffr: upstream body read failed: "+readErr.Error(), http.StatusBadGateway)
				return
			}
			recordedResp.Body = string(full)
		}

		exch := &cassette.HTTPExchange{
			Request: cassette.HTTPRequest{
				Method:  r.Method,
				Path:    r.URL.Path,
				Query:   r.URL.RawQuery,
				Headers: filterHeaders(r.Header),
				Body:    string(body),
			},
			Response: recordedResp,
		}
		if caps := matcher.ExtractCaptures(rec.Rules(), r.Method, r.URL.Path, string(body)); len(caps) > 0 {
			exch.Match = &cassette.MatchMeta{Captures: caps}
		}
		if appendErr := rec.Append(cassette.Interaction{Type: "http", HTTP: exch}); appendErr != nil {
			slog.Warn("cassette write failed", "err", appendErr)
		}

		slog.Info(r.Method+" "+r.URL.Path,
			"status", resp.StatusCode,
			"dur", fmtDur(time.Since(start)),
			"src", "upstream")

		if !isSSE {
			copyHeadersExceptHopByHop(w.Header(), resp.Header)
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write([]byte(recordedResp.Body))
		}
	})
}

// ReplayHandler serves recorded responses. It uses the matcher to find the
// next exchange for each incoming request and returns 599 with an explanatory
// body when no recorded response matches — chosen over a generic 500 so test
// failures point at the proxy rather than the application.
func ReplayHandler(m *matcher.Matcher) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "buffr: failed to read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()

		ex := m.Take(r.Method, r.URL.Path, string(body))
		if ex == nil {
			slog.Warn(r.Method+" "+r.URL.Path, "src", "miss")
			http.Error(w, fmt.Sprintf("buffr: no cassette match for %s %s", r.Method, r.URL.Path), 599)
			return
		}

		writeReplay(w, r, ex, m.Rules(), string(body), start)
	})
}

func isEventStream(h http.Header) bool {
	ct := h.Get("Content-Type")
	for _, p := range splitAndTrim(ct, ';') {
		if p == "text/event-stream" {
			return true
		}
	}
	return false
}

func splitAndTrim(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, trimSpaces(s[start:i]))
			start = i + 1
		}
	}
	out = append(out, trimSpaces(s[start:]))
	return out
}

func trimSpaces(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

// hopByHop is the canonical list from RFC 7230 §6.1. We also strip Host and
// Authorization so cassettes don't carry secrets back to disk by default — a
// real production-grade tool might want a redaction pipeline; for an MVP
// dropping them is acceptable since matching ignores headers anyway.
var hopByHop = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

func copyHeadersExceptHopByHop(dst, src http.Header) {
	for k, vs := range src {
		if _, skip := hopByHop[http.CanonicalHeaderKey(k)]; skip {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func filterHeaders(src http.Header) map[string][]string {
	out := make(map[string][]string, len(src))
	for k, vs := range src {
		if _, skip := hopByHop[http.CanonicalHeaderKey(k)]; skip {
			continue
		}
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// singleJoin joins two URL paths preserving exactly one '/' between them.
func singleJoin(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case a[len(a)-1] == '/' && b[0] == '/':
		return a + b[1:]
	case a[len(a)-1] != '/' && b[0] != '/':
		return a + "/" + b
	default:
		return a + b
	}
}
