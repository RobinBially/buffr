// Package proxy implements the HTTP + WebSocket record/replay handlers.
package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"buffr/internal/cassette"
	"buffr/internal/matcher"
)

// matchHostKey carries the destination host through the request context in
// forward-proxy (MITM) mode, where a single handler can serve many hosts. The
// reverse-proxy paths never set it, so matchHost returns "" and matching/
// recording behave exactly as before host-aware matching was added.
type ctxKey int

const matchHostKey ctxKey = iota

// WithMatchHost tags a request context with the destination host so the shared
// record/replay handlers fold it into the cassette match key. The forward proxy
// sets this before dispatching an intercepted request to a per-host handler.
func WithMatchHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, matchHostKey, host)
}

// matchHost returns the destination host previously stored by WithMatchHost, or
// "" when none was set (reverse-proxy mode).
func matchHost(r *http.Request) string {
	if h, ok := r.Context().Value(matchHostKey).(string); ok {
		return h
	}
	return ""
}

// ReplayNoDelay skips the recorded inter-chunk / inter-frame delays on replay.
// Tests usually don't need the original streaming cadence, and replaying it
// (per-chunk time.Sleep) dominates e2e runtime, so this defaults to on. Set
// BUFFR_REPLAY_NODELAY=0 to restore the recorded cadence (e.g. when the
// streaming timing itself is under test). Exported so it can be toggled in
// tests; defaults from the env at startup.
var ReplayNoDelay = os.Getenv("BUFFR_REPLAY_NODELAY") != "0"

// EgressTransport is the RoundTripper used for upstream HTTP requests in
// record/auto mode. It defaults to http.DefaultTransport (system-root TLS
// verification, which is what real upstreams need). It is exported so
// forward-proxy tests can point egress at a local test server; production code
// should leave it at the default.
var EgressTransport http.RoundTripper = http.DefaultTransport

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

		ex := m.Take(r.Method, matchHost(r), r.URL.Path, string(body))
		if ex == nil {
			// Cache miss — RecordHandler logs this as src=upstream.
			r.Body = io.NopCloser(bytes.NewReader(body))
			record.ServeHTTP(w, r)
			return
		}

		// Cache hit — replay from cassette. Take advances a cyclic per-key
		// cursor without consuming the entry, so identical follow-up calls keep
		// replaying with no re-add needed.
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
			if c.DelayMs > 0 && !ReplayNoDelay {
				time.Sleep(time.Duration(c.DelayMs) * time.Millisecond)
			}
			chunk := cassette.DecodeBody(c.Data, c.DataB64)
			// sync_response substitutions only apply to text chunks; a binary
			// chunk (stored via DataB64) is replayed byte-for-byte.
			if c.DataB64 == "" {
				chunk = []byte(matcher.ApplyReplacements(string(chunk), repls))
			}
			if _, werr := w.Write(chunk); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	} else {
		full := cassette.DecodeBody(ex.Response.Body, ex.Response.BodyB64)
		if ex.Response.BodyB64 == "" {
			full = []byte(matcher.ApplyReplacements(string(full), repls))
		}
		_, _ = w.Write(full)
	}
	slog.Info(r.Method+" "+r.URL.Path,
		"status", ex.Response.Status,
		"dur", fmtDur(time.Since(start)),
		"src", "cassette")
}

// RouteUpgrade dispatches WebSocket upgrade requests to wsHandler and every
// other request to httpHandler. The signal is the Upgrade header; relying on it
// (rather than the path) means callers don't have to declare which paths serve
// WS — the protocol tells us. Shared by the reverse-proxy entrypoint and the
// forward proxy's per-host handlers.
func RouteUpgrade(wsHandler, httpHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			wsHandler.ServeHTTP(w, r)
			return
		}
		httpHandler.ServeHTTP(w, r)
	})
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

		resp, err := EgressTransport.RoundTrip(upReq)
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
					data, b64 := cassette.EncodeBody(chunk)
					recordedResp.BodyChunks = append(recordedResp.BodyChunks, cassette.Chunk{
						Data:    data,
						DataB64: b64,
						DelayMs: int(now.Sub(lastChunkAt) / time.Millisecond),
					})
					lastChunkAt = now
				}
				if readErr != nil {
					break
				}
			}
		}

		var fullBody []byte
		if !isSSE {
			full, readErr := io.ReadAll(resp.Body)
			if readErr != nil && readErr != io.EOF {
				slog.Error("upstream body read failed", "err", readErr)
				http.Error(w, "buffr: upstream body read failed: "+readErr.Error(), http.StatusBadGateway)
				return
			}
			fullBody = full
			// Store the body verbatim: valid UTF-8 in Body, anything else
			// (gzip/br/binary the catch-all proxy sees) base64 in BodyB64. The
			// original Content-Encoding header is preserved, so replay is
			// byte-faithful without buffr having to decompress anything.
			recordedResp.Body, recordedResp.BodyB64 = cassette.EncodeBody(full)
		}

		exch := &cassette.HTTPExchange{
			Request: cassette.HTTPRequest{
				Method:  r.Method,
				Host:    matchHost(r),
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
			_, _ = w.Write(fullBody)
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

		ex := m.Take(r.Method, matchHost(r), r.URL.Path, string(body))
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
