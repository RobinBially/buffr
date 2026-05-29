// Package forward implements buffr's forward-proxy (TLS-MITM) mode.
//
// Unlike the reverse-proxy modes — where each upstream needs an explicit
// --target and the client points a base_url at buffr — the forward proxy
// intercepts ALL outbound traffic the client routes through it via the standard
// HTTPS_PROXY / HTTP_PROXY env vars. It terminates TLS with a per-host leaf cert
// signed by a CA the client trusts (see package mitm), then records/replays per
// destination host using the very same handlers the reverse modes use.
//
// The flow for an HTTPS request:
//
//	client --CONNECT host:443--> buffr
//	buffr  --200 Connection Established--> client
//	client <==TLS handshake (buffr serves a leaf for host)==> buffr
//	client --GET /path (now in plaintext to buffr)--> buffr
//	buffr  --real HTTPS--> host        (record / cassette-miss)
//
// Once TLS is terminated the intercepted leg is ordinary HTTP/1.1, so buffr
// drives it through the standard net/http server and reuses proxy.AutoHandler /
// RecordHandler / ReplayHandler unchanged. This file is the plumbing around
// them: CONNECT handling, the bypass tunnel, and per-host dispatch.
package forward

import (
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"buffr/internal/cassette"
	"buffr/internal/matcher"
	"buffr/internal/mitm"
	"buffr/internal/proxy"
)

// HostConfig is the resolved per-host configuration: which cassette to use and
// which match.ignore rules to apply. Host is the exact destination host, or "*"
// for the catch-all fallback.
type HostConfig struct {
	Host     string
	Cassette string
	Rules    []matcher.IgnoreRule
}

// Config drives a forward-proxy instance.
type Config struct {
	Mode    string       // record | replay | auto
	Bypass  []string     // host names/suffixes tunneled without recording
	Hosts   []HostConfig // per-host config; an entry with Host=="*" is the fallback
	DataDir string       // default dir for per-host cassettes (<DataDir>/<host>.json)
	CA      *mitm.CA
}

// Forward is a forward-proxy server. Build one with New and mount Handler on an
// http.Server.
type Forward struct {
	cfg       Config
	tlsConfig *tls.Config

	mu       sync.Mutex
	handlers map[string]http.Handler // key: scheme + "://" + host
}

// New returns a Forward ready to serve. The mode defaults to "auto".
func New(cfg Config) *Forward {
	if cfg.Mode == "" {
		cfg.Mode = "auto"
	}
	return &Forward{
		cfg:       cfg,
		tlsConfig: cfg.CA.TLSConfig(),
		handlers:  map[string]http.Handler{},
	}
}

// Handler returns the http.Handler for the proxy's listen port. It dispatches
// CONNECT (HTTPS) and absolute-form (plain HTTP) forward-proxy requests.
func (f *Forward) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			f.handleConnect(w, r)
			return
		}
		if r.URL.IsAbs() {
			f.handlePlainHTTP(w, r)
			return
		}
		// Origin-form request straight to the proxy port — the client isn't
		// using buffr as a proxy. Nothing useful to do.
		http.Error(w, "buffr: forward proxy expects CONNECT or absolute-form requests; set HTTPS_PROXY/HTTP_PROXY", http.StatusBadRequest)
	})
}

// handleConnect intercepts an HTTPS tunnel. Bypassed hosts are spliced raw;
// everything else is MITM'd: buffr completes the TLS handshake with a leaf cert
// and serves the decrypted requests through the per-host record/replay handler.
func (f *Forward) handleConnect(w http.ResponseWriter, r *http.Request) {
	authority := r.Host // "host:port"
	host, _ := splitHost(authority, "443")

	if f.isBypassed(host) {
		f.tunnel(w, authority, host)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "buffr: connection does not support hijacking", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hj.Hijack()
	if err != nil {
		slog.Error("CONNECT hijack failed", "host", host, "err", err)
		return
	}
	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		clientConn.Close()
		return
	}

	tlsConn := tls.Server(clientConn, f.tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		// Common and benign: cert-pinned / HSTS clients reject the leaf. Log at
		// debug volume so it doesn't drown real failures.
		slog.Warn("CONNECT TLS handshake failed (client may pin certs)", "host", host, "err", err)
		tlsConn.Close()
		return
	}

	egress := egressHost("https", host, portOf(authority, "443"))
	nc := &notifyConn{Conn: tlsConn, closed: make(chan struct{})}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			f.serveHost(w, req, "https", host, egress)
		}),
	}
	// Serve blocks until the intercepted connection closes (including the full
	// lifetime of a hijacked WebSocket), so the tunnel stays open as long as the
	// client keeps it open.
	_ = srv.Serve(&oneShotListener{conn: nc, closed: nc.closed})
}

// handlePlainHTTP handles an absolute-form `GET http://host/...` forward request.
func (f *Forward) handlePlainHTTP(w http.ResponseWriter, r *http.Request) {
	host, _ := splitHost(r.URL.Host, "80")
	if f.isBypassed(host) {
		f.passthroughHTTP(w, r)
		return
	}
	egress := egressHost("http", host, portOf(r.URL.Host, "80"))
	f.serveHost(w, r, "http", host, egress)
}

// serveHost dispatches a (now-plaintext) request to the per-host handler,
// tagging the context with the destination host so matching/recording fold it
// into the cassette key.
func (f *Forward) serveHost(w http.ResponseWriter, r *http.Request, scheme, host, egress string) {
	h := f.hostHandler(scheme, host, egress)
	r = r.WithContext(proxy.WithMatchHost(r.Context(), host))
	h.ServeHTTP(w, r)
}

// hostHandler returns (building and caching on first use) the record/replay
// handler for one destination. It mirrors runInstances in cmd/buffr but keyed by
// host instead of port, and with the upstream target derived from the request.
func (f *Forward) hostHandler(scheme, host, egress string) http.Handler {
	key := scheme + "://" + host
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.handlers[key]; ok {
		return h
	}

	hc := f.resolveHost(host)
	target := &url.URL{Scheme: scheme, Host: egress}

	var h http.Handler
	switch f.cfg.Mode {
	case "record":
		rec := proxy.NewRecorder(hc.Cassette, hc.Rules...)
		h = proxy.RouteUpgrade(
			proxy.RecordWSHandler(target, rec),
			proxy.RecordHandler(target, rec),
		)
	case "replay":
		c, err := cassette.Load(hc.Cassette)
		if err != nil {
			// No cassette (or unreadable) in strict replay → every request will
			// miss with a loud 599. Log once so the cause is visible.
			slog.Warn("replay: cassette unavailable, all requests to host will miss",
				"host", host, "cassette", hc.Cassette, "err", err)
			c = &cassette.Cassette{Version: cassette.CurrentVersion}
		}
		m := matcher.New(c, matcher.JSONBodyNormalizer, hc.Rules...)
		rep := proxy.NewWSReplayer(c)
		h = proxy.RouteUpgrade(
			proxy.ReplayWSHandler(rep),
			proxy.ReplayHandler(m),
		)
	default: // auto
		rec, existing := proxy.NewAutoRecorder(hc.Cassette, hc.Rules...)
		m := matcher.New(existing, matcher.JSONBodyNormalizer, hc.Rules...)
		rep := proxy.NewWSReplayer(existing)
		h = proxy.RouteUpgrade(
			proxy.AutoWSHandler(target, rec, rep),
			proxy.AutoHandler(target, rec, m),
		)
	}
	f.handlers[key] = h
	slog.Info("forward host ready", "mode", f.cfg.Mode, "host", host, "cassette", hc.Cassette)
	return h
}

// resolveHost finds the config for host: an exact match, else the "*" fallback,
// else a synthesized default writing to <DataDir>/<host>.json. An unlisted host
// is recorded under its own cassette — unlisted is not the same as bypassed.
func (f *Forward) resolveHost(host string) HostConfig {
	var star *HostConfig
	for i := range f.cfg.Hosts {
		hc := &f.cfg.Hosts[i]
		if hc.Host == host {
			return *hc
		}
		if hc.Host == "*" {
			star = hc
		}
	}
	if star != nil {
		return *star
	}
	return HostConfig{Host: host, Cassette: filepath.Join(f.cfg.DataDir, host+".json")}
}

// isBypassed reports whether host should be tunneled without recording. A host
// matches a bypass entry when it equals the entry or is a subdomain of it
// (so "qdrant" matches "qdrant" and "x.qdrant"). This is buffr's NO_PROXY
// equivalent for infra/local hosts.
func (f *Forward) isBypassed(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, b := range f.cfg.Bypass {
		b = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(b, ".")))
		if b == "" {
			continue
		}
		if host == b || strings.HasSuffix(host, "."+b) {
			return true
		}
	}
	return false
}

// tunnel splices a raw TCP connection between the client and the upstream for a
// bypassed CONNECT — no TLS interception, no recording.
func (f *Forward) tunnel(w http.ResponseWriter, authority, host string) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "buffr: connection does not support hijacking", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	defer clientConn.Close()

	upstream, err := net.DialTimeout("tcp", authority, 30*time.Second)
	if err != nil {
		_, _ = clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		slog.Warn("CONNECT "+host, "err", err, "src", "bypass")
		return
	}
	defer upstream.Close()

	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		return
	}
	slog.Info("CONNECT "+host, "src", "bypass")

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, clientConn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(clientConn, upstream); done <- struct{}{} }()
	<-done // first side to close ends the tunnel
}

// passthroughHTTP forwards a bypassed plain-HTTP request to the upstream without
// recording it.
func (f *Forward) passthroughHTTP(w http.ResponseWriter, r *http.Request) {
	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
	if err != nil {
		http.Error(w, "buffr: bad passthrough request: "+err.Error(), http.StatusBadGateway)
		return
	}
	outReq.Header = r.Header.Clone()
	resp, err := http.DefaultTransport.RoundTrip(outReq)
	if err != nil {
		http.Error(w, "buffr: passthrough failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	slog.Info(r.Method+" "+r.URL.Host+r.URL.Path, "status", resp.StatusCode, "src", "bypass")
}

// splitHost splits "host:port" into (host, port), defaulting the port when the
// authority has none. A bare host (no colon) is returned unchanged with defPort.
func splitHost(authority, defPort string) (host, port string) {
	h, p, err := net.SplitHostPort(authority)
	if err != nil {
		return authority, defPort
	}
	if p == "" {
		p = defPort
	}
	return h, p
}

func portOf(authority, defPort string) string {
	_, p := splitHost(authority, defPort)
	return p
}

// egressHost builds the Host for the upstream URL: the bare host for the scheme's
// standard port, otherwise host:port so non-standard ports survive.
func egressHost(scheme, host, port string) string {
	std := "443"
	if scheme == "http" {
		std = "80"
	}
	if port == "" || port == std {
		return host
	}
	return net.JoinHostPort(host, port)
}
