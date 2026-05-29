package forward

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"buffr/internal/cassette"
	"buffr/internal/mitm"
	"buffr/internal/proxy"
)

// The forward-proxy tests run entirely offline. A local httptest server plays
// the "real upstream"; the buffr forward proxy sits in front of it; a client
// whose Transport.Proxy is buffr and whose RootCAs trust the buffr CA drives the
// whole CONNECT → MITM → egress → record → replay path. Egress is pointed back
// at the test server via the overridable proxy.EgressTransport / EgressDialer,
// with a DialContext that remaps fake hostnames (api.test, hf.test, …) to the
// real 127.0.0.1:port — so cassettes get clean, distinct host keys without DNS.

func newCA(t *testing.T) *mitm.CA {
	t.Helper()
	dir := t.TempDir()
	ca, err := mitm.LoadOrCreateCA(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateCA: %v", err)
	}
	return ca
}

// setEgress points buffr's upstream egress at the test servers named in mapping
// ("host:443" -> "127.0.0.1:port"), trusting their self-signed certs. Restored
// after the test.
func setEgress(t *testing.T, mapping map[string]string) {
	t.Helper()
	prevT, prevD := proxy.EgressTransport, proxy.EgressDialer
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		if real, ok := mapping[addr]; ok {
			addr = real
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	}
	proxy.EgressTransport = &http.Transport{
		DialContext:     dial,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	proxy.EgressDialer = &websocket.Dialer{
		NetDialContext:   dial,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
		HandshakeTimeout: 5 * time.Second,
	}
	t.Cleanup(func() { proxy.EgressTransport, proxy.EgressDialer = prevT, prevD })
}

// startForward starts a forward proxy with cfg and returns its proxy URL.
func startForward(t *testing.T, cfg Config) string {
	t.Helper()
	fwd := New(cfg)
	srv := httptest.NewServer(fwd.Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

// mitmClient builds an HTTP client that proxies through proxyURL and trusts the
// buffr CA (the consumer-side contract: HTTPS_PROXY + SSL_CERT_FILE).
func mitmClient(t *testing.T, proxyURL string, ca *mitm.CA) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.CertPEM())
	pu, err := url.Parse(proxyURL)
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Proxy:           http.ProxyURL(pu),
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}
}

func getBody(t *testing.T, c *http.Client, url string) (int, string) {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func hostPort(u string) string {
	pu, _ := url.Parse(u)
	return pu.Host
}

// Criterion 2: a client with only HTTPS_PROXY + CA set captures a call in auto
// mode and replays it offline in strict-replay mode, src=cassette, 0 egress.
func TestCaptureThenReplayOffline(t *testing.T) {
	var hits int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"path":"` + r.URL.Path + `"}`))
	}))
	defer upstream.Close()

	ca := newCA(t)
	data := t.TempDir()
	setEgress(t, map[string]string{"hf.test:443": hostPort(upstream.URL)})

	// Record (auto).
	recURL := startForward(t, Config{Mode: "auto", DataDir: data, CA: ca})
	rc := mitmClient(t, recURL, ca)
	status, body := getBody(t, rc, "https://hf.test/api/models")
	if status != 200 || body == "" {
		t.Fatalf("record: status=%d body=%q", status, body)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("record should hit upstream once, got %d", got)
	}

	// Replay (strict) over the same cassette dir, upstream now stopped.
	upstream.Close()
	repURL := startForward(t, Config{Mode: "replay", DataDir: data, CA: ca})
	pc := mitmClient(t, repURL, ca)
	status, body2 := getBody(t, pc, "https://hf.test/api/models")
	if status != 200 {
		t.Fatalf("replay: status=%d (want 200 from cassette)", status)
	}
	if body2 != body {
		t.Fatalf("replay body %q != recorded %q", body2, body)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("replay must not egress; upstream hits = %d, want 1", got)
	}
}

// Criterion 3: one run hitting several hosts records into per-host cassettes and
// replays all from cassette, 0 egress.
func TestMultiHostSingleRun(t *testing.T) {
	mk := func(tag string, hits *int32) *httptest.Server {
		return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(hits, 1)
			_, _ = w.Write([]byte(tag))
		}))
	}
	var hHits, sHits int32
	hf := mk("from-hf", &hHits)
	defer hf.Close()
	serper := mk("from-serper", &sHits)
	defer serper.Close()

	ca := newCA(t)
	data := t.TempDir()
	setEgress(t, map[string]string{
		"hf.test:443":     hostPort(hf.URL),
		"serper.test:443": hostPort(serper.URL),
	})

	recURL := startForward(t, Config{Mode: "auto", DataDir: data, CA: ca})
	rc := mitmClient(t, recURL, ca)
	if _, b := getBody(t, rc, "https://hf.test/x"); b != "from-hf" {
		t.Fatalf("hf body = %q", b)
	}
	if _, b := getBody(t, rc, "https://serper.test/search"); b != "from-serper" {
		t.Fatalf("serper body = %q", b)
	}

	// Each host wrote its own cassette.
	for _, h := range []string{"hf.test", "serper.test"} {
		if _, err := cassette.Load(filepath.Join(data, h+".json")); err != nil {
			t.Fatalf("expected per-host cassette for %s: %v", h, err)
		}
	}

	hf.Close()
	serper.Close()
	hBefore, sBefore := atomic.LoadInt32(&hHits), atomic.LoadInt32(&sHits)

	repURL := startForward(t, Config{Mode: "replay", DataDir: data, CA: ca})
	pc := mitmClient(t, repURL, ca)
	if _, b := getBody(t, pc, "https://hf.test/x"); b != "from-hf" {
		t.Fatalf("replay hf body = %q", b)
	}
	if _, b := getBody(t, pc, "https://serper.test/search"); b != "from-serper" {
		t.Fatalf("replay serper body = %q", b)
	}
	if atomic.LoadInt32(&hHits) != hBefore || atomic.LoadInt32(&sHits) != sBefore {
		t.Fatalf("replay must not egress to either host")
	}
}

// Criterion 4: N distinct POSTs to the same host+path record and replay in order
// without collision.
func TestSequentialSameKey(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// Echo the request body so each distinct request has a distinct response.
		_, _ = w.Write([]byte("resp:" + string(body)))
	}))
	defer upstream.Close()

	ca := newCA(t)
	data := t.TempDir()
	setEgress(t, map[string]string{"api.test:443": hostPort(upstream.URL)})

	bodies := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}

	post := func(c *http.Client, url, body string) string {
		resp, err := c.Post(url, "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}

	recURL := startForward(t, Config{Mode: "auto", DataDir: data, CA: ca})
	rc := mitmClient(t, recURL, ca)
	for _, b := range bodies {
		if got := post(rc, "https://api.test/v1/responses", b); got != "resp:"+b {
			t.Fatalf("record echo = %q, want %q", got, "resp:"+b)
		}
	}

	upstream.Close()
	repURL := startForward(t, Config{Mode: "replay", DataDir: data, CA: ca})
	pc := mitmClient(t, repURL, ca)
	for _, b := range bodies {
		if got := post(pc, "https://api.test/v1/responses", b); got != "resp:"+b {
			t.Fatalf("replay echo = %q, want %q (sequential same-key collision?)", got, "resp:"+b)
		}
	}
}

// Criterion 5: an SSE response replays byte-faithfully; with ReplayNoDelay the
// recorded inter-chunk delays are dropped.
func TestStreamingReplay(t *testing.T) {
	const gap = 60 * time.Millisecond
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: chunk%d\n\n", i)
			fl.Flush()
			time.Sleep(gap)
		}
	}))
	defer upstream.Close()

	ca := newCA(t)
	data := t.TempDir()
	setEgress(t, map[string]string{"sse.test:443": hostPort(upstream.URL)})

	recURL := startForward(t, Config{Mode: "auto", DataDir: data, CA: ca})
	_, recBody := getBody(t, mitmClient(t, recURL, ca), "https://sse.test/v1/stream")
	want := "data: chunk0\n\ndata: chunk1\n\ndata: chunk2\n\n"
	if recBody != want {
		t.Fatalf("record SSE body = %q, want %q", recBody, want)
	}

	upstream.Close()

	// Replay with NoDelay: identical bytes, but fast (well under the recorded
	// ~120ms of inter-chunk delay).
	prev := proxy.ReplayNoDelay
	proxy.ReplayNoDelay = true
	t.Cleanup(func() { proxy.ReplayNoDelay = prev })

	repURL := startForward(t, Config{Mode: "replay", DataDir: data, CA: ca})
	start := time.Now()
	_, repBody := getBody(t, mitmClient(t, repURL, ca), "https://sse.test/v1/stream")
	elapsed := time.Since(start)
	if repBody != want {
		t.Fatalf("replay SSE body = %q, want %q", repBody, want)
	}
	if elapsed > 2*gap {
		t.Fatalf("ReplayNoDelay should drop delays; replay took %v (>= %v)", elapsed, 2*gap)
	}
}

// Binary/compressed bodies survive the cassette round-trip byte-for-byte. (Plan
// design decision 4: gzip stored verbatim in body_b64 and replayed with its
// Content-Encoding intact.)
func TestGzipBodyByteFaithful(t *testing.T) {
	const payload = `{"message":"a fairly long json body that should compress"}`
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte(payload))
	_ = zw.Close()
	gzBytes := gz.Bytes()

	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(gzBytes)
	}))
	defer upstream.Close()

	ca := newCA(t)
	data := t.TempDir()
	setEgress(t, map[string]string{"gzip.test:443": hostPort(upstream.URL)})

	recURL := startForward(t, Config{Mode: "auto", DataDir: data, CA: ca})
	// The client transport auto-negotiates + decompresses gzip, so the final
	// body it sees should be the original payload on both record and replay.
	_, recBody := getBody(t, mitmClient(t, recURL, ca), "https://gzip.test/blob")
	if recBody != payload {
		t.Fatalf("record body = %q, want %q", recBody, payload)
	}

	// The cassette must hold the body as binary (base64), not corrupted UTF-8.
	c, err := cassette.Load(filepath.Join(data, "gzip.test.json"))
	if err != nil {
		t.Fatalf("load cassette: %v", err)
	}
	resp := c.Interactions[0].HTTP.Response
	if resp.BodyB64 == "" || resp.Body != "" {
		t.Fatalf("gzip body should be stored in BodyB64, got Body=%q BodyB64=%q", resp.Body, resp.BodyB64)
	}
	if !bytes.Equal(cassette.DecodeBody(resp.Body, resp.BodyB64), gzBytes) {
		t.Fatalf("stored gzip bytes do not match original")
	}

	upstream.Close()
	repURL := startForward(t, Config{Mode: "replay", DataDir: data, CA: ca})
	_, repBody := getBody(t, mitmClient(t, repURL, ca), "https://gzip.test/blob")
	if repBody != payload {
		t.Fatalf("replay body = %q, want %q", repBody, payload)
	}
}

// Criterion 6: a host in bypass is tunneled, not recorded. Uses 127.0.0.1 so the
// raw CONNECT tunnel can dial it directly (no DNS), and trusts the upstream's
// own cert since bypass does no MITM.
func TestBypassTunnel(t *testing.T) {
	var hits int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte("direct"))
	}))
	defer upstream.Close()

	ca := newCA(t)
	data := t.TempDir()
	proxyURL := startForward(t, Config{Mode: "auto", DataDir: data, CA: ca, Bypass: []string{"127.0.0.1"}})

	// Client trusts the upstream's real cert (the proxy does not intercept).
	pool := x509.NewCertPool()
	pool.AddCert(upstream.Certificate())
	pu, _ := url.Parse(proxyURL)
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		Proxy:           http.ProxyURL(pu),
		TLSClientConfig: &tls.Config{RootCAs: pool},
	}}

	status, body := getBody(t, client, upstream.URL) // https://127.0.0.1:PORT
	if status != 200 || body != "direct" {
		t.Fatalf("bypass: status=%d body=%q", status, body)
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("bypass should reach upstream directly, hits=%d", hits)
	}
	// Nothing should have been recorded for a bypassed host.
	if entries, _ := filepath.Glob(filepath.Join(data, "*.json")); len(entries) != 0 {
		t.Fatalf("bypass must not write a cassette, found %v", entries)
	}
}

// Criterion 7: an unrecorded request in strict replay fails loudly (599) and
// never hits upstream.
func TestStrictReplayMiss(t *testing.T) {
	ca := newCA(t)
	data := t.TempDir()               // empty: no cassettes
	setEgress(t, map[string]string{}) // any egress attempt would fail to dial

	repURL := startForward(t, Config{Mode: "replay", DataDir: data, CA: ca})
	status, _ := getBody(t, mitmClient(t, repURL, ca), "https://api.test/v1/never-recorded")
	if status != 599 {
		t.Fatalf("strict replay miss should return 599, got %d", status)
	}
}

// Criterion 9: a wss upstream is recorded and replayed frame-faithfully. The
// upstream is stopped before replay to prove the session serves from cassette.
func TestWebSocketRecordReplay(t *testing.T) {
	wsUpgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_, msg, err := c.ReadMessage()
		if err != nil {
			return
		}
		_ = c.WriteMessage(websocket.TextMessage, []byte("echo:"+string(msg)))
		_ = c.WriteMessage(websocket.TextMessage, []byte("push"))
		_, _, _ = c.ReadMessage() // wait for client close
	}))
	defer upstream.Close()

	ca := newCA(t)
	data := t.TempDir()
	setEgress(t, map[string]string{"realtime.test:443": hostPort(upstream.URL)})

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.CertPEM())

	dialWS := func(proxyURL string) *websocket.Conn {
		pu, _ := url.Parse(proxyURL)
		d := &websocket.Dialer{
			Proxy:            http.ProxyURL(pu),
			TLSClientConfig:  &tls.Config{RootCAs: pool},
			HandshakeTimeout: 5 * time.Second,
		}
		conn, _, err := d.Dial("wss://realtime.test/v1/realtime", nil)
		if err != nil {
			t.Fatalf("ws dial: %v", err)
		}
		return conn
	}

	exchange := func(conn *websocket.Conn) {
		defer conn.Close()
		if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
			t.Fatalf("ws write: %v", err)
		}
		_, m1, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("ws read1: %v", err)
		}
		if string(m1) != "echo:hello" {
			t.Fatalf("ws m1 = %q, want echo:hello", m1)
		}
		_, m2, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("ws read2: %v", err)
		}
		if string(m2) != "push" {
			t.Fatalf("ws m2 = %q, want push", m2)
		}
	}

	// Record.
	recURL := startForward(t, Config{Mode: "auto", DataDir: data, CA: ca})
	exchange(dialWS(recURL))

	// Replay with the upstream gone.
	upstream.Close()
	repURL := startForward(t, Config{Mode: "replay", DataDir: data, CA: ca})
	exchange(dialWS(repURL))
}

// Criterion 8 (CA lifecycle) is covered by package mitm's tests (chain verify,
// persistence). Every test in this file additionally confirms a freshly trusted
// client completes a real MITM'd TLS handshake against the proxy with no
// verification error, via mitmClient.
