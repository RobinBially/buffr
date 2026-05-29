package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"buffr/internal/cassette"
	"buffr/internal/matcher"
)

func TestRecordPlainHTTP(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"echo":"` + string(body) + `"}`))
	}))
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	path := filepath.Join(t.TempDir(), "cass.json")
	rec := NewRecorder(path)
	proxy := httptest.NewServer(RecordHandler(target, rec))
	defer proxy.Close()

	resp, err := http.Post(proxy.URL+"/v1/chat", "application/json", strings.NewReader(`{"prompt":"x"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"echo":"`) {
		t.Errorf("response body lost: %s", body)
	}

	loaded, err := cassette.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Interactions) != 1 {
		t.Fatalf("interactions: got %d, want 1", len(loaded.Interactions))
	}
	ex := loaded.Interactions[0].HTTP
	if ex.Request.Method != "POST" || ex.Request.Path != "/v1/chat" {
		t.Errorf("request mismatch: %+v", ex.Request)
	}
	if ex.Request.Body != `{"prompt":"x"}` {
		t.Errorf("body mismatch: %q", ex.Request.Body)
	}
	if ex.Response.Status != 201 {
		t.Errorf("response status: %d", ex.Response.Status)
	}
	if len(ex.Response.BodyChunks) != 0 {
		t.Errorf("non-SSE response should have empty BodyChunks; got %d", len(ex.Response.BodyChunks))
	}
}

func TestRecordSSE(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		f := w.(http.Flusher)
		_, _ = w.Write([]byte("data: one\n\n"))
		f.Flush()
		time.Sleep(15 * time.Millisecond)
		_, _ = w.Write([]byte("data: two\n\n"))
		f.Flush()
	}))
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	path := filepath.Join(t.TempDir(), "cass.json")
	rec := NewRecorder(path)
	proxy := httptest.NewServer(RecordHandler(target, rec))
	defer proxy.Close()

	resp, err := http.Get(proxy.URL + "/v1/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	loaded, _ := cassette.Load(path)
	chunks := loaded.Interactions[0].HTTP.Response.BodyChunks
	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 SSE chunks, got %d", len(chunks))
	}
	if !strings.Contains(chunks[0].Data, "one") || !strings.Contains(chunks[len(chunks)-1].Data, "two") {
		t.Errorf("chunk contents wrong: %+v", chunks)
	}
}

func TestReplayPlainHTTP(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "http", HTTP: &cassette.HTTPExchange{
			Request:  cassette.HTTPRequest{Method: "POST", Path: "/v1/chat", Body: `{"q":1}`},
			Response: cassette.HTTPResponse{Status: 200, Headers: map[string][]string{"Content-Type": {"application/json"}}, Body: `{"a":1}`},
		}},
	}}
	m := matcher.New(c, nil)
	srv := httptest.NewServer(ReplayHandler(m))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat", "application/json", strings.NewReader(`{"q":1}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"a":1}` {
		t.Errorf("body: got %q, want %q", body, `{"a":1}`)
	}
}

func TestReplayRepeatableSameKey(t *testing.T) {
	// Acceptance criterion #1: replaying a cassette with same-key duplicates
	// must be idempotent across runs. Issue the same two-call workload three
	// times against the same running ReplayHandler (no reload) and assert every
	// request matches the cassette — 0 misses, responses in recorded order each
	// run.
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "http", HTTP: &cassette.HTTPExchange{
			Request:  cassette.HTTPRequest{Method: "POST", Path: "/v1/responses", Body: `{"step":"x"}`},
			Response: cassette.HTTPResponse{Status: 200, Body: "first"},
		}},
		{Type: "http", HTTP: &cassette.HTTPExchange{
			Request:  cassette.HTTPRequest{Method: "POST", Path: "/v1/responses", Body: `{"step":"x"}`},
			Response: cassette.HTTPResponse{Status: 201, Body: "second"},
		}},
	}}
	m := matcher.New(c, nil)
	srv := httptest.NewServer(ReplayHandler(m))
	defer srv.Close()

	for run := 1; run <= 3; run++ {
		for _, want := range []struct {
			status int
			body   string
		}{{200, "first"}, {201, "second"}} {
			resp, err := http.Post(srv.URL+"/v1/responses", "application/json", strings.NewReader(`{"step":"x"}`))
			if err != nil {
				t.Fatalf("run %d POST: %v", run, err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != want.status || string(body) != want.body {
				t.Fatalf("run %d: got %d %q, want %d %q", run, resp.StatusCode, body, want.status, want.body)
			}
		}
	}
}

func TestReplaySSE(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "http", HTTP: &cassette.HTTPExchange{
			Request: cassette.HTTPRequest{Method: "GET", Path: "/stream"},
			Response: cassette.HTTPResponse{
				Status:  200,
				Headers: map[string][]string{"Content-Type": {"text/event-stream"}},
				BodyChunks: []cassette.Chunk{
					{Data: "data: a\n\n"},
					{Data: "data: b\n\n", DelayMs: 5},
				},
			},
		}},
	}}
	m := matcher.New(c, nil)
	srv := httptest.NewServer(ReplayHandler(m))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "data: a") || !strings.Contains(string(body), "data: b") {
		t.Errorf("replay missed chunks: %q", body)
	}
}

func TestSyncResponseEchoesLiveRunIDIntoReplay(t *testing.T) {
	// Full round trip: upstream echoes run_id back, we record one call, then
	// replay against a new request with a different run_id and verify the
	// response carries the new value (not the recorded one).
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// echo the body so the response contains whatever run_id the request had
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"echoed":` + string(body) + `}`))
	}))
	defer upstream.Close()

	rule := matcher.IgnoreRule{
		In:           matcher.IgnoreInBody,
		Pattern:      regexp.MustCompile(`/runs/\d{8}-\d{6}-\d{3}/`),
		ReplaceWith:  "/runs/<RUN_ID>/",
		SyncResponse: true,
	}
	target, _ := url.Parse(upstream.URL)
	path := filepath.Join(t.TempDir(), "cass.json")

	// --- record phase ---
	rec := NewRecorder(path, rule)
	recProxy := httptest.NewServer(RecordHandler(target, rec))
	originalBody := `{"path":"/runs/20260524-140516-982/out.txt"}`
	resp, err := http.Post(recProxy.URL+"/v1/chat", "application/json", strings.NewReader(originalBody))
	if err != nil {
		t.Fatalf("record POST: %v", err)
	}
	recBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	recProxy.Close()

	if !strings.Contains(string(recBody), "20260524-140516-982") {
		t.Fatalf("record response should echo original run_id: %s", recBody)
	}

	// Verify capture was persisted.
	loaded, err := cassette.Load(path)
	if err != nil {
		t.Fatalf("load cassette: %v", err)
	}
	if loaded.Interactions[0].HTTP.Match == nil ||
		len(loaded.Interactions[0].HTTP.Match.Captures) != 1 ||
		loaded.Interactions[0].HTTP.Match.Captures[0].Captured != "/runs/20260524-140516-982/" {
		t.Fatalf("cassette capture not persisted: %+v", loaded.Interactions[0].HTTP.Match)
	}

	// --- replay phase ---
	m := matcher.New(loaded, matcher.JSONBodyNormalizer, rule)
	repProxy := httptest.NewServer(ReplayHandler(m))
	defer repProxy.Close()

	liveBody := `{"path":"/runs/20260524-140543-781/out.txt"}`
	resp, err = http.Post(repProxy.URL+"/v1/chat", "application/json", strings.NewReader(liveBody))
	if err != nil {
		t.Fatalf("replay POST: %v", err)
	}
	defer resp.Body.Close()
	replayBody, _ := io.ReadAll(resp.Body)

	// The replayed response should carry the *live* run_id, not the recorded one.
	if strings.Contains(string(replayBody), "20260524-140516-982") {
		t.Errorf("replay leaked the recorded run_id: %s", replayBody)
	}
	if !strings.Contains(string(replayBody), "20260524-140543-781") {
		t.Errorf("replay did not echo the live run_id: %s", replayBody)
	}
}

func TestSyncResponseRewritesSSEChunks(t *testing.T) {
	rule := matcher.IgnoreRule{
		In:           matcher.IgnoreInBody,
		Pattern:      regexp.MustCompile(`/runs/\d{8}-\d{6}-\d{3}/`),
		SyncResponse: true,
	}
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "http", HTTP: &cassette.HTTPExchange{
			Request: cassette.HTTPRequest{Method: "POST", Path: "/stream", Body: `{"run":"/runs/20260101-000000-001/"}`},
			Response: cassette.HTTPResponse{
				Status:  200,
				Headers: map[string][]string{"Content-Type": {"text/event-stream"}},
				BodyChunks: []cassette.Chunk{
					{Data: "data: started /runs/20260101-000000-001/\n\n"},
					{Data: "data: done /runs/20260101-000000-001/\n\n"},
				},
			},
			Match: &cassette.MatchMeta{Captures: []cassette.Capture{
				{Pattern: `/runs/\d{8}-\d{6}-\d{3}/`, Captured: "/runs/20260101-000000-001/"},
			}},
		}},
	}}
	m := matcher.New(c, matcher.JSONBodyNormalizer, rule)
	srv := httptest.NewServer(ReplayHandler(m))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/stream", "application/json",
		strings.NewReader(`{"run":"/runs/20260524-140543-781/"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "20260101-000000-001") {
		t.Errorf("recorded run_id leaked into stream: %s", body)
	}
	if strings.Count(string(body), "20260524-140543-781") != 2 {
		t.Errorf("expected live run_id in both chunks, got: %s", body)
	}
}

func TestAutoReplaysWithinSameSession(t *testing.T) {
	// Regression: matcher.New snapshots the cassette at startup; freshly recorded
	// exchanges have to be fed back into the pool, otherwise a second identical
	// request in the same session goes upstream and the cassette grows duplicates
	// forever. The hook lives in AutoHandler.
	var upstreamHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"hit":` + strings.Repeat("x", 0) + `"upstream"}`))
	}))
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	path := filepath.Join(t.TempDir(), "cass.json")
	rec, existing := NewAutoRecorder(path)
	m := matcher.New(existing, matcher.JSONBodyNormalizer)
	proxy := httptest.NewServer(AutoHandler(target, rec, m))
	defer proxy.Close()

	for i := 0; i < 3; i++ {
		resp, err := http.Post(proxy.URL+"/v1/chat", "application/json", strings.NewReader(`{"prompt":"same"}`))
		if err != nil {
			t.Fatalf("POST #%d: %v", i, err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}

	if upstreamHits != 1 {
		t.Errorf("upstream hits: got %d, want 1 (first miss only; replays should follow)", upstreamHits)
	}
}

func TestReplayMissReturns599(t *testing.T) {
	c := &cassette.Cassette{}
	m := matcher.New(c, nil)
	srv := httptest.NewServer(ReplayHandler(m))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 599 {
		t.Errorf("expected 599 on cassette miss, got %d", resp.StatusCode)
	}
}
