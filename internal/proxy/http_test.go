package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
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
