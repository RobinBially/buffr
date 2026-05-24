package matcher

import (
	"testing"

	"buffr/internal/cassette"
)

func ex(method, path, body string, status int) cassette.Interaction {
	return cassette.Interaction{
		Type: "http",
		HTTP: &cassette.HTTPExchange{
			Request:  cassette.HTTPRequest{Method: method, Path: path, Body: body},
			Response: cassette.HTTPResponse{Status: status},
		},
	}
}

func TestMatchAndConsume(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		ex("POST", "/v1/chat", `{"prompt":"hi"}`, 200),
		ex("POST", "/v1/chat", `{"prompt":"hi"}`, 201), // duplicate request, different response
	}}
	m := New(c, nil)

	got := m.Take("POST", "/v1/chat", `{"prompt":"hi"}`)
	if got == nil || got.Response.Status != 200 {
		t.Fatalf("first take should return status 200, got %+v", got)
	}
	got = m.Take("POST", "/v1/chat", `{"prompt":"hi"}`)
	if got == nil || got.Response.Status != 201 {
		t.Fatalf("second take should return status 201, got %+v", got)
	}
	if m.Take("POST", "/v1/chat", `{"prompt":"hi"}`) != nil {
		t.Fatalf("third take should return nil (pool exhausted)")
	}
}

func TestNoMatch(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		ex("POST", "/a", "body-a", 200),
	}}
	m := New(c, nil)
	if m.Take("POST", "/b", "body-a") != nil {
		t.Errorf("path mismatch should not match")
	}
	if m.Take("GET", "/a", "body-a") != nil {
		t.Errorf("method mismatch should not match")
	}
	if m.Take("POST", "/a", "different") != nil {
		t.Errorf("body mismatch should not match")
	}
	if m.Remaining() != 1 {
		t.Errorf("remaining: got %d, want 1", m.Remaining())
	}
}

func TestJSONNormalizerIgnoresKeyOrder(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		ex("POST", "/x", `{"a":1,"b":2}`, 200),
	}}
	m := New(c, JSONBodyNormalizer)
	if got := m.Take("POST", "/x", `{"b":2,"a":1}`); got == nil {
		t.Fatalf("JSON normalizer should match reordered keys")
	}
}

func TestJSONNormalizerFallsBackOnInvalidJSON(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		ex("POST", "/x", "not json", 200),
	}}
	m := New(c, JSONBodyNormalizer)
	if got := m.Take("POST", "/x", "not json"); got == nil {
		t.Fatalf("non-JSON body should still match exactly")
	}
}

func TestSkipsNonHTTP(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "websocket", WebSocket: &cassette.WSSession{}},
		ex("GET", "/", "", 200),
	}}
	m := New(c, nil)
	if m.Remaining() != 1 {
		t.Errorf("ws interaction must not be in HTTP pool; got remaining=%d", m.Remaining())
	}
}
