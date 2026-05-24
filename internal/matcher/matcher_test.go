package matcher

import (
	"regexp"
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

func TestIgnoreRuleBodyMatchesAcrossRuns(t *testing.T) {
	// Real-world case: opencode embeds a per-run output path in the prompt.
	// Without normalization the second run never hits the cassette.
	recorded := `{"prompt":"write to /runs/20250101-120000-001/out.txt"}`
	live := `{"prompt":"write to /runs/20260524-093045-042/out.txt"}`

	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		ex("POST", "/v1/chat", recorded, 200),
	}}
	rule := IgnoreRule{
		In:          IgnoreInBody,
		Pattern:     regexp.MustCompile(`/runs/\d{8}-\d{6}-\d{3}/`),
		ReplaceWith: "/runs/<RUN_ID>/",
	}
	m := New(c, JSONBodyNormalizer, rule)
	if got := m.Take("POST", "/v1/chat", live); got == nil {
		t.Fatalf("rule should normalize per-run ID so live request matches recorded one")
	}
}

func TestIgnoreRulePathMatchesAcrossRuns(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		ex("GET", "/v1/runs/20250101-120000-001/messages", "", 200),
	}}
	rule := IgnoreRule{
		In:          IgnoreInPath,
		Pattern:     regexp.MustCompile(`/runs/\d{8}-\d{6}-\d{3}/`),
		ReplaceWith: "/runs/<RUN_ID>/",
	}
	m := New(c, nil, rule)
	if got := m.Take("GET", "/v1/runs/20260524-093045-042/messages", ""); got == nil {
		t.Fatalf("path rule should normalize per-run ID so live request matches recorded one")
	}
}

func TestIgnoreRuleDoesNotCrossPaths(t *testing.T) {
	// A body rule must not leak into path matching: differing paths still miss.
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		ex("POST", "/a", `{"id":"X"}`, 200),
	}}
	rule := IgnoreRule{
		In:          IgnoreInBody,
		Pattern:     regexp.MustCompile(`"id":"[^"]*"`),
		ReplaceWith: `"id":"<ID>"`,
	}
	m := New(c, JSONBodyNormalizer, rule)
	if m.Take("POST", "/b", `{"id":"Y"}`) != nil {
		t.Errorf("body rule must not paper over path mismatch")
	}
}

func TestExtractCapturesRecordsLiteralMatch(t *testing.T) {
	rules := []IgnoreRule{
		{
			In:           IgnoreInBody,
			Pattern:      regexp.MustCompile(`/runs/\d{8}-\d{6}-\d{3}/`),
			SyncResponse: true,
		},
	}
	caps := ExtractCaptures(rules, "POST", "/v1/chat", `{"path":"/runs/20260524-140516-982/out.txt"}`)
	if len(caps) != 1 {
		t.Fatalf("want 1 capture, got %d", len(caps))
	}
	if caps[0].Captured != "/runs/20260524-140516-982/" {
		t.Errorf("captured = %q", caps[0].Captured)
	}
	if caps[0].Pattern != `/runs/\d{8}-\d{6}-\d{3}/` {
		t.Errorf("pattern stored = %q", caps[0].Pattern)
	}
}

func TestExtractCapturesSkipsNonSyncRule(t *testing.T) {
	rules := []IgnoreRule{
		{In: IgnoreInBody, Pattern: regexp.MustCompile(`abc`)}, // no SyncResponse
	}
	if caps := ExtractCaptures(rules, "POST", "/", "abc"); len(caps) != 0 {
		t.Errorf("non-sync rules should yield no captures, got %v", caps)
	}
}

func TestComputeSyncReplacementsPairsLiveAndRecorded(t *testing.T) {
	rule := IgnoreRule{
		In:           IgnoreInBody,
		Pattern:      regexp.MustCompile(`/runs/\d{8}-\d{6}-\d{3}/`),
		SyncResponse: true,
	}
	ex := &cassette.HTTPExchange{
		Match: &cassette.MatchMeta{Captures: []cassette.Capture{
			{Pattern: `/runs/\d{8}-\d{6}-\d{3}/`, Captured: "/runs/20260524-140516-982/"},
		}},
	}
	live := `{"path":"/runs/20260524-140543-781/out.txt"}`
	reps := ComputeSyncReplacements([]IgnoreRule{rule}, "POST", "/v1/chat", live, ex)
	if len(reps) != 1 {
		t.Fatalf("want 1 replacement, got %d", len(reps))
	}
	if reps[0].From != "/runs/20260524-140516-982/" || reps[0].To != "/runs/20260524-140543-781/" {
		t.Errorf("replacement wrong: %+v", reps[0])
	}
}

func TestComputeSyncReplacementsSkipsWhenIdentical(t *testing.T) {
	rule := IgnoreRule{
		In:           IgnoreInBody,
		Pattern:      regexp.MustCompile(`abc`),
		SyncResponse: true,
	}
	ex := &cassette.HTTPExchange{
		Match: &cassette.MatchMeta{Captures: []cassette.Capture{{Pattern: `abc`, Captured: "abc"}}},
	}
	reps := ComputeSyncReplacements([]IgnoreRule{rule}, "POST", "/", "abc", ex)
	if len(reps) != 0 {
		t.Errorf("identical live/recorded should not produce a replacement; got %v", reps)
	}
}

func TestApplyReplacements(t *testing.T) {
	out := ApplyReplacements(
		`{"id":"OLD","other":"OLD","tag":"NEW"}`,
		[]SyncReplacement{{From: "OLD", To: "NEW"}},
	)
	want := `{"id":"NEW","other":"NEW","tag":"NEW"}`
	if out != want {
		t.Errorf("got %q want %q", out, want)
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
