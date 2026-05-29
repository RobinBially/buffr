package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"buffr/internal/cassette"
)

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// echoServer is a minimal WebSocket server: it sends one greeting on connect,
// then echoes everything it receives until the client closes.
func echoServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.WriteMessage(websocket.TextMessage, []byte("hello"))
		for {
			mt, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			_ = c.WriteMessage(mt, msg)
		}
	}))
}

func TestRecordWS(t *testing.T) {
	upstream := echoServer(t)
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	cassPath := filepath.Join(t.TempDir(), "ws.json")
	rec := NewRecorder(cassPath)
	proxy := httptest.NewServer(RecordWSHandler(target, rec))
	defer proxy.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(proxy.URL)+"/realtime", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Receive greeting
	_, msg, err := conn.ReadMessage()
	if err != nil || string(msg) != "hello" {
		t.Fatalf("greeting: %q err=%v", msg, err)
	}
	// Send + echo
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, msg, err = conn.ReadMessage()
	if err != nil || string(msg) != "ping" {
		t.Fatalf("echo: %q err=%v", msg, err)
	}
	_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	conn.Close()

	// Give the proxy a beat to flush its append.
	time.Sleep(100 * time.Millisecond)

	loaded, err := cassette.Load(cassPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Interactions) != 1 {
		t.Fatalf("interactions: got %d, want 1", len(loaded.Interactions))
	}
	session := loaded.Interactions[0].WebSocket
	if session == nil {
		t.Fatalf("interaction[0] is not a websocket session")
	}
	// Recorded order: s→c hello, c→s ping, s→c ping, c→s close, s→c close.
	// Order of the trailing close frames is racy — assert prefix only.
	if len(session.Frames) < 3 {
		t.Fatalf("expected ≥3 frames, got %d", len(session.Frames))
	}
	if session.Frames[0].Direction != cassette.DirServerToClient || session.Frames[0].Data != "hello" {
		t.Errorf("frame 0: %+v", session.Frames[0])
	}
	if session.Frames[1].Direction != cassette.DirClientToServer || session.Frames[1].Data != "ping" {
		t.Errorf("frame 1: %+v", session.Frames[1])
	}
	if session.Frames[2].Direction != cassette.DirServerToClient || session.Frames[2].Data != "ping" {
		t.Errorf("frame 2: %+v", session.Frames[2])
	}
}

func TestReplayWSHappyPath(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/realtime"},
			Frames: []cassette.WSFrame{
				{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "hello"},
				{Direction: cassette.DirClientToServer, Opcode: cassette.OpText, Data: "ping"},
				{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "pong", DelayMs: 5},
			},
		}},
	}}
	rep := NewWSReplayer(c)
	srv := httptest.NewServer(ReplayWSHandler(rep))
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL)+"/realtime", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	if err != nil || string(msg) != "hello" {
		t.Fatalf("greeting: %q err=%v", msg, err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, msg, err = conn.ReadMessage()
	if err != nil || string(msg) != "pong" {
		t.Fatalf("pong: %q err=%v", msg, err)
	}
}

// TestReplayWSMatchesByPath guards the audio-processing case: recordings for
// /realtime and /diarize live in one cassette, and the client may connect in a
// different order than recorded. Each connection must get the session for its
// own path — not whatever happens to be first — or the two sides deadlock.
func TestReplayWSMatchesByPath(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/realtime"},
			Frames: []cassette.WSFrame{
				{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "session.created"},
			},
		}},
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/diarize"},
			Frames: []cassette.WSFrame{
				{Direction: cassette.DirClientToServer, Opcode: cassette.OpBinary, Data: "audio"},
			},
		}},
	}}
	rep := NewWSReplayer(c)
	srv := httptest.NewServer(ReplayWSHandler(rep))
	defer srv.Close()

	// Connect to /realtime even though /diarize is the second recorded session.
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL)+"/realtime", nil)
	if err != nil {
		t.Fatalf("dial /realtime: %v", err)
	}
	defer conn.Close()
	_, msg, err := conn.ReadMessage()
	if err != nil || string(msg) != "session.created" {
		t.Fatalf("expected session.created for /realtime, got %q err=%v", msg, err)
	}
}

func TestReplayWSDriftClosesConnection(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Frames: []cassette.WSFrame{
				{Direction: cassette.DirClientToServer, Opcode: cassette.OpText, Data: "expected-ping"},
			},
		}},
	}}
	rep := NewWSReplayer(c)
	srv := httptest.NewServer(ReplayWSHandler(rep))
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL)+"/r", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("wrong-ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatalf("expected connection close on cassette drift, got no error")
	}
}

// TestReplayWSRepeatableAcrossRuns is the WS analogue of acceptance criterion
// #1: connecting to the same recorded path repeatedly against the same running
// replayer must keep replaying the session, not 599 after the first connection.
func TestReplayWSRepeatableAcrossRuns(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/realtime"},
			Frames: []cassette.WSFrame{
				{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "hello"},
			},
		}},
	}}
	rep := NewWSReplayer(c)
	srv := httptest.NewServer(ReplayWSHandler(rep))
	defer srv.Close()

	for run := 1; run <= 3; run++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL)+"/realtime", nil)
		if err != nil {
			t.Fatalf("run %d dial: %v", run, err)
		}
		_, msg, err := conn.ReadMessage()
		if err != nil || string(msg) != "hello" {
			t.Fatalf("run %d greeting: %q err=%v", run, msg, err)
		}
		conn.Close()
	}
}

// TestReplayWSDisambiguatesSamePathByQuery guards the tie-breaker: two sessions
// recorded on the same path but with different handshake queries (the
// wss://…/v1/realtime?model=A vs ?model=B case) must each be served to the
// connection that asked for them, regardless of connection order — not just the
// next one in recorded order. Repeated across runs to prove idempotency.
func TestReplayWSDisambiguatesSamePathByQuery(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/v1/realtime", Query: "model=A"},
			Frames:  []cassette.WSFrame{{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "from-A"}},
		}},
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/v1/realtime", Query: "model=B"},
			Frames:  []cassette.WSFrame{{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "from-B"}},
		}},
	}}
	rep := NewWSReplayer(c)
	srv := httptest.NewServer(ReplayWSHandler(rep))
	defer srv.Close()

	greeting := func(query string) string {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL)+"/v1/realtime?"+query, nil)
		if err != nil {
			t.Fatalf("dial %s: %v", query, err)
		}
		defer conn.Close()
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read %s: %v", query, err)
		}
		return string(msg)
	}

	for run := 1; run <= 3; run++ {
		// Connect B before A — recorded order is A then B — to prove the query,
		// not arrival order, selects the session.
		if got := greeting("model=B"); got != "from-B" {
			t.Fatalf("run %d: model=B got %q, want from-B", run, got)
		}
		if got := greeting("model=A"); got != "from-A" {
			t.Fatalf("run %d: model=A got %q, want from-A", run, got)
		}
	}
}

// TestReplayWSSamePathSameQueryKeepsOrder guards that the query tie-breaker does
// not disturb genuine same-key duplicates: two sessions on one path with the
// same (here empty) query still replay in recorded order and wrap idempotently.
func TestReplayWSSamePathSameQueryKeepsOrder(t *testing.T) {
	c := &cassette.Cassette{Interactions: []cassette.Interaction{
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/stream"},
			Frames:  []cassette.WSFrame{{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "first"}},
		}},
		{Type: "websocket", WebSocket: &cassette.WSSession{
			Request: cassette.WSRequest{Path: "/stream"},
			Frames:  []cassette.WSFrame{{Direction: cassette.DirServerToClient, Opcode: cassette.OpText, Data: "second"}},
		}},
	}}
	rep := NewWSReplayer(c)
	srv := httptest.NewServer(ReplayWSHandler(rep))
	defer srv.Close()

	greet := func() string {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL)+"/stream", nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return string(msg)
	}

	for run := 1; run <= 2; run++ {
		if got := greet(); got != "first" {
			t.Fatalf("run %d call 1: got %q, want first", run, got)
		}
		if got := greet(); got != "second" {
			t.Fatalf("run %d call 2: got %q, want second", run, got)
		}
	}
}

func TestReplayWSMissReturns599(t *testing.T) {
	rep := NewWSReplayer(&cassette.Cassette{})
	srv := httptest.NewServer(ReplayWSHandler(rep))
	defer srv.Close()

	_, resp, err := websocket.DefaultDialer.Dial(wsURL(srv.URL)+"/r", nil)
	if err == nil {
		t.Fatalf("expected handshake failure")
	}
	if resp == nil {
		t.Fatalf("expected upgrade response with 599, got nil response")
	}
	if resp.StatusCode != 599 {
		t.Errorf("status: got %d, want 599", resp.StatusCode)
	}
}
