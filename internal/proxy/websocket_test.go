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
