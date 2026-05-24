package cassette

import (
	"path/filepath"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	c := &Cassette{
		Interactions: []Interaction{
			{
				Type: "http",
				HTTP: &HTTPExchange{
					Request: HTTPRequest{
						Method: "POST",
						Path:   "/v1/chat/completions",
						Body:   `{"model":"gpt-4","messages":[]}`,
					},
					Response: HTTPResponse{
						Status: 200,
						BodyChunks: []Chunk{
							{Data: "data: hello\n\n", DelayMs: 0},
							{Data: "data: world\n\n", DelayMs: 12},
						},
					},
				},
			},
			{
				Type: "websocket",
				WebSocket: &WSSession{
					Request: WSRequest{Path: "/v1/realtime"},
					Frames: []WSFrame{
						{Direction: DirClientToServer, Opcode: OpText, Data: `{"hello":1}`},
						{Direction: DirServerToClient, Opcode: OpText, Data: `{"reply":1}`, DelayMs: 50},
					},
				},
			},
		},
	}

	path := filepath.Join(t.TempDir(), "x.json")
	if err := Save(path, c); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Version != CurrentVersion {
		t.Errorf("version: got %d, want %d", got.Version, CurrentVersion)
	}
	if len(got.Interactions) != 2 {
		t.Fatalf("interactions: got %d, want 2", len(got.Interactions))
	}
	if got.Interactions[0].HTTP.Request.Method != "POST" {
		t.Errorf("http method round-trip mismatch")
	}
	if len(got.Interactions[0].HTTP.Response.BodyChunks) != 2 {
		t.Errorf("sse chunks dropped on round-trip")
	}
	if got.Interactions[1].WebSocket.Frames[1].DelayMs != 50 {
		t.Errorf("ws frame delay dropped on round-trip")
	}
}

func TestLoadRejectsFutureVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.json")
	c := &Cassette{Version: CurrentVersion + 99}
	if err := Save(path, c); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatalf("expected error loading newer-version cassette")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("/no/such/path.json"); err == nil {
		t.Fatalf("expected error loading missing file")
	}
}
