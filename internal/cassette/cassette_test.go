package cassette

import (
	"bytes"
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

func TestEncodeDecodeBody(t *testing.T) {
	cases := []struct {
		name      string
		in        []byte
		wantPlain bool // expect storage in the plain field (vs base64)
	}{
		{"empty", nil, true},
		{"utf8 text", []byte(`{"ok":true}`), true},
		{"gzip-like binary", []byte{0x1f, 0x8b, 0x08, 0x00, 0xff, 0xfe, 0x00, 0x01}, false},
		{"invalid utf8", []byte{0xff, 0xfe, 0xfd}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plain, b64 := EncodeBody(tc.in)
			if tc.wantPlain && b64 != "" {
				t.Errorf("expected plain storage, got base64 %q", b64)
			}
			if !tc.wantPlain && len(tc.in) > 0 && plain != "" {
				t.Errorf("expected base64 storage, got plain %q", plain)
			}
			got := DecodeBody(plain, b64)
			if !bytes.Equal(got, tc.in) && !(len(got) == 0 && len(tc.in) == 0) {
				t.Errorf("round-trip mismatch: got %v, want %v", got, tc.in)
			}
		})
	}
}

func TestBinaryBodyRoundTripThroughDisk(t *testing.T) {
	raw := []byte{0x00, 0x1f, 0x8b, 0xff, 0xfe, 0x42, 0x00}
	plain, b64 := EncodeBody(raw)
	c := &Cassette{Interactions: []Interaction{{
		Type: "http",
		HTTP: &HTTPExchange{
			Request:  HTTPRequest{Method: "GET", Host: "huggingface.co", Path: "/model.bin"},
			Response: HTTPResponse{Status: 200, Body: plain, BodyB64: b64},
		},
	}}}
	path := filepath.Join(t.TempDir(), "bin.json")
	if err := Save(path, c); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	resp := got.Interactions[0].HTTP.Response
	if decoded := DecodeBody(resp.Body, resp.BodyB64); !bytes.Equal(decoded, raw) {
		t.Errorf("binary body corrupted through disk: got %v, want %v", decoded, raw)
	}
	if got.Interactions[0].HTTP.Request.Host != "huggingface.co" {
		t.Errorf("host field dropped on round-trip")
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
