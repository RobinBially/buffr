// Package cassette defines the on-disk format buffr uses to record and replay
// HTTP and WebSocket interactions.
//
// A cassette is a single JSON file holding an ordered list of interactions.
// Each interaction is either:
//
//   - an HTTP exchange (request + response, with body_chunks for SSE), or
//   - a WebSocket session (one upgrade request followed by an ordered list of
//     frames in both directions).
//
// Bodies and frame payloads are stored as plain strings when valid UTF-8.
// Otherwise they are base64-encoded with `_b64` field suffixes (e.g.
// `data_b64` next to `data`). The format is intentionally human-readable so
// cassettes can be reviewed in diffs and edited by hand if needed.
package cassette

import (
	"encoding/json"
	"fmt"
	"os"
)

// CurrentVersion is the schema version this package writes. Older cassettes
// remain readable but are upgraded on save.
const CurrentVersion = 1

// Cassette is the top-level structure persisted to disk.
type Cassette struct {
	Version      int           `json:"version"`
	Interactions []Interaction `json:"interactions"`
}

// Interaction is one HTTP exchange or one WebSocket session.
//
// The Type field discriminates which subfield is meaningful. Using a tagged
// envelope (rather than two separate top-level arrays) keeps the recorded
// chronological order intact, which matters when the same test makes a mix of
// HTTP and WS calls in sequence.
type Interaction struct {
	Type      string         `json:"type"` // "http" or "websocket"
	HTTP      *HTTPExchange  `json:"http,omitempty"`
	WebSocket *WSSession     `json:"websocket,omitempty"`
}

// HTTPExchange pairs one request with the response it produced.
//
// Match carries optional per-exchange metadata produced by the matcher's
// ignore rules — currently the literal substrings each sync_response rule
// captured from the request, so replay can swap them back into the response
// for the live request's equivalent values.
type HTTPExchange struct {
	Request  HTTPRequest  `json:"request"`
	Response HTTPResponse `json:"response"`
	Match    *MatchMeta   `json:"match,omitempty"`
}

// MatchMeta is the recorded side-information from matching rules.
type MatchMeta struct {
	Captures []Capture `json:"captures,omitempty"`
}

// Capture is one literal substring that a sync_response rule extracted from
// the request at record time. Pattern is the rule's regex source — kept so a
// replay can locate the right capture even if rule order changes in config.
type Capture struct {
	Pattern  string `json:"pattern"`
	Captured string `json:"captured"`
}

// HTTPRequest captures the parts of a request used for matching.
//
// Headers are kept lowercase keys → values list (HTTP headers are
// case-insensitive). Body holds the raw request body as a string. Hop-by-hop
// and proxy-sensitive headers (Host, Connection, Authorization values) are
// recorded as the client sent them so replay can verify drift, but matching
// ignores them by default.
type HTTPRequest struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Query   string              `json:"query,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    string              `json:"body,omitempty"`
}

// HTTPResponse is the full response, including SSE chunking when applicable.
//
// For non-streaming responses, Body holds the full body and BodyChunks is
// empty. For SSE / chunked responses, BodyChunks is non-empty and Body is
// left blank; the replay path streams the chunks in order, honoring each
// chunk's DelayMs from the previous chunk.
type HTTPResponse struct {
	Status     int                 `json:"status"`
	Headers    map[string][]string `json:"headers,omitempty"`
	Body       string              `json:"body,omitempty"`
	BodyChunks []Chunk             `json:"body_chunks,omitempty"`
}

// Chunk is one piece of a streamed response body.
//
// DelayMs is the wall-clock delay relative to the previous chunk (or to the
// start of the response for the first chunk). Capturing the delay rather than
// an absolute timestamp keeps cassettes diff-friendly when the wall clock of
// the recording session is irrelevant.
type Chunk struct {
	Data    string `json:"data"`
	DelayMs int    `json:"delay_ms"`
}

// WSSession is the upgrade request plus the full ordered transcript of frames.
type WSSession struct {
	Request WSRequest `json:"request"`
	Frames  []WSFrame `json:"frames"`
}

// WSRequest is the HTTP upgrade portion of the WebSocket handshake.
type WSRequest struct {
	Path    string              `json:"path"`
	Query   string              `json:"query,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
}

// Frame direction constants. We use snake_case strings instead of an enum so
// cassettes stay readable when viewed as JSON.
const (
	DirClientToServer = "client_to_server"
	DirServerToClient = "server_to_client"
)

// Frame opcode constants matching RFC 6455 control/data frame categories.
const (
	OpText   = "text"
	OpBinary = "binary"
	OpClose  = "close"
)

// WSFrame is one WebSocket frame, captured with the data payload and a delay
// relative to the previous frame.
//
// Data is the UTF-8 payload for text frames. For binary frames the payload is
// stored base64-encoded in DataB64 (Data is left empty). On replay, Data wins
// if both are set.
type WSFrame struct {
	Direction string `json:"direction"`
	Opcode    string `json:"opcode"`
	Data      string `json:"data,omitempty"`
	DataB64   string `json:"data_b64,omitempty"`
	DelayMs   int    `json:"delay_ms"`
	// CloseCode is populated for opcode=close so the replayer can echo the
	// recorded close code. Zero means "no recorded code".
	CloseCode int `json:"close_code,omitempty"`
}

// Load reads a cassette from disk. Missing files are surfaced as a wrapped
// error so callers can distinguish "no cassette yet" (record mode is fine)
// from "broken cassette" (replay mode should fail loudly).
func Load(path string) (*Cassette, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Cassette
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("cassette %s: invalid JSON: %w", path, err)
	}
	if c.Version == 0 {
		c.Version = CurrentVersion
	}
	if c.Version > CurrentVersion {
		return nil, fmt.Errorf("cassette %s: version %d is newer than this build supports (max %d)", path, c.Version, CurrentVersion)
	}
	return &c, nil
}

// Save writes a cassette to disk with indented JSON. Indentation costs a
// little disk space but makes review diffs sane.
func Save(path string, c *Cassette) error {
	if c.Version == 0 {
		c.Version = CurrentVersion
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
