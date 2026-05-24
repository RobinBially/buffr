package proxy

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"buffr/internal/cassette"
)

// upgrader matches the typical browser/CLI defaults. CheckOrigin always
// returns true because buffr is a test proxy by design — it doesn't enforce
// cross-origin policies, it just relays whatever the client and server want
// to say to each other.
var upgrader = websocket.Upgrader{
	CheckOrigin:      func(r *http.Request) bool { return true },
	ReadBufferSize:   4096,
	WriteBufferSize:  4096,
	HandshakeTimeout: 30 * time.Second,
}

// RecordWSHandler proxies WebSocket connections to the upstream target and
// records every frame in both directions to the recorder.
//
// Headers from the client's upgrade request are forwarded to the upstream
// (minus hop-by-hop and the WebSocket-specific ones the dialer sets itself).
// On any error after the upgrade succeeds, the connection is closed and a
// partial recording is flushed so debugging is possible.
func RecordWSHandler(target *url.URL, rec *Recorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			// upgrader already responded with an error status
			return
		}
		defer clientConn.Close()

		upstreamURL := *target
		upstreamURL.Scheme = wsScheme(upstreamURL.Scheme)
		upstreamURL.Path = singleJoin(upstreamURL.Path, r.URL.Path)
		upstreamURL.RawQuery = r.URL.RawQuery

		upstreamHeader := http.Header{}
		copyUpgradeHeaders(upstreamHeader, r.Header)

		upConn, upResp, err := websocket.DefaultDialer.Dial(upstreamURL.String(), upstreamHeader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "buffr: ws dial to %s failed: %v\n", upstreamURL.String(), err)
			if upResp != nil {
				upResp.Body.Close()
			}
			_ = clientConn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseProtocolError, "buffr: upstream dial failed"),
				time.Now().Add(time.Second),
			)
			return
		}
		defer upConn.Close()

		session := &cassette.WSSession{
			Request: cassette.WSRequest{
				Path:    r.URL.Path,
				Query:   r.URL.RawQuery,
				Headers: filterHeaders(r.Header),
			},
		}
		var (
			mu      sync.Mutex
			lastTS  = time.Now()
			closeCh = make(chan struct{})
			once    sync.Once
		)
		appendFrame := func(direction string, msgType int, payload []byte, closeCode int) {
			mu.Lock()
			defer mu.Unlock()
			now := time.Now()
			frame := cassette.WSFrame{
				Direction: direction,
				Opcode:    opcodeFromMessageType(msgType),
				DelayMs:   int(now.Sub(lastTS) / time.Millisecond),
				CloseCode: closeCode,
			}
			if msgType == websocket.BinaryMessage {
				frame.DataB64 = base64.StdEncoding.EncodeToString(payload)
			} else {
				frame.Data = string(payload)
			}
			session.Frames = append(session.Frames, frame)
			lastTS = now
		}
		closeOnce := func() { once.Do(func() { close(closeCh) }) }

		// Client → Upstream pump
		go func() {
			defer closeOnce()
			for {
				msgType, payload, err := clientConn.ReadMessage()
				if err != nil {
					closeCode := closeErrorCode(err)
					appendFrame(cassette.DirClientToServer, websocket.CloseMessage, nil, closeCode)
					_ = upConn.WriteMessage(websocket.CloseMessage,
						websocket.FormatCloseMessage(closeCode, ""))
					return
				}
				appendFrame(cassette.DirClientToServer, msgType, payload, 0)
				if err := upConn.WriteMessage(msgType, payload); err != nil {
					return
				}
			}
		}()

		// Upstream → Client pump (this goroutine "owns" the function lifetime
		// so we can wait synchronously for either side to terminate).
		go func() {
			defer closeOnce()
			for {
				msgType, payload, err := upConn.ReadMessage()
				if err != nil {
					closeCode := closeErrorCode(err)
					appendFrame(cassette.DirServerToClient, websocket.CloseMessage, nil, closeCode)
					_ = clientConn.WriteMessage(websocket.CloseMessage,
						websocket.FormatCloseMessage(closeCode, ""))
					return
				}
				appendFrame(cassette.DirServerToClient, msgType, payload, 0)
				if err := clientConn.WriteMessage(msgType, payload); err != nil {
					return
				}
			}
		}()

		<-closeCh
		if err := rec.Append(cassette.Interaction{Type: "websocket", WebSocket: session}); err != nil {
			fmt.Fprintf(os.Stderr, "buffr: failed to append ws session: %v\n", err)
		}
	})
}

// ReplayWSHandler serves a WebSocket session from the cassette. It uses the
// next un-consumed websocket interaction (sessions are matched by order of
// appearance, not by request shape — the upgrade request rarely contains
// stable per-test info worth matching on).
//
// Strict mode: every client-to-server frame must match the recorded frame
// at the corresponding position. On drift, the proxy closes the connection
// with a 1011 (internal error) so the test fails loudly rather than silently
// diverging from the recording.
type wsReplayer struct {
	mu       sync.Mutex
	sessions []*cassette.WSSession
}

// NewWSReplayer takes ownership of every WebSocket session in the cassette.
func NewWSReplayer(c *cassette.Cassette) *wsReplayer {
	r := &wsReplayer{}
	for _, it := range c.Interactions {
		if it.Type == "websocket" && it.WebSocket != nil {
			r.sessions = append(r.sessions, it.WebSocket)
		}
	}
	return r
}

func (r *wsReplayer) Remaining() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}

func (r *wsReplayer) take() *cassette.WSSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sessions) == 0 {
		return nil
	}
	s := r.sessions[0]
	r.sessions = r.sessions[1:]
	return s
}

// ReplayWSHandler returns a Handler that serves the next recorded session per
// WebSocket connection. If the cassette has no more WS sessions, the upgrade
// is rejected with a 599 — same convention as the HTTP replay miss.
func ReplayWSHandler(rep *wsReplayer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := rep.take()
		if session == nil {
			http.Error(w, "buffr: no cassette ws session for "+r.URL.Path, 599)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Two passes over the frames in order: server-to-client are written
		// to the client (honoring delay), client-to-server are read and
		// validated. When the next recorded frame is c→s, block on reading
		// until the client sends; when it's s→c, sleep + write.
		cursor := 0
		for cursor < len(session.Frames) {
			f := session.Frames[cursor]
			switch f.Direction {
			case cassette.DirServerToClient:
				if f.DelayMs > 0 {
					time.Sleep(time.Duration(f.DelayMs) * time.Millisecond)
				}
				msgType, payload, err := frameToMessage(f)
				if err != nil {
					return
				}
				if err := conn.WriteMessage(msgType, payload); err != nil {
					return
				}
				cursor++
			case cassette.DirClientToServer:
				msgType, payload, err := conn.ReadMessage()
				if err != nil {
					// Client closed early — only acceptable if the recorded
					// frame is also a close. Otherwise emit a clear error
					// before returning so the test sees what diverged.
					if f.Opcode != cassette.OpClose {
						fmt.Fprintf(os.Stderr,
							"buffr: cassette drift — recorded frame[%d] is c→s %s but client closed: %v\n",
							cursor, f.Opcode, err)
					}
					return
				}
				if err := validateFrame(f, msgType, payload); err != nil {
					fmt.Fprintf(os.Stderr,
						"buffr: cassette drift at frame[%d]: %v\n", cursor, err)
					_ = conn.WriteControl(
						websocket.CloseMessage,
						websocket.FormatCloseMessage(1011, "buffr: cassette drift"),
						time.Now().Add(time.Second),
					)
					return
				}
				cursor++
			default:
				return
			}
		}
	})
}

func frameToMessage(f cassette.WSFrame) (int, []byte, error) {
	switch f.Opcode {
	case cassette.OpText:
		return websocket.TextMessage, []byte(f.Data), nil
	case cassette.OpBinary:
		if f.DataB64 != "" {
			data, err := base64.StdEncoding.DecodeString(f.DataB64)
			if err != nil {
				return 0, nil, err
			}
			return websocket.BinaryMessage, data, nil
		}
		return websocket.BinaryMessage, []byte(f.Data), nil
	case cassette.OpClose:
		return websocket.CloseMessage, websocket.FormatCloseMessage(f.CloseCode, ""), nil
	}
	return 0, nil, fmt.Errorf("unknown opcode %q", f.Opcode)
}

func validateFrame(expected cassette.WSFrame, gotType int, gotData []byte) error {
	wantOp := expected.Opcode
	gotOp := opcodeFromMessageType(gotType)
	if wantOp != gotOp {
		return fmt.Errorf("opcode mismatch: recorded=%s got=%s", wantOp, gotOp)
	}
	switch wantOp {
	case cassette.OpText:
		if expected.Data != string(gotData) {
			return fmt.Errorf("text payload mismatch:\nrecorded: %q\ngot:      %q", expected.Data, string(gotData))
		}
	case cassette.OpBinary:
		var wantBin []byte
		if expected.DataB64 != "" {
			b, err := base64.StdEncoding.DecodeString(expected.DataB64)
			if err != nil {
				return err
			}
			wantBin = b
		} else {
			wantBin = []byte(expected.Data)
		}
		if string(wantBin) != string(gotData) {
			return fmt.Errorf("binary payload mismatch (lengths: recorded=%d got=%d)", len(wantBin), len(gotData))
		}
	case cassette.OpClose:
		// Treat any close as matching a close in the cassette — clients send
		// many subtly-different close frames and exact-match would be noise.
	}
	return nil
}

func opcodeFromMessageType(t int) string {
	switch t {
	case websocket.TextMessage:
		return cassette.OpText
	case websocket.BinaryMessage:
		return cassette.OpBinary
	case websocket.CloseMessage:
		return cassette.OpClose
	}
	return "unknown"
}

func closeErrorCode(err error) int {
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return websocket.CloseAbnormalClosure
}

func wsScheme(s string) string {
	switch strings.ToLower(s) {
	case "http":
		return "ws"
	case "https":
		return "wss"
	}
	return s
}

// copyUpgradeHeaders forwards the client's headers to the upstream dial,
// minus the headers gorilla/websocket sets itself (it manages them through
// the dialer config). Connection/Upgrade-related headers would conflict.
var wsManagedHeaders = map[string]struct{}{
	"Upgrade":                  {},
	"Connection":               {},
	"Sec-Websocket-Version":    {},
	"Sec-Websocket-Key":        {},
	"Sec-Websocket-Extensions": {},
	"Host":                     {},
}

func copyUpgradeHeaders(dst, src http.Header) {
	for k, vs := range src {
		if _, skip := wsManagedHeaders[http.CanonicalHeaderKey(k)]; skip {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
