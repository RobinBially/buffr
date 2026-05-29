package forward

import (
	"io"
	"net"
	"sync"
)

// oneShotListener adapts a single, already-accepted connection into a
// net.Listener so it can be driven by the standard net/http server. Reusing
// http.Server (rather than hand-rolling request parsing) gives the intercepted
// TLS leg free HTTP/1.1 parsing, keep-alive, chunked transfer, SSE flushing, and
// hijack-for-WebSocket support.
//
// Accept hands out the connection exactly once. The second Accept blocks until
// the connection closes and then returns io.EOF, so http.Server.Serve stays
// running for the full lifetime of the connection — including a hijacked
// WebSocket — and returns cleanly once it ends.
type oneShotListener struct {
	conn   net.Conn
	closed chan struct{} // closed when conn.Close() runs (via notifyConn)

	mu   sync.Mutex
	used bool
}

func (l *oneShotListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if !l.used {
		l.used = true
		l.mu.Unlock()
		return l.conn, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, io.EOF
}

func (l *oneShotListener) Close() error { return nil }

func (l *oneShotListener) Addr() net.Addr { return l.conn.LocalAddr() }

// notifyConn closes a channel the first time the connection is closed. The
// oneShotListener watches that channel to know when http.Server is done with the
// connection (the server closes it on EOF; gorilla closes it at the end of a
// hijacked WebSocket session).
type notifyConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *notifyConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}
