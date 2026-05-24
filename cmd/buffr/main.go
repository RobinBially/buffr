// Command buffr is a record/replay HTTP + WebSocket proxy for deterministic
// tests against external APIs.
//
//	buffr record --target <url> --port <p> --cassette <file>
//	buffr replay --cassette <file> --port <p>
//
// Every flag can also be set via an environment variable (flag takes precedence):
//
//	BUFFR_MODE      subcommand to run when no CLI argument is given (record|replay)
//	BUFFR_TARGET    upstream URL  (record mode)
//	BUFFR_PORT      local port    (default 8080)
//	BUFFR_CASSETTE  cassette file path
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"buffr/internal/cassette"
	"buffr/internal/matcher"
	"buffr/internal/proxy"
)

func main() {
	// Allow BUFFR_MODE to supply the subcommand when no CLI argument is given,
	// which is the typical Docker entrypoint pattern.
	args := os.Args[1:]
	if len(args) == 0 {
		if mode := os.Getenv("BUFFR_MODE"); mode != "" {
			args = []string{mode}
		}
	}

	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "record":
		os.Exit(runRecord(args[1:]))
	case "replay":
		os.Exit(runReplay(args[1:]))
	case "-h", "--help", "help":
		usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", args[0])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `buffr — record/replay HTTP + WebSocket proxy

Usage:
  buffr record  --target <url> --port <p> --cassette <file>
  buffr replay  --cassette <file> --port <p>

Environment variables (flags take precedence):
  BUFFR_MODE      subcommand (record|replay) — used when no CLI argument is given
  BUFFR_TARGET    upstream URL for record mode
  BUFFR_PORT      local port (default 8080)
  BUFFR_CASSETTE  cassette file path

Examples:
  buffr record --target https://api.openai.com --port 8080 --cassette session.json
  buffr replay --cassette session.json --port 8080
  BUFFR_MODE=replay BUFFR_CASSETTE=/data/session.json buffr`)
}

// envStr returns the value of the environment variable key, or fallback if unset.
func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envInt returns the integer value of key, or fallback if unset or unparseable.
func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		fmt.Fprintf(os.Stderr, "buffr: invalid %s=%q, using default %d\n", key, v, fallback)
	}
	return fallback
}

func runRecord(args []string) int {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	target := fs.String("target", envStr("BUFFR_TARGET", ""), "upstream URL to proxy to (required) [BUFFR_TARGET]")
	port := fs.Int("port", envInt("BUFFR_PORT", 8080), "local port to listen on [BUFFR_PORT]")
	cass := fs.String("cassette", envStr("BUFFR_CASSETTE", ""), "cassette file to write (required) [BUFFR_CASSETTE]")
	_ = fs.Parse(args)
	if *target == "" || *cass == "" {
		fmt.Fprintln(os.Stderr, "record: --target and --cassette are required")
		return 2
	}
	u, err := url.Parse(*target)
	if err != nil || u.Scheme == "" || u.Host == "" {
		fmt.Fprintf(os.Stderr, "record: invalid --target %q\n", *target)
		return 2
	}
	rec := proxy.NewRecorder(*cass)
	mux := http.NewServeMux()
	mux.Handle("/", routeUpgrade(
		proxy.RecordWSHandler(u, rec),
		proxy.RecordHandler(u, rec),
	))
	return serve(*port, mux, "record", *cass)
}

func runReplay(args []string) int {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	cass := fs.String("cassette", envStr("BUFFR_CASSETTE", ""), "cassette file to read (required) [BUFFR_CASSETTE]")
	port := fs.Int("port", envInt("BUFFR_PORT", 8080), "local port to listen on [BUFFR_PORT]")
	_ = fs.Parse(args)
	if *cass == "" {
		fmt.Fprintln(os.Stderr, "replay: --cassette is required")
		return 2
	}
	c, err := cassette.Load(*cass)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: failed to load cassette: %v\n", err)
		return 1
	}
	m := matcher.New(c, matcher.JSONBodyNormalizer)
	rep := proxy.NewWSReplayer(c)
	mux := http.NewServeMux()
	mux.Handle("/", routeUpgrade(
		proxy.ReplayWSHandler(rep),
		proxy.ReplayHandler(m),
	))
	return serve(*port, mux, "replay", *cass)
}

// routeUpgrade dispatches WebSocket upgrade requests to wsHandler and every
// other request to httpHandler. The signal is the Upgrade header; relying on
// it (rather than the path) means callers don't have to declare which paths
// serve WS — the protocol tells us.
func routeUpgrade(wsHandler, httpHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			wsHandler.ServeHTTP(w, r)
			return
		}
		httpHandler.ServeHTTP(w, r)
	})
}

func serve(port int, h http.Handler, mode, cass string) int {
	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{Addr: addr, Handler: h}
	go func() {
		fmt.Fprintf(os.Stderr, "buffr %s on %s → cassette=%s\n", mode, addr, cass)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "buffr: server error: %v\n", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	fmt.Fprintln(os.Stderr, "buffr: shutting down…")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	return 0
}
