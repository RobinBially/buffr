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
	// Multi-instance mode: BUFFR_0_TARGET (and optionally BUFFR_1_TARGET, …)
	// takes precedence over any subcommand.
	if instances := loadInstances(); len(instances) > 0 {
		os.Exit(runInstances(instances))
	}

	switch args[0] {
	case "record":
		os.Exit(runRecord(args[1:]))
	case "replay":
		os.Exit(runReplay(args[1:]))
	case "auto":
		os.Exit(runAuto(args[1:]))
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
  buffr record  --target <url> [--port <p>] [--cassette <file>]
  buffr replay  [--cassette <file>] [--port <p>]
  buffr auto    --target <url> [--port <p>] [--cassette <file>]

Environment variables (flags take precedence):
  BUFFR_MODE      subcommand (record|replay|auto) — used when no CLI argument is given
  BUFFR_TARGET    upstream URL for record/auto mode
  BUFFR_PORT      local port (default 8080)
  BUFFR_CASSETTE  cassette file path (auto-generated from target host if omitted)

Multi-instance (one process, many ports):
  BUFFR_0_TARGET=https://api.openai.com   BUFFR_0_PORT=8081
  BUFFR_1_TARGET=https://api.anthropic.com BUFFR_1_PORT=8082
  …  (BUFFR_N_CASSETTE and BUFFR_N_MODE are optional per instance)

Examples:
  buffr auto --target https://api.openai.com --port 8080
  buffr replay --cassette session.json --port 8080
  BUFFR_MODE=auto BUFFR_TARGET=https://api.openai.com buffr`)
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
	if *target == "" {
		fmt.Fprintln(os.Stderr, "record: --target is required")
		return 2
	}
	u, err := url.Parse(*target)
	if err != nil || u.Scheme == "" || u.Host == "" {
		fmt.Fprintf(os.Stderr, "record: invalid --target %q\n", *target)
		return 2
	}
	cassPath := cassettePath(*cass, u.Host)
	rec := proxy.NewRecorder(cassPath)
	mux := http.NewServeMux()
	mux.Handle("/", routeUpgrade(
		proxy.RecordWSHandler(u, rec),
		proxy.RecordHandler(u, rec),
	))
	return serve(*port, mux, "record", cassPath)
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

// cassettePath returns path if non-empty, otherwise derives a filename from
// the target host (e.g. "api.openai.com.json") in the current directory.
func cassettePath(path, targetHost string) string {
	if path != "" {
		return path
	}
	return targetHost + ".json"
}

func runAuto(args []string) int {
	fs := flag.NewFlagSet("auto", flag.ExitOnError)
	target := fs.String("target", envStr("BUFFR_TARGET", ""), "upstream URL to proxy to (required) [BUFFR_TARGET]")
	port := fs.Int("port", envInt("BUFFR_PORT", 8080), "local port to listen on [BUFFR_PORT]")
	cass := fs.String("cassette", envStr("BUFFR_CASSETTE", ""), "cassette file (auto-generated if omitted) [BUFFR_CASSETTE]")
	_ = fs.Parse(args)
	if *target == "" {
		fmt.Fprintln(os.Stderr, "auto: --target is required")
		return 2
	}
	u, err := url.Parse(*target)
	if err != nil || u.Scheme == "" || u.Host == "" {
		fmt.Fprintf(os.Stderr, "auto: invalid --target %q\n", *target)
		return 2
	}
	cassPath := cassettePath(*cass, u.Host)
	rec, existing := proxy.NewAutoRecorder(cassPath)
	m := matcher.New(existing, matcher.JSONBodyNormalizer)
	rep := proxy.NewWSReplayer(existing)
	mux := http.NewServeMux()
	mux.Handle("/", routeUpgrade(
		proxy.AutoWSHandler(u, rec, rep),
		proxy.AutoHandler(u, rec, m),
	))
	return serve(*port, mux, "auto", cassPath)
}

// instanceConfig holds the parsed configuration for one proxy instance.
type instanceConfig struct {
	mode     string
	target   *url.URL
	port     int
	cassette string
}

// loadInstances reads BUFFR_0_TARGET, BUFFR_1_TARGET, … from the environment
// and returns one instanceConfig per indexed group found. Returns nil when no
// indexed variables are set, signalling single-instance mode.
func loadInstances() []instanceConfig {
	var out []instanceConfig
	for i := 0; ; i++ {
		pfx := fmt.Sprintf("BUFFR_%d_", i)
		rawTarget := os.Getenv(pfx + "TARGET")
		if rawTarget == "" {
			break
		}
		u, err := url.Parse(rawTarget)
		if err != nil || u.Scheme == "" || u.Host == "" {
			fmt.Fprintf(os.Stderr, "buffr: invalid %sTARGET=%q, skipping\n", pfx, rawTarget)
			continue
		}
		mode := os.Getenv(pfx + "MODE")
		if mode == "" {
			mode = "auto"
		}
		port := envInt(pfx+"PORT", 8080+i)
		cass := cassettePath(os.Getenv(pfx+"CASSETTE"), u.Host)
		out = append(out, instanceConfig{mode: mode, target: u, port: port, cassette: cass})
	}
	return out
}

// runInstances starts one HTTP server per instanceConfig in separate goroutines
// and blocks until SIGINT/SIGTERM, then shuts them all down gracefully.
func runInstances(instances []instanceConfig) int {
	var servers []*http.Server
	for _, cfg := range instances {
		cfg := cfg
		var mux *http.ServeMux
		switch cfg.mode {
		case "record":
			rec := proxy.NewRecorder(cfg.cassette)
			mux = http.NewServeMux()
			mux.Handle("/", routeUpgrade(
				proxy.RecordWSHandler(cfg.target, rec),
				proxy.RecordHandler(cfg.target, rec),
			))
		case "replay":
			c, err := cassette.Load(cfg.cassette)
			if err != nil {
				fmt.Fprintf(os.Stderr, "buffr: failed to load cassette %s: %v\n", cfg.cassette, err)
				return 1
			}
			m := matcher.New(c, matcher.JSONBodyNormalizer)
			rep := proxy.NewWSReplayer(c)
			mux = http.NewServeMux()
			mux.Handle("/", routeUpgrade(
				proxy.ReplayWSHandler(rep),
				proxy.ReplayHandler(m),
			))
		default: // "auto"
			rec, existing := proxy.NewAutoRecorder(cfg.cassette)
			m := matcher.New(existing, matcher.JSONBodyNormalizer)
			rep := proxy.NewWSReplayer(existing)
			mux = http.NewServeMux()
			mux.Handle("/", routeUpgrade(
				proxy.AutoWSHandler(cfg.target, rec, rep),
				proxy.AutoHandler(cfg.target, rec, m),
			))
		}
		addr := fmt.Sprintf(":%d", cfg.port)
		srv := &http.Server{Addr: addr, Handler: mux}
		servers = append(servers, srv)
		go func() {
			fmt.Fprintf(os.Stderr, "buffr %s on %s → target=%s cassette=%s\n",
				cfg.mode, addr, cfg.target, cfg.cassette)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "buffr: server error on %s: %v\n", addr, err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	fmt.Fprintln(os.Stderr, "buffr: shutting down…")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(ctx)
	}
	return 0
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
