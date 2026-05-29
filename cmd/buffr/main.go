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
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"buffr/internal/cassette"
	"buffr/internal/forward"
	"buffr/internal/matcher"
	"buffr/internal/mitm"
	"buffr/internal/proxy"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.String(slog.TimeKey, a.Value.Time().Format("15:04:05.000"))
			}
			return a
		},
	})))

	// Allow BUFFR_MODE to supply the subcommand when no CLI argument is given,
	// which is the typical Docker entrypoint pattern.
	args := os.Args[1:]
	if len(args) == 0 {
		if mode := os.Getenv("BUFFR_MODE"); mode != "" {
			args = []string{mode}
		}
	}

	// Forward-proxy (TLS-MITM) mode and the `ca` helper are checked before the
	// reverse-proxy multi-instance config so they take precedence when selected.
	// `ca` just prints the CA cert; proxy mode is triggered by the `proxy`
	// subcommand (incl. BUFFR_MODE=proxy, mapped above) or the BUFFR_PROXY env.
	if len(args) > 0 && args[0] == "ca" {
		os.Exit(runCA(args[1:]))
	}
	if len(args) > 0 && args[0] == "proxy" {
		os.Exit(runProxy(args[1:]))
	}
	if os.Getenv("BUFFR_PROXY") != "" {
		os.Exit(runProxy(nil))
	}

	// Multi-instance mode: BUFFR_TARGETS or BUFFR_0_TARGET take precedence
	// over any subcommand — check before the args-empty guard so containers
	// using only BUFFR_TARGETS don't need BUFFR_MODE.
	if instances := loadInstances(); len(instances) > 0 {
		os.Exit(runInstances(instances))
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

Usage (reverse proxy — point the client's base_url at buffr):
  buffr record  --target <url> [--port <p>] [--cassette <file>]
  buffr replay  [--cassette <file>] [--port <p>]
  buffr auto    --target <url> [--port <p>] [--cassette <file>]

Usage (forward proxy — TLS-MITM, catch-all via HTTPS_PROXY):
  buffr proxy   [--port <p>] [--record|--replay|--auto]
  buffr ca      [--cert <file>] [--key <file>]   # print the CA cert PEM to trust

Environment variables (flags take precedence):
  BUFFR_MODE      subcommand (record|replay|auto|proxy) — used when no CLI argument is given
  BUFFR_TARGET    upstream URL for record/auto mode
  BUFFR_PORT      local port (default 8080)
  BUFFR_CASSETTE  cassette file path (auto-generated from target host if omitted)

Forward-proxy env:
  BUFFR_PROXY     YAML config (mode, bypass list, per-host cassette + match.ignore)
  BUFFR_CA_CERT   CA cert path (default <data>/buffr-ca.pem)
  BUFFR_CA_KEY    CA key path  (default <data>/buffr-ca.key)
  BUFFR_DATA_DIR  base dir for default cassettes + CA (default ".")
  Client sets: HTTPS_PROXY=http://buffr:8080  SSL_CERT_FILE=<buffr-ca.pem>  NO_PROXY=...

  BUFFR_PROXY: |
    mode: auto
    bypass: [localhost, 127.0.0.1, qdrant, s3]
    hosts:
      - host: huggingface.co
        cassette: /data/hf.json
      - host: '*'            # fallback for any other host

Multi-instance — YAML list (recommended):
  BUFFR_TARGETS='- target: https://api.openai.com
    port: 8081
  - target: https://api.anthropic.com
    port: 8082'

  Per-entry fields: target (required), port, cassette, mode, match.ignore.
  match.ignore rewrites per-run noise (run IDs, UUIDs) so cassette matches hit
  even when the body or path varies between runs:
    match:
      ignore:
        - in: request.body         # or request.path
          pattern: '/runs/\d{8}-\d{6}-\d{3}/'
          replace_with: '/runs/<RUN_ID>/'
          sync_response: true      # echo the live value back in the response

Multi-instance — indexed env vars (alternative):
  BUFFR_0_TARGET=https://api.openai.com   BUFFR_0_PORT=8081
  BUFFR_1_TARGET=https://api.anthropic.com BUFFR_1_PORT=8082
  (BUFFR_N_CASSETTE and BUFFR_N_MODE optional; indices must be contiguous from 0;
   match.ignore is only available via BUFFR_TARGETS YAML)

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
		slog.Warn("invalid env var, using default", "key", key, "value", v, "default", fallback)
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
	mux.Handle("/", proxy.RouteUpgrade(
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
	mux.Handle("/", proxy.RouteUpgrade(
		proxy.ReplayWSHandler(rep),
		proxy.ReplayHandler(m),
	))
	return serve(*port, mux, "replay", *cass)
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
	mux.Handle("/", proxy.RouteUpgrade(
		proxy.AutoWSHandler(u, rec, rep),
		proxy.AutoHandler(u, rec, m),
	))
	return serve(*port, mux, "auto", cassPath)
}

// dataDir is the base directory for default cassettes and the CA files. In the
// Docker image WORKDIR is /data, so the default "." resolves there.
func dataDir() string { return envStr("BUFFR_DATA_DIR", ".") }

func caCertPath() string { return envStr("BUFFR_CA_CERT", filepath.Join(dataDir(), "buffr-ca.pem")) }
func caKeyPath() string  { return envStr("BUFFR_CA_KEY", filepath.Join(dataDir(), "buffr-ca.key")) }

// yamlProxy is the schema for the BUFFR_PROXY env var (forward-proxy mode).
type yamlProxy struct {
	Mode   string          `yaml:"mode"`
	Bypass []string        `yaml:"bypass"`
	Hosts  []yamlProxyHost `yaml:"hosts"`
}

// yamlProxyHost configures one destination host. Host "*" is the catch-all
// fallback for any host not listed explicitly.
type yamlProxyHost struct {
	Host     string     `yaml:"host"`
	Cassette string     `yaml:"cassette"`
	Match    *yamlMatch `yaml:"match,omitempty"`
}

// runCA prints the buffr root CA certificate (generating + persisting it if it
// does not exist yet) so the client can trust it via SSL_CERT_FILE /
// REQUESTS_CA_BUNDLE / the OS trust store.
func runCA(args []string) int {
	fs := flag.NewFlagSet("ca", flag.ExitOnError)
	cert := fs.String("cert", caCertPath(), "CA cert path [BUFFR_CA_CERT]")
	key := fs.String("key", caKeyPath(), "CA key path [BUFFR_CA_KEY]")
	_ = fs.Parse(args)
	ca, err := mitm.LoadOrCreateCA(*cert, *key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ca: %v\n", err)
		return 1
	}
	if _, err := os.Stdout.Write(ca.CertPEM()); err != nil {
		return 1
	}
	return 0
}

// runProxy starts the forward-proxy (TLS-MITM) server.
func runProxy(args []string) int {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	port := fs.Int("port", envInt("BUFFR_PORT", 8080), "local port to listen on [BUFFR_PORT]")
	record := fs.Bool("record", false, "force record mode (always egress + record)")
	replay := fs.Bool("replay", false, "force replay mode (cassette only, miss → 599)")
	auto := fs.Bool("auto", false, "force auto mode (replay on hit, record on miss) [default]")
	_ = fs.Parse(args)

	cfg := parseProxyConfig(os.Getenv("BUFFR_PROXY"))

	// Mode precedence: explicit CLI flag > BUFFR_PROXY yaml `mode` > auto.
	switch {
	case *record:
		cfg.Mode = "record"
	case *replay:
		cfg.Mode = "replay"
	case *auto:
		cfg.Mode = "auto"
	}
	if cfg.Mode == "" {
		cfg.Mode = "auto"
	}

	ca, err := mitm.LoadOrCreateCA(caCertPath(), caKeyPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "proxy: failed to load/create CA: %v\n", err)
		return 1
	}
	cfg.CA = ca
	cfg.DataDir = dataDir()

	// The client must trust this cert. Surface where it lives so the consumer
	// can mount/point SSL_CERT_FILE at it (it is already written to disk by
	// LoadOrCreateCA when freshly generated).
	slog.Info("forward proxy CA ready", "cert", caCertPath(), "trust_via", "SSL_CERT_FILE / REQUESTS_CA_BUNDLE")

	fwd := forward.New(cfg)
	addr := fmt.Sprintf(":%d", *port)
	srv := &http.Server{Addr: addr, Handler: fwd.Handler()}
	go func() {
		slog.Info("listening", "mode", "proxy:"+cfg.Mode, "addr", addr, "bypass", cfg.Bypass)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	slog.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	return 0
}

// parseProxyConfig turns the BUFFR_PROXY YAML (plus the standard NO_PROXY env)
// into a forward.Config. CA and DataDir are filled in by the caller.
func parseProxyConfig(raw string) forward.Config {
	var p yamlProxy
	if raw != "" {
		if err := yaml.Unmarshal([]byte(raw), &p); err != nil {
			slog.Error("failed to parse BUFFR_PROXY", "err", err)
		}
	}

	bypass := append([]string(nil), p.Bypass...)
	// Honor the client's NO_PROXY too: clients usually skip the proxy for those
	// hosts anyway, but if buffr still sees one it should tunnel, not record.
	for _, key := range []string{"NO_PROXY", "no_proxy"} {
		for _, h := range strings.Split(os.Getenv(key), ",") {
			if h = strings.TrimSpace(h); h != "" {
				bypass = append(bypass, h)
			}
		}
	}

	hosts := make([]forward.HostConfig, 0, len(p.Hosts))
	for i, h := range p.Hosts {
		if h.Host == "" {
			slog.Warn("BUFFR_PROXY host entry missing host, skipping", "index", i)
			continue
		}
		hosts = append(hosts, forward.HostConfig{
			Host:     h.Host,
			Cassette: proxyCassettePath(h.Cassette, h.Host),
			Rules:    compileIgnoreRules(h.Match, i),
		})
	}

	return forward.Config{Mode: p.Mode, Bypass: bypass, Hosts: hosts}
}

// proxyCassettePath resolves a host entry's cassette: the configured path, or a
// default under the data dir ("misc.json" for the "*" fallback, "<host>.json"
// otherwise).
func proxyCassettePath(path, host string) string {
	if path != "" {
		return path
	}
	if host == "*" {
		return filepath.Join(dataDir(), "misc.json")
	}
	return filepath.Join(dataDir(), host+".json")
}

// instanceConfig holds the parsed configuration for one proxy instance.
type instanceConfig struct {
	mode     string
	target   *url.URL
	port     int
	cassette string
	rules    []matcher.IgnoreRule
}

// yamlInstance is the per-entry schema for BUFFR_TARGETS YAML.
type yamlInstance struct {
	Target   string     `yaml:"target"`
	Port     int        `yaml:"port"`
	Cassette string     `yaml:"cassette"`
	Mode     string     `yaml:"mode"`
	Match    *yamlMatch `yaml:"match,omitempty"`
}

// yamlMatch holds optional matching tweaks for one target.
type yamlMatch struct {
	Ignore []yamlIgnoreRule `yaml:"ignore"`
}

// yamlIgnoreRule rewrites a substring of the request before matching so
// per-run noise (run IDs, UUIDs, timestamps) does not defeat cassette hits.
// SyncResponse additionally propagates the live request's matched value into
// the replayed response.
type yamlIgnoreRule struct {
	In           string `yaml:"in"`            // request.body | request.path
	Pattern      string `yaml:"pattern"`       // Go regex
	ReplaceWith  string `yaml:"replace_with"`  // replacement text
	SyncResponse bool   `yaml:"sync_response"` // echo live value in replayed response
}

// loadInstances resolves multi-instance config from the environment.
// BUFFR_TARGETS (YAML list) takes precedence over BUFFR_0_TARGET / BUFFR_1_TARGET / … indexed vars.
func loadInstances() []instanceConfig {
	if raw := os.Getenv("BUFFR_TARGETS"); raw != "" {
		return parseYAMLTargets(raw)
	}
	return parseIndexedTargets()
}

// parseYAMLTargets parses the value of BUFFR_TARGETS as a YAML list.
//
//	BUFFR_TARGETS: |
//	  - target: https://api.openai.com
//	    port: 8081
//	  - target: https://api.anthropic.com
//	    port: 8082
//	    mode: replay
//	    cassette: /data/anthropic.json
func parseYAMLTargets(raw string) []instanceConfig {
	var entries []yamlInstance
	if err := yaml.Unmarshal([]byte(raw), &entries); err != nil {
		slog.Error("failed to parse BUFFR_TARGETS", "err", err)
		return nil
	}
	out := make([]instanceConfig, 0, len(entries))
	for i, e := range entries {
		u, err := url.Parse(e.Target)
		if err != nil || u.Scheme == "" || u.Host == "" {
			slog.Warn("invalid target in BUFFR_TARGETS, skipping", "index", i, "value", e.Target)
			continue
		}
		mode := e.Mode
		if mode == "" {
			mode = "auto"
		}
		port := e.Port
		if port == 0 {
			port = 8080 + i
		}
		out = append(out, instanceConfig{
			mode:     mode,
			target:   u,
			port:     port,
			cassette: cassettePath(e.Cassette, u.Host),
			rules:    compileIgnoreRules(e.Match, i),
		})
	}
	return out
}

// compileIgnoreRules turns the per-target match.ignore YAML into ready-to-use
// matcher.IgnoreRule values. Invalid entries (bad "in", bad regex) are logged
// and skipped rather than failing the whole config — a typo in one rule should
// not take down all proxies.
func compileIgnoreRules(m *yamlMatch, instanceIdx int) []matcher.IgnoreRule {
	if m == nil || len(m.Ignore) == 0 {
		return nil
	}
	out := make([]matcher.IgnoreRule, 0, len(m.Ignore))
	for j, r := range m.Ignore {
		switch r.In {
		case matcher.IgnoreInBody, matcher.IgnoreInPath:
		default:
			slog.Warn("invalid match.ignore.in, skipping rule",
				"instance", instanceIdx, "rule", j, "in", r.In,
				"allowed", []string{matcher.IgnoreInBody, matcher.IgnoreInPath})
			continue
		}
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			slog.Warn("invalid match.ignore.pattern, skipping rule",
				"instance", instanceIdx, "rule", j, "pattern", r.Pattern, "err", err)
			continue
		}
		out = append(out, matcher.IgnoreRule{
			In:           r.In,
			Pattern:      re,
			ReplaceWith:  r.ReplaceWith,
			SyncResponse: r.SyncResponse,
		})
	}
	return out
}

// parseIndexedTargets reads BUFFR_0_TARGET, BUFFR_1_TARGET, … until the first
// missing index.
func parseIndexedTargets() []instanceConfig {
	var out []instanceConfig
	for i := 0; ; i++ {
		pfx := fmt.Sprintf("BUFFR_%d_", i)
		rawTarget := os.Getenv(pfx + "TARGET")
		if rawTarget == "" {
			break
		}
		u, err := url.Parse(rawTarget)
		if err != nil || u.Scheme == "" || u.Host == "" {
			slog.Warn("invalid target, skipping instance", "key", pfx+"TARGET", "value", rawTarget)
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
			rec := proxy.NewRecorder(cfg.cassette, cfg.rules...)
			mux = http.NewServeMux()
			mux.Handle("/", proxy.RouteUpgrade(
				proxy.RecordWSHandler(cfg.target, rec),
				proxy.RecordHandler(cfg.target, rec),
			))
		case "replay":
			c, err := cassette.Load(cfg.cassette)
			if err != nil {
				slog.Error("failed to load cassette", "path", cfg.cassette, "err", err)
				return 1
			}
			m := matcher.New(c, matcher.JSONBodyNormalizer, cfg.rules...)
			rep := proxy.NewWSReplayer(c)
			mux = http.NewServeMux()
			mux.Handle("/", proxy.RouteUpgrade(
				proxy.ReplayWSHandler(rep),
				proxy.ReplayHandler(m),
			))
		default: // "auto"
			rec, existing := proxy.NewAutoRecorder(cfg.cassette, cfg.rules...)
			m := matcher.New(existing, matcher.JSONBodyNormalizer, cfg.rules...)
			rep := proxy.NewWSReplayer(existing)
			mux = http.NewServeMux()
			mux.Handle("/", proxy.RouteUpgrade(
				proxy.AutoWSHandler(cfg.target, rec, rep),
				proxy.AutoHandler(cfg.target, rec, m),
			))
		}
		addr := fmt.Sprintf(":%d", cfg.port)
		srv := &http.Server{Addr: addr, Handler: mux}
		servers = append(servers, srv)
		go func() {
			slog.Info("listening", "mode", cfg.mode, "addr", addr, "target", cfg.target, "cassette", cfg.cassette)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("server error", "addr", addr, "err", err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	slog.Info("shutting down")
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
		slog.Info("listening", "mode", mode, "addr", addr, "cassette", cass)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	slog.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	return 0
}
