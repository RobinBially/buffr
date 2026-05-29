package main

import (
	"path/filepath"
	"testing"

	"buffr/internal/matcher"
)

func TestCassettePath(t *testing.T) {
	tests := []struct {
		path, host, want string
	}{
		{"explicit.json", "api.openai.com", "explicit.json"},
		{"", "api.openai.com", "api.openai.com.json"},
		{"", "api.anthropic.com", "api.anthropic.com.json"},
	}
	for _, tc := range tests {
		got := cassettePath(tc.path, tc.host)
		if got != tc.want {
			t.Errorf("cassettePath(%q, %q) = %q, want %q", tc.path, tc.host, got, tc.want)
		}
	}
}

func TestParseYAMLTargets(t *testing.T) {
	t.Run("two entries with defaults", func(t *testing.T) {
		yaml := `
- target: https://api.openai.com
  port: 8081
- target: https://api.anthropic.com
  port: 8082
`
		got := parseYAMLTargets(yaml)
		if len(got) != 2 {
			t.Fatalf("want 2 instances, got %d", len(got))
		}
		if got[0].target.Host != "api.openai.com" {
			t.Errorf("instance 0: host = %q, want api.openai.com", got[0].target.Host)
		}
		if got[0].port != 8081 {
			t.Errorf("instance 0: port = %d, want 8081", got[0].port)
		}
		if got[0].mode != "auto" {
			t.Errorf("instance 0: mode = %q, want auto", got[0].mode)
		}
		if got[0].cassette != "api.openai.com.json" {
			t.Errorf("instance 0: cassette = %q, want api.openai.com.json", got[0].cassette)
		}
		if got[1].target.Host != "api.anthropic.com" {
			t.Errorf("instance 1: host = %q, want api.anthropic.com", got[1].target.Host)
		}
	})

	t.Run("explicit mode and cassette", func(t *testing.T) {
		yaml := `
- target: https://api.openai.com
  port: 9000
  mode: replay
  cassette: /data/openai.json
`
		got := parseYAMLTargets(yaml)
		if len(got) != 1 {
			t.Fatalf("want 1 instance, got %d", len(got))
		}
		if got[0].mode != "replay" {
			t.Errorf("mode = %q, want replay", got[0].mode)
		}
		if got[0].cassette != "/data/openai.json" {
			t.Errorf("cassette = %q, want /data/openai.json", got[0].cassette)
		}
		if got[0].port != 9000 {
			t.Errorf("port = %d, want 9000", got[0].port)
		}
	})

	t.Run("port defaults to 8080+index", func(t *testing.T) {
		yaml := `
- target: https://api.openai.com
- target: https://api.anthropic.com
`
		got := parseYAMLTargets(yaml)
		if len(got) != 2 {
			t.Fatalf("want 2 instances, got %d", len(got))
		}
		if got[0].port != 8080 {
			t.Errorf("instance 0: port = %d, want 8080", got[0].port)
		}
		if got[1].port != 8081 {
			t.Errorf("instance 1: port = %d, want 8081", got[1].port)
		}
	})

	t.Run("invalid target is skipped", func(t *testing.T) {
		yaml := `
- target: not-a-url
  port: 8081
- target: https://api.openai.com
  port: 8082
`
		got := parseYAMLTargets(yaml)
		if len(got) != 1 {
			t.Fatalf("want 1 valid instance, got %d", len(got))
		}
		if got[0].target.Host != "api.openai.com" {
			t.Errorf("host = %q, want api.openai.com", got[0].target.Host)
		}
	})

	t.Run("invalid YAML returns nil", func(t *testing.T) {
		got := parseYAMLTargets("{ not: valid: yaml: [")
		if got != nil {
			t.Errorf("want nil for invalid YAML, got %v", got)
		}
	})

	t.Run("empty input returns empty slice", func(t *testing.T) {
		got := parseYAMLTargets("")
		if len(got) != 0 {
			t.Errorf("want empty, got %d instances", len(got))
		}
	})
}

func TestParseYAMLTargetsWithMatchIgnore(t *testing.T) {
	yaml := `
- target: http://lm-studio.local:1234
  port: 8083
  cassette: /data/lm-studio.json
  match:
    ignore:
      - in: request.body
        pattern: '/runs/\d{8}-\d{6}-\d{3}/'
        replace_with: '/runs/<RUN_ID>/'
      - in: request.path
        pattern: '/tasks/[0-9a-f-]+'
        replace_with: '/tasks/<TASK_ID>'
`
	got := parseYAMLTargets(yaml)
	if len(got) != 1 {
		t.Fatalf("want 1 instance, got %d", len(got))
	}
	if len(got[0].rules) != 2 {
		t.Fatalf("want 2 ignore rules, got %d", len(got[0].rules))
	}
	if got[0].rules[0].In != matcher.IgnoreInBody {
		t.Errorf("rule 0 In = %q, want %q", got[0].rules[0].In, matcher.IgnoreInBody)
	}
	if got[0].rules[0].ReplaceWith != "/runs/<RUN_ID>/" {
		t.Errorf("rule 0 ReplaceWith = %q", got[0].rules[0].ReplaceWith)
	}
	if !got[0].rules[0].Pattern.MatchString("/runs/20250101-120000-001/") {
		t.Errorf("rule 0 pattern should match a run-id path")
	}
	if got[0].rules[1].In != matcher.IgnoreInPath {
		t.Errorf("rule 1 In = %q, want %q", got[0].rules[1].In, matcher.IgnoreInPath)
	}
}

func TestParseYAMLTargetsSyncResponse(t *testing.T) {
	yaml := `
- target: http://lm-studio.local:1234
  match:
    ignore:
      - in: request.body
        pattern: 'foo'
        replace_with: 'FOO'
        sync_response: true
      - in: request.body
        pattern: 'bar'
        replace_with: 'BAR'
        # sync_response omitted -> defaults to false
`
	got := parseYAMLTargets(yaml)
	if len(got) != 1 || len(got[0].rules) != 2 {
		t.Fatalf("want 1 instance with 2 rules, got instances=%d rules=%d", len(got), len(got[0].rules))
	}
	if !got[0].rules[0].SyncResponse {
		t.Errorf("rule 0 SyncResponse should be true")
	}
	if got[0].rules[1].SyncResponse {
		t.Errorf("rule 1 SyncResponse should default to false")
	}
}

func TestParseYAMLTargetsSkipsInvalidIgnoreRules(t *testing.T) {
	yaml := `
- target: http://example.com
  port: 8083
  match:
    ignore:
      - in: request.headers          # unsupported "in"
        pattern: 'foo'
        replace_with: 'bar'
      - in: request.body
        pattern: '['                  # invalid regex
        replace_with: ''
      - in: request.body              # this one is valid
        pattern: 'abc'
        replace_with: 'xyz'
`
	got := parseYAMLTargets(yaml)
	if len(got) != 1 {
		t.Fatalf("want 1 instance, got %d", len(got))
	}
	if len(got[0].rules) != 1 {
		t.Fatalf("want 1 valid rule (2 invalid skipped), got %d", len(got[0].rules))
	}
	if got[0].rules[0].Pattern.String() != "abc" {
		t.Errorf("surviving rule pattern = %q, want abc", got[0].rules[0].Pattern.String())
	}
}

func TestParseYAMLTargetsNoMatchBlock(t *testing.T) {
	yaml := `
- target: https://api.openai.com
  port: 8081
`
	got := parseYAMLTargets(yaml)
	if len(got) != 1 {
		t.Fatalf("want 1 instance, got %d", len(got))
	}
	if got[0].rules != nil {
		t.Errorf("want nil rules when no match block, got %v", got[0].rules)
	}
}

func TestParseIndexedTargets(t *testing.T) {
	t.Run("two indexed entries", func(t *testing.T) {
		t.Setenv("BUFFR_0_TARGET", "https://api.openai.com")
		t.Setenv("BUFFR_0_PORT", "8081")
		t.Setenv("BUFFR_1_TARGET", "https://api.anthropic.com")
		t.Setenv("BUFFR_1_PORT", "8082")

		got := parseIndexedTargets()
		if len(got) != 2 {
			t.Fatalf("want 2 instances, got %d", len(got))
		}
		if got[0].target.Host != "api.openai.com" {
			t.Errorf("instance 0 host = %q", got[0].target.Host)
		}
		if got[1].target.Host != "api.anthropic.com" {
			t.Errorf("instance 1 host = %q", got[1].target.Host)
		}
	})

	t.Run("stops at first missing index", func(t *testing.T) {
		t.Setenv("BUFFR_0_TARGET", "https://api.openai.com")
		// BUFFR_1_TARGET intentionally absent
		t.Setenv("BUFFR_2_TARGET", "https://api.anthropic.com")

		got := parseIndexedTargets()
		if len(got) != 1 {
			t.Fatalf("want 1 instance (stops at gap), got %d", len(got))
		}
	})

	t.Run("no indexed vars returns empty", func(t *testing.T) {
		got := parseIndexedTargets()
		if len(got) != 0 {
			t.Errorf("want empty, got %d", len(got))
		}
	})
}

func TestParseProxyConfig(t *testing.T) {
	t.Run("mode, bypass and per-host cassette", func(t *testing.T) {
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")
		t.Setenv("BUFFR_DATA_DIR", "/data")
		cfg := parseProxyConfig(`
mode: replay
bypass: [localhost, 127.0.0.1, qdrant]
hosts:
  - host: huggingface.co
    cassette: /data/hf.json
  - host: google.serper.dev
  - host: '*'
`)
		if cfg.Mode != "replay" {
			t.Errorf("mode = %q, want replay", cfg.Mode)
		}
		if len(cfg.Bypass) != 3 {
			t.Errorf("bypass = %v, want 3 entries", cfg.Bypass)
		}
		if len(cfg.Hosts) != 3 {
			t.Fatalf("hosts = %d, want 3", len(cfg.Hosts))
		}
		// explicit cassette path kept verbatim
		if cfg.Hosts[0].Host != "huggingface.co" || cfg.Hosts[0].Cassette != "/data/hf.json" {
			t.Errorf("host 0 = %+v", cfg.Hosts[0])
		}
		// missing cassette → <data>/<host>.json
		if cfg.Hosts[1].Cassette != filepath.Join("/data", "google.serper.dev.json") {
			t.Errorf("host 1 cassette = %q", cfg.Hosts[1].Cassette)
		}
		// "*" fallback → <data>/misc.json
		if cfg.Hosts[2].Host != "*" || cfg.Hosts[2].Cassette != filepath.Join("/data", "misc.json") {
			t.Errorf("host 2 = %+v", cfg.Hosts[2])
		}
	})

	t.Run("NO_PROXY merged into bypass", func(t *testing.T) {
		t.Setenv("NO_PROXY", "s3.local, registry.internal")
		t.Setenv("no_proxy", "extra.host")
		cfg := parseProxyConfig(`bypass: [qdrant]`)
		want := map[string]bool{"qdrant": true, "s3.local": true, "registry.internal": true, "extra.host": true}
		if len(cfg.Bypass) != len(want) {
			t.Fatalf("bypass = %v, want %d entries", cfg.Bypass, len(want))
		}
		for _, b := range cfg.Bypass {
			if !want[b] {
				t.Errorf("unexpected bypass entry %q", b)
			}
		}
	})

	t.Run("empty config defaults to no hosts and empty mode", func(t *testing.T) {
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")
		cfg := parseProxyConfig("")
		if cfg.Mode != "" {
			t.Errorf("mode = %q, want empty (caller defaults to auto)", cfg.Mode)
		}
		if len(cfg.Hosts) != 0 || len(cfg.Bypass) != 0 {
			t.Errorf("expected empty hosts/bypass, got hosts=%v bypass=%v", cfg.Hosts, cfg.Bypass)
		}
	})

	t.Run("invalid YAML is tolerated, not fatal", func(t *testing.T) {
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")
		cfg := parseProxyConfig("mode: [this is not a scalar")
		if len(cfg.Hosts) != 0 {
			t.Errorf("malformed YAML should yield no hosts, got %v", cfg.Hosts)
		}
	})

	t.Run("host entry missing host is skipped", func(t *testing.T) {
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")
		cfg := parseProxyConfig(`
hosts:
  - cassette: /data/orphan.json
  - host: keep.me
`)
		if len(cfg.Hosts) != 1 || cfg.Hosts[0].Host != "keep.me" {
			t.Fatalf("want only the named host, got %+v", cfg.Hosts)
		}
	})

	t.Run("per-host match.ignore compiled into rules", func(t *testing.T) {
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")
		cfg := parseProxyConfig(`
hosts:
  - host: api.test
    match:
      ignore:
        - in: request.body
          pattern: '/runs/\d+/'
          replace_with: '/runs/<ID>/'
`)
		if len(cfg.Hosts) != 1 || len(cfg.Hosts[0].Rules) != 1 {
			t.Fatalf("want 1 host with 1 rule, got %+v", cfg.Hosts)
		}
		if cfg.Hosts[0].Rules[0].In != matcher.IgnoreInBody {
			t.Errorf("rule In = %q, want request.body", cfg.Hosts[0].Rules[0].In)
		}
		if !cfg.Hosts[0].Rules[0].Pattern.MatchString("/runs/42/") {
			t.Errorf("compiled rule pattern should match a run path")
		}
	})
}

func TestProxyCassettePath(t *testing.T) {
	t.Setenv("BUFFR_DATA_DIR", "/data")
	tests := []struct {
		path, host, want string
	}{
		{"/explicit/path.json", "huggingface.co", "/explicit/path.json"},
		{"", "huggingface.co", filepath.Join("/data", "huggingface.co.json")},
		{"", "*", filepath.Join("/data", "misc.json")},
	}
	for _, tc := range tests {
		if got := proxyCassettePath(tc.path, tc.host); got != tc.want {
			t.Errorf("proxyCassettePath(%q, %q) = %q, want %q", tc.path, tc.host, got, tc.want)
		}
	}
}

func TestLoadInstancesYAMLTakesPrecedence(t *testing.T) {
	t.Setenv("BUFFR_TARGETS", `
- target: https://api.openai.com
  port: 8081
`)
	t.Setenv("BUFFR_0_TARGET", "https://api.anthropic.com")
	t.Setenv("BUFFR_0_PORT", "9000")

	got := loadInstances()
	if len(got) != 1 {
		t.Fatalf("want 1 instance from YAML, got %d", len(got))
	}
	if got[0].target.Host != "api.openai.com" {
		t.Errorf("expected YAML to take precedence, got host %q", got[0].target.Host)
	}
}
