package main

import (
	"testing"
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
