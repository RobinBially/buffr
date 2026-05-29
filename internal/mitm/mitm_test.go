package mitm

import (
	"crypto/tls"
	"crypto/x509"
	"path/filepath"
	"testing"
)

func newCA(t *testing.T) (*CA, string, string) {
	t.Helper()
	dir := t.TempDir()
	certPath := filepath.Join(dir, "buffr-ca.pem")
	keyPath := filepath.Join(dir, "buffr-ca.key")
	ca, err := LoadOrCreateCA(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateCA: %v", err)
	}
	return ca, certPath, keyPath
}

func TestLeafChainsToCAAndHasSAN(t *testing.T) {
	ca, _, _ := newCA(t)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca.CertPEM()) {
		t.Fatal("failed to add CA cert to pool")
	}

	leaf, err := ca.LeafFor("huggingface.co")
	if err != nil {
		t.Fatalf("LeafFor: %v", err)
	}
	if leaf.Leaf == nil {
		t.Fatal("leaf.Leaf not populated")
	}

	// The leaf must verify against the CA pool for the requested DNS name —
	// exactly what the client does, so this proves a freshly trusted client
	// validates the MITM'd TLS with no warning.
	if _, err := leaf.Leaf.Verify(x509.VerifyOptions{
		DNSName: "huggingface.co",
		Roots:   roots,
	}); err != nil {
		t.Fatalf("leaf failed to verify against CA: %v", err)
	}

	if _, err := leaf.Leaf.Verify(x509.VerifyOptions{
		DNSName: "wrong.example.com",
		Roots:   roots,
	}); err == nil {
		t.Fatal("leaf should NOT verify for a different host")
	}
}

func TestLeafCacheReturnsSameCert(t *testing.T) {
	ca, _, _ := newCA(t)
	a, err := ca.LeafFor("api.openai.com")
	if err != nil {
		t.Fatalf("LeafFor: %v", err)
	}
	b, err := ca.LeafFor("api.openai.com")
	if err != nil {
		t.Fatalf("LeafFor: %v", err)
	}
	if a != b {
		t.Error("repeat LeafFor for the same host should return the cached cert")
	}
}

func TestCAPersistsAcrossReload(t *testing.T) {
	ca, certPath, keyPath := newCA(t)
	first := string(ca.CertPEM())

	// Simulate a restart: load from the same files.
	reloaded, err := LoadOrCreateCA(certPath, keyPath)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if string(reloaded.CertPEM()) != first {
		t.Error("CA cert changed across reload; consumer trust would break every run")
	}

	// A leaf minted by the reloaded CA must still chain to the original cert.
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(first))
	leaf, err := reloaded.LeafFor("example.com")
	if err != nil {
		t.Fatalf("LeafFor after reload: %v", err)
	}
	if _, err := leaf.Leaf.Verify(x509.VerifyOptions{DNSName: "example.com", Roots: roots}); err != nil {
		t.Fatalf("reloaded CA's leaf does not chain to original cert: %v", err)
	}
}

func TestTLSConfigServesPerSNILeaf(t *testing.T) {
	ca, _, _ := newCA(t)
	cfg := ca.TLSConfig()
	cert, err := cfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "serper.dev"})
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if cert.Leaf.Subject.CommonName != "serper.dev" {
		t.Errorf("served leaf CN = %q, want serper.dev", cert.Leaf.Subject.CommonName)
	}
	if len(cfg.NextProtos) != 1 || cfg.NextProtos[0] != "http/1.1" {
		t.Errorf("ALPN should advertise only http/1.1, got %v", cfg.NextProtos)
	}
}
