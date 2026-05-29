// Package mitm provides the certificate authority and on-the-fly leaf
// certificates buffr's forward-proxy mode uses to terminate (intercept) TLS.
//
// On first use buffr generates a root CA and persists it so the same CA is
// reused across runs — the client trusts it once (via SSL_CERT_FILE /
// REQUESTS_CA_BUNDLE / the OS trust store). For every intercepted host buffr
// mints a leaf certificate signed by that CA, with the host in subjectAltName,
// and serves it to the client. Leaves are minted in-memory and cached; they are
// never persisted.
//
// All crypto is standard library (crypto/x509, crypto/ecdsa) — no extra
// dependencies. ECDSA P-256 is used throughout: fast to generate (the CA is
// created once, leaves on demand) and universally accepted by modern TLS stacks
// (httpx, requests, aiohttp, Go).
package mitm

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"sync"
	"time"
)

// CA is a root certificate authority plus a cache of the leaf certificates it
// has signed. It is safe for concurrent use.
type CA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte

	// leafKey is a single key reused for every leaf. Reusing one key across
	// hosts is a standard MITM optimization — these certs live only in memory
	// for the duration of a test run, so per-host keys would buy nothing.
	leafKey *ecdsa.PrivateKey

	mu     sync.Mutex
	leaves map[string]*tls.Certificate
}

// LoadOrCreateCA loads the CA from certPath/keyPath, or generates and persists a
// new one if either file is missing. Persisting means the consumer trusts the CA
// once and every later run reuses it.
func LoadOrCreateCA(certPath, keyPath string) (*CA, error) {
	if fileExists(certPath) && fileExists(keyPath) {
		return loadCA(certPath, keyPath)
	}
	return createCA(certPath, keyPath)
}

func loadCA(certPath, keyPath string) (*CA, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert %s: %w", certPath, err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read CA key %s: %w", keyPath, err)
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("CA cert %s: not valid PEM", certPath)
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("CA cert %s: %w", certPath, err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, fmt.Errorf("CA key %s: not valid PEM", keyPath)
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("CA key %s: %w", keyPath, err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return &CA{cert: cert, key: key, certPEM: certPEM, leafKey: leafKey, leaves: map[string]*tls.Certificate{}}, nil
}

func createCA(certPath, keyPath string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := randSerial()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "buffr local CA",
			Organization: []string{"buffr"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return nil, fmt.Errorf("write CA cert %s: %w", certPath, err)
	}
	// The private key is more sensitive than the cert; 0600. It is only a local
	// test-fixture CA, but there is no reason to leave it world-readable.
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return nil, fmt.Errorf("write CA key %s: %w", keyPath, err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return &CA{cert: cert, key: key, certPEM: certPEM, leafKey: leafKey, leaves: map[string]*tls.Certificate{}}, nil
}

// CertPEM returns the PEM-encoded CA certificate — what `buffr ca` prints and
// what gets written to <data>/buffr-ca.pem for the client to trust.
func (c *CA) CertPEM() []byte { return c.certPEM }

// LeafFor returns a leaf certificate valid for host, minting and caching it on
// first request. host should be the bare hostname (no port).
func (c *CA) LeafFor(host string) (*tls.Certificate, error) {
	c.mu.Lock()
	if leaf, ok := c.leaves[host]; ok {
		c.mu.Unlock()
		return leaf, nil
	}
	c.mu.Unlock()

	leaf, err := c.mintLeaf(host)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	// Re-check: another goroutine may have minted the same host concurrently.
	if existing, ok := c.leaves[host]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.leaves[host] = leaf
	c.mu.Unlock()
	return leaf, nil
}

func (c *CA) mintLeaf(host string) (*tls.Certificate, error) {
	serial, err := randSerial()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().AddDate(2, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	// An IP literal must go in IPAddresses, not DNSNames, or the client's TLS
	// verification of the address fails. CONNECT to a bare IP and SNI-less
	// connections both surface here.
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &c.leafKey.PublicKey, c.key)
	if err != nil {
		return nil, err
	}
	// Parse the DER so Leaf carries Raw — required for x509 verification and to
	// let the TLS stack avoid re-parsing on every handshake.
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{
		Certificate: [][]byte{der, c.cert.Raw},
		PrivateKey:  c.leafKey,
		Leaf:        leaf,
	}, nil
}

// TLSConfig returns a server-side tls.Config that serves a per-SNI leaf cert.
// Only HTTP/1.1 is advertised via ALPN: the intercepted client↔buffr leg stays
// HTTP/1.1 so buffr never has to speak HTTP/2 framing. Egress to the real
// upstream may still negotiate h2 transparently via http.DefaultTransport.
func (c *CA) TLSConfig() *tls.Config {
	return &tls.Config{
		NextProtos: []string{"http/1.1"},
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			host := hello.ServerName
			if host == "" {
				// No SNI: fall back to a stable placeholder so the handshake can
				// still complete. CONNECT-authority-based hosts are wired in by
				// the forward proxy, which passes a non-empty ServerName.
				host = "unknown"
			}
			return c.LeafFor(host)
		},
	}
}

func randSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}
