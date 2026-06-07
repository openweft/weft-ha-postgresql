package fencing

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLoadClientTLSConfig_EmptyCA_Refuses : the operator MUST pass a CA
// pin or explicitly opt-in to insecure via main.go. A nil/empty arg
// returning a usable config would be a silent downgrade.
func TestLoadClientTLSConfig_EmptyCA_Refuses(t *testing.T) {
	_, err := LoadClientTLSConfig("", "", "", "")
	if err == nil {
		t.Fatal("LoadClientTLSConfig with empty CA path should error")
	}
}

// TestLoadClientTLSConfig_CAOnly : just a CA bundle is the minimum
// valid config (server-auth only).
func TestLoadClientTLSConfig_CAOnly(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCA(t, caPath)

	cfg, err := LoadClientTLSConfig(caPath, "", "", "")
	if err != nil {
		t.Fatalf("LoadClientTLSConfig: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs not populated")
	}
	if cfg.MinVersion < 0x303 { // TLS 1.2
		t.Errorf("MinVersion = %x ; want >= TLS 1.2", cfg.MinVersion)
	}
	if len(cfg.Certificates) != 0 {
		t.Errorf("Certificates should be empty when no mTLS pair ; got %d", len(cfg.Certificates))
	}
}

// TestLoadClientTLSConfig_MTLSRequiresBoth : the operator either sets
// none of the mTLS flags, or both. A half-config is a config bug worth
// surfacing early.
func TestLoadClientTLSConfig_MTLSRequiresBoth(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCA(t, caPath)

	// Only --weft-tls-cert
	if _, err := LoadClientTLSConfig(caPath, filepath.Join(dir, "client.pem"), "", ""); err == nil {
		t.Error("LoadClientTLSConfig with cert but no key should error")
	}
	// Only --weft-tls-key
	if _, err := LoadClientTLSConfig(caPath, "", filepath.Join(dir, "client.key"), ""); err == nil {
		t.Error("LoadClientTLSConfig with key but no cert should error")
	}
}

// TestLoadClientTLSConfig_BadCA : unparseable PEM => error, NOT silent
// fallback to an empty pool.
func TestLoadClientTLSConfig_BadCA(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("garbage not a PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadClientTLSConfig(caPath, "", "", ""); err == nil {
		t.Error("LoadClientTLSConfig with garbage PEM should error")
	}
}

// TestLoadClientTLSConfig_ServerName : explicit ServerName flows through.
func TestLoadClientTLSConfig_ServerName(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCA(t, caPath)

	cfg, err := LoadClientTLSConfig(caPath, "", "", "weft-agent.dc1.internal")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "weft-agent.dc1.internal" {
		t.Errorf("ServerName = %q ; want weft-agent.dc1.internal", cfg.ServerName)
	}
}

// writeTestCA emits a tiny self-signed ECDSA cert at path so the loader
// has something real to parse without bringing in test fixtures.
func writeTestCA(t *testing.T, path string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
}
