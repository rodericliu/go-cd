package admin

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"consumer-dispatcher/internal/config"
)

const (
	tokenA = "token-a-0123456789abcdef"
	tokenB = "token-b-0123456789abcdef"
)

func serve(t *testing.T, h http.Handler, method, path, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(`{"count":0}`))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestTokenAuth(t *testing.T) {
	h := newTestHandlerWith(t, Options{Tokens: []string{tokenA, tokenB}}, slog.New(slog.DiscardHandler))
	tests := []struct {
		name     string
		method   string
		path     string
		auth     string
		wantCode int
	}{
		{name: "healthz is open", method: http.MethodGet, path: "/healthz", wantCode: http.StatusOK},
		{name: "metrics is open", method: http.MethodGet, path: "/metrics", wantCode: http.StatusOK},
		{name: "readyz is open", method: http.MethodGet, path: "/readyz", wantCode: http.StatusServiceUnavailable},
		{name: "api without token", method: http.MethodGet, path: "/api/jobs", wantCode: http.StatusUnauthorized},
		{name: "api wrong token", method: http.MethodGet, path: "/api/jobs", auth: "Bearer wrong-token-0123456789", wantCode: http.StatusUnauthorized},
		{name: "api token prefix only", method: http.MethodGet, path: "/api/jobs", auth: "Bearer " + tokenA[:10], wantCode: http.StatusUnauthorized},
		{name: "api token with suffix", method: http.MethodGet, path: "/api/jobs", auth: "Bearer " + tokenA + "x", wantCode: http.StatusUnauthorized},
		{name: "api basic scheme", method: http.MethodGet, path: "/api/jobs", auth: "Basic " + tokenA, wantCode: http.StatusUnauthorized},
		{name: "api first token", method: http.MethodGet, path: "/api/jobs", auth: "Bearer " + tokenA, wantCode: http.StatusOK},
		{name: "api second token, lowercase scheme", method: http.MethodGet, path: "/api/jobs", auth: "bearer " + tokenB, wantCode: http.StatusOK},
		{name: "api write with token", method: http.MethodPost, path: "/api/jobs/pay/workers", auth: "Bearer " + tokenA, wantCode: http.StatusOK},
		{name: "api write without token", method: http.MethodPost, path: "/api/jobs/pay/workers", wantCode: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serve(t, h, tt.method, tt.path, tt.auth)
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d; body: %s", rec.Code, tt.wantCode, rec.Body)
			}
			if rec.Code == http.StatusUnauthorized && !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
				t.Errorf("missing WWW-Authenticate header")
			}
		})
	}
}

func TestNoAuthConfiguredAllowsAPI(t *testing.T) {
	h := newTestHandler(t)
	if rec := serve(t, h, http.MethodGet, "/api/jobs", ""); rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestAuditLog(t *testing.T) {
	var logs syncBuffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	h := newTestHandlerWith(t, Options{Tokens: []string{tokenA, tokenB}}, log)

	serve(t, h, http.MethodPost, "/api/jobs/pay/workers", "Bearer "+tokenB)
	serve(t, h, http.MethodGet, "/api/jobs", "Bearer "+tokenA) // reads are debug-only
	serve(t, h, http.MethodPost, "/api/jobs/pay/purge", "Bearer not-a-valid-token-123")

	out := logs.String()
	for _, want := range []string{
		`"msg":"admin api","method":"POST","path":"/api/jobs/pay/workers","status":200,"identity":"token#2"`,
		`"msg":"admin auth failed","method":"POST","path":"/api/jobs/pay/purge"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %s\n%s", want, out)
		}
	}
	if strings.Contains(out, `"path":"/api/jobs","status"`) {
		t.Errorf("GET was logged at info level:\n%s", out)
	}
	for _, secret := range []string{tokenA, tokenB, "not-a-valid-token-123"} {
		if strings.Contains(out, secret) {
			t.Fatalf("token leaked into logs:\n%s", out)
		}
	}
}

// testPKI writes a CA, a server cert for 127.0.0.1 and two client certs
// (one signed by the CA, one self-signed) into dir.
type testPKI struct {
	caFile, serverCert, serverKey string
	caPool                        *x509.CertPool
	client, rogue                 tls.Certificate
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()

	caKey := newKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	issue := func(cn string, usage x509.ExtKeyUsage, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) ([]byte, *ecdsa.PrivateKey) {
		key := newKey(t)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		}
		if parent == nil {
			parent, parentKey = tmpl, key
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
		if err != nil {
			t.Fatal(err)
		}
		return der, key
	}

	p := testPKI{caPool: pool}
	p.caFile = writePEM(t, dir, "ca.pem", "CERTIFICATE", caDER)
	srvDER, srvKey := issue("127.0.0.1", x509.ExtKeyUsageServerAuth, caCert, caKey)
	p.serverCert = writePEM(t, dir, "server.pem", "CERTIFICATE", srvDER)
	p.serverKey = writePEM(t, dir, "server-key.pem", "EC PRIVATE KEY", marshalKey(t, srvKey))

	cliDER, cliKey := issue("ops-alice", x509.ExtKeyUsageClientAuth, caCert, caKey)
	p.client = tls.Certificate{Certificate: [][]byte{cliDER}, PrivateKey: cliKey}
	rogueDER, rogueKey := issue("mallory", x509.ExtKeyUsageClientAuth, nil, nil)
	p.rogue = tls.Certificate{Certificate: [][]byte{rogueDER}, PrivateKey: rogueKey}
	return p
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func marshalKey(t *testing.T, k *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	tlsCfg, err := TLSConfig(config.AdminTLS{CertFile: pki.serverCert, KeyFile: pki.serverKey, ClientCAFile: pki.caFile})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		tokens   []string
		cert     *tls.Certificate
		force    bool // present cert even if the server's CA list doesn't match
		auth     string
		path     string
		wantCode int // 0 means the TLS handshake must fail
	}{
		{name: "probe without client cert", path: "/healthz", wantCode: http.StatusOK},
		{name: "metrics without client cert", path: "/metrics", wantCode: http.StatusOK},
		{name: "api without client cert", path: "/api/jobs", wantCode: http.StatusUnauthorized},
		{name: "api with valid client cert", cert: &pki.client, path: "/api/jobs", wantCode: http.StatusOK},
		// Go clients skip a cert not issued by an acceptable CA, so it arrives as "no cert".
		{name: "api with untrusted client cert", cert: &pki.rogue, path: "/api/jobs", wantCode: http.StatusUnauthorized},
		{name: "api with forced untrusted client cert", cert: &pki.rogue, force: true, path: "/api/jobs", wantCode: 0},
		{name: "cert and token: cert only", tokens: []string{tokenA}, cert: &pki.client, path: "/api/jobs", wantCode: http.StatusUnauthorized},
		{name: "cert and token: both", tokens: []string{tokenA}, cert: &pki.client, auth: "Bearer " + tokenA, path: "/api/jobs", wantCode: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandlerWith(t, Options{Tokens: tt.tokens, RequireClientCert: true}, slog.New(slog.DiscardHandler))
			srv := httptest.NewUnstartedServer(h)
			srv.TLS = tlsCfg
			srv.StartTLS()
			t.Cleanup(srv.Close)

			clientTLS := &tls.Config{RootCAs: pki.caPool, MinVersion: tls.VersionTLS12}
			switch {
			case tt.force:
				cert := tt.cert
				clientTLS.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return cert, nil }
			case tt.cert != nil:
				clientTLS.Certificates = []tls.Certificate{*tt.cert}
			}
			client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS}, Timeout: 5 * time.Second}

			req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+tt.path, nil)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			resp, err := client.Do(req)
			if tt.wantCode == 0 {
				if err == nil {
					_ = resp.Body.Close()
					t.Fatalf("request succeeded with status %d, want TLS handshake failure", resp.StatusCode)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.wantCode {
				t.Fatalf("code = %d, want %d", resp.StatusCode, tt.wantCode)
			}
		})
	}
}

func TestTLSConfigErrors(t *testing.T) {
	pki := newTestPKI(t)
	notPEM := filepath.Join(t.TempDir(), "ca.txt")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		cfg  config.AdminTLS
	}{
		{name: "missing cert", cfg: config.AdminTLS{CertFile: "/nonexistent.pem", KeyFile: pki.serverKey}},
		{name: "missing client ca", cfg: config.AdminTLS{CertFile: pki.serverCert, KeyFile: pki.serverKey, ClientCAFile: "/nonexistent.pem"}},
		{name: "client ca without certificates", cfg: config.AdminTLS{CertFile: pki.serverCert, KeyFile: pki.serverKey, ClientCAFile: notPEM}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := TLSConfig(tt.cfg); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
