package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"consumer-dispatcher/internal/config"
)

// protectedPrefix is the path prefix that requires authentication. Probes and
// /metrics stay open so Kubernetes and Prometheus need no credentials.
const protectedPrefix = "/api/"

// Options configure admin API authentication.
type Options struct {
	// Tokens accepted as "Authorization: Bearer <token>". Empty disables token auth.
	Tokens []string
	// RequireClientCert requires a verified TLS client certificate on /api/*.
	// The TLS listener must use tls.VerifyClientCertIfGiven (see TLSConfig).
	RequireClientCert bool
}

type authenticator struct {
	// digests can be swapped on config reload.
	digests    atomic.Pointer[[][sha256.Size]byte]
	clientCert bool
	log        *slog.Logger
}

func newAuthenticator(opts Options, log *slog.Logger) *authenticator {
	a := &authenticator{clientCert: opts.RequireClientCert, log: log}
	a.setTokens(opts.Tokens)
	return a
}

func (a *authenticator) setTokens(tokens []string) {
	digests := make([][sha256.Size]byte, 0, len(tokens))
	for _, t := range tokens {
		digests = append(digests, sha256.Sum256([]byte(t)))
	}
	a.digests.Store(&digests)
}

func (a *authenticator) tokens() [][sha256.Size]byte { return *a.digests.Load() }

// enabled reports whether any authentication is configured.
func (a *authenticator) enabled() bool { return len(a.tokens()) > 0 || a.clientCert }

// authenticate returns a non-secret identity for audit logs, or an error
// message suitable for the client.
func (a *authenticator) authenticate(r *http.Request) (string, error) {
	var ids []string
	if a.clientCert {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			return "", errors.New("client certificate required")
		}
		ids = append(ids, "cert:"+r.TLS.VerifiedChains[0][0].Subject.CommonName)
	}
	if digests := a.tokens(); len(digests) > 0 {
		idx, ok := matchToken(digests, r.Header.Get("Authorization"))
		if !ok {
			return "", errors.New("missing or invalid bearer token")
		}
		ids = append(ids, "token#"+strconv.Itoa(idx+1))
	}
	if len(ids) == 0 {
		return "anonymous", nil
	}
	return strings.Join(ids, ","), nil
}

// matchToken compares SHA-256 digests in constant time, so neither the token
// content nor its length leaks through timing.
func matchToken(digests [][sha256.Size]byte, header string) (int, bool) {
	const scheme = "bearer "
	if len(header) <= len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return 0, false
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(header[len(scheme):])))
	match := -1
	for i, d := range digests {
		if subtle.ConstantTimeCompare(sum[:], d[:]) == 1 && match < 0 {
			match = i
		}
	}
	return match, match >= 0
}

// protect authenticates /api/* and writes an audit log line for each call.
func (a *authenticator) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, protectedPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		identity, err := a.authenticate(r)
		if err != nil {
			a.log.Warn("admin auth failed",
				"method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr, "reason", err.Error())
			if len(a.tokens()) > 0 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="consumer-dispatcher"`)
			}
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		// Mutating calls are always audited; reads only at debug level.
		level := slog.LevelInfo
		if r.Method == http.MethodGet {
			level = slog.LevelDebug
		}
		a.log.Log(r.Context(), level, "admin api",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"identity", identity, "remote", r.RemoteAddr, "duration_ms", time.Since(start).Milliseconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// TLSConfig builds the admin server TLS config. With a client CA, client
// certificates are verified when presented; whether one is required is
// decided per path by the auth middleware, so probes and /metrics keep
// working without one.
func TLSConfig(c config.AdminTLS) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load admin certificate: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if c.ClientCAFile == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("client ca %s: no PEM certificates found", c.ClientCAFile)
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.VerifyClientCertIfGiven
	return cfg, nil
}
