package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
)

const (
	// MinTokenLength rejects admin tokens that are easy to guess.
	MinTokenLength = 16

	defaultFailedBodyMaxBytes = 512
	maxFailedBodyMaxBytes     = 64 << 10
)

// Admin configures the admin HTTP server.
type Admin struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	// Tokens are accepted as "Authorization: Bearer <token>" on /api/*.
	// Empty entries (e.g. an unset ${ADMIN_TOKEN:-}) are ignored; no tokens
	// means the API is unauthenticated.
	Tokens []string `yaml:"tokens"`
	TLS    AdminTLS `yaml:"tls"`
}

// AdminTLS enables HTTPS, and optionally client-certificate auth for /api/*.
type AdminTLS struct {
	CertFile     string `yaml:"cert_file"`
	KeyFile      string `yaml:"key_file"`
	ClientCAFile string `yaml:"client_ca_file"`
}

// Enabled reports whether the admin server serves HTTPS.
func (t AdminTLS) Enabled() bool { return t.CertFile != "" }

// RequireClientCert reports whether /api/* requires a verified client certificate.
func (t AdminTLS) RequireClientCert() bool { return t.ClientCAFile != "" }

// Addr returns admin listen address.
func (a Admin) Addr() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(a.Port))
}

func (a *Admin) normalize() error {
	if a.Host == "" {
		a.Host = "0.0.0.0"
	}
	if a.Port <= 0 {
		a.Port = 8888
	}

	tokens := make([]string, 0, len(a.Tokens))
	for i, t := range a.Tokens {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if len(t) < MinTokenLength {
			return fmt.Errorf("admin.tokens[%d]: must be at least %d characters", i, MinTokenLength)
		}
		tokens = append(tokens, t)
	}
	a.Tokens = tokens

	switch {
	case (a.TLS.CertFile == "") != (a.TLS.KeyFile == ""):
		return errors.New("admin.tls: cert_file and key_file must be set together")
	case a.TLS.ClientCAFile != "" && !a.TLS.Enabled():
		return errors.New("admin.tls: client_ca_file requires cert_file and key_file")
	}
	return nil
}

// Log configures logging.
type Log struct {
	// Level is one of debug, info, warn, error (default info).
	Level      string     `yaml:"level"`
	FailedBody FailedBody `yaml:"failed_body"`
}

// FailedBody controls whether the body of a dead-lettered message is logged.
// Bodies may contain sensitive data, so this is off by default.
type FailedBody struct {
	Enabled bool `yaml:"enabled"`
	// MaxBytes truncates the logged body (default 512, max 65536).
	MaxBytes int `yaml:"max_bytes"`
	// SampleRate is the fraction of failed messages whose body is logged, in (0, 1] (default 1).
	SampleRate *float64 `yaml:"sample_rate"`
}

// Rate returns the effective sample rate.
func (f FailedBody) Rate() float64 {
	if f.SampleRate == nil {
		return 1
	}
	return *f.SampleRate
}

// SlogLevel returns the configured level.
func (l Log) SlogLevel() slog.Level {
	var lv slog.Level
	_ = lv.UnmarshalText([]byte(l.Level)) // validated in normalize
	return lv
}

func (l *Log) normalize() error {
	if l.Level == "" {
		l.Level = "info"
	}
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(l.Level)); err != nil {
		return fmt.Errorf("log.level %q: want debug, info, warn or error", l.Level)
	}

	fb := &l.FailedBody
	if fb.MaxBytes == 0 {
		fb.MaxBytes = defaultFailedBodyMaxBytes
	}
	if fb.MaxBytes < 0 || fb.MaxBytes > maxFailedBodyMaxBytes {
		return fmt.Errorf("log.failed_body.max_bytes must be between 1 and %d", maxFailedBodyMaxBytes)
	}
	if r := fb.Rate(); r <= 0 || r > 1 {
		return errors.New("log.failed_body.sample_rate must be in (0, 1]")
	}
	return nil
}
