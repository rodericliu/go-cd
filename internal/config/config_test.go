package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func loadJobs(t *testing.T, content string) []ResolvedJob {
	t.Helper()
	_, jobs, err := Load(writeConfig(t, content))
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func TestExpandEnvAndLoad(t *testing.T) {
	t.Setenv("RABBIT_HOST", "mq.example")
	t.Setenv("RABBIT_PASS", "s3cret")

	cfg, jobs, err := Load(writeConfig(t, `
defaults:
  url: http://app.example/consumerDispatch/dispatch
  count: 2
  timeout_ms: 15000
admin:
  port: 9090
servers:
  a:
    host: ${RABBIT_HOST}
    port: 5672
    username: guest
    password: ${RABBIT_PASS}
    vhost: /v1
jobs:
  - name: mail
    server: a
    url: ${MISSING_URL:-http://fallback/dispatch}
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Admin.Port != 9090 {
		t.Fatalf("admin port=%d", cfg.Admin.Port)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs=%d", len(jobs))
	}
	j := jobs[0]
	if j.Server.Host != "mq.example" || j.Server.Password != "s3cret" {
		t.Fatalf("server not expanded: %+v", j.Server)
	}
	if j.URL != "http://fallback/dispatch" {
		t.Fatalf("url default=%q", j.URL)
	}
	if j.Count != 2 || j.Queue != "mail" || j.Exchange != "mail_router" {
		t.Fatalf("defaults not applied: %+v", j)
	}
	if j.DLQ != "mail.dlq" {
		t.Fatalf("dlq=%q", j.DLQ)
	}
	if j.RetryLimit != 1 {
		t.Fatalf("retry default=%d, want 1", j.RetryLimit)
	}
	if j.DLQTTL != 7*24*time.Hour {
		t.Fatalf("dlq ttl default=%v, want 7 days", j.DLQTTL)
	}
}

func TestDLQTTLDays(t *testing.T) {
	jobs := loadJobs(t, `
defaults: {url: "http://x/d", dlq_ttl_days: 3}
servers:
  a: {host: h, port: 5672, username: u, password: p, vhost: /}
jobs:
  - {name: inherit, server: a}
  - {name: override, server: a, dlq_ttl_days: 14}
  - {name: never, server: a, dlq_ttl_days: 0}
`)
	want := []time.Duration{3 * 24 * time.Hour, 14 * 24 * time.Hour, 0}
	for i, w := range want {
		if jobs[i].DLQTTL != w {
			t.Errorf("%s: dlq ttl=%v, want %v", jobs[i].Name, jobs[i].DLQTTL, w)
		}
	}
}

// Environment values must be used verbatim, never re-parsed as YAML.
func TestExpandEnvValuesAreLiteral(t *testing.T) {
	tests := []struct {
		name     string
		password string // YAML source for the password field
		env      string // value of PW
		want     string
	}{
		{name: "bare dollar kept", password: "pa$word", want: "pa$word"},
		{name: "dollar brace without name kept", password: "'a${}b'", want: "a${}b"},
		{name: "hash in env value", password: "${PW}", env: "abc #tail", want: "abc #tail"},
		{name: "colon in env value", password: "${PW}", env: "a: b", want: "a: b"},
		{name: "numeric env value into string", password: "${PW}", env: "12345", want: "12345"},
		{name: "bool-like env value", password: "${PW}", env: "true", want: "true"},
		{name: "null-like env value", password: "${PW}", env: "null", want: "null"},
		{name: "tilde env value", password: `"${PW}"`, env: "~", want: "~"},
		{name: "quoted default", password: `"${MISSING_PW:-x y}"`, want: "x y"},
		{name: "embedded", password: "pre-${PW}-post", env: "mid", want: "pre-mid-post"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PW", tt.env)
			jobs := loadJobs(t, `
defaults: {url: "http://x/d"}
servers:
  a:
    host: h
    port: 5672
    username: u
    vhost: /
    password: `+tt.password+`
jobs:
  - {name: j, server: a}
`)
			if got := jobs[0].Server.Password; got != tt.want {
				t.Fatalf("password = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExpandEnvTypedScalar(t *testing.T) {
	t.Setenv("RABBIT_PORT", "5673")
	jobs := loadJobs(t, `
defaults: {url: "http://x/d"}
servers:
  a: {host: h, port: "${RABBIT_PORT}", username: u, password: p, vhost: /}
  b:
    host: h
    port: ${MISSING_PORT:-5674}
    username: u
    password: p
    vhost: /
jobs:
  - {name: j, server: [a, b]}
`)
	if jobs[0].Server.Port != 5673 || jobs[1].Server.Port != 5674 {
		t.Fatalf("ports = %d, %d", jobs[0].Server.Port, jobs[1].Server.Port)
	}
}

func TestExplicitZeroValues(t *testing.T) {
	jobs := loadJobs(t, `
defaults: {url: "http://x/d", retry_limit: 3, count: 2}
servers:
  a: {host: h, port: 5672, username: u, password: p, vhost: /}
jobs:
  - {name: zero, server: a, retry_limit: 0, count: 0}
  - {name: inherit, server: a}
`)
	if jobs[0].RetryLimit != 0 || jobs[0].Count != 0 {
		t.Fatalf("explicit zero lost: retry=%d count=%d", jobs[0].RetryLimit, jobs[0].Count)
	}
	if jobs[1].RetryLimit != 3 || jobs[1].Count != 2 {
		t.Fatalf("defaults not inherited: retry=%d count=%d", jobs[1].RetryLimit, jobs[1].Count)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		job     string
		wantErr string
	}{
		{name: "relative url", job: `{name: j, server: a, url: "/dispatch"}`, wantErr: "url"},
		{name: "bad scheme", job: `{name: j, server: a, url: "ftp://x/d"}`, wantErr: "scheme"},
		{name: "bad exchange type", job: `{name: j, server: a, type: direkt}`, wantErr: "type"},
		{name: "bad regexp", job: `{name: j, server: a, block_patterns: "("}`, wantErr: "block_patterns"},
		{name: "negative retry", job: `{name: j, server: a, retry_limit: -1}`, wantErr: "retry_limit"},
		{name: "negative count", job: `{name: j, server: a, count: -1}`, wantErr: "count"},
		{name: "negative dlq ttl", job: `{name: j, server: a, dlq_ttl_days: -1}`, wantErr: "dlq_ttl_days"},
		{name: "unknown server", job: `{name: j, server: zz}`, wantErr: "unknown server"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Load(writeConfig(t, `
defaults: {url: "http://x/d"}
servers:
  a: {host: h, port: 5672, username: u, password: p, vhost: /}
jobs:
  - `+tt.job+"\n"))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestServerListExpandsJobs(t *testing.T) {
	jobs := loadJobs(t, `
defaults:
  url: http://app.example/dispatch
servers:
  a:
    host: mq1
    port: 5672
    username: guest
    password: guest
    vhost: /
  b:
    host: mq2
    port: 5672
    username: guest
    password: guest
    vhost: /
jobs:
  - name: mail
    server: [a, b]
    count: 2
`)
	if len(jobs) != 2 {
		t.Fatalf("jobs=%d want 2", len(jobs))
	}
	if jobs[0].LogicName != "mail-a" || jobs[1].LogicName != "mail-b" {
		t.Fatalf("logic names: %q %q", jobs[0].LogicName, jobs[1].LogicName)
	}
	if jobs[0].Server.Host != "mq1" || jobs[1].Server.Host != "mq2" {
		t.Fatalf("hosts: %q %q", jobs[0].Server.Host, jobs[1].Server.Host)
	}
	if jobs[0].Count != 2 || jobs[1].Count != 2 || jobs[0].Queue != "mail" {
		t.Fatalf("shared fields not copied: %+v %+v", jobs[0], jobs[1])
	}
}

func TestExampleConfigLoads(t *testing.T) {
	_, jobs, err := Load(filepath.Join("..", "..", "configs", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) == 0 {
		t.Fatal("no jobs in example config")
	}
}

const minimalServersAndJobs = `
servers:
  a: {host: h, port: 5672, username: u, password: p, vhost: /}
jobs:
  - {name: j, server: a, url: "http://x/d"}
`

func TestAdminAndLogDefaults(t *testing.T) {
	cfg, _, err := Load(writeConfig(t, minimalServersAndJobs))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Admin.Tokens) != 0 || cfg.Admin.TLS.Enabled() || cfg.Admin.TLS.RequireClientCert() {
		t.Fatalf("admin auth should be off by default: %+v", cfg.Admin)
	}
	fb := cfg.Log.FailedBody
	if cfg.Log.Level != "info" || fb.Enabled || fb.MaxBytes != 512 || fb.Rate() != 1 {
		t.Fatalf("log defaults: %+v", cfg.Log)
	}
	f := cfg.Log.File
	if f.Enabled() || cfg.Log.Stdout || f.MaxSizeMB != 100 || *f.MaxBackups != 10 || *f.MaxAgeDays != 30 || !*f.Compress {
		t.Fatalf("log file defaults: %+v", f)
	}
}

func TestLogFileSettings(t *testing.T) {
	t.Setenv("LOG_FILE", "/var/log/cd/cd.log")
	cfg, _, err := Load(writeConfig(t, `
log:
  stdout: true
  file: {path: "${LOG_FILE}", max_size_mb: 50, max_backups: 0, max_age_days: 0, compress: false}
`+minimalServersAndJobs))
	if err != nil {
		t.Fatal(err)
	}
	f := cfg.Log.File
	if f.Path != "/var/log/cd/cd.log" || !cfg.Log.Stdout || f.MaxSizeMB != 50 || *f.MaxBackups != 0 || *f.MaxAgeDays != 0 || *f.Compress {
		t.Fatalf("log file: %+v stdout=%v (explicit zeros must be kept)", f, cfg.Log.Stdout)
	}
}

func TestAdminTokensFromEnv(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "  0123456789abcdef-secret  ")
	cfg, _, err := Load(writeConfig(t, `
admin:
  tokens: ["${ADMIN_TOKEN}", "${UNSET_TOKEN:-}"]
log:
  level: DEBUG
  failed_body: {enabled: true, max_bytes: 100, sample_rate: 0.25}
`+minimalServersAndJobs))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Admin.Tokens) != 1 || cfg.Admin.Tokens[0] != "0123456789abcdef-secret" {
		t.Fatalf("tokens = %q (unset entries dropped, whitespace trimmed)", cfg.Admin.Tokens)
	}
	if cfg.Log.SlogLevel().String() != "DEBUG" || cfg.Log.FailedBody.Rate() != 0.25 || cfg.Log.FailedBody.MaxBytes != 100 {
		t.Fatalf("log = %+v", cfg.Log)
	}
}

func TestAdminAndLogValidation(t *testing.T) {
	tests := []struct {
		name    string
		section string
		wantErr string
	}{
		{name: "short token", section: "admin: {tokens: [short]}", wantErr: "at least 16"},
		{name: "cert without key", section: "admin: {tls: {cert_file: c.pem}}", wantErr: "together"},
		{name: "client ca without cert", section: "admin: {tls: {client_ca_file: ca.pem}}", wantErr: "client_ca_file requires"},
		{name: "bad log level", section: "log: {level: verbose}", wantErr: "log.level"},
		{name: "max_bytes too large", section: "log: {failed_body: {max_bytes: 1000000}}", wantErr: "max_bytes"},
		{name: "sample_rate zero", section: "log: {failed_body: {sample_rate: 0}}", wantErr: "sample_rate"},
		{name: "sample_rate above one", section: "log: {failed_body: {sample_rate: 1.5}}", wantErr: "sample_rate"},
		{name: "negative max_size_mb", section: "log: {file: {path: a.log, max_size_mb: -1}}", wantErr: "max_size_mb"},
		{name: "negative max_backups", section: "log: {file: {path: a.log, max_backups: -1}}", wantErr: "max_backups"},
		{name: "negative max_age_days", section: "log: {file: {path: a.log, max_age_days: -1}}", wantErr: "max_age_days"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Load(writeConfig(t, tt.section+"\n"+minimalServersAndJobs))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestAdminAddr(t *testing.T) {
	if got := (Admin{Host: "::1", Port: 8888}).Addr(); got != "[::1]:8888" {
		t.Fatalf("addr = %q", got)
	}
}
