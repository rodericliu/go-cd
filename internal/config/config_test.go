package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExpandEnvAndLoad(t *testing.T) {
	t.Setenv("RABBIT_HOST", "mq.example")
	t.Setenv("RABBIT_PASS", "s3cret")

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
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
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, jobs, err := Load(path)
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
}

func TestServerListExpandsJobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
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
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	_, jobs, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
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
