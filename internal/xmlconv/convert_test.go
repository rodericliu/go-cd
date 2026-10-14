package xmlconv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"consumer-dispatcher/internal/config"
)

func TestConvertBasicAndMerge(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<jobs xmlns="tnc">
  <conf>
    <request-pre>http://app/dispatch</request-pre>
    <request-count>2</request-count>
    <request-timeout>15000</request-timeout>
  </conf>
  <monitor httpHost="127.0.0.1" httpPort="8888" rHost="127.0.0.1" rPort="9999"/>
  <servers>
    <queueserver name="a" host="mq1" port="5672" userName="guest" password="p1" vhost="/"/>
    <queueserver name="b" host="mq2" port="5672" userName="guest" password="p2" vhost="/v2" redisHost="r1"/>
  </servers>
  <job name="mail" server="a" timeout="20000"/>
  <job name="mail" server="b" timeout="20000"/>
  <job name="pay" server="a" url="http://other/dispatch"/>
</jobs>`

	out, err := Convert([]byte(xml), Options{MergeServers: true})
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	if !strings.Contains(text, "DLQ") {
		t.Fatalf("expected redis/DLQ note in header, got:\n%s", text)
	}

	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
	_, jobs, err := config.Load(path)
	if err != nil {
		t.Fatalf("load converted yaml: %v\n%s", err, text)
	}
	// mail merges to 2 servers => 2 resolved jobs; pay => 1; total 3
	if len(jobs) != 3 {
		t.Fatalf("resolved jobs=%d want 3\n%s", len(jobs), text)
	}

	var raw yamlFile
	if err := yaml.Unmarshal(out, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.Defaults.URL != "http://app/dispatch" || raw.Defaults.Count != 2 {
		t.Fatalf("defaults: %+v", raw.Defaults)
	}
	if len(raw.Jobs) != 2 {
		t.Fatalf("yaml jobs=%d want 2 (mail merged + pay)\n%s", len(raw.Jobs), text)
	}
	var mailServers any
	for _, j := range raw.Jobs {
		if j.Name == "mail" {
			mailServers = j.Server
		}
	}
	switch v := mailServers.(type) {
	case []any:
		if len(v) != 2 {
			t.Fatalf("mail server list len=%d\n%s", len(v), text)
		}
	case []string:
		if len(v) != 2 {
			t.Fatalf("mail server list len=%d\n%s", len(v), text)
		}
	default:
		t.Fatalf("mail server want list, got %#v\n%s", mailServers, text)
	}
}

func TestConvertAdminAndLogSettings(t *testing.T) {
	xml := `
<jobs>
  <conf><request-pre>http://x</request-pre></conf>
  <monitor httpHost="0.0.0.0" httpPort="9"/>
  <servers>
    <queueserver name="a" host="h" port="5672" userName="u" password="p" vhost="/"/>
  </servers>
  <job name="only" server="a"/>
</jobs>`
	out, err := Convert([]byte(xml), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "ADMIN_TOKEN") {
		t.Fatalf("admin token note/placeholder missing:\n%s", out)
	}
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("env unset", func(t *testing.T) {
		t.Setenv("ADMIN_TOKEN", "")
		t.Setenv("LOG_LEVEL", "")
		t.Setenv("LOG_FILE", "")
		cfg, _, err := config.Load(path)
		if err != nil {
			t.Fatalf("load: %v\n%s", err, out)
		}
		fb := cfg.Log.FailedBody
		if len(cfg.Admin.Tokens) != 0 || cfg.Log.Level != "info" || fb.Enabled || fb.MaxBytes != 512 || fb.Rate() != 1 {
			t.Fatalf("admin=%+v log=%+v", cfg.Admin, cfg.Log)
		}
		if cfg.Log.File.Enabled() || cfg.Log.Stdout {
			t.Fatalf("log file should be off without LOG_FILE: %+v", cfg.Log)
		}
	})

	t.Run("env set", func(t *testing.T) {
		t.Setenv("ADMIN_TOKEN", "0123456789abcdef")
		t.Setenv("LOG_LEVEL", "debug")
		t.Setenv("LOG_FILE", "/var/log/cd.log")
		cfg, _, err := config.Load(path)
		if err != nil {
			t.Fatalf("load: %v\n%s", err, out)
		}
		if len(cfg.Admin.Tokens) != 1 || cfg.Admin.Tokens[0] != "0123456789abcdef" || cfg.Log.Level != "debug" ||
			cfg.Log.File.Path != "/var/log/cd.log" {
			t.Fatalf("admin=%+v log=%+v", cfg.Admin, cfg.Log)
		}
	})
}

func TestConvertSingleServerString(t *testing.T) {
	xml := `
<jobs>
  <conf><request-pre>http://x</request-pre></conf>
  <monitor httpHost="0.0.0.0" httpPort="9"/>
  <servers>
    <queueserver name="a" host="h" port="5672" userName="u" password="p" vhost="/"/>
  </servers>
  <job name="only" server="a"/>
</jobs>`
	out, err := Convert([]byte(xml), Options{MergeServers: true, EnvSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "SERVER_A_PASSWORD") {
		t.Fatalf("env secret missing:\n%s", out)
	}
	var raw yamlFile
	if err := yaml.Unmarshal(out, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw.Jobs[0].Server.(string); !ok {
		t.Fatalf("single server should be string, got %#v", raw.Jobs[0].Server)
	}
}
