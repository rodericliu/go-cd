package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"consumer-dispatcher/internal/admin"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/manager"
)

type fakeJobs struct {
	calls  int
	dryRun bool
	jobs   []config.ResolvedJob
	opts   manager.Options
}

func (f *fakeJobs) Reload(_ context.Context, jobs []config.ResolvedJob, opts manager.Options, dryRun bool) (manager.JobChanges, error) {
	f.calls++
	f.jobs, f.opts, f.dryRun = jobs, opts, dryRun
	return manager.JobChanges{Unchanged: len(jobs)}, nil
}

const baseConfig = `
servers:
  a: {host: h, port: 5672, username: u, password: p, vhost: /}
jobs:
  - {name: j, server: a, url: "http://x/d"}
`

const (
	tokenOld = "old-token-0123456789"
	tokenNew = "new-token-0123456789"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func newTestReloader(t *testing.T) (*reloader, *fakeJobs, *[]string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, path, "admin: {port: 8888, tokens: ["+tokenOld+"]}\n"+baseConfig)
	cur, _, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	level := new(slog.LevelVar)
	level.Set(cur.Log.SlogLevel())
	jobs := &fakeJobs{}
	tokens := slices.Clone(cur.Admin.Tokens)
	rl := &reloader{
		path: path, log: slog.New(slog.DiscardHandler), level: level, jobs: jobs,
		tokens: func(t []string) { tokens = t }, cur: cur,
	}
	return rl, jobs, &tokens, path
}

func TestReloaderAppliesSettings(t *testing.T) {
	rl, jobs, tokens, path := newTestReloader(t)
	writeFile(t, path, `
admin: {port: 9999, tokens: [`+tokenNew+`]}
log:
  level: debug
  file: {path: /tmp/cd.log}
  failed_body: {enabled: true}
`+baseConfig)

	t.Run("dry run reports without applying", func(t *testing.T) {
		res, err := rl.reload(t.Context(), true)
		if err != nil {
			t.Fatal(err)
		}
		if !jobs.dryRun || !res.DryRun {
			t.Fatalf("dry run not passed through: %+v", res)
		}
		if !slices.Equal(res.Settings, []string{"admin.tokens", "log.level", "log.failed_body"}) {
			t.Fatalf("settings = %v", res.Settings)
		}
		if (*tokens)[0] != tokenOld || rl.level.Level() != slog.LevelInfo {
			t.Fatalf("dry run applied settings: tokens=%v level=%v", *tokens, rl.level.Level())
		}
	})

	t.Run("apply", func(t *testing.T) {
		res, err := rl.reload(t.Context(), false)
		if err != nil {
			t.Fatal(err)
		}
		if jobs.dryRun || len(jobs.jobs) != 1 || !jobs.opts.FailedBody.Enabled {
			t.Fatalf("jobs reload: dry=%v jobs=%d opts=%+v", jobs.dryRun, len(jobs.jobs), jobs.opts)
		}
		if !slices.Equal(*tokens, []string{tokenNew}) || rl.level.Level() != slog.LevelDebug {
			t.Fatalf("tokens=%v level=%v", *tokens, rl.level.Level())
		}
		if !slices.Equal(res.RestartRequired, []string{"admin.host/port", "log.file"}) {
			t.Fatalf("restart_required = %v", res.RestartRequired)
		}
	})

	t.Run("restart-only settings are reported until restart", func(t *testing.T) {
		res, err := rl.reload(t.Context(), false)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Settings) != 0 || !slices.Equal(res.RestartRequired, []string{"admin.host/port", "log.file"}) {
			t.Fatalf("settings=%v restart_required=%v", res.Settings, res.RestartRequired)
		}
	})
}

func TestReloaderRejectsInvalidConfig(t *testing.T) {
	rl, jobs, tokens, path := newTestReloader(t)
	writeFile(t, path, "admin: {tokens: [short]}\n"+baseConfig)

	_, err := rl.reload(t.Context(), false)
	if !errors.Is(err, admin.ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
	if jobs.calls != 0 || (*tokens)[0] != tokenOld {
		t.Fatalf("invalid config was applied: calls=%d tokens=%v", jobs.calls, *tokens)
	}
}
