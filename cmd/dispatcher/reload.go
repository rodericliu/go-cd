package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"consumer-dispatcher/internal/admin"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/manager"
)

// jobReloader is the part of *manager.Manager the reloader needs.
type jobReloader interface {
	Reload(ctx context.Context, jobs []config.ResolvedJob, opts manager.Options, dryRun bool) (manager.JobChanges, error)
}

// reloader re-reads the config file on POST /api/reload or SIGHUP.
// Jobs, admin.tokens, log.level and log.failed_body are applied; listener
// and log output settings are reported as requiring a restart.
type reloader struct {
	path   string
	log    *slog.Logger
	level  *slog.LevelVar
	jobs   jobReloader
	tokens func([]string)

	mu  sync.Mutex
	cur *config.File // settings currently in effect
}

func (r *reloader) reload(ctx context.Context, dryRun bool) (admin.ReloadResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	next, jobs, err := config.Load(r.path)
	if err != nil {
		r.log.Error("reload rejected, keeping current config", "err", err, "dry_run", dryRun)
		return admin.ReloadResult{}, fmt.Errorf("%w: %w", admin.ErrInvalidConfig, err)
	}

	changes, err := r.jobs.Reload(ctx, jobs, manager.Options{FailedBody: next.Log.FailedBody}, dryRun)
	if err != nil {
		return admin.ReloadResult{}, err
	}
	res := admin.ReloadResult{
		DryRun:          dryRun,
		Jobs:            changes,
		Settings:        changedSettings(r.cur, next),
		RestartRequired: restartRequired(r.cur, next),
	}
	if dryRun {
		return res, nil
	}

	if !slices.Equal(r.cur.Admin.Tokens, next.Admin.Tokens) {
		r.tokens(next.Admin.Tokens)
		if len(next.Admin.Tokens) == 0 && !next.Admin.TLS.RequireClientCert() {
			r.log.Warn("admin API has no authentication after reload")
		}
	}
	r.level.Set(next.Log.SlogLevel())

	// Settings that need a restart keep their running values, so they are
	// reported again on the next reload until the process restarts.
	applied := *next
	applied.Admin.Host, applied.Admin.Port, applied.Admin.TLS = r.cur.Admin.Host, r.cur.Admin.Port, r.cur.Admin.TLS
	applied.Log.File, applied.Log.Stdout = r.cur.Log.File, r.cur.Log.Stdout
	r.cur = &applied

	r.log.Info("config reloaded",
		"added", changes.Added, "removed", changes.Removed, "updated", changes.Updated,
		"scaled", changes.Scaled, "recreated", changes.Recreated, "unchanged", changes.Unchanged,
		"settings", res.Settings, "restart_required", res.RestartRequired)
	return res, nil
}

func changedSettings(cur, next *config.File) []string {
	out := []string{}
	if !slices.Equal(cur.Admin.Tokens, next.Admin.Tokens) {
		out = append(out, "admin.tokens")
	}
	if cur.Log.SlogLevel() != next.Log.SlogLevel() {
		out = append(out, "log.level")
	}
	if !sameFailedBody(cur.Log.FailedBody, next.Log.FailedBody) {
		out = append(out, "log.failed_body")
	}
	return out
}

func restartRequired(cur, next *config.File) []string {
	out := []string{}
	if cur.Admin.Host != next.Admin.Host || cur.Admin.Port != next.Admin.Port {
		out = append(out, "admin.host/port")
	}
	if cur.Admin.TLS != next.Admin.TLS {
		out = append(out, "admin.tls")
	}
	if !sameLogFile(cur.Log.File, next.Log.File) {
		out = append(out, "log.file")
	}
	if cur.Log.Stdout != next.Log.Stdout {
		out = append(out, "log.stdout")
	}
	return out
}

func sameFailedBody(a, b config.FailedBody) bool {
	return a.Enabled == b.Enabled && a.MaxBytes == b.MaxBytes && a.Rate() == b.Rate()
}

// sameLogFile compares normalized settings (pointers are always set by config.Load).
func sameLogFile(a, b config.LogFile) bool {
	return a.Path == b.Path && a.MaxSizeMB == b.MaxSizeMB && a.LocalTime == b.LocalTime &&
		*a.MaxBackups == *b.MaxBackups && *a.MaxAgeDays == *b.MaxAgeDays && *a.Compress == *b.Compress
}
