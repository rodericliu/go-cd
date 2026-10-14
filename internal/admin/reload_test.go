package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"testing"

	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/manager"
)

func newTestServer(t *testing.T, opts Options) *Server {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	mgr := manager.New([]config.ResolvedJob{}, manager.Options{}, log)
	t.Cleanup(func() { mgr.StopAll(t.Context()) })
	return New(mgr, opts, log)
}

func TestReloadEndpoint(t *testing.T) {
	var gotDryRun bool
	var reloadErr error
	srv := newTestServer(t, Options{Tokens: []string{tokenA}})
	srv.SetReloadFunc(func(_ context.Context, dryRun bool) (ReloadResult, error) {
		gotDryRun = dryRun
		if reloadErr != nil {
			return ReloadResult{}, reloadErr
		}
		return ReloadResult{DryRun: dryRun, Jobs: manager.JobChanges{Added: []string{"sms-a"}}}, nil
	})
	h := srv.Handler()

	tests := []struct {
		name       string
		path       string
		auth       string
		err        error
		wantCode   int
		wantDryRun bool
	}{
		{name: "requires auth", path: "/api/reload", wantCode: http.StatusUnauthorized},
		{name: "apply", path: "/api/reload", auth: "Bearer " + tokenA, wantCode: http.StatusOK},
		{name: "dry run", path: "/api/reload?dry_run=true", auth: "Bearer " + tokenA, wantCode: http.StatusOK, wantDryRun: true},
		{name: "bad dry_run", path: "/api/reload?dry_run=maybe", auth: "Bearer " + tokenA, wantCode: http.StatusBadRequest},
		{name: "invalid config", path: "/api/reload", auth: "Bearer " + tokenA,
			err: fmt.Errorf("%w: jobs[0]: url required", ErrInvalidConfig), wantCode: http.StatusBadRequest},
		{name: "not running", path: "/api/reload", auth: "Bearer " + tokenA,
			err: manager.ErrNotRunning, wantCode: http.StatusServiceUnavailable},
		{name: "other error", path: "/api/reload", auth: "Bearer " + tokenA,
			err: errors.New("boom"), wantCode: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDryRun, reloadErr = false, tt.err
			rec := serve(t, h, http.MethodPost, tt.path, tt.auth)
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d; body: %s", rec.Code, tt.wantCode, rec.Body)
			}
			if rec.Code != http.StatusOK {
				return
			}
			var res ReloadResult
			if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
				t.Fatal(err)
			}
			if gotDryRun != tt.wantDryRun || res.DryRun != tt.wantDryRun || len(res.Jobs.Added) != 1 {
				t.Fatalf("dry_run passed=%v, result=%+v", gotDryRun, res)
			}
		})
	}
}

func TestReloadNotConfigured(t *testing.T) {
	h := newTestServer(t, Options{}).Handler()
	if rec := serve(t, h, http.MethodPost, "/api/reload", ""); rec.Code != http.StatusNotImplemented {
		t.Fatalf("code = %d, want 501", rec.Code)
	}
}

func TestSetTokens(t *testing.T) {
	srv := newTestServer(t, Options{Tokens: []string{tokenA}})
	h := srv.Handler()
	srv.SetTokens([]string{tokenB})

	if rec := serve(t, h, http.MethodGet, "/api/jobs", "Bearer "+tokenA); rec.Code != http.StatusUnauthorized {
		t.Fatalf("old token: code = %d, want 401", rec.Code)
	}
	if rec := serve(t, h, http.MethodGet, "/api/jobs", "Bearer "+tokenB); rec.Code != http.StatusOK {
		t.Fatalf("new token: code = %d, want 200", rec.Code)
	}

	srv.SetTokens(nil)
	if srv.AuthEnabled() {
		t.Fatal("auth should be disabled without tokens")
	}
	if rec := serve(t, h, http.MethodGet, "/api/jobs", ""); rec.Code != http.StatusOK {
		t.Fatalf("no tokens: code = %d, want 200", rec.Code)
	}
}
