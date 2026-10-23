package admin

import (
	"context"
	"errors"
	"net/http"

	"consumer-dispatcher/internal/manager"
)

// ErrInvalidConfig marks reload errors caused by the config file; the API
// reports them as 400 and the running config stays in effect.
var ErrInvalidConfig = errors.New("invalid config")

// ReloadResult is the response of POST /api/reload.
type ReloadResult struct {
	DryRun bool               `json:"dry_run"`
	Jobs   manager.JobChanges `json:"jobs"`
	// Settings lists reloaded settings outside jobs, e.g. admin.tokens or log.level.
	Settings []string `json:"settings"`
	// RestartRequired lists changed settings that only take effect after a restart.
	RestartRequired []string `json:"restart_required"`
}

// ReloadFunc reloads the config file. With dryRun it only reports changes.
type ReloadFunc func(ctx context.Context, dryRun bool) (ReloadResult, error)

// SetTokens replaces the accepted bearer tokens (config reload).
func (s *Server) SetTokens(tokens []string) { s.auth.setTokens(tokens) }

func (s *Server) reload(w http.ResponseWriter, r *http.Request) {
	if s.reloadFn == nil {
		http.Error(w, "reload is not available", http.StatusNotImplemented)
		return
	}
	dryRun, ok := queryBool(w, r, "dry_run")
	if !ok {
		return
	}
	res, err := s.reloadFn(r.Context(), dryRun)
	switch {
	case errors.Is(err, ErrInvalidConfig):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, manager.ErrNotRunning):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusOK, res)
	}
}
