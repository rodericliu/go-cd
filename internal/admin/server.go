package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"consumer-dispatcher/internal/manager"
)

const (
	maxBodyBytes     = 1 << 20
	readinessTimeout = 2 * time.Second
)

// Server exposes REST admin + Prometheus metrics.
type Server struct {
	mgr *manager.Manager
	log *slog.Logger
	mux *http.ServeMux
}

// New registers all admin routes for mgr.
func New(mgr *manager.Manager, log *slog.Logger) *Server {
	s := &Server{mgr: mgr, log: log, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the root handler with panic recovery.
func (s *Server) Handler() http.Handler {
	return recovery(s.log, s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.liveness)
	s.mux.HandleFunc("GET /readyz", s.readiness)
	s.mux.Handle("GET /metrics", promhttp.Handler())

	s.mux.HandleFunc("GET /api/jobs", s.listJobs)
	s.mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	s.mux.HandleFunc("POST /api/jobs/{id}/workers", s.setWorkers)
	s.mux.HandleFunc("POST /api/jobs/{id}/url", s.setURL)
	s.mux.HandleFunc("POST /api/jobs/{id}/block-patterns", s.setPatterns)
	s.mux.HandleFunc("POST /api/jobs/{id}/discard-match", s.setDiscard)
	s.mux.HandleFunc("POST /api/jobs/{id}/try", s.tryJob)
	s.mux.HandleFunc("POST /api/jobs/{id}/purge", s.purge)
	s.mux.HandleFunc("POST /api/jobs/{id}/purge-dlq", s.purgeDLQ)
	s.mux.HandleFunc("GET /api/jobs/{id}/peek", s.peek)
	// take removes messages, so it is not a safe (GET) method.
	s.mux.HandleFunc("POST /api/jobs/{id}/take", s.take)
	s.mux.HandleFunc("GET /api/jobs/{id}/dlq", s.peekDLQ)
	s.mux.HandleFunc("POST /api/jobs/{id}/replay-dlq", s.replayDLQ)
}

func (s *Server) liveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	if !s.mgr.Ready() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	if err := s.mgr.Ping(ctx); err != nil {
		s.log.Warn("readiness ping failed", "err", err)
		http.Error(w, "amqp: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) listJobs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.List())
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.mgr.Get(r.PathValue("id"))
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) setWorkers(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Count *int `json:"count"`
	}
	if !decodeJSON(w, r, &body, false) {
		return
	}
	if body.Count == nil {
		http.Error(w, "count required", http.StatusBadRequest)
		return
	}
	if err := s.mgr.SetWorkers(r.PathValue("id"), *body.Count); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"workers": *body.Count})
}

func (s *Server) setURL(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL string `json:"url"`
	}
	if !decodeJSON(w, r, &body, false) {
		return
	}
	if body.URL == "" {
		http.Error(w, "url required", http.StatusBadRequest)
		return
	}
	if err := s.mgr.SetURL(r.PathValue("id"), body.URL); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": body.URL})
}

func (s *Server) setPatterns(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Patterns string `json:"patterns"`
	}
	if !decodeJSON(w, r, &body, false) {
		return
	}
	if err := s.mgr.SetBlockPatterns(r.PathValue("id"), body.Patterns); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"patterns": body.Patterns})
}

func (s *Server) setDiscard(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Discard bool `json:"discard"`
	}
	if !decodeJSON(w, r, &body, false) {
		return
	}
	if err := s.mgr.SetDiscardMatchJobs(r.PathValue("id"), body.Discard); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"discard": body.Discard})
}

func (s *Server) tryJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Body string `json:"body"`
	}
	if !decodeJSON(w, r, &body, false) {
		return
	}
	result, err := s.mgr.Try(r.Context(), r.PathValue("id"), body.Body)
	if err != nil {
		if isClientErr(err) {
			writeMgrErr(w, err)
			return
		}
		// The consumer's failure is the result of the trial, not an admin error.
		writeJSON(w, http.StatusOK, map[string]any{"result": result, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"result": result})
}

func (s *Server) purge(w http.ResponseWriter, r *http.Request) {
	n, err := s.mgr.PurgeQueue(r.PathValue("id"))
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"purged": n})
}

func (s *Server) purgeDLQ(w http.ResponseWriter, r *http.Request) {
	n, err := s.mgr.PurgeDLQ(r.PathValue("id"))
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"purged": n})
}

func (s *Server) peek(w http.ResponseWriter, r *http.Request) {
	s.peekOrTake(w, r, false)
}

func (s *Server) take(w http.ResponseWriter, r *http.Request) {
	s.peekOrTake(w, r, true)
}

func (s *Server) peekOrTake(w http.ResponseWriter, r *http.Request, ack bool) {
	n, ok := queryCount(w, r)
	if !ok {
		return
	}
	msgs, err := s.mgr.Peek(r.PathValue("id"), n, ack)
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs, "count": len(msgs)})
}

func (s *Server) peekDLQ(w http.ResponseWriter, r *http.Request) {
	n, ok := queryCount(w, r)
	if !ok {
		return
	}
	msgs, err := s.mgr.PeekDLQ(r.PathValue("id"), n)
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs, "count": len(msgs)})
}

func (s *Server) replayDLQ(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Count int `json:"count"`
	}
	if !decodeJSON(w, r, &body, true) {
		return
	}
	n, err := s.mgr.ReplayDLQ(r.Context(), r.PathValue("id"), body.Count)
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"replayed": n})
}

// decodeJSON decodes a size-limited JSON body into v. It writes a 400 and
// returns false on failure. An empty body is accepted when allowEmpty is set.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any, allowEmpty bool) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	err := json.NewDecoder(r.Body).Decode(v)
	if err == nil || (allowEmpty && errors.Is(err, io.EOF)) {
		return true
	}
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return false
	}
	http.Error(w, fmt.Sprintf("invalid json body: %v", err), http.StatusBadRequest)
	return false
}

// queryCount parses the optional ?count= parameter (missing means 0 = default).
func queryCount(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("count")
	if raw == "" {
		return 0, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		http.Error(w, "count must be an integer", http.StatusBadRequest)
		return 0, false
	}
	return n, true
}

func isClientErr(err error) bool {
	return errors.Is(err, manager.ErrJobNotFound) ||
		errors.Is(err, manager.ErrAmbiguousJob) ||
		errors.Is(err, manager.ErrInvalidArgument)
}

func writeMgrErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, manager.ErrJobNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, manager.ErrAmbiguousJob):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, manager.ErrInvalidArgument):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func recovery(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			// ErrAbortHandler is net/http's sentinel for aborting a response.
			if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(rec)
			}
			log.Error("panic recovered", "panic", rec, "stack", string(debug.Stack()))
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}()
		next.ServeHTTP(w, r)
	})
}
