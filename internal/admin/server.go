package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"consumer-dispatcher/internal/manager"
)

// Server exposes REST admin + Prometheus metrics.
type Server struct {
	mgr *manager.Manager
	log *slog.Logger
	mux *http.ServeMux
}

func New(mgr *manager.Manager, log *slog.Logger) *Server {
	s := &Server{mgr: mgr, log: log, mux: http.NewServeMux()}
	s.routes()
	return s
}

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
	s.mux.HandleFunc("GET /api/jobs/{id}/take", s.take)
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
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	for name, srv := range s.mgr.Servers() {
		if err := s.mgr.Hub().Ping(name, srv); err != nil {
			s.log.Warn("readiness ping failed", "server", name, "err", err)
			http.Error(w, "amqp: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		select {
		case <-ctx.Done():
			http.Error(w, "readiness timeout", http.StatusServiceUnavailable)
			return
		default:
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) listJobs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.List())
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	for _, j := range s.mgr.List() {
		if j.LogicName == id || j.Name == id {
			writeJSON(w, http.StatusOK, j)
			return
		}
	}
	http.Error(w, manager.ErrJobNotFound.Error(), http.StatusNotFound)
}

func (s *Server) setWorkers(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.mgr.SetWorkers(id, body.Count); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"workers": body.Count})
}

func (s *Server) setURL(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.URL == "" {
		http.Error(w, "url required", http.StatusBadRequest)
		return
	}
	if err := s.mgr.SetURL(id, body.URL); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": body.URL})
}

func (s *Server) setPatterns(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Patterns string `json:"patterns"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.mgr.SetBlockPatterns(id, body.Patterns); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"patterns": body.Patterns})
}

func (s *Server) setDiscard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Discard bool `json:"discard"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.mgr.SetDiscardMatchJobs(id, body.Discard); err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"discard": body.Discard})
}

func (s *Server) tryJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.mgr.Try(r.Context(), id, body.Body)
	if err != nil {
		if errors.Is(err, manager.ErrJobNotFound) {
			writeMgrErr(w, err)
			return
		}
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
	n, _ := strconv.Atoi(r.URL.Query().Get("count"))
	msgs, err := s.mgr.Peek(r.PathValue("id"), n, ack)
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs, "count": len(msgs)})
}

func (s *Server) peekDLQ(w http.ResponseWriter, r *http.Request) {
	n, _ := strconv.Atoi(r.URL.Query().Get("count"))
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
	_ = json.NewDecoder(r.Body).Decode(&body)
	n, err := s.mgr.ReplayDLQ(r.Context(), r.PathValue("id"), body.Count)
	if err != nil {
		writeMgrErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"replayed": n})
}

func writeMgrErr(w http.ResponseWriter, err error) {
	if errors.Is(err, manager.ErrJobNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func recovery(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic recovered", "panic", rec, "stack", string(debug.Stack()))
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
