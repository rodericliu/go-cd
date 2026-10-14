package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/manager"
)

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	srv := config.Server{Host: "127.0.0.1", Port: 1, Username: "u", VHost: "/", ReconnectSeconds: 30}
	job := func(name, server string) config.ResolvedJob {
		return config.ResolvedJob{
			Name: name, LogicName: name + "-" + server, ServerName: server, Server: srv,
			Queue: name, DLQ: name + ".dlq", URL: "http://consumer/d", Timeout: time.Second,
		}
	}
	log := slog.New(slog.DiscardHandler)
	mgr := manager.New([]config.ResolvedJob{job("mail", "a"), job("mail", "b"), job("pay", "a")}, log)
	t.Cleanup(func() { mgr.StopAll(t.Context()) })
	return New(mgr, log).Handler()
}

func TestRoutes(t *testing.T) {
	h := newTestHandler(t)
	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantCode int
		wantBody string
	}{
		{name: "healthz", method: http.MethodGet, path: "/healthz", wantCode: http.StatusOK},
		{name: "readyz before start", method: http.MethodGet, path: "/readyz", wantCode: http.StatusServiceUnavailable},
		{name: "list", method: http.MethodGet, path: "/api/jobs", wantCode: http.StatusOK, wantBody: `"mail-a"`},
		{name: "get by logic name", method: http.MethodGet, path: "/api/jobs/mail-b", wantCode: http.StatusOK, wantBody: `"mail-b"`},
		{name: "get by unique name", method: http.MethodGet, path: "/api/jobs/pay", wantCode: http.StatusOK, wantBody: `"pay-a"`},
		{name: "get ambiguous name", method: http.MethodGet, path: "/api/jobs/mail", wantCode: http.StatusConflict, wantBody: "mail-a, mail-b"},
		{name: "get unknown", method: http.MethodGet, path: "/api/jobs/nope", wantCode: http.StatusNotFound},
		{name: "workers ambiguous", method: http.MethodPost, path: "/api/jobs/mail/workers", body: `{"count":1}`, wantCode: http.StatusConflict},
		{name: "workers missing count", method: http.MethodPost, path: "/api/jobs/pay/workers", body: `{}`, wantCode: http.StatusBadRequest},
		{name: "workers out of range", method: http.MethodPost, path: "/api/jobs/pay/workers", body: `{"count":100000}`, wantCode: http.StatusBadRequest},
		{name: "workers bad json", method: http.MethodPost, path: "/api/jobs/pay/workers", body: `{`, wantCode: http.StatusBadRequest},
		{name: "workers zero", method: http.MethodPost, path: "/api/jobs/pay/workers", body: `{"count":0}`, wantCode: http.StatusOK},
		{name: "url invalid", method: http.MethodPost, path: "/api/jobs/pay/url", body: `{"url":"not a url"}`, wantCode: http.StatusBadRequest},
		{name: "url empty", method: http.MethodPost, path: "/api/jobs/pay/url", body: `{}`, wantCode: http.StatusBadRequest},
		{name: "patterns invalid", method: http.MethodPost, path: "/api/jobs/pay/block-patterns", body: `{"patterns":"("}`, wantCode: http.StatusBadRequest},
		{name: "peek count not int", method: http.MethodGet, path: "/api/jobs/pay/peek?count=x", wantCode: http.StatusBadRequest},
		{name: "peek count too big", method: http.MethodGet, path: "/api/jobs/pay/peek?count=1000000", wantCode: http.StatusBadRequest},
		{name: "dlq count too big", method: http.MethodGet, path: "/api/jobs/pay/dlq?count=1000000", wantCode: http.StatusBadRequest},
		{name: "take via GET rejected", method: http.MethodGet, path: "/api/jobs/pay/take", wantCode: http.StatusMethodNotAllowed},
		{name: "replay bad json", method: http.MethodPost, path: "/api/jobs/pay/replay-dlq", body: `{`, wantCode: http.StatusBadRequest},
		{name: "try unknown job", method: http.MethodPost, path: "/api/jobs/nope/try", body: `{"body":"x"}`, wantCode: http.StatusNotFound},
		{name: "body too large", method: http.MethodPost, path: "/api/jobs/pay/try",
			body: `{"body":"` + strings.Repeat("a", maxBodyBytes) + `"}`, wantCode: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d; body: %s", rec.Code, tt.wantCode, rec.Body)
			}
			if tt.wantBody != "" && !strings.Contains(rec.Body.String(), tt.wantBody) {
				t.Fatalf("body %q does not contain %q", rec.Body, tt.wantBody)
			}
		})
	}
}

func TestSetURLThenGet(t *testing.T) {
	h := newTestHandler(t)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/jobs/mail-a/url", strings.NewReader(`{"url":"https://new/d"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("set url: %d %s", rec.Code, rec.Body)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/mail-a", nil))
	var got manager.JobStatus
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.URL != "https://new/d" {
		t.Fatalf("url = %q", got.URL)
	}
}

func TestRecovery(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	h := recovery(log, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d", rec.Code)
	}

	abort := recovery(log, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }))
	defer func() {
		if r := recover(); r != http.ErrAbortHandler { //nolint:errorlint // sentinel identity is the contract
			t.Fatalf("recovered %v, want http.ErrAbortHandler re-panicked", r)
		}
	}()
	abort.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
}
