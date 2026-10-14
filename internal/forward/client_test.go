package forward

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

type captured struct {
	method, contentType, host string
	form                      url.Values
}

func TestPost(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		respBody string
		host     string
		wantOK   bool
		wantBody string
	}{
		{name: "ok with whitespace", status: http.StatusOK, respBody: " ok \n", wantOK: true, wantBody: "ok"},
		{name: "ok case insensitive", status: http.StatusOK, respBody: "OK", wantOK: true, wantBody: "OK"},
		{name: "host header override", status: http.StatusOK, respBody: "ok", host: "virtual.example", wantOK: true, wantBody: "ok"},
		{name: "200 but not ok", status: http.StatusOK, respBody: "error", wantOK: false, wantBody: "error"},
		{name: "500", status: http.StatusInternalServerError, respBody: "boom", wantOK: false,
			wantBody: "http code: 500\nstatus: 500 Internal Server Error\nboom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Assertions run on the test goroutine; the handler only records.
			got := make(chan captured, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				form, _ := url.ParseQuery(string(raw))
				got <- captured{method: r.Method, contentType: r.Header.Get("Content-Type"), host: r.Host, form: form}
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.respBody)
			}))
			t.Cleanup(srv.Close)

			res := New(5*time.Second).Post(t.Context(), srv.URL, tt.host, "mail", "hello & bye")

			if res.OK != tt.wantOK || res.Body != tt.wantBody || res.StatusCode != tt.status {
				t.Fatalf("res = %+v, want ok=%v body=%q status=%d", res, tt.wantOK, tt.wantBody, tt.status)
			}
			req := <-got
			if req.method != http.MethodPost {
				t.Errorf("method = %s", req.method)
			}
			if req.contentType != "application/x-www-form-urlencoded" {
				t.Errorf("content-type = %s", req.contentType)
			}
			if req.form.Get("queueName") != "mail" || req.form.Get("bodyData") != "hello & bye" {
				t.Errorf("form = %v", req.form)
			}
			if tt.host != "" && req.host != tt.host {
				t.Errorf("host = %q, want %q", req.host, tt.host)
			}
		})
	}
}

func TestPostTransportError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	endpoint := srv.URL
	srv.Close() // nothing listens any more

	res := New(time.Second).Post(t.Context(), endpoint, "", "q", "b")
	if res.OK || res.Err == nil {
		t.Fatalf("res = %+v, want transport error", res)
	}
}
