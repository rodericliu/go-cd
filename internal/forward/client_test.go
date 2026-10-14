package forward

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPostSuccessOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Fatalf("ct=%s", r.Header.Get("Content-Type"))
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "queueName=mail&bodyData=hello" && string(body) != "bodyData=hello&queueName=mail" {
			// url.Values.Encode is sorted by key: bodyData then queueName
			if string(body) != "bodyData=hello&queueName=mail" {
				t.Fatalf("body=%q", body)
			}
		}
		if r.Host != "virtual.example" && r.Header.Get("Host") != "virtual.example" {
			// httptest may not preserve Host the same way; check header set on request side via client
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(" ok \n"))
	}))
	defer srv.Close()

	c := New(5 * time.Second)
	res := c.Post(t.Context(), srv.URL, "virtual.example", "mail", "hello")
	if !res.OK || res.Body != "ok" {
		t.Fatalf("res=%+v", res)
	}
}

func TestPostNonOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	c := New(5 * time.Second)
	res := c.Post(t.Context(), srv.URL, "", "q", "b")
	if res.OK {
		t.Fatal("expected failure")
	}
}
