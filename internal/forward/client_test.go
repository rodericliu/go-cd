package forward

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
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

// Many jobs post to the same host concurrently; after a burst, every
// connection must go back to the idle pool instead of being re-dialed.
func TestConnectionsReusedUnderConcurrency(t *testing.T) {
	const concurrency = 60 // above the previous per-host idle limit of 25

	var newConns atomic.Int64
	var mu sync.Mutex
	var arrived int
	release := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hold every request until all of the wave have arrived, so the
		// client really has `concurrency` connections open at once.
		mu.Lock()
		arrived++
		if arrived == concurrency {
			close(release)
		}
		wait := release
		mu.Unlock()
		<-wait
		_, _ = io.WriteString(w, "ok")
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	c := New(5 * time.Second)
	wave := func() {
		mu.Lock()
		arrived, release = 0, make(chan struct{})
		mu.Unlock()
		var wg sync.WaitGroup
		for range concurrency {
			wg.Go(func() {
				if res := c.Post(t.Context(), srv.URL, "", "q", "b"); !res.OK {
					t.Errorf("post: %+v", res)
				}
			})
		}
		wg.Wait()
	}

	wave()
	if n := newConns.Load(); n != concurrency {
		t.Fatalf("first wave opened %d connections, want %d", n, concurrency)
	}
	// The transport returns a connection to the pool on its read loop
	// goroutine, shortly after the body is read; let that settle.
	time.Sleep(100 * time.Millisecond)
	wave()
	if n := newConns.Load(); n != concurrency {
		t.Fatalf("second wave opened %d new connections, want 0 (all reused)", n-concurrency)
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
