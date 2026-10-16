package forward

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const (
	maxResponseBytes = 1 << 20

	// maxIdleConnsPerHost is high on purpose: many jobs usually post to the
	// same consumer host through this shared client, so concurrency per host
	// is the sum of their workers. Connections over the limit would be closed
	// after every request and re-dialed. Idle connections only exist up to the
	// concurrency actually reached and expire after idleConnTimeout, so a
	// generous limit costs nothing when unused.
	maxIdleConnsPerHost = 512
	// idleConnTimeout must stay below the consumer's keep-alive timeout
	// (nginx/openresty default 75s). Otherwise a POST may be sent on a
	// connection the server is closing; POST is not retried by net/http, so
	// it would fail.
	idleConnTimeout = 60 * time.Second
)

// Client posts queue messages to HTTP consumers (legacy form contract).
// Safe for concurrent use; create once and share.
type Client struct {
	http   *http.Client
	safety atomic.Int64 // time.Duration
}

// New returns a shared HTTP client. Per-request deadlines come from ctx;
// safetyTimeout is an upper bound on top of that.
func New(safetyTimeout time.Duration) *Client {
	// Start from the default transport to keep proxy support, dial/TLS
	// timeouts and HTTP/2, then tune pooling.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 0 // no global limit; see maxIdleConnsPerHost
	tr.MaxIdleConnsPerHost = maxIdleConnsPerHost
	tr.IdleConnTimeout = idleConnTimeout
	c := &Client{http: &http.Client{Transport: tr}}
	c.SetSafetyTimeout(safetyTimeout)
	return c
}

// SetSafetyTimeout changes the upper bound for each request (<= 0 means 2m).
// It is applied through the request context, so it is safe to call while
// requests are in flight (e.g. on config reload).
func (c *Client) SetSafetyTimeout(d time.Duration) {
	if d <= 0 {
		d = 2 * time.Minute
	}
	c.safety.Store(int64(d))
}

// Result is the trimmed response body (or error description).
type Result struct {
	Body       string
	StatusCode int
	OK         bool
	Err        error
}

// Post sends application/x-www-form-urlencoded: queueName + bodyData.
// Success matches legacy Java: HTTP 200 and body equals "ok" (case-insensitive).
func (c *Client) Post(ctx context.Context, endpoint, hostHeader, queueName, body string) Result {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.safety.Load()))
	defer cancel()

	form := url.Values{}
	form.Set("queueName", queueName)
	form.Set("bodyData", body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Result{Body: err.Error(), Err: fmt.Errorf("new request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if hostHeader != "" {
		req.Host = hostHeader
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Result{Body: err.Error(), Err: fmt.Errorf("http post: %w", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return Result{StatusCode: resp.StatusCode, Body: err.Error(), Err: fmt.Errorf("read body: %w", err)}
	}
	text := strings.TrimSpace(string(raw))
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("http code: %d\nstatus: %s", resp.StatusCode, resp.Status)
		if text != "" {
			msg = msg + "\n" + text
		}
		return Result{StatusCode: resp.StatusCode, Body: msg}
	}
	return Result{
		StatusCode: resp.StatusCode,
		Body:       text,
		OK:         strings.EqualFold(text, "ok"),
	}
}
