package forward

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client posts queue messages to HTTP consumers (legacy form contract).
// Safe for concurrent use; create once and share.
type Client struct {
	http *http.Client
}

// New returns a shared HTTP client. Per-request deadlines come from ctx;
// client timeout is a safety upper bound.
func New(safetyTimeout time.Duration) *Client {
	if safetyTimeout <= 0 {
		safetyTimeout = 2 * time.Minute
	}
	return &Client{
		http: &http.Client{
			Timeout: safetyTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 25,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
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
		req.Header.Set("Host", hostHeader)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Result{Body: err.Error(), Err: fmt.Errorf("http post: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
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
