package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/config"
)

func TestTruncateUTF8(t *testing.T) {
	tests := []struct {
		in        string
		n         int
		want      string
		truncated bool
	}{
		{in: "hello", n: 10, want: "hello"},
		{in: "hello", n: 5, want: "hello"},
		{in: "hello", n: 3, want: "hel", truncated: true},
		{in: "héllo", n: 2, want: "h", truncated: true}, // 'é' is 2 bytes; never split it
		{in: "héllo", n: 3, want: "hé", truncated: true},
		{in: "日本", n: 4, want: "日", truncated: true},
		{in: "日本", n: 2, want: "", truncated: true},
	}
	for _, tt := range tests {
		got, truncated := truncateUTF8([]byte(tt.in), tt.n)
		if got != tt.want || truncated != tt.truncated {
			t.Errorf("truncateUTF8(%q, %d) = %q, %v; want %q, %v", tt.in, tt.n, got, truncated, tt.want, tt.truncated)
		}
	}
}

// deadLetterLog runs one failing message through handle and returns the
// decoded log record whose msg equals wantMsg.
func deadLetterLog(t *testing.T, fb config.FailedBody, pubErr error, body, wantMsg string) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	job := testJob()
	job.RetryLimit = 0
	w := newWorker(job, nil, &fakeForwarder{results: []bool{false}}, &Stats{}, Options{FailedBody: fb}, log)

	w.handle(t.Context(), &fakePublisher{err: pubErr}, nil, amqp.Delivery{Acknowledger: &fakeAcker{}, Body: []byte(body)})

	for line := range strings.Lines(buf.String()) {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		if rec["msg"] == wantMsg {
			return rec
		}
	}
	t.Fatalf("no %q log record in:\n%s", wantMsg, buf.String())
	return nil
}

func TestFailedBodyLogging(t *testing.T) {
	never := 1e-12
	tests := []struct {
		name          string
		fb            config.FailedBody
		pubErr        error
		wantMsg       string
		wantBody      string // "" means the body must not be logged
		wantTruncated bool
	}{
		{name: "disabled by default", fb: config.FailedBody{MaxBytes: 512},
			wantMsg: "message dead-lettered"},
		{name: "enabled", fb: config.FailedBody{Enabled: true, MaxBytes: 512},
			wantMsg: "message dead-lettered", wantBody: `{"order":42}`},
		{name: "enabled and truncated", fb: config.FailedBody{Enabled: true, MaxBytes: 5},
			wantMsg: "message dead-lettered", wantBody: `{"ord`, wantTruncated: true},
		{name: "sampled out", fb: config.FailedBody{Enabled: true, MaxBytes: 512, SampleRate: &never},
			wantMsg: "message dead-lettered"},
		{name: "logged on dlq publish failure", fb: config.FailedBody{Enabled: true, MaxBytes: 512}, pubErr: errors.New("nacked"),
			wantMsg: "publish dlq failed, nack requeue", wantBody: `{"order":42}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := deadLetterLog(t, tt.fb, tt.pubErr, `{"order":42}`, tt.wantMsg)
			body, has := rec["body"]
			if tt.wantBody == "" {
				if has {
					t.Fatalf("body logged: %v", rec)
				}
				return
			}
			if body != tt.wantBody || rec["body_truncated"] != tt.wantTruncated || rec["body_bytes"] != float64(12) {
				t.Fatalf("record = %v, want body %q truncated=%v", rec, tt.wantBody, tt.wantTruncated)
			}
		})
	}
}
