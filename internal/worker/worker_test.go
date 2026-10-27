package worker

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/forward"
)

var discardLog = slog.New(slog.DiscardHandler)

// fakeAcker records how a delivery was settled.
type fakeAcker struct {
	mu      sync.Mutex
	acks    int
	nacks   int
	requeue bool
}

func (a *fakeAcker) Ack(uint64, bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks++
	return nil
}

func (a *fakeAcker) Nack(_ uint64, _ bool, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nacks++
	a.requeue = requeue
	return nil
}

func (a *fakeAcker) Reject(uint64, bool) error { return errors.New("unexpected reject") }

type fakePublisher struct {
	err   error
	calls int
	queue string
	msg   amqp.Publishing
}

func (p *fakePublisher) Publish(_ context.Context, queue string, msg amqp.Publishing) error {
	p.calls++
	p.queue = queue
	p.msg = msg
	return p.err
}

// fakeForwarder returns results in order; the last one repeats.
type fakeForwarder struct {
	mu      sync.Mutex
	results []bool
	calls   int
	// block, when set, makes Post wait for ctx cancellation.
	block bool
}

func (f *fakeForwarder) Post(ctx context.Context, _, _, _, _ string) forward.Result {
	f.mu.Lock()
	i := min(f.calls, len(f.results)-1)
	f.calls++
	block := f.block
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return forward.Result{Body: ctx.Err().Error(), Err: ctx.Err()}
	}
	if f.results[i] {
		return forward.Result{OK: true, Body: "ok", StatusCode: 200}
	}
	return forward.Result{Body: "fail", StatusCode: 500}
}

func (f *fakeForwarder) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func testJob() config.ResolvedJob {
	return config.ResolvedJob{
		Name:       "mail",
		LogicName:  "mail-a",
		ServerName: "a",
		Server:     config.Server{ReconnectSeconds: 30},
		Queue:      "mail",
		DLQ:        "mail.dlq",
		URL:        "http://consumer/dispatch",
		Timeout:    time.Second,
		RetryLimit: 2,
	}
}

func TestHandle(t *testing.T) {
	tests := []struct {
		name        string
		retryLimit  int
		results     []bool
		pubErr      error
		blockRe     string
		discard     bool
		wantCalls   int
		wantAck     bool
		wantRequeue bool
		wantDLQ     bool
		wantDone    int64
		wantFailed  int64
	}{
		{name: "first attempt ok", retryLimit: 2, results: []bool{true},
			wantCalls: 1, wantAck: true, wantDone: 1},
		{name: "ok after retry", retryLimit: 2, results: []bool{false, true},
			wantCalls: 2, wantAck: true, wantDone: 1},
		{name: "retries exhausted go to dlq", retryLimit: 2, results: []bool{false},
			wantCalls: 3, wantAck: true, wantDLQ: true, wantFailed: 1},
		{name: "retry_limit 0 means one attempt", retryLimit: 0, results: []bool{false},
			wantCalls: 1, wantAck: true, wantDLQ: true, wantFailed: 1},
		{name: "dlq publish failure requeues", retryLimit: 0, results: []bool{false}, pubErr: errors.New("nacked"),
			wantCalls: 1, wantRequeue: true, wantDLQ: true, wantFailed: 1},
		{name: "blocked and discarded", retryLimit: 2, results: []bool{true}, blockRe: "^hel", discard: true,
			wantCalls: 0, wantAck: true, wantDone: 1},
		{name: "blocked but not discarded is forwarded", retryLimit: 2, results: []bool{true}, blockRe: "^hel",
			wantCalls: 1, wantAck: true, wantDone: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				job := testJob()
				job.RetryLimit = tt.retryLimit
				job.DiscardMatchJobs = tt.discard
				job.BlockPatterns = tt.blockRe
				var re *regexp.Regexp
				if tt.blockRe != "" {
					re = regexp.MustCompile(tt.blockRe)
				}

				fwd := &fakeForwarder{results: tt.results}
				pub := &fakePublisher{err: tt.pubErr}
				acker := &fakeAcker{}
				stats := &Stats{}
				w := newWorker(job, nil, fwd, stats, Options{}, discardLog)

				w.handle(t.Context(), pub, re, amqp.Delivery{Acknowledger: acker, Body: []byte("hello")})

				if fwd.Calls() != tt.wantCalls {
					t.Errorf("forward calls = %d, want %d", fwd.Calls(), tt.wantCalls)
				}
				if (acker.acks == 1) != tt.wantAck || (acker.nacks == 1) != tt.wantRequeue {
					t.Errorf("acks=%d nacks=%d, want ack=%v requeue=%v", acker.acks, acker.nacks, tt.wantAck, tt.wantRequeue)
				}
				if tt.wantRequeue && !acker.requeue {
					t.Error("nack without requeue")
				}
				if (pub.calls == 1) != tt.wantDLQ {
					t.Errorf("dlq publishes = %d, want dlq=%v", pub.calls, tt.wantDLQ)
				}
				if tt.wantDLQ && pub.queue != "mail.dlq" {
					t.Errorf("dlq queue = %q", pub.queue)
				}
				if stats.completed.Load() != tt.wantDone || stats.failed.Load() != tt.wantFailed {
					t.Errorf("stats completed=%d failed=%d, want %d/%d",
						stats.completed.Load(), stats.failed.Load(), tt.wantDone, tt.wantFailed)
				}
			})
		})
	}
}

// An aborted worker must requeue its in-flight message, never dead-letter it.
func TestHandleAbortRequeues(t *testing.T) {
	tests := []struct {
		name  string
		block bool // abort during the HTTP call vs during retry backoff
	}{
		{name: "abort during http call", block: true},
		{name: "abort during backoff", block: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				fwd := &fakeForwarder{results: []bool{false}, block: tt.block}
				pub := &fakePublisher{}
				acker := &fakeAcker{}
				stats := &Stats{}
				job := testJob()
				job.RetryLimit = 5
				w := newWorker(job, nil, fwd, stats, Options{}, discardLog)

				done := make(chan struct{})
				go func() {
					defer close(done)
					w.handle(ctx, pub, nil, amqp.Delivery{Acknowledger: acker, Body: []byte("x")})
				}()
				synctest.Wait() // blocked in Post or in backoff
				cancel()
				<-done

				if pub.calls != 0 {
					t.Fatalf("aborted message was dead-lettered")
				}
				if acker.nacks != 1 || !acker.requeue || acker.acks != 0 {
					t.Fatalf("acks=%d nacks=%d requeue=%v, want one requeue", acker.acks, acker.nacks, acker.requeue)
				}
				if stats.failed.Load() != 0 {
					t.Fatalf("aborted message counted as failed")
				}
			})
		})
	}
}

func TestBackoffIsBoundedAndJittered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := testJob()
		job.RetryLimit = 10
		w := newWorker(job, nil, &fakeForwarder{results: []bool{false}}, &Stats{}, Options{}, discardLog)

		start := time.Now()
		w.forwardWithRetry(t.Context(), "x")
		elapsed := time.Since(start) // virtual time: sum of backoffs only

		// Upper bound: 100+200+400+800+1600+3200+5000*4 ms = 26.3s
		if elapsed > 26300*time.Millisecond || elapsed < 13*time.Second {
			t.Fatalf("total backoff = %v, outside expected jittered range", elapsed)
		}
	})
}

func TestJitterRange(t *testing.T) {
	for range 1000 {
		if d := jitter(time.Second); d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("jitter = %v", d)
		}
	}
}

func TestDLQMessage(t *testing.T) {
	d := amqp.Delivery{
		Headers:     amqp.Table{"trace": "t1"},
		ContentType: "text/plain",
		Body:        []byte("payload"),
	}
	msg := dlqMessage(testJob(), d)
	if string(msg.Body) != "payload" || msg.ContentType != "text/plain" || msg.DeliveryMode != amqp.Persistent {
		t.Fatalf("msg = %+v", msg)
	}
	for k, want := range map[string]any{"trace": "t1", "x-original-queue": "mail", "x-job": "mail-a"} {
		if msg.Headers[k] != want {
			t.Errorf("header %s = %v, want %v", k, msg.Headers[k], want)
		}
	}
	if _, ok := d.Headers["x-job"]; ok {
		t.Error("original delivery headers were mutated")
	}
}

func TestDLQMessageTTL(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
		want string
	}{
		{name: "no ttl", ttl: 0, want: ""},
		{name: "7 days", ttl: 7 * 24 * time.Hour, want: "604800000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := testJob()
			job.DLQTTL = tt.ttl
			if got := dlqMessage(job, amqp.Delivery{}).Expiration; got != tt.want {
				t.Fatalf("expiration = %q, want %q", got, tt.want)
			}
		})
	}
}
