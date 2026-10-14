package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/config"
)

// fakeSource fails every Channel call, optionally after blocking until release
// is closed (simulating a slow dial).
type fakeSource struct {
	mu      sync.Mutex
	calls   int
	release chan struct{}
}

func (s *fakeSource) Channel(string, config.Server) (*amqp.Channel, error) {
	s.mu.Lock()
	s.calls++
	release := s.release
	s.mu.Unlock()
	if release != nil {
		<-release
	}
	return nil, errors.New("broker unavailable")
}

func (s *fakeSource) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newTestPool(src ChannelSource) *Pool {
	return NewPool(testJob(), src, &fakeForwarder{results: []bool{true}}, Options{}, discardLog)
}

// synctest.Test fails if any goroutine is still running at the end, so these
// tests also verify that no worker goroutine leaks.

func TestPoolScaleAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &fakeSource{}
		p := newTestPool(src)

		p.SetWorkers(3)
		synctest.Wait()
		if got := p.WorkerCount(); got != 3 {
			t.Fatalf("workers = %d, want 3", got)
		}
		if got := src.Calls(); got != 3 {
			t.Fatalf("channel calls = %d, want 3", got)
		}

		// Workers retry after reconnect_seconds.
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if got := src.Calls(); got != 6 {
			t.Fatalf("channel calls after reconnect = %d, want 6", got)
		}

		p.SetWorkers(1)
		if got := p.WorkerCount(); got != 1 {
			t.Fatalf("workers = %d, want 1", got)
		}
		p.SetWorkers(-5)
		if got := p.WorkerCount(); got != 0 {
			t.Fatalf("workers = %d, want 0", got)
		}

		p.SetWorkers(2)
		p.Shutdown(t.Context())
		if got := p.WorkerCount(); got != 0 {
			t.Fatalf("workers after shutdown = %d", got)
		}
		p.SetWorkers(4) // no-op once closed
		if got := p.WorkerCount(); got != 0 {
			t.Fatalf("pool restarted after shutdown: %d workers", got)
		}
	})
}

// Scaling down must not hold the pool lock while a worker is busy, so admin
// reads (Stats/Job) stay responsive.
func TestPoolScaleDownDoesNotBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &fakeSource{release: make(chan struct{})}
		p := newTestPool(src)
		p.SetWorkers(2)
		synctest.Wait() // both workers stuck inside Channel()

		returned := make(chan struct{})
		go func() {
			p.SetWorkers(0)
			_, _, _ = p.Stats()
			_ = p.Job()
			p.UpdateJob(func(j *config.ResolvedJob) { j.URL = "http://new/d" })
			close(returned)
		}()
		synctest.Wait()
		select {
		case <-returned:
		default:
			t.Fatal("pool operations blocked on a busy worker")
		}

		close(src.release)
		p.Shutdown(t.Context())
	})
}

// Shutdown must also wait for workers that were retired by an earlier scale-down.
func TestPoolShutdownWaitsForRetiring(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &fakeSource{release: make(chan struct{})}
		p := newTestPool(src)
		p.SetWorkers(1)
		synctest.Wait()
		p.SetWorkers(0) // worker is retiring but still inside Channel()

		shutdownDone := make(chan struct{})
		go func() {
			p.Shutdown(context.Background())
			close(shutdownDone)
		}()
		synctest.Wait()
		select {
		case <-shutdownDone:
			t.Fatal("shutdown returned while a retiring worker was still running")
		default:
		}

		close(src.release)
		<-shutdownDone
	})
}

func TestPoolStatsSurviveRescale(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(&fakeSource{})
		p.stats.completed.Add(5)
		p.stats.failed.Add(2)

		p.SetWorkers(2)
		p.UpdateJob(func(j *config.ResolvedJob) { j.BlockPatterns = "x" })
		p.SetWorkers(0)

		if done, failed, _ := p.Stats(); done != 5 || failed != 2 {
			t.Fatalf("stats = %d/%d, want 5/2", done, failed)
		}
		if p.Job().BlockPatterns != "x" {
			t.Fatal("job update lost")
		}
		p.Shutdown(t.Context())
	})
}
