package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/metrics"
)

// Stats are cumulative per-job counters that survive scaling and job updates.
type Stats struct {
	completed atomic.Int64
	failed    atomic.Int64
}

// Options are settings shared by all workers of a pool.
type Options struct {
	FailedBody config.FailedBody
}

// Pool manages N workers for one job and supports scaling.
//
// Workers being removed are stopped in the background; the pool lock is never
// held while waiting for a worker to exit.
type Pool struct {
	src   ChannelSource
	fwd   Forwarder
	log   *slog.Logger
	stats *Stats
	opts  Options

	mu       sync.Mutex
	job      config.ResolvedJob
	workers  []*Worker
	retiring map[*Worker]struct{}
	closed   bool
}

// NewPool returns a pool with no running workers.
func NewPool(job config.ResolvedJob, src ChannelSource, fwd Forwarder, opts Options, log *slog.Logger) *Pool {
	return &Pool{
		job:      job,
		src:      src,
		fwd:      fwd,
		log:      log,
		opts:     opts,
		stats:    &Stats{},
		retiring: make(map[*Worker]struct{}),
	}
}

// SetWorkers scales the pool to n workers (n < 0 is treated as 0).
// It returns without waiting for removed workers to finish their in-flight message.
func (p *Pool) SetWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.resizeLocked(max(n, 0))
}

// Override scales the pool to n and turns autoscale off, so the autoscaler
// does not undo a manual change. It reports whether autoscale was on.
func (p *Pool) Override(n int) (paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	paused = p.job.Autoscale.Enabled
	p.job.Autoscale.Enabled = false
	p.resizeLocked(max(n, 0))
	return paused
}

// Autoscale scales the pool to n only while autoscale is enabled, so it
// cannot override a concurrent manual change. It reports whether it resized.
func (p *Pool) Autoscale(n int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.job.Autoscale.Enabled {
		return false
	}
	p.resizeLocked(max(n, 0))
	return true
}

// SetAutoscale replaces the autoscale settings. Workers keep running.
func (p *Pool) SetAutoscale(a config.Autoscale) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.job.Autoscale = a
}

func (p *Pool) resizeLocked(n int) {
	cur := len(p.workers)
	switch {
	case n > cur:
		for range n - cur {
			w := newWorker(p.job, p.src, p.fwd, p.stats, p.opts, p.log)
			w.start()
			p.workers = append(p.workers, w)
		}
	case n < cur:
		p.retireLocked(p.workers[n:])
		p.workers = p.workers[:n:n]
	}
	metrics.Workers.WithLabelValues(p.job.LogicName).Set(float64(len(p.workers)))
}

// retireLocked signals ws to stop and reaps them in the background.
func (p *Pool) retireLocked(ws []*Worker) {
	for _, w := range ws {
		w.requestStop()
		p.retiring[w] = struct{}{}
		go func() {
			<-w.done
			w.abort()
			p.mu.Lock()
			delete(p.retiring, w)
			p.mu.Unlock()
		}()
	}
}

// UpdateJob applies mut to the job config and replaces all workers so the
// change takes effect. Old workers finish their in-flight message first.
func (p *Pool) UpdateJob(mut func(*config.ResolvedJob)) {
	p.replace(func() { mut(&p.job) })
}

// UpdateOptions replaces the pool options and its workers, like UpdateJob.
func (p *Pool) UpdateOptions(opts Options) {
	p.replace(func() { p.opts = opts })
}

func (p *Pool) replace(mut func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	n := len(p.workers)
	p.retireLocked(p.workers)
	p.workers = nil
	mut()
	p.resizeLocked(n)
}

// Shutdown stops all workers, including ones still retiring, and waits for
// them. When ctx ends, in-flight messages are aborted and requeued.
// The pool cannot be restarted afterwards.
func (p *Pool) Shutdown(ctx context.Context) {
	p.mu.Lock()
	p.closed = true
	all := make([]*Worker, 0, len(p.workers)+len(p.retiring))
	all = append(all, p.workers...)
	for w := range p.retiring {
		all = append(all, w)
	}
	p.workers = nil
	metrics.Workers.WithLabelValues(p.job.LogicName).Set(0)
	p.mu.Unlock()

	var wg sync.WaitGroup
	for _, w := range all {
		wg.Go(func() { w.shutdown(ctx) })
	}
	wg.Wait()
}

// WorkerCount returns the number of active (non-retiring) workers.
func (p *Pool) WorkerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

// Stats returns cumulative counters and the active worker count.
func (p *Pool) Stats() (completed, failed int64, workers int) {
	return p.stats.completed.Load(), p.stats.failed.Load(), p.WorkerCount()
}

// Job returns a copy of the current job config.
func (p *Pool) Job() config.ResolvedJob {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.job
}
