package worker

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/forward"
	"consumer-dispatcher/internal/metrics"
)

// Worker consumes one RabbitMQ queue and forwards messages over HTTP.
// Lifecycle is controlled via stop (chan struct{}), not a stored context.
type Worker struct {
	job    config.ResolvedJob
	hub    *broker.Hub
	client *forward.Client
	log    *slog.Logger

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	completed atomic.Int64
	failed    atomic.Int64
}

func newWorker(job config.ResolvedJob, hub *broker.Hub, client *forward.Client, log *slog.Logger) *Worker {
	return &Worker{
		job:    job,
		hub:    hub,
		client: client,
		log:    log.With("job", job.LogicName),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (w *Worker) start() { go w.loop() }

func (w *Worker) Stop() {
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
}

func (w *Worker) Completed() int64        { return w.completed.Load() }
func (w *Worker) Failed() int64           { return w.failed.Load() }
func (w *Worker) Job() config.ResolvedJob { return w.job }
func (w *Worker) stopped() bool {
	select {
	case <-w.stop:
		return true
	default:
		return false
	}
}

func (w *Worker) loop() {
	defer close(w.done)
	for {
		if w.stopped() {
			return
		}
		if err := w.consumeOnce(); err != nil {
			if w.stopped() {
				return
			}
			w.log.Error("consume session ended", "err", err)
			select {
			case <-w.stop:
				return
			case <-time.After(time.Duration(w.job.Server.ReconnectSeconds) * time.Second):
			}
		}
	}
}

func (w *Worker) consumeOnce() error {
	ch, err := w.hub.Channel(w.job.ServerName, w.job.Server)
	if err != nil {
		return err
	}
	defer ch.Close()

	if w.job.DeclareTopology {
		if err := declareTopology(ch, w.job); err != nil {
			return err
		}
	} else if _, err := ch.QueueDeclarePassive(w.job.Queue, w.job.QueueDurable, false, false, false, nil); err != nil {
		return fmt.Errorf("queue passive: %w", err)
	}

	if err := ch.Qos(w.job.Prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	deliveries, err := ch.Consume(w.job.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	w.log.Info("consuming", "queue", w.job.Queue, "url", w.job.URL)

	closeErr := make(chan *amqp.Error, 1)
	ch.NotifyClose(closeErr)

	var blockRe *regexp.Regexp
	if w.job.BlockPatterns != "" {
		blockRe, err = regexp.Compile(w.job.BlockPatterns)
		if err != nil {
			return fmt.Errorf("block_patterns: %w", err)
		}
	}

	for {
		select {
		case <-w.stop:
			return nil
		case amqpErr := <-closeErr:
			if amqpErr != nil {
				return fmt.Errorf("channel closed: %w", error(amqpErr))
			}
			return fmt.Errorf("channel closed")
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			w.handle(ch, blockRe, d)
		}
	}
}

func declareTopology(ch *amqp.Channel, job config.ResolvedJob) error {
	if err := ch.ExchangeDeclare(job.Exchange, job.Type, job.ExchangeDurable, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange declare: %w", err)
	}
	if _, err := ch.QueueDeclare(job.Queue, job.QueueDurable, false, false, false, nil); err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}
	if err := ch.QueueBind(job.Queue, job.RouteKey, job.Exchange, false, nil); err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}
	if _, err := ch.QueueDeclare(job.DLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("dlq declare: %w", err)
	}
	return nil
}

func (w *Worker) handle(ch *amqp.Channel, blockRe *regexp.Regexp, d amqp.Delivery) {
	metrics.InFlight.WithLabelValues(w.job.LogicName).Inc()
	defer metrics.InFlight.WithLabelValues(w.job.LogicName).Dec()

	body := string(d.Body)
	if blockRe != nil && blockRe.MatchString(body) {
		w.log.Info("matched block pattern", "pattern", w.job.BlockPatterns)
		if w.job.DiscardMatchJobs {
			_ = d.Ack(false)
			w.completed.Add(1)
			metrics.Processed.WithLabelValues(w.job.LogicName, "discarded").Inc()
			return
		}
	}

	baseCtx, stopCancel := contextFromStop(w.stop)
	defer stopCancel()

	start := time.Now()
	ok := w.forwardWithRetry(baseCtx, body)
	metrics.Duration.WithLabelValues(w.job.LogicName).Observe(time.Since(start).Seconds())

	if ok {
		_ = d.Ack(false)
		w.completed.Add(1)
		metrics.Processed.WithLabelValues(w.job.LogicName, "success").Inc()
		return
	}

	pubCtx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
	err := publishDLQ(pubCtx, ch, w.job, d)
	cancel()
	if err != nil {
		w.log.Error("publish dlq failed, nack requeue", "err", err)
		_ = d.Nack(false, true)
		w.failed.Add(1)
		metrics.Processed.WithLabelValues(w.job.LogicName, "failed").Inc()
		return
	}
	_ = d.Ack(false)
	w.failed.Add(1)
	metrics.Processed.WithLabelValues(w.job.LogicName, "dlq").Inc()
}

func (w *Worker) forwardWithRetry(parent context.Context, body string) bool {
	attempts := max(w.job.RetryLimit, 1)
	total := 1 + attempts
	backoff := time.Duration(50+rand.IntN(50)) * time.Millisecond

	for i := range total {
		if err := parent.Err(); err != nil {
			return false
		}
		ctx, cancel := context.WithTimeout(parent, w.job.Timeout)
		res := w.client.Post(ctx, w.job.URL, w.job.URLHost, w.job.Name, body)
		cancel()
		if res.OK {
			w.log.Info("forward ok", "attempt", i+1, "result", res.Body)
			return true
		}
		w.log.Warn("forward failed", "attempt", i+1, "result", res.Body, "err", res.Err)
		if i == total-1 {
			break
		}
		select {
		case <-parent.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
	return false
}

// contextFromStop returns a context cancelled when stop is closed.
// Context is not stored on the worker; callers must cancel when done.
func contextFromStop(stop <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func publishDLQ(ctx context.Context, ch *amqp.Channel, job config.ResolvedJob, d amqp.Delivery) error {
	headers := amqp.Table{}
	maps.Copy(headers, d.Headers)
	headers["x-original-queue"] = job.Queue
	headers["x-job"] = job.LogicName
	headers["x-failed-at"] = time.Now().UTC().Format(time.RFC3339)

	return ch.PublishWithContext(ctx, "", job.DLQ, false, false, amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		Body:            d.Body,
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
	})
}

// Try posts a one-off body without touching the queue (admin).
func Try(ctx context.Context, client *forward.Client, job config.ResolvedJob, body string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, job.Timeout)
	defer cancel()
	res := client.Post(ctx, job.URL, job.URLHost, job.Name, body)
	if res.Err != nil {
		return res.Body, res.Err
	}
	return res.Body, nil
}

// Pool manages N workers for one job and supports scaling.
type Pool struct {
	mu      sync.Mutex
	job     config.ResolvedJob
	hub     *broker.Hub
	client  *forward.Client
	log     *slog.Logger
	workers []*Worker
}

func NewPool(job config.ResolvedJob, hub *broker.Hub, client *forward.Client, log *slog.Logger) *Pool {
	return &Pool{job: job, hub: hub, client: client, log: log}
}

func (p *Pool) Start(n int) {
	p.SetWorkers(n)
}

func (p *Pool) SetWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n < 0 {
		n = 0
	}
	p.setLocked(n)
}

func (p *Pool) setLocked(n int) {
	cur := len(p.workers)
	switch {
	case n > cur:
		for range n - cur {
			w := newWorker(p.job, p.hub, p.client, p.log)
			w.start()
			p.workers = append(p.workers, w)
		}
	case n < cur:
		stopped := p.workers[n:]
		p.workers = p.workers[:n]
		for _, w := range stopped {
			w.Stop()
		}
	}
	metrics.Workers.WithLabelValues(p.job.LogicName).Set(float64(len(p.workers)))
}

func (p *Pool) Stop() { p.SetWorkers(0) }

func (p *Pool) WorkerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

func (p *Pool) Stats() (completed, failed int64, workers int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.workers {
		completed += w.Completed()
		failed += w.Failed()
	}
	return completed, failed, len(p.workers)
}

func (p *Pool) Job() config.ResolvedJob {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.job
}

func (p *Pool) UpdateJob(mut func(*config.ResolvedJob)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.workers)
	for _, w := range p.workers {
		w.Stop()
	}
	p.workers = nil
	mut(&p.job)
	p.setLocked(n)
}
