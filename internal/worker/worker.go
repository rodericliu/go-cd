package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"regexp"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/forward"
	"consumer-dispatcher/internal/metrics"
)

const (
	initialBackoff    = 100 * time.Millisecond
	maxBackoff        = 5 * time.Second
	dlqPublishTimeout = 10 * time.Second
)

// Outcome labels for metrics.Processed.
const (
	outcomeSuccess   = "success"
	outcomeDiscarded = "discarded"
	outcomeDLQ       = "dlq"
	outcomeFailed    = "failed"
	outcomeRequeued  = "requeued"
)

// ChannelSource opens AMQP channels. Implemented by *broker.Hub.
type ChannelSource interface {
	Channel(serverName string, srv config.Server) (*amqp.Channel, error)
}

// Forwarder posts one message to the HTTP consumer. Implemented by *forward.Client.
type Forwarder interface {
	Post(ctx context.Context, endpoint, hostHeader, queueName, body string) forward.Result
}

type publisher interface {
	Publish(ctx context.Context, queue string, msg amqp.Publishing) error
}

// Worker consumes one RabbitMQ queue and forwards messages over HTTP.
//
// Stopping is two-phase: requestStop stops taking new deliveries and lets the
// in-flight message finish; abort additionally cancels the in-flight message,
// which is then requeued rather than dead-lettered.
type Worker struct {
	job   config.ResolvedJob
	src   ChannelSource
	fwd   Forwarder
	log   *slog.Logger
	stats *Stats

	stop     chan struct{}
	stopOnce sync.Once
	abort    context.CancelFunc
	done     chan struct{}
}

func newWorker(job config.ResolvedJob, src ChannelSource, fwd Forwarder, stats *Stats, log *slog.Logger) *Worker {
	return &Worker{
		job:   job,
		src:   src,
		fwd:   fwd,
		log:   log.With("job", job.LogicName),
		stats: stats,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

func (w *Worker) start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.abort = cancel
	go w.loop(ctx)
}

// requestStop asks the worker to stop after the in-flight message. Non-blocking.
func (w *Worker) requestStop() {
	w.stopOnce.Do(func() { close(w.stop) })
}

// shutdown stops the worker and waits for it. If ctx ends first, the in-flight
// message is aborted (and requeued) before waiting for the worker to exit.
func (w *Worker) shutdown(ctx context.Context) {
	w.requestStop()
	select {
	case <-w.done:
	case <-ctx.Done():
		w.abort()
		<-w.done
	}
	w.abort()
}

func (w *Worker) stopping() bool {
	select {
	case <-w.stop:
		return true
	default:
		return false
	}
}

func (w *Worker) loop(ctx context.Context) {
	defer close(w.done)
	for {
		err := w.consumeOnce(ctx)
		if w.stopping() {
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

// consumeOnce runs one consume session. It returns nil only when stopping.
func (w *Worker) consumeOnce(ctx context.Context) error {
	var blockRe *regexp.Regexp
	if w.job.BlockPatterns != "" {
		re, err := regexp.Compile(w.job.BlockPatterns)
		if err != nil {
			return fmt.Errorf("block_patterns: %w", err)
		}
		blockRe = re
	}

	ch, err := w.src.Channel(w.job.ServerName, w.job.Server)
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	// Closing the channel requeues any prefetched but unacked deliveries.
	defer func() {
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			w.log.Warn("channel close", "err", err)
		}
	}()

	if err := prepareTopology(ch, w.job); err != nil {
		return err
	}
	if err := ch.Qos(w.job.Prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	pub, err := broker.NewPublisher(ch)
	if err != nil {
		return err
	}
	closeErr := ch.NotifyClose(make(chan *amqp.Error, 1))

	deliveries, err := ch.Consume(w.job.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	w.log.Info("consuming", "queue", w.job.Queue, "url", w.job.URL)

	for {
		// Give stop priority over deliveries that are already buffered.
		if w.stopping() {
			return nil
		}
		select {
		case <-w.stop:
			return nil
		case <-ctx.Done():
			return nil
		case amqpErr := <-closeErr:
			if amqpErr != nil {
				return fmt.Errorf("channel closed: %s", amqpErr.Error())
			}
			return errors.New("channel closed")
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			w.handle(ctx, pub, blockRe, d)
		}
	}
}

func prepareTopology(ch *amqp.Channel, job config.ResolvedJob) error {
	if !job.DeclareTopology {
		// Topology is managed elsewhere; verify both queues exist so failed
		// messages are never published to a missing DLQ.
		if _, err := ch.QueueDeclarePassive(job.Queue, job.QueueDurable, false, false, false, nil); err != nil {
			return fmt.Errorf("queue passive: %w", err)
		}
		if _, err := ch.QueueDeclarePassive(job.DLQ, true, false, false, false, nil); err != nil {
			return fmt.Errorf("dlq passive: %w", err)
		}
		return nil
	}
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

// handle processes one delivery. ctx is cancelled only when the worker is aborted.
func (w *Worker) handle(ctx context.Context, pub publisher, blockRe *regexp.Regexp, d amqp.Delivery) {
	metrics.InFlight.WithLabelValues(w.job.LogicName).Inc()
	defer metrics.InFlight.WithLabelValues(w.job.LogicName).Dec()

	body := string(d.Body)
	if blockRe != nil && blockRe.MatchString(body) {
		w.log.Info("matched block pattern", "pattern", w.job.BlockPatterns)
		if w.job.DiscardMatchJobs {
			w.ack(d, outcomeDiscarded)
			w.stats.completed.Add(1)
			return
		}
	}

	start := time.Now()
	ok := w.forwardWithRetry(ctx, body)
	metrics.Duration.WithLabelValues(w.job.LogicName).Observe(time.Since(start).Seconds())

	switch {
	case ok:
		w.ack(d, outcomeSuccess)
		w.stats.completed.Add(1)
	case ctx.Err() != nil:
		// Aborted by shutdown, not a consumer failure: give it back to the queue.
		w.log.Info("aborted in-flight message, requeue")
		w.nack(d, outcomeRequeued)
	default:
		w.deadLetter(ctx, pub, d)
	}
}

func (w *Worker) deadLetter(ctx context.Context, pub publisher, d amqp.Delivery) {
	pubCtx, cancel := context.WithTimeout(ctx, dlqPublishTimeout)
	defer cancel()

	if err := pub.Publish(pubCtx, w.job.DLQ, dlqMessage(w.job, d)); err != nil {
		if ctx.Err() != nil {
			w.nack(d, outcomeRequeued)
			return
		}
		w.log.Error("publish dlq failed, nack requeue", "err", err)
		w.nack(d, outcomeFailed)
		w.stats.failed.Add(1)
		return
	}
	w.ack(d, outcomeDLQ)
	w.stats.failed.Add(1)
}

func (w *Worker) ack(d amqp.Delivery, outcome string) {
	if err := d.Ack(false); err != nil {
		// The broker will redeliver it; the consumer may see a duplicate.
		w.log.Warn("ack failed", "outcome", outcome, "err", err)
	}
	metrics.Processed.WithLabelValues(w.job.LogicName, outcome).Inc()
}

func (w *Worker) nack(d amqp.Delivery, outcome string) {
	if err := d.Nack(false, true); err != nil {
		w.log.Warn("nack failed", "outcome", outcome, "err", err)
	}
	metrics.Processed.WithLabelValues(w.job.LogicName, outcome).Inc()
}

// forwardWithRetry makes 1 + RetryLimit attempts with jittered exponential backoff.
func (w *Worker) forwardWithRetry(ctx context.Context, body string) bool {
	total := 1 + w.job.RetryLimit
	backoff := initialBackoff

	for i := range total {
		if i > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(jitter(backoff)):
			}
			backoff = min(backoff*2, maxBackoff)
		}
		if ctx.Err() != nil {
			return false
		}
		attemptCtx, cancel := context.WithTimeout(ctx, w.job.Timeout)
		res := w.fwd.Post(attemptCtx, w.job.URL, w.job.URLHost, w.job.Name, body)
		cancel()
		if res.OK {
			w.log.Debug("forward ok", "attempt", i+1, "result", res.Body)
			return true
		}
		w.log.Warn("forward failed", "attempt", i+1, "of", total, "result", res.Body, "err", res.Err)
	}
	return false
}

// jitter returns a random duration in [d/2, d].
func jitter(d time.Duration) time.Duration {
	half := d / 2
	return half + rand.N(half+1)
}

func dlqMessage(job config.ResolvedJob, d amqp.Delivery) amqp.Publishing {
	headers := amqp.Table{}
	maps.Copy(headers, d.Headers)
	headers["x-original-queue"] = job.Queue
	headers["x-job"] = job.LogicName
	headers["x-failed-at"] = time.Now().UTC().Format(time.RFC3339)

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		Body:            d.Body,
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
	}
}

// Try posts a one-off body without touching the queue (admin).
func Try(ctx context.Context, fwd Forwarder, job config.ResolvedJob, body string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, job.Timeout)
	defer cancel()
	res := fwd.Post(ctx, job.URL, job.URLHost, job.Name, body)
	if res.Err != nil {
		return res.Body, res.Err
	}
	return res.Body, nil
}
