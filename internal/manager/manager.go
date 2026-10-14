package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/forward"
	"consumer-dispatcher/internal/worker"
)

// ErrJobNotFound is returned when a job id does not match any pool.
var ErrJobNotFound = errors.New("job not found")

// Manager owns shared broker connections and all job worker pools.
type Manager struct {
	mu     sync.RWMutex
	pools  map[string]*worker.Pool
	hub    *broker.Hub
	client *forward.Client
	log    *slog.Logger
	ready  atomic.Bool
}

func New(jobs []config.ResolvedJob, log *slog.Logger) *Manager {
	maxTimeout := 30 * time.Second
	for _, j := range jobs {
		if j.Timeout > maxTimeout {
			maxTimeout = j.Timeout
		}
	}
	m := &Manager{
		pools:  make(map[string]*worker.Pool, len(jobs)),
		hub:    broker.NewHub(log),
		client: forward.New(maxTimeout + 5*time.Second),
		log:    log,
	}
	for _, j := range jobs {
		m.pools[j.LogicName] = worker.NewPool(j, m.hub, m.client, log)
	}
	return m
}

func (m *Manager) StartAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.pools {
		j := p.Job()
		m.log.Info("starting job", "job", j.String())
		p.Start(j.Count)
	}
	m.ready.Store(true)
}

func (m *Manager) StopAll() {
	m.ready.Store(false)
	m.mu.RLock()
	pools := make([]*worker.Pool, 0, len(m.pools))
	for _, p := range m.pools {
		pools = append(pools, p)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, p := range pools {
		wg.Go(p.Stop)
	}
	wg.Wait()
	m.hub.Close()
}

func (m *Manager) Ready() bool { return m.ready.Load() }

func (m *Manager) Hub() *broker.Hub { return m.hub }

func (m *Manager) List() []JobStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]JobStatus, 0, len(m.pools))
	for _, p := range m.pools {
		j := p.Job()
		ok, fail, n := p.Stats()
		out = append(out, JobStatus{
			LogicName:        j.LogicName,
			Name:             j.Name,
			Server:           j.ServerName,
			Queue:            j.Queue,
			URL:              j.URL,
			Workers:          n,
			Completed:        ok,
			Failed:           fail,
			Prefetch:         j.Prefetch,
			RetryLimit:       j.RetryLimit,
			DLQ:              j.DLQ,
			BlockPatterns:    j.BlockPatterns,
			DiscardMatchJobs: j.DiscardMatchJobs,
		})
	}
	return out
}

type JobStatus struct {
	LogicName        string `json:"logic_name"`
	Name             string `json:"name"`
	Server           string `json:"server"`
	Queue            string `json:"queue"`
	URL              string `json:"url"`
	Workers          int    `json:"workers"`
	Completed        int64  `json:"completed"`
	Failed           int64  `json:"failed"`
	Prefetch         int    `json:"prefetch"`
	RetryLimit       int    `json:"retry_limit"`
	DLQ              string `json:"dlq"`
	BlockPatterns    string `json:"block_patterns"`
	DiscardMatchJobs bool   `json:"discard_match_jobs"`
}

func (m *Manager) pool(logicName string) (*worker.Pool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.pools[logicName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrJobNotFound, logicName)
	}
	return p, nil
}

func (m *Manager) resolveLogicName(id string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.pools[id]; ok {
		return id, nil
	}
	for _, p := range m.pools {
		if p.Job().Name == id {
			return p.Job().LogicName, nil
		}
	}
	return "", fmt.Errorf("%w: %s", ErrJobNotFound, id)
}

func (m *Manager) SetWorkers(id string, n int) error {
	logic, err := m.resolveLogicName(id)
	if err != nil {
		return err
	}
	p, err := m.pool(logic)
	if err != nil {
		return err
	}
	p.SetWorkers(n)
	return nil
}

func (m *Manager) SetURL(id, rawURL string) error {
	logic, err := m.resolveLogicName(id)
	if err != nil {
		return err
	}
	p, err := m.pool(logic)
	if err != nil {
		return err
	}
	p.UpdateJob(func(j *config.ResolvedJob) { j.URL = rawURL })
	return nil
}

func (m *Manager) SetBlockPatterns(id, patterns string) error {
	logic, err := m.resolveLogicName(id)
	if err != nil {
		return err
	}
	p, err := m.pool(logic)
	if err != nil {
		return err
	}
	p.UpdateJob(func(j *config.ResolvedJob) { j.BlockPatterns = patterns })
	return nil
}

func (m *Manager) SetDiscardMatchJobs(id string, discard bool) error {
	logic, err := m.resolveLogicName(id)
	if err != nil {
		return err
	}
	p, err := m.pool(logic)
	if err != nil {
		return err
	}
	p.UpdateJob(func(j *config.ResolvedJob) { j.DiscardMatchJobs = discard })
	return nil
}

func (m *Manager) Try(ctx context.Context, id, body string) (string, error) {
	logic, err := m.resolveLogicName(id)
	if err != nil {
		return "", err
	}
	p, err := m.pool(logic)
	if err != nil {
		return "", err
	}
	return worker.Try(ctx, m.client, p.Job(), body)
}

func (m *Manager) withChannel(ctx context.Context, id string, fn func(context.Context, *amqp.Channel, config.ResolvedJob) error) error {
	logic, err := m.resolveLogicName(id)
	if err != nil {
		return err
	}
	p, err := m.pool(logic)
	if err != nil {
		return err
	}
	j := p.Job()
	ch, err := m.hub.Channel(j.ServerName, j.Server)
	if err != nil {
		return err
	}
	defer ch.Close()
	return fn(ctx, ch, j)
}

func (m *Manager) PurgeQueue(id string) (int, error) {
	var n int
	err := m.withChannel(context.Background(), id, func(_ context.Context, ch *amqp.Channel, j config.ResolvedJob) error {
		count, err := ch.QueuePurge(j.Queue, false)
		n = count
		return err
	})
	return n, err
}

func (m *Manager) PurgeDLQ(id string) (int, error) {
	var n int
	err := m.withChannel(context.Background(), id, func(_ context.Context, ch *amqp.Channel, j config.ResolvedJob) error {
		count, err := ch.QueuePurge(j.DLQ, false)
		n = count
		return err
	})
	return n, err
}

func (m *Manager) Peek(id string, count int, ack bool) ([]string, error) {
	if count <= 0 {
		count = 10
	}
	msgs := make([]string, 0, count)
	err := m.withChannel(context.Background(), id, func(_ context.Context, ch *amqp.Channel, j config.ResolvedJob) error {
		for range count {
			d, ok, err := ch.Get(j.Queue, ack)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			msgs = append(msgs, string(d.Body))
		}
		return nil
	})
	return msgs, err
}

func (m *Manager) PeekDLQ(id string, count int) ([]string, error) {
	if count <= 0 {
		count = 10
	}
	msgs := make([]string, 0, count)
	err := m.withChannel(context.Background(), id, func(_ context.Context, ch *amqp.Channel, j config.ResolvedJob) error {
		for range count {
			d, ok, err := ch.Get(j.DLQ, false)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			msgs = append(msgs, string(d.Body))
		}
		return nil
	})
	return msgs, err
}

// ReplayDLQ re-publishes up to count DLQ messages back to the main queue.
func (m *Manager) ReplayDLQ(ctx context.Context, id string, count int) (int, error) {
	if count <= 0 {
		count = 10
	}
	replayed := 0
	err := m.withChannel(ctx, id, func(ctx context.Context, ch *amqp.Channel, j config.ResolvedJob) error {
		for range count {
			d, ok, err := ch.Get(j.DLQ, true)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			if err := ch.PublishWithContext(ctx, "", j.Queue, false, false, amqp.Publishing{
				Headers:         d.Headers,
				ContentType:     d.ContentType,
				ContentEncoding: d.ContentEncoding,
				Body:            d.Body,
				DeliveryMode:    amqp.Persistent,
			}); err != nil {
				return fmt.Errorf("republish: %w", err)
			}
			replayed++
		}
		return nil
	})
	return replayed, err
}

// Servers returns unique server bindings for readiness checks.
func (m *Manager) Servers() map[string]config.Server {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]config.Server)
	for _, p := range m.pools {
		j := p.Job()
		out[j.ServerName] = j.Server
	}
	return out
}
