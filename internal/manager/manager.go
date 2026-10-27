package manager

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/forward"
	"consumer-dispatcher/internal/worker"
)

const (
	// MaxWorkers caps workers per job set through the admin API.
	MaxWorkers = config.MaxWorkers
	// MaxPeek caps messages returned by one peek/take/dlq/replay call.
	MaxPeek = 100

	defaultPeek = 10
)

var (
	// ErrJobNotFound is returned when a job id does not match any pool.
	ErrJobNotFound = errors.New("job not found")
	// ErrAmbiguousJob is returned when a bare job name matches several servers.
	ErrAmbiguousJob = errors.New("job name is ambiguous")
	// ErrInvalidArgument is returned for out-of-range or malformed admin input.
	ErrInvalidArgument = errors.New("invalid argument")
)

// Manager owns shared broker connections and all job worker pools.
type Manager struct {
	hub    *broker.Hub
	client *forward.Client
	log    *slog.Logger
	ready  atomic.Bool

	mu    sync.RWMutex
	pools map[string]*worker.Pool
	// file holds jobs as last loaded from the config file, so a reload can
	// tell file changes apart from changes made through the admin API.
	file map[string]config.ResolvedJob
	opts Options

	reloadMu sync.Mutex // serializes Reload

	inspectMu  sync.Mutex
	inspecting map[inspectKey]bool // servers with a queue inspection in flight

	// readyFn reports each job's main queue stats to the autoscaler.
	readyFn    func(context.Context, []config.ResolvedJob) []QueueStats
	stopScaler func() // set by StartAll; stops and waits for the autoscaler
}

// Options configure all pools created by a Manager.
type Options = worker.Options

// New creates pools for jobs; call StartAll to begin consuming.
func New(jobs []config.ResolvedJob, opts Options, log *slog.Logger) *Manager {
	m := &Manager{
		pools:  make(map[string]*worker.Pool, len(jobs)),
		file:   make(map[string]config.ResolvedJob, len(jobs)),
		opts:   opts,
		hub:    broker.NewHub(log),
		client: forward.New(clientTimeout(jobs)),
		log:    log,

		inspecting: make(map[inspectKey]bool),
	}
	m.readyFn = m.readyCounts
	for _, j := range jobs {
		m.pools[j.LogicName] = worker.NewPool(j, m.hub, m.client, opts, log)
		m.file[j.LogicName] = j
	}
	return m
}

func clientTimeout(jobs []config.ResolvedJob) time.Duration {
	return MaxJobTimeout(jobs) + 5*time.Second
}

// snapshot returns the current pools.
func (m *Manager) snapshot() []*worker.Pool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Collect(maps.Values(m.pools))
}

// MaxTimeout returns the largest per-request timeout of the current jobs (at least 30s).
func (m *Manager) MaxTimeout() time.Duration {
	pools := m.snapshot()
	jobs := make([]config.ResolvedJob, 0, len(pools))
	for _, p := range pools {
		jobs = append(jobs, p.Job())
	}
	return MaxJobTimeout(jobs)
}

// MaxJobTimeout returns the largest per-request timeout among jobs (at least 30s).
func MaxJobTimeout(jobs []config.ResolvedJob) time.Duration {
	maxTimeout := 30 * time.Second
	for _, j := range jobs {
		maxTimeout = max(maxTimeout, j.Timeout)
	}
	return maxTimeout
}

// StartAll starts every pool with its configured worker count and the
// autoscaler. Call it once.
func (m *Manager) StartAll() {
	for _, p := range m.snapshot() {
		j := p.Job()
		m.log.Info("starting job", "job", j.String())
		p.SetWorkers(j.InitialWorkers())
	}
	m.startAutoscaler()
	m.ready.Store(true)
}

// StopAll marks the manager not ready, stops all pools and closes broker
// connections. In-flight messages are aborted and requeued when ctx ends.
func (m *Manager) StopAll(ctx context.Context) {
	m.ready.Store(false)
	// Wait for a running reload so it cannot start pools after this point.
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if m.stopScaler != nil {
		m.stopScaler()
		m.stopScaler = nil
	}
	shutdownPools(ctx, m.snapshot())
	m.hub.Close()
}

func shutdownPools(ctx context.Context, pools []*worker.Pool) {
	var wg sync.WaitGroup
	for _, p := range pools {
		wg.Go(func() { p.Shutdown(ctx) })
	}
	wg.Wait()
}

// Ready reports whether StartAll has run and StopAll has not.
func (m *Manager) Ready() bool { return m.ready.Load() }

// Ping verifies connectivity to every RabbitMQ server used by a job.
func (m *Manager) Ping(ctx context.Context) error {
	servers := make(map[string]config.Server)
	for _, p := range m.snapshot() {
		j := p.Job()
		servers[j.ServerName] = j.Server
	}
	for name, srv := range servers {
		if err := m.hub.Ping(ctx, name, srv); err != nil {
			return fmt.Errorf("server %q: %w", name, err)
		}
	}
	return nil
}

// JobStatus is the admin view of one job.
type JobStatus struct {
	LogicName        string          `json:"logic_name"`
	Name             string          `json:"name"`
	Server           string          `json:"server"`
	Queue            string          `json:"queue"`
	URL              string          `json:"url"`
	Workers          int             `json:"workers"`
	Completed        int64           `json:"completed"`
	Failed           int64           `json:"failed"`
	Prefetch         int             `json:"prefetch"`
	RetryLimit       int             `json:"retry_limit"`
	DLQ              string          `json:"dlq"`
	DLQTTLDays       int             `json:"dlq_ttl_days"`
	BlockPatterns    string          `json:"block_patterns"`
	DiscardMatchJobs bool            `json:"discard_match_jobs"`
	Autoscale        AutoscaleStatus `json:"autoscale"`
	// Depth is only set when requested, since it queries the broker.
	Depth *Depth `json:"depth,omitempty"`
}

func status(p *worker.Pool) JobStatus {
	j := p.Job()
	ok, fail, n := p.Stats()
	return JobStatus{
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
		DLQTTLDays:       int(j.DLQTTL / (24 * time.Hour)),
		BlockPatterns:    j.BlockPatterns,
		DiscardMatchJobs: j.DiscardMatchJobs,
		Autoscale:        autoscaleStatus(j.Autoscale),
	}
}

// List returns all jobs sorted by logic name.
func (m *Manager) List() []JobStatus {
	pools := m.snapshot()
	out := make([]JobStatus, 0, len(pools))
	for _, p := range pools {
		out = append(out, status(p))
	}
	slices.SortFunc(out, func(a, b JobStatus) int { return cmp.Compare(a.LogicName, b.LogicName) })
	return out
}

// Get returns the status of one job.
func (m *Manager) Get(id string) (JobStatus, error) {
	p, err := m.pool(id)
	if err != nil {
		return JobStatus{}, err
	}
	return status(p), nil
}

// pool resolves id as a logic name ("name-server"), or as a bare job name when
// that name is bound to exactly one server.
func (m *Manager) pool(id string) (*worker.Pool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if p, ok := m.pools[id]; ok {
		return p, nil
	}
	var matches []string
	for logic, p := range m.pools {
		if p.Job().Name == id {
			matches = append(matches, logic)
		}
	}
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("%w: %s", ErrJobNotFound, id)
	case 1:
		return m.pools[matches[0]], nil
	default:
		slices.Sort(matches)
		return nil, fmt.Errorf("%w: %q matches %s; use one of them", ErrAmbiguousJob, id, strings.Join(matches, ", "))
	}
}

// SetWorkers scales a job to n workers (0..MaxWorkers). A manual count takes
// precedence over autoscale: it turns autoscale off for the job until it is
// enabled again, and reports whether it did.
func (m *Manager) SetWorkers(id string, n int) (autoscalePaused bool, err error) {
	if n < 0 || n > MaxWorkers {
		return false, fmt.Errorf("%w: count must be between 0 and %d", ErrInvalidArgument, MaxWorkers)
	}
	p, err := m.pool(id)
	if err != nil {
		return false, err
	}
	if p.Override(n) {
		m.log.Info("autoscale paused by manual workers change", "job", p.Job().LogicName, "workers", n)
		return true, nil
	}
	return false, nil
}

// SetURL changes the consumer URL of a job.
func (m *Manager) SetURL(id, rawURL string) error {
	if err := config.ValidateURL(rawURL); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	p, err := m.pool(id)
	if err != nil {
		return err
	}
	p.UpdateJob(func(j *config.ResolvedJob) { j.URL = rawURL })
	return nil
}

// SetBlockPatterns changes the block regexp of a job ("" disables it).
func (m *Manager) SetBlockPatterns(id, patterns string) error {
	if err := config.ValidatePattern(patterns); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	p, err := m.pool(id)
	if err != nil {
		return err
	}
	p.UpdateJob(func(j *config.ResolvedJob) { j.BlockPatterns = patterns })
	return nil
}

// SetDiscardMatchJobs toggles whether blocked messages are discarded.
func (m *Manager) SetDiscardMatchJobs(id string, discard bool) error {
	p, err := m.pool(id)
	if err != nil {
		return err
	}
	p.UpdateJob(func(j *config.ResolvedJob) { j.DiscardMatchJobs = discard })
	return nil
}

// Try posts body to the job's consumer without touching the queue.
func (m *Manager) Try(ctx context.Context, id, body string) (string, error) {
	p, err := m.pool(id)
	if err != nil {
		return "", err
	}
	return worker.Try(ctx, m.client, p.Job(), body)
}

func (m *Manager) withChannel(id string, fn func(*amqp.Channel, config.ResolvedJob) error) error {
	p, err := m.pool(id)
	if err != nil {
		return err
	}
	j := p.Job()
	ch, err := m.hub.Channel(j.ServerName, j.Server)
	if err != nil {
		return err
	}
	defer func() {
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			m.log.Warn("channel close", "err", err)
		}
	}()
	return fn(ch, j)
}

// PurgeQueue removes all ready messages from the job's main queue.
func (m *Manager) PurgeQueue(id string) (int, error) {
	var n int
	err := m.withChannel(id, func(ch *amqp.Channel, j config.ResolvedJob) error {
		var err error
		n, err = ch.QueuePurge(j.Queue, false)
		return err
	})
	return n, err
}

// PurgeDLQ removes all ready messages from the job's DLQ.
func (m *Manager) PurgeDLQ(id string) (int, error) {
	var n int
	err := m.withChannel(id, func(ch *amqp.Channel, j config.ResolvedJob) error {
		var err error
		n, err = ch.QueuePurge(j.DLQ, false)
		return err
	})
	return n, err
}

func peekCount(count int) (int, error) {
	if count == 0 {
		return defaultPeek, nil
	}
	if count < 0 || count > MaxPeek {
		return 0, fmt.Errorf("%w: count must be between 1 and %d", ErrInvalidArgument, MaxPeek)
	}
	return count, nil
}

// Peek returns up to count bodies from the main queue (0 means default).
// With ack=false the messages are returned to the queue when the channel closes.
func (m *Manager) Peek(id string, count int, ack bool) ([]string, error) {
	return m.get(id, count, ack, func(j config.ResolvedJob) string { return j.Queue })
}

// PeekDLQ returns up to count bodies from the DLQ without removing them.
func (m *Manager) PeekDLQ(id string, count int) ([]string, error) {
	return m.get(id, count, false, func(j config.ResolvedJob) string { return j.DLQ })
}

func (m *Manager) get(id string, count int, ack bool, queue func(config.ResolvedJob) string) ([]string, error) {
	count, err := peekCount(count)
	if err != nil {
		return nil, err
	}
	msgs := make([]string, 0, count)
	err = m.withChannel(id, func(ch *amqp.Channel, j config.ResolvedJob) error {
		for range count {
			d, ok, err := ch.Get(queue(j), ack)
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

// ReplayDLQ moves up to count DLQ messages back to the main queue. Each
// message is acked on the DLQ only after the broker confirms the republish.
func (m *Manager) ReplayDLQ(ctx context.Context, id string, count int) (int, error) {
	count, err := peekCount(count)
	if err != nil {
		return 0, err
	}
	replayed := 0
	err = m.withChannel(id, func(ch *amqp.Channel, j config.ResolvedJob) error {
		pub, err := broker.NewPublisher(ch)
		if err != nil {
			return err
		}
		for range count {
			d, ok, err := ch.Get(j.DLQ, false)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if err := pub.Publish(ctx, j.Queue, amqp.Publishing{
				Headers:         d.Headers,
				ContentType:     d.ContentType,
				ContentEncoding: d.ContentEncoding,
				Body:            d.Body,
				DeliveryMode:    amqp.Persistent,
			}); err != nil {
				// Unacked: returned to the DLQ when the channel closes.
				return fmt.Errorf("republish: %w", err)
			}
			if err := d.Ack(false); err != nil {
				return fmt.Errorf("ack dlq message: %w", err)
			}
			replayed++
		}
		return nil
	})
	return replayed, err
}
