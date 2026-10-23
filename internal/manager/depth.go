package manager

import (
	"cmp"
	"context"
	"errors"
	"slices"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
)

// errInspectBusy is reported while an earlier inspection of the same server
// is still running (e.g. dialing an unreachable broker), so polling cannot
// pile up goroutines.
var errInspectBusy = errors.New("previous inspection of this server is still running")

// inspector identifies who queries queue depths. Each one has its own
// in-flight guard, so admin polling never makes the autoscaler skip a round.
type inspector string

const (
	inspectAdmin     inspector = "admin"
	inspectAutoscale inspector = "autoscale"
)

// inspectKey is a server being inspected by one inspector.
type inspectKey struct {
	by     inspector
	server string // broker.Key
}

// QueueStats is the broker's view of one queue.
type QueueStats struct {
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
	Error     string `json:"error,omitempty"`
}

// Depth holds the ready message counts of a job's main queue and DLQ.
type Depth struct {
	Queue QueueStats `json:"queue"`
	DLQ   QueueStats `json:"dlq"`
}

// ListWithDepth is List plus queue depths. Servers are inspected
// concurrently; queues not inspected before ctx ends report an error.
func (m *Manager) ListWithDepth(ctx context.Context) []JobStatus {
	pools := m.snapshot()
	out := make([]JobStatus, 0, len(pools))
	jobs := make([]config.ResolvedJob, 0, len(pools))
	for _, p := range pools {
		out = append(out, status(p))
		jobs = append(jobs, p.Job())
	}
	m.fillDepth(ctx, out, jobs)
	slices.SortFunc(out, func(a, b JobStatus) int { return cmp.Compare(a.LogicName, b.LogicName) })
	return out
}

// GetWithDepth is Get plus queue depths.
func (m *Manager) GetWithDepth(ctx context.Context, id string) (JobStatus, error) {
	p, err := m.pool(id)
	if err != nil {
		return JobStatus{}, err
	}
	out := []JobStatus{status(p)}
	m.fillDepth(ctx, out, []config.ResolvedJob{p.Job()})
	return out[0], nil
}

type inspectTarget struct {
	name   string
	srv    config.Server
	queues []string
}

type inspectResult struct {
	key   string
	stats map[string]QueueStats
}

func (m *Manager) fillDepth(ctx context.Context, out []JobStatus, jobs []config.ResolvedJob) {
	lookup := m.queueStats(ctx, inspectAdmin, jobs, func(j config.ResolvedJob) []string { return []string{j.Queue, j.DLQ} })
	for i, j := range jobs {
		out[i].Depth = &Depth{Queue: lookup(j, j.Queue), DLQ: lookup(j, j.DLQ)}
	}
}

// readyCounts returns the stats of each job's main queue, in order.
func (m *Manager) readyCounts(ctx context.Context, jobs []config.ResolvedJob) []QueueStats {
	lookup := m.queueStats(ctx, inspectAutoscale, jobs, func(j config.ResolvedJob) []string { return []string{j.Queue} })
	out := make([]QueueStats, len(jobs))
	for i, j := range jobs {
		out[i] = lookup(j, j.Queue)
	}
	return out
}

// queueStats inspects queues(j) of every job, one goroutine per server, and
// returns a lookup by job and queue name. A server still being inspected by
// the same inspector is reported busy rather than queried again.
func (m *Manager) queueStats(ctx context.Context, by inspector, jobs []config.ResolvedJob, queues func(config.ResolvedJob) []string) func(config.ResolvedJob, string) QueueStats {
	targets := make(map[string]*inspectTarget)
	for _, j := range jobs {
		key := broker.Key(j.ServerName, j.Server)
		t, ok := targets[key]
		if !ok {
			t = &inspectTarget{name: j.ServerName, srv: j.Server}
			targets[key] = t
		}
		t.queues = append(t.queues, queues(j)...)
	}

	results := make(chan inspectResult, len(targets))
	for key, t := range targets {
		ik := inspectKey{by: by, server: key}
		if !m.beginInspect(ik) {
			results <- inspectResult{key: key}
			continue
		}
		go func() {
			defer m.endInspect(ik)
			results <- inspectResult{key: key, stats: m.inspect(t)}
		}()
	}

	stats := make(map[string]map[string]QueueStats, len(targets))
	errs := make(map[string]string, len(targets))
collect:
	for range targets {
		select {
		case r := <-results:
			if r.stats == nil {
				errs[r.key] = errInspectBusy.Error()
				continue
			}
			stats[r.key] = r.stats
		case <-ctx.Done():
			break collect
		}
	}

	return func(j config.ResolvedJob, queue string) QueueStats {
		key := broker.Key(j.ServerName, j.Server)
		if msg, ok := errs[key]; ok {
			return QueueStats{Error: msg}
		}
		byQueue, ok := stats[key]
		if !ok {
			return QueueStats{Error: "timed out"}
		}
		return byQueue[queue]
	}
}

func (m *Manager) beginInspect(key inspectKey) bool {
	m.inspectMu.Lock()
	defer m.inspectMu.Unlock()
	if m.inspecting[key] {
		return false
	}
	m.inspecting[key] = true
	return true
}

func (m *Manager) endInspect(key inspectKey) {
	m.inspectMu.Lock()
	defer m.inspectMu.Unlock()
	delete(m.inspecting, key)
}

// inspect passively declares each queue on one channel. A failed declare
// (e.g. a missing queue) closes the channel, so the next queue reopens it.
func (m *Manager) inspect(t *inspectTarget) map[string]QueueStats {
	out := make(map[string]QueueStats, len(t.queues))
	var ch *amqp.Channel
	defer func() {
		if ch != nil {
			_ = ch.Close()
		}
	}()
	for _, q := range t.queues {
		if _, done := out[q]; done {
			continue
		}
		if ch == nil || ch.IsClosed() {
			var err error
			if ch, err = m.hub.Channel(t.name, t.srv); err != nil {
				// The connection is down; every remaining queue fails the same way.
				for _, rest := range t.queues {
					if _, done := out[rest]; !done {
						out[rest] = QueueStats{Error: err.Error()}
					}
				}
				return out
			}
		}
		info, err := ch.QueueDeclarePassive(q, false, false, false, false, nil)
		if err != nil {
			out[q] = QueueStats{Error: err.Error()}
			continue
		}
		out[q] = QueueStats{Messages: info.Messages, Consumers: info.Consumers}
	}
	return out
}
