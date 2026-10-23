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
	targets := make(map[string]*inspectTarget)
	for _, j := range jobs {
		key := broker.Key(j.ServerName, j.Server)
		t, ok := targets[key]
		if !ok {
			t = &inspectTarget{name: j.ServerName, srv: j.Server}
			targets[key] = t
		}
		t.queues = append(t.queues, j.Queue, j.DLQ)
	}

	results := make(chan inspectResult, len(targets))
	for key, t := range targets {
		if !m.beginInspect(key) {
			results <- inspectResult{key: key}
			continue
		}
		go func() {
			defer m.endInspect(key)
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

	lookup := func(key, queue string) QueueStats {
		if msg, ok := errs[key]; ok {
			return QueueStats{Error: msg}
		}
		byQueue, ok := stats[key]
		if !ok {
			return QueueStats{Error: "timed out"}
		}
		return byQueue[queue]
	}
	for i, j := range jobs {
		key := broker.Key(j.ServerName, j.Server)
		out[i].Depth = &Depth{Queue: lookup(key, j.Queue), DLQ: lookup(key, j.DLQ)}
	}
}

func (m *Manager) beginInspect(key string) bool {
	m.inspectMu.Lock()
	defer m.inspectMu.Unlock()
	if m.inspecting[key] {
		return false
	}
	m.inspecting[key] = true
	return true
}

func (m *Manager) endInspect(key string) {
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
