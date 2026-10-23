package manager

import (
	"context"
	"fmt"
	"slices"
	"time"

	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/metrics"
	"consumer-dispatcher/internal/worker"
)

const (
	// autoscaleTick is how often the autoscaler checks which jobs are due;
	// each job is evaluated every autoscale.interval_seconds.
	autoscaleTick = time.Second
	// autoscaleQueryTimeout bounds one round of queue depth queries.
	autoscaleQueryTimeout = 5 * time.Second
	// minFailureSamples is how many messages must finish within one interval
	// before the failure ratio can block scaling up, so a couple of failures
	// on a quiet queue do not count as a failing consumer.
	minFailureSamples = 20
)

// Reasons logged with a scaling decision.
const (
	reasonBacklog  = "backlog"
	reasonDrained  = "drained"
	reasonBelowMin = "below_min"
	reasonAboveMax = "above_max"
	reasonFailures = "failure_ratio"
)

// AutoscaleStatus is the admin view of a job's autoscale settings.
type AutoscaleStatus struct {
	Enabled               bool    `json:"enabled"`
	Min                   int     `json:"min"`
	Max                   int     `json:"max"`
	MessagesPerWorker     int     `json:"messages_per_worker"`
	IntervalSeconds       int     `json:"interval_seconds"`
	ScaleDownDelaySeconds int     `json:"scale_down_delay_seconds"`
	MaxStepUp             int     `json:"max_step_up"`
	MaxFailureRatio       float64 `json:"max_failure_ratio"`
}

func autoscaleStatus(a config.Autoscale) AutoscaleStatus {
	return AutoscaleStatus{
		Enabled:               a.Enabled,
		Min:                   a.Min,
		Max:                   a.Max,
		MessagesPerWorker:     a.MessagesPerWorker,
		IntervalSeconds:       int(a.Interval / time.Second),
		ScaleDownDelaySeconds: int(a.ScaleDownDelay / time.Second),
		MaxStepUp:             a.MaxStepUp,
		MaxFailureRatio:       a.MaxFailureRatio,
	}
}

// AutoscalePatch changes some autoscale settings of a job; nil fields are kept.
type AutoscalePatch struct {
	Enabled               *bool    `json:"enabled"`
	Min                   *int     `json:"min"`
	Max                   *int     `json:"max"`
	MessagesPerWorker     *int     `json:"messages_per_worker"`
	IntervalSeconds       *int     `json:"interval_seconds"`
	ScaleDownDelaySeconds *int     `json:"scale_down_delay_seconds"`
	MaxStepUp             *int     `json:"max_step_up"`
	MaxFailureRatio       *float64 `json:"max_failure_ratio"`
}

func (p AutoscalePatch) apply(a config.Autoscale) config.Autoscale {
	set := func(dst *int, v *int) {
		if v != nil {
			*dst = *v
		}
	}
	if p.Enabled != nil {
		a.Enabled = *p.Enabled
	}
	set(&a.Min, p.Min)
	set(&a.Max, p.Max)
	set(&a.MessagesPerWorker, p.MessagesPerWorker)
	set(&a.MaxStepUp, p.MaxStepUp)
	if p.IntervalSeconds != nil {
		a.Interval = time.Duration(*p.IntervalSeconds) * time.Second
	}
	if p.ScaleDownDelaySeconds != nil {
		a.ScaleDownDelay = time.Duration(*p.ScaleDownDelaySeconds) * time.Second
	}
	if p.MaxFailureRatio != nil {
		a.MaxFailureRatio = *p.MaxFailureRatio
	}
	return a
}

// SetAutoscale changes a job's autoscale settings without restarting its
// workers. When autoscale is turned off the current worker count is kept.
func (m *Manager) SetAutoscale(id string, patch AutoscalePatch) (AutoscaleStatus, error) {
	p, err := m.pool(id)
	if err != nil {
		return AutoscaleStatus{}, err
	}
	a := patch.apply(p.Job().Autoscale)
	if err := a.Validate(); err != nil {
		return AutoscaleStatus{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	p.SetAutoscale(a)
	return autoscaleStatus(a), nil
}

// scaleState is the autoscaler's memory of one pool. It is only used by the
// autoscaler goroutine.
type scaleState struct {
	next  time.Time // next evaluation
	since time.Time // when autoscale was (re)enabled for the pool
	// samples are the desired counts within the scale-down delay; workers
	// are only removed down to their peak.
	samples []sample
	// completed and failed are the pool counters at the last evaluation.
	completed, failed int64

	blocked  bool // scale-up is held by the failure ratio
	depthErr bool // the last depth query failed
}

type sample struct {
	at      time.Time
	desired int
}

// decide returns the worker count for cur workers, ready queued messages and
// the done/failed messages finished since the previous evaluation, along with
// the desired count and the reason (empty when nothing changes).
func (s *scaleState) decide(a config.Autoscale, cur, ready int, done, failed int64, now time.Time) (target, desired int, reason string) {
	raw := (ready + a.MessagesPerWorker - 1) / a.MessagesPerWorker
	desired = min(max(raw, a.Min), a.Max)

	s.samples = append(s.samples, sample{at: now, desired: desired})
	cutoff := now.Add(-a.ScaleDownDelay)
	keep := slices.IndexFunc(s.samples, func(x sample) bool { return !x.at.Before(cutoff) })
	s.samples = slices.Delete(s.samples, 0, keep)

	switch {
	case cur > a.Max:
		return a.Max, desired, reasonAboveMax
	case desired > cur:
		target, reason = desired, reasonBacklog
		if raw < a.Min {
			reason = reasonBelowMin
		}
		if finished := done + failed; finished >= minFailureSamples && float64(failed)/float64(finished) > a.MaxFailureRatio {
			target, reason = cur, reasonFailures
		} else if a.MaxStepUp > 0 {
			target = min(target, cur+a.MaxStepUp)
		}
		// Min is a floor even while the consumer is failing.
		if target < a.Min {
			target, reason = a.Min, reasonBelowMin
		}
		return target, desired, reason
	case desired < cur:
		if now.Sub(s.since) < a.ScaleDownDelay {
			return cur, desired, ""
		}
		peak := 0
		for _, x := range s.samples {
			peak = max(peak, x.desired)
		}
		if peak < cur {
			return peak, desired, reasonDrained
		}
	}
	return cur, desired, ""
}

// startAutoscaler runs the autoscaler until StopAll.
func (m *Manager) startAutoscaler() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	m.stopScaler = func() {
		cancel()
		<-done
	}
	go func() {
		defer close(done)
		states := make(map[*worker.Pool]*scaleState)
		t := time.NewTicker(autoscaleTick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				m.autoscaleOnce(ctx, states, now)
			}
		}
	}()
}

// autoscaleOnce evaluates every pool that has autoscale enabled and is due.
func (m *Manager) autoscaleOnce(ctx context.Context, states map[*worker.Pool]*scaleState, now time.Time) {
	var (
		due  []*worker.Pool
		jobs []config.ResolvedJob
		live = make(map[string]bool)
	)
	for _, p := range m.snapshot() {
		j := p.Job()
		if !j.Autoscale.Enabled {
			continue
		}
		live[j.LogicName] = true
		s := states[p]
		if s == nil {
			done, failed, _ := p.Stats()
			s = &scaleState{next: now, since: now, completed: done, failed: failed}
			states[p] = s
		}
		if now.Before(s.next) {
			continue
		}
		s.next = now.Add(j.Autoscale.Interval)
		due = append(due, p)
		jobs = append(jobs, j)
	}
	// Forget pools that were removed, recreated or had autoscale turned off;
	// turning it on again starts afresh.
	for p := range states {
		if j := p.Job(); !j.Autoscale.Enabled || !m.hasPool(p) {
			delete(states, p)
			if !live[j.LogicName] {
				metrics.QueueReady.DeleteLabelValues(j.LogicName)
				metrics.AutoscaleDesired.DeleteLabelValues(j.LogicName)
			}
		}
	}
	if len(due) == 0 {
		return
	}

	qctx, cancel := context.WithTimeout(ctx, autoscaleQueryTimeout)
	ready := m.readyFn(qctx, jobs)
	cancel()
	for i, p := range due {
		m.evaluate(p, jobs[i], states[p], ready[i], now)
	}
}

func (m *Manager) hasPool(p *worker.Pool) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pools[p.Job().LogicName] == p
}

func (m *Manager) evaluate(p *worker.Pool, j config.ResolvedJob, s *scaleState, q QueueStats, now time.Time) {
	done, failed, cur := p.Stats()
	dDone, dFailed := done-s.completed, failed-s.failed
	s.completed, s.failed = done, failed
	log := m.log.With("job", j.LogicName)

	if q.Error != "" {
		if !s.depthErr {
			log.Warn("autoscale: queue depth unavailable, keeping workers", "workers", cur, "err", q.Error)
		}
		s.depthErr = true
		return
	}
	s.depthErr = false

	target, desired, reason := s.decide(j.Autoscale, cur, q.Messages, dDone, dFailed, now)
	metrics.QueueReady.WithLabelValues(j.LogicName).Set(float64(q.Messages))
	metrics.AutoscaleDesired.WithLabelValues(j.LogicName).Set(float64(desired))

	if blocked := reason == reasonFailures; blocked != s.blocked {
		s.blocked = blocked
		if blocked {
			log.Warn("autoscale: scale-up held, consumer failure ratio too high",
				"workers", cur, "desired", desired, "ready", q.Messages,
				"failed", dFailed, "completed", dDone, "max_failure_ratio", j.Autoscale.MaxFailureRatio)
		} else {
			log.Info("autoscale: scale-up resumed", "failed", dFailed, "completed", dDone)
		}
	}
	if target == cur {
		return
	}
	if p.Autoscale(target) {
		log.Info("autoscale", "from", cur, "to", target, "ready", q.Messages, "reason", reason)
	}
}
