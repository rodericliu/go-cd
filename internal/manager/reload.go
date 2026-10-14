package manager

import (
	"context"
	"errors"
	"slices"
	"time"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/worker"
)

// reloadStopTimeout bounds how long a reload waits for removed or recreated
// jobs to finish their in-flight messages; unfinished ones are requeued.
const reloadStopTimeout = 15 * time.Second

// ErrNotRunning is returned by Reload before StartAll or after StopAll.
var ErrNotRunning = errors.New("manager is not running")

// JobChanges lists, by logic name, what a reload changes.
type JobChanges struct {
	// Added jobs start consuming.
	Added []string `json:"added"`
	// Removed jobs stop after finishing in-flight messages.
	Removed []string `json:"removed"`
	// Updated jobs changed runtime settings (url, timeout, retry_limit, ...);
	// their workers are replaced gracefully.
	Updated []string `json:"updated"`
	// Scaled jobs only changed count.
	Scaled []string `json:"scaled"`
	// Recreated jobs changed queue, topology or server and are rebuilt from
	// the file; changes made through the admin API are dropped.
	Recreated []string `json:"recreated"`
	Unchanged int      `json:"unchanged"`
}

// Changed reports whether any job is affected.
func (c JobChanges) Changed() bool {
	return len(c.Added)+len(c.Removed)+len(c.Updated)+len(c.Scaled)+len(c.Recreated) > 0
}

type changeKind int

const (
	unchanged changeKind = iota
	scaled
	updated
	recreated
)

// classify compares the previous and new file config of one job.
func classify(old, cur config.ResolvedJob) changeKind {
	if old == cur {
		return unchanged
	}
	// If the jobs only differ in runtime settings, the pool can be updated in place.
	o := old
	copyRuntime(&o, cur)
	if o != cur {
		return recreated
	}
	o = old
	o.Count = cur.Count
	if o == cur {
		return scaled
	}
	return updated
}

// copyRuntime copies the settings that workers can pick up without
// redeclaring topology or reconnecting.
func copyRuntime(dst *config.ResolvedJob, src config.ResolvedJob) {
	dst.URL = src.URL
	dst.URLHost = src.URLHost
	dst.Encoding = src.Encoding
	dst.Timeout = src.Timeout
	dst.Count = src.Count
	dst.RetryLimit = src.RetryLimit
	dst.Prefetch = src.Prefetch
	dst.BlockPatterns = src.BlockPatterns
	dst.DiscardMatchJobs = src.DiscardMatchJobs
}

// applyFileChanges sets on dst only the runtime settings that changed in the
// file between old and cur, keeping admin API changes to the others. Count is
// handled separately because it is applied by scaling.
func applyFileChanges(dst *config.ResolvedJob, old, cur config.ResolvedJob) {
	if old.URL != cur.URL {
		dst.URL = cur.URL
	}
	if old.URLHost != cur.URLHost {
		dst.URLHost = cur.URLHost
	}
	if old.Encoding != cur.Encoding {
		dst.Encoding = cur.Encoding
	}
	if old.Timeout != cur.Timeout {
		dst.Timeout = cur.Timeout
	}
	if old.RetryLimit != cur.RetryLimit {
		dst.RetryLimit = cur.RetryLimit
	}
	if old.Prefetch != cur.Prefetch {
		dst.Prefetch = cur.Prefetch
	}
	if old.BlockPatterns != cur.BlockPatterns {
		dst.BlockPatterns = cur.BlockPatterns
	}
	if old.DiscardMatchJobs != cur.DiscardMatchJobs {
		dst.DiscardMatchJobs = cur.DiscardMatchJobs
	}
}

func sameOptions(a, b Options) bool {
	fa, fb := a.FailedBody, b.FailedBody
	return fa.Enabled == fb.Enabled && fa.MaxBytes == fb.MaxBytes && fa.Rate() == fb.Rate()
}

type reloadPlan struct {
	changes JobChanges
	kinds   map[string]changeKind
	added   []config.ResolvedJob
}

func (m *Manager) plan(jobs []config.ResolvedJob) reloadPlan {
	p := reloadPlan{kinds: make(map[string]changeKind, len(jobs))}
	// Non-nil so the API returns [] rather than null.
	c := &p.changes
	c.Added, c.Removed, c.Updated, c.Scaled, c.Recreated = []string{}, []string{}, []string{}, []string{}, []string{}
	next := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		next[j.LogicName] = true
		old, ok := m.file[j.LogicName]
		if !ok {
			p.added = append(p.added, j)
			p.changes.Added = append(p.changes.Added, j.LogicName)
			continue
		}
		k := classify(old, j)
		p.kinds[j.LogicName] = k
		switch k {
		case unchanged:
			p.changes.Unchanged++
		case scaled:
			p.changes.Scaled = append(p.changes.Scaled, j.LogicName)
		case updated:
			p.changes.Updated = append(p.changes.Updated, j.LogicName)
		case recreated:
			p.changes.Recreated = append(p.changes.Recreated, j.LogicName)
		}
	}
	for name := range m.file {
		if !next[name] {
			p.changes.Removed = append(p.changes.Removed, name)
		}
	}
	for _, s := range [][]string{p.changes.Added, p.changes.Removed, p.changes.Updated, p.changes.Scaled, p.changes.Recreated} {
		slices.Sort(s)
	}
	return p
}

// Reload applies a newly loaded job list. Jobs are matched by logic name and
// compared with the previous file config, so settings changed through the
// admin API survive unless the same setting changed in the file. With dryRun
// it only reports what would change. opts changes (log.failed_body) replace
// the workers of every job.
func (m *Manager) Reload(ctx context.Context, jobs []config.ResolvedJob, opts Options, dryRun bool) (JobChanges, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if !m.ready.Load() {
		return JobChanges{}, ErrNotRunning
	}

	p := m.plan(jobs)
	if dryRun {
		return p.changes, nil
	}

	byName := make(map[string]config.ResolvedJob, len(jobs))
	for _, j := range jobs {
		byName[j.LogicName] = j
	}

	// 1. Stop removed and recreated jobs. They leave the map first so admin
	// calls no longer reach them while they drain.
	stop := slices.Concat(p.changes.Removed, p.changes.Recreated)
	m.mu.Lock()
	stopping := make([]*worker.Pool, 0, len(stop))
	for _, name := range stop {
		stopping = append(stopping, m.pools[name])
		delete(m.pools, name)
	}
	m.mu.Unlock()
	stopCtx, cancel := context.WithTimeout(ctx, reloadStopTimeout)
	shutdownPools(stopCtx, stopping)
	cancel()
	for _, name := range p.changes.Removed {
		m.log.Info("reload: job removed", "job", name)
	}

	// 2. Allow longer timeouts before new or updated jobs use them.
	m.client.SetSafetyTimeout(clientTimeout(jobs))

	// 3. Existing jobs: options, runtime settings, scaling.
	optsChanged := !sameOptions(m.opts, opts)
	for _, pool := range m.snapshot() {
		name := pool.Job().LogicName
		cur := byName[name]
		if optsChanged {
			pool.UpdateOptions(opts)
		}
		switch p.kinds[name] {
		case updated:
			old := m.file[name]
			pool.UpdateJob(func(j *config.ResolvedJob) { applyFileChanges(j, old, cur) })
			m.log.Info("reload: job updated", "job", cur.String())
		case scaled:
			m.log.Info("reload: job scaled", "job", name, "workers", cur.Count)
		}
		if k := p.kinds[name]; (k == updated || k == scaled) && m.file[name].Count != cur.Count {
			pool.SetWorkers(cur.Count)
		}
	}

	// 4. Start added and recreated jobs.
	var start []config.ResolvedJob
	start = append(start, p.added...)
	for _, name := range p.changes.Recreated {
		start = append(start, byName[name])
	}
	m.mu.Lock()
	for _, j := range start {
		pool := worker.NewPool(j, m.hub, m.client, opts, m.log)
		m.pools[j.LogicName] = pool
		pool.SetWorkers(j.Count)
		m.log.Info("reload: job started", "job", j.String())
	}
	m.mu.Unlock()

	m.file = byName
	m.opts = opts

	// 5. Close connections no job uses any more.
	inUse := make(map[string]bool)
	for _, j := range jobs {
		inUse[broker.Key(j.ServerName, j.Server)] = true
	}
	for _, key := range m.hub.Prune(inUse) {
		m.log.Info("reload: amqp connection closed", "amqp", key)
	}
	return p.changes, nil
}
