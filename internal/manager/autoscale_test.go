package manager

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/worker"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testAutoscale() config.Autoscale {
	a := config.DefaultAutoscale()
	a.Enabled, a.Min, a.Max = true, 1, 10
	a.MessagesPerWorker = 100
	a.ScaleDownDelay = time.Minute
	return a
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name         string
		mutate       func(*config.Autoscale)
		cur, ready   int
		done, failed int64
		elapsed      time.Duration // since autoscale started for the pool
		wantTarget   int
		wantReason   string
	}{
		{name: "steady", cur: 2, ready: 150, wantTarget: 2},
		{name: "backlog scales up", cur: 2, ready: 501, wantTarget: 6, wantReason: reasonBacklog},
		{name: "capped at max", cur: 2, ready: 100000, wantTarget: 10, wantReason: reasonBacklog},
		{name: "step limit", mutate: func(a *config.Autoscale) { a.MaxStepUp = 2 }, cur: 2, ready: 900, wantTarget: 4, wantReason: reasonBacklog},
		{name: "failures hold scale up", cur: 2, ready: 900, done: 10, failed: 15, wantTarget: 2, wantReason: reasonFailures},
		{name: "few failures ignored", cur: 2, ready: 900, done: 0, failed: minFailureSamples - 1, wantTarget: 9, wantReason: reasonBacklog},
		{name: "ratio at limit allowed", cur: 2, ready: 900, done: 20, failed: 20, wantTarget: 9, wantReason: reasonBacklog},
		{name: "ratio 1 never blocks", mutate: func(a *config.Autoscale) { a.MaxFailureRatio = 1 }, cur: 2, ready: 900, failed: 100, wantTarget: 9, wantReason: reasonBacklog},
		{name: "scale down waits for delay", cur: 5, ready: 0, elapsed: 30 * time.Second, wantTarget: 5},
		{name: "scale down after delay", cur: 5, ready: 0, elapsed: time.Minute, wantTarget: 1, wantReason: reasonDrained},
		{name: "no delay", mutate: func(a *config.Autoscale) { a.ScaleDownDelay = 0 }, cur: 5, ready: 0, wantTarget: 1, wantReason: reasonDrained},
		{name: "min zero", mutate: func(a *config.Autoscale) { a.Min, a.ScaleDownDelay = 0, 0 }, cur: 1, ready: 0, wantTarget: 0, wantReason: reasonDrained},
		{name: "from zero", mutate: func(a *config.Autoscale) { a.Min = 0 }, cur: 0, ready: 1, wantTarget: 1, wantReason: reasonBacklog},
		{name: "below min", mutate: func(a *config.Autoscale) { a.Min = 3 }, cur: 0, ready: 0, wantTarget: 3, wantReason: reasonBelowMin},
		{name: "below min with backlog", cur: 0, ready: 450, wantTarget: 5, wantReason: reasonBacklog},
		{name: "below min while failing", mutate: func(a *config.Autoscale) { a.Min = 3 }, cur: 1, ready: 900, failed: 100, wantTarget: 3, wantReason: reasonBelowMin},
		{name: "above max at once", cur: 50, ready: 100000, wantTarget: 10, wantReason: reasonAboveMax},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := testAutoscale()
			if tt.mutate != nil {
				tt.mutate(&a)
			}
			s := &scaleState{since: t0}
			got, _, reason := s.decide(a, tt.cur, tt.ready, tt.done, tt.failed, t0.Add(tt.elapsed))
			if got != tt.wantTarget || reason != tt.wantReason {
				t.Fatalf("decide = %d %q, want %d %q", got, reason, tt.wantTarget, tt.wantReason)
			}
		})
	}
}

func TestDecideScalesDownToRecentPeak(t *testing.T) {
	a := testAutoscale()
	s := &scaleState{since: t0}
	// The backlog falls from 8 workers' worth to 3, then to nothing. Within
	// the 1m delay workers only drop to the peak of the last minute.
	steps := []struct {
		at    time.Duration
		ready int
	}{{0, 800}, {50 * time.Second, 300}, {70 * time.Second, 0}, {111 * time.Second, 0}}
	want := []int{8, 8, 3, 1}
	cur := 8
	for i, st := range steps {
		got, _, _ := s.decide(a, cur, st.ready, 0, 0, t0.Add(st.at))
		if got != want[i] {
			t.Fatalf("step %d (t=%s): target = %d, want %d", i, st.at, got, want[i])
		}
		cur = got
	}
}

// fakeReady serves queue depths to the autoscaler.
type fakeReady struct {
	mu    sync.Mutex
	ready map[string]int
	err   error
}

func (f *fakeReady) set(job string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ready[job] = n
}

func (f *fakeReady) fn(_ context.Context, jobs []config.ResolvedJob) []QueueStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]QueueStats, len(jobs))
	for i, j := range jobs {
		if f.err != nil {
			out[i] = QueueStats{Error: f.err.Error()}
			continue
		}
		out[i] = QueueStats{Messages: f.ready[j.LogicName]}
	}
	return out
}

func newAutoscaleManager(t *testing.T) (*Manager, *fakeReady) {
	t.Helper()
	jobs := testJobs()
	a := testAutoscale()
	a.Interval = 10 * time.Second
	jobs[2].Autoscale = a // pay-a
	m := New(jobs, Options{}, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { m.StopAll(t.Context()) })
	f := &fakeReady{ready: map[string]int{}}
	m.readyFn = f.fn
	return m, f
}

func workers(t *testing.T, m *Manager, id string) int {
	t.Helper()
	j, err := m.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return j.Workers
}

func TestAutoscaleOnce(t *testing.T) {
	m, f := newAutoscaleManager(t)
	states := map[*worker.Pool]*scaleState{}
	tick := func(d time.Duration) { m.autoscaleOnce(t.Context(), states, t0.Add(d)) }

	f.set("pay-a", 450)
	f.set("mail-a", 10000)
	tick(0)
	if got := workers(t, m, "pay-a"); got != 5 {
		t.Fatalf("pay-a workers = %d, want 5", got)
	}
	if got := workers(t, m, "mail-a"); got != 0 {
		t.Fatalf("mail-a has no autoscale but has %d workers", got)
	}

	// Not due before the interval.
	f.set("pay-a", 900)
	tick(5 * time.Second)
	if got := workers(t, m, "pay-a"); got != 5 {
		t.Fatalf("evaluated before interval: %d", got)
	}
	tick(10 * time.Second)
	if got := workers(t, m, "pay-a"); got != 9 {
		t.Fatalf("pay-a workers = %d, want 9", got)
	}

	// A failed depth query keeps the current count.
	f.err = errors.New("broker down")
	f.set("pay-a", 0)
	tick(100 * time.Second)
	if got := workers(t, m, "pay-a"); got != 9 {
		t.Fatalf("workers changed on depth error: %d", got)
	}
	f.err = nil
	tick(200 * time.Second)
	if got := workers(t, m, "pay-a"); got != 1 {
		t.Fatalf("pay-a workers = %d, want 1 after drain", got)
	}

	// A manual count pauses autoscale.
	paused, err := m.SetWorkers("pay-a", 7)
	if err != nil || !paused {
		t.Fatalf("SetWorkers = %v, %v; want paused", paused, err)
	}
	f.set("pay-a", 5000)
	tick(300 * time.Second)
	if j, _ := m.Get("pay-a"); j.Workers != 7 || j.Autoscale.Enabled {
		t.Fatalf("after manual change: workers=%d autoscale=%v", j.Workers, j.Autoscale.Enabled)
	}
	if len(states) != 0 {
		t.Fatalf("state kept for paused pool: %d", len(states))
	}

	// Enabling it again resumes scaling.
	if _, err := m.SetAutoscale("pay-a", AutoscalePatch{Enabled: new(true)}); err != nil {
		t.Fatal(err)
	}
	tick(310 * time.Second)
	if got := workers(t, m, "pay-a"); got != 10 {
		t.Fatalf("pay-a workers = %d, want max 10", got)
	}
}

func TestSetAutoscale(t *testing.T) {
	m, _ := newAutoscaleManager(t)
	if _, err := m.SetAutoscale("mail-a", AutoscalePatch{Enabled: new(true)}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("enable without max: err = %v", err)
	}
	if _, err := m.SetAutoscale("pay-a", AutoscalePatch{Min: new(20)}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("min above max: err = %v", err)
	}
	got, err := m.SetAutoscale("pay-a", AutoscalePatch{Max: new(32), ScaleDownDelaySeconds: new(0)})
	if err != nil {
		t.Fatal(err)
	}
	want := autoscaleStatus(testAutoscale())
	want.Max, want.ScaleDownDelaySeconds, want.IntervalSeconds = 32, 0, 10
	if got != want {
		t.Fatalf("status = %+v, want %+v", got, want)
	}
	if j, _ := m.Get("pay-a"); j.Autoscale != want {
		t.Fatalf("job autoscale = %+v", j.Autoscale)
	}
}

func TestManualWorkersWithoutAutoscale(t *testing.T) {
	m, _ := newAutoscaleManager(t)
	paused, err := m.SetWorkers("mail-a", 2)
	if err != nil || paused {
		t.Fatalf("SetWorkers = %v, %v", paused, err)
	}
}
