package manager

import (
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"consumer-dispatcher/internal/config"
)

func TestClassify(t *testing.T) {
	base := testJobs()[0]
	tests := []struct {
		name   string
		mutate func(*config.ResolvedJob)
		want   changeKind
	}{
		{name: "same", mutate: func(*config.ResolvedJob) {}, want: unchanged},
		{name: "count only", mutate: func(j *config.ResolvedJob) { j.Count = 5 }, want: scaled},
		{name: "autoscale only", mutate: func(j *config.ResolvedJob) { j.Autoscale.Max = 8 }, want: scaled},
		{name: "autoscale and url", mutate: func(j *config.ResolvedJob) { j.Autoscale.Max = 8; j.URL = "http://new/d" }, want: updated},
		{name: "url", mutate: func(j *config.ResolvedJob) { j.URL = "http://new/d" }, want: updated},
		{name: "timeout and count", mutate: func(j *config.ResolvedJob) { j.Timeout = time.Minute; j.Count = 3 }, want: updated},
		{name: "block patterns", mutate: func(j *config.ResolvedJob) { j.BlockPatterns = "x" }, want: updated},
		{name: "queue", mutate: func(j *config.ResolvedJob) { j.Queue = "other" }, want: recreated},
		{name: "exchange", mutate: func(j *config.ResolvedJob) { j.Exchange = "ex" }, want: recreated},
		{name: "server password", mutate: func(j *config.ResolvedJob) { j.Server.Password = "new" }, want: recreated},
		{name: "queue and url", mutate: func(j *config.ResolvedJob) { j.Queue = "other"; j.URL = "http://new/d" }, want: recreated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur := base
			tt.mutate(&cur)
			if got := classify(base, cur); got != tt.want {
				t.Fatalf("classify = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReloadNotRunning(t *testing.T) {
	m := newTestManager(t)
	if _, err := m.Reload(t.Context(), testJobs(), Options{}, false); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("err = %v, want ErrNotRunning", err)
	}
}

// reloadJobs is testJobs with: mail-b's queue changed (recreated), pay-a's
// retry_limit changed (updated), mail-a unchanged and sms-a added.
func reloadJobs() []config.ResolvedJob {
	jobs := testJobs()
	jobs[1].Queue = "mail_v2"
	jobs[2].RetryLimit = 4
	sms := jobs[0]
	sms.Name, sms.LogicName, sms.Queue, sms.DLQ = "sms", "sms-a", "sms", "sms.dlq"
	return append(jobs, sms)
}

func names(m *Manager) []string {
	var out []string
	for _, j := range m.List() {
		out = append(out, j.LogicName)
	}
	return out
}

func TestReload(t *testing.T) {
	initial := append(testJobs(), config.ResolvedJob{
		Name: "old", LogicName: "old-a", ServerName: "a", Server: testJobs()[0].Server,
		Queue: "old", DLQ: "old.dlq", URL: "http://consumer/d", Timeout: time.Second,
	})
	m := New(initial, Options{}, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { m.StopAll(t.Context()) })
	m.StartAll()

	// Admin API changes: pay-a's URL is not touched by the file, so it survives.
	if err := m.SetURL("pay-a", "http://runtime/d"); err != nil {
		t.Fatal(err)
	}

	want := JobChanges{
		Added: []string{"sms-a"}, Removed: []string{"old-a"}, Updated: []string{"pay-a"},
		Scaled: []string{}, Recreated: []string{"mail-b"}, Unchanged: 1,
	}
	check := func(got JobChanges) {
		t.Helper()
		if !slices.Equal(got.Added, want.Added) || !slices.Equal(got.Removed, want.Removed) ||
			!slices.Equal(got.Updated, want.Updated) || !slices.Equal(got.Scaled, want.Scaled) ||
			!slices.Equal(got.Recreated, want.Recreated) || got.Unchanged != want.Unchanged {
			t.Fatalf("changes = %+v, want %+v", got, want)
		}
	}

	t.Run("dry run changes nothing", func(t *testing.T) {
		got, err := m.Reload(t.Context(), reloadJobs(), Options{}, true)
		if err != nil {
			t.Fatal(err)
		}
		check(got)
		if n := names(m); !slices.Equal(n, []string{"mail-a", "mail-b", "old-a", "pay-a"}) {
			t.Fatalf("jobs = %v", n)
		}
	})

	t.Run("apply", func(t *testing.T) {
		got, err := m.Reload(t.Context(), reloadJobs(), Options{}, false)
		if err != nil {
			t.Fatal(err)
		}
		check(got)
		if n := names(m); !slices.Equal(n, []string{"mail-a", "mail-b", "pay-a", "sms-a"}) {
			t.Fatalf("jobs = %v", n)
		}
		pay, _ := m.Get("pay-a")
		if pay.URL != "http://runtime/d" || pay.RetryLimit != 4 {
			t.Fatalf("pay-a = %+v, want runtime URL kept and retry_limit 4", pay)
		}
		if mail, _ := m.Get("mail-b"); mail.Queue != "mail_v2" {
			t.Fatalf("mail-b queue = %q", mail.Queue)
		}
		if _, err := m.Get("old-a"); !errors.Is(err, ErrJobNotFound) {
			t.Fatalf("old-a err = %v", err)
		}
	})

	t.Run("same file again is a no-op", func(t *testing.T) {
		got, err := m.Reload(t.Context(), reloadJobs(), Options{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if got.Changed() || got.Unchanged != 4 {
			t.Fatalf("changes = %+v", got)
		}
	})
}

func TestReloadScales(t *testing.T) {
	m := New(testJobs(), Options{}, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { m.StopAll(t.Context()) })
	m.StartAll()

	// A runtime scale is kept while the file count does not change...
	if _, err := m.SetWorkers("mail-a", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reload(t.Context(), testJobs(), Options{}, false); err != nil {
		t.Fatal(err)
	}
	if j, _ := m.Get("mail-a"); j.Workers != 3 {
		t.Fatalf("workers = %d, want runtime value 3", j.Workers)
	}

	// ...and replaced once it does.
	jobs := testJobs()
	jobs[0].Count = 2
	got, err := m.Reload(t.Context(), jobs, Options{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Scaled, []string{"mail-a"}) {
		t.Fatalf("changes = %+v", got)
	}
	if j, _ := m.Get("mail-a"); j.Workers != 2 {
		t.Fatalf("workers = %d, want 2", j.Workers)
	}
}

func TestReloadAutoscale(t *testing.T) {
	m := New(testJobs(), Options{}, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { m.StopAll(t.Context()) })
	m.StartAll()

	reload := func(jobs []config.ResolvedJob) {
		t.Helper()
		if _, err := m.Reload(t.Context(), jobs, Options{}, false); err != nil {
			t.Fatal(err)
		}
	}
	get := func() JobStatus {
		t.Helper()
		j, err := m.Get("pay-a")
		if err != nil {
			t.Fatal(err)
		}
		return j
	}

	// Turned on in the file: settings applied, workers left to the autoscaler.
	on := testJobs()
	on[2].Count = 2
	on[2].Autoscale = testAutoscale()
	reload(on)
	if j := get(); !j.Autoscale.Enabled || j.Autoscale.Max != 10 || j.Workers != 0 {
		t.Fatalf("after enabling: %+v", j)
	}

	// Paused by a manual change; a file change to another autoscale field
	// keeps it paused.
	if _, err := m.SetWorkers("pay-a", 4); err != nil {
		t.Fatal(err)
	}
	on2 := testJobs()
	on2[2].Count = 2
	on2[2].Autoscale = testAutoscale()
	on2[2].Autoscale.Max = 20
	reload(on2)
	if j := get(); j.Autoscale.Enabled || j.Autoscale.Max != 20 || j.Workers != 4 {
		t.Fatalf("after max change: %+v", j)
	}

	// Turned off in the file: back to count.
	if _, err := m.SetAutoscale("pay-a", AutoscalePatch{Enabled: new(true)}); err != nil {
		t.Fatal(err)
	}
	off := testJobs()
	off[2].Count = 2
	reload(off)
	if j := get(); j.Autoscale.Enabled || j.Workers != 2 {
		t.Fatalf("after disabling: %+v", j)
	}
}
