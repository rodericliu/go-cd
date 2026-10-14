package manager

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"consumer-dispatcher/internal/config"
)

func testJobs() []config.ResolvedJob {
	srv := config.Server{Host: "127.0.0.1", Port: 1, Username: "u", VHost: "/", ReconnectSeconds: 30}
	job := func(name, server string) config.ResolvedJob {
		return config.ResolvedJob{
			Name: name, LogicName: name + "-" + server, ServerName: server, Server: srv,
			Queue: name, DLQ: name + ".dlq", URL: "http://consumer/d", Timeout: time.Second,
		}
	}
	return []config.ResolvedJob{job("mail", "a"), job("mail", "b"), job("pay", "a")}
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m := New(testJobs(), slog.New(slog.DiscardHandler))
	t.Cleanup(func() { m.StopAll(t.Context()) })
	return m
}

func TestResolveJob(t *testing.T) {
	m := newTestManager(t)
	tests := []struct {
		id        string
		wantLogic string
		wantErr   error
	}{
		{id: "mail-a", wantLogic: "mail-a"},
		{id: "mail-b", wantLogic: "mail-b"},
		{id: "pay", wantLogic: "pay-a"}, // bare name, single server
		{id: "mail", wantErr: ErrAmbiguousJob},
		{id: "nope", wantErr: ErrJobNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			// Repeat to catch nondeterministic map-order resolution.
			for range 20 {
				got, err := m.Get(tt.id)
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Fatalf("err = %v, want %v", err, tt.wantErr)
					}
					continue
				}
				if err != nil || got.LogicName != tt.wantLogic {
					t.Fatalf("got %q, %v; want %q", got.LogicName, err, tt.wantLogic)
				}
			}
		})
	}
}

func TestAdminInputValidation(t *testing.T) {
	m := newTestManager(t)
	tests := []struct {
		name string
		call func() error
	}{
		{"workers negative", func() error { return m.SetWorkers("pay", -1) }},
		{"workers too many", func() error { return m.SetWorkers("pay", MaxWorkers+1) }},
		{"url relative", func() error { return m.SetURL("pay", "/dispatch") }},
		{"url bad scheme", func() error { return m.SetURL("pay", "ftp://x/d") }},
		{"bad regexp", func() error { return m.SetBlockPatterns("pay", "(") }},
		{"peek too many", func() error { _, err := m.Peek("pay", MaxPeek+1, false); return err }},
		{"peek negative", func() error { _, err := m.PeekDLQ("pay", -1); return err }},
		{"replay too many", func() error { _, err := m.ReplayDLQ(t.Context(), "pay", MaxPeek+1); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if got, _ := m.Get("pay"); got.URL != "http://consumer/d" || got.BlockPatterns != "" {
		t.Fatalf("invalid input modified job: %+v", got)
	}
}

func TestSetWorkersAndUpdates(t *testing.T) {
	m := newTestManager(t)
	if err := m.SetWorkers("mail-b", 3); err != nil {
		t.Fatal(err)
	}
	if err := m.SetURL("mail-b", "https://new.example/d"); err != nil {
		t.Fatal(err)
	}
	b, _ := m.Get("mail-b")
	a, _ := m.Get("mail-a")
	if b.Workers != 3 || b.URL != "https://new.example/d" {
		t.Fatalf("mail-b = %+v", b)
	}
	if a.Workers != 0 || a.URL != "http://consumer/d" {
		t.Fatalf("mail-a was modified: %+v", a)
	}
	if err := m.SetWorkers("mail", 1); !errors.Is(err, ErrAmbiguousJob) {
		t.Fatalf("err = %v, want ErrAmbiguousJob", err)
	}
}

func TestListSorted(t *testing.T) {
	m := newTestManager(t)
	got := m.List()
	want := []string{"mail-a", "mail-b", "pay-a"}
	if len(got) != len(want) {
		t.Fatalf("len = %d", len(got))
	}
	for i, w := range want {
		if got[i].LogicName != w {
			t.Fatalf("List()[%d] = %q, want %q", i, got[i].LogicName, w)
		}
	}
}
