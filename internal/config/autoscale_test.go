package config

import (
	"strings"
	"testing"
	"time"
)

func TestAutoscaleInheritance(t *testing.T) {
	jobs := loadJobs(t, `
defaults:
  url: "http://x/d"
  count: 3
  autoscale: {max: 16, messages_per_worker: 50, max_failure_ratio: 0.8}
servers:
  a: {host: h, port: 5672, username: u, password: p, vhost: /}
jobs:
  - name: auto
    server: a
    autoscale: {enabled: true, min: 0, scale_down_delay_seconds: 0}
  - {name: fixed, server: a}
`)
	want := Autoscale{
		Enabled: true, Min: 0, Max: 16, MessagesPerWorker: 50,
		Interval: 10 * time.Second, ScaleDownDelay: 0, MaxFailureRatio: 0.8,
	}
	if got := jobs[0].Autoscale; got != want {
		t.Fatalf("auto = %+v, want %+v", got, want)
	}
	if a := jobs[1].Autoscale; a.Enabled || a.Min != 1 || a.ScaleDownDelay != 2*time.Minute {
		t.Fatalf("fixed = %+v", a)
	}
	if jobs[0].InitialWorkers() != 3 || jobs[1].InitialWorkers() != 3 {
		t.Fatalf("initial = %d, %d", jobs[0].InitialWorkers(), jobs[1].InitialWorkers())
	}
}

func TestInitialWorkersClamped(t *testing.T) {
	j := ResolvedJob{Count: 1, Autoscale: Autoscale{Enabled: true, Min: 2, Max: 4}}
	if got := j.InitialWorkers(); got != 2 {
		t.Fatalf("below min: %d", got)
	}
	j.Count = 9
	if got := j.InitialWorkers(); got != 4 {
		t.Fatalf("above max: %d", got)
	}
	j.Autoscale.Enabled = false
	if got := j.InitialWorkers(); got != 9 {
		t.Fatalf("disabled: %d", got)
	}
}

func TestAutoscaleValidation(t *testing.T) {
	tests := []struct {
		name    string
		as      string
		wantErr string
	}{
		{name: "enabled without max", as: `{enabled: true}`, wantErr: "autoscale.max is required"},
		{name: "min above max", as: `{enabled: true, min: 5, max: 2}`, wantErr: "min must be <= "},
		{name: "max too big", as: `{max: 1000}`, wantErr: "autoscale.max"},
		{name: "zero per worker", as: `{messages_per_worker: 0}`, wantErr: "messages_per_worker"},
		{name: "zero interval", as: `{interval_seconds: 0}`, wantErr: "interval_seconds"},
		{name: "negative step", as: `{max_step_up: -1}`, wantErr: "max_step_up"},
		{name: "ratio zero", as: `{max_failure_ratio: 0}`, wantErr: "max_failure_ratio"},
		{name: "ratio above one", as: `{max_failure_ratio: 1.5}`, wantErr: "max_failure_ratio"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Load(writeConfig(t, `
defaults: {url: "http://x/d"}
servers:
  a: {host: h, port: 5672, username: u, password: p, vhost: /}
jobs:
  - {name: j, server: a, autoscale: `+tt.as+"}\n"))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
	// Disabled settings without max are fine.
	if err := DefaultAutoscale().Validate(); err != nil {
		t.Fatal(err)
	}
}
