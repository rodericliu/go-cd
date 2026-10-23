package manager

import (
	"context"
	"testing"

	"consumer-dispatcher/internal/broker"
)

func TestListWithDepthUnreachable(t *testing.T) {
	m := newTestManager(t)
	got := m.ListWithDepth(t.Context())
	if len(got) != 3 || got[0].LogicName != "mail-a" {
		t.Fatalf("got %+v", got)
	}
	for _, j := range got {
		if j.Depth == nil || j.Depth.Queue.Error == "" || j.Depth.DLQ.Error == "" {
			t.Fatalf("%s: depth = %+v, want errors", j.LogicName, j.Depth)
		}
	}
}

func TestDepthSkipsServerBeingInspected(t *testing.T) {
	m := newTestManager(t)
	job := testJobs()[2] // pay-a
	key := broker.Key(job.ServerName, job.Server)
	if !m.beginInspect(key) {
		t.Fatal("beginInspect on idle server = false")
	}
	defer m.endInspect(key)

	got, err := m.GetWithDepth(t.Context(), "pay-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Depth.Queue.Error != errInspectBusy.Error() {
		t.Fatalf("queue error = %q, want busy", got.Depth.Queue.Error)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, _ = m.GetWithDepth(ctx, "mail-b") // other server: not busy, but ctx is done
	if got.Depth == nil || got.Depth.Queue.Error == "" {
		t.Fatalf("depth = %+v, want error", got.Depth)
	}
}
