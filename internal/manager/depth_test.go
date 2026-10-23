package manager

import (
	"context"
	"testing"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
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
	key := inspectKey{by: inspectAdmin, server: broker.Key(job.ServerName, job.Server)}
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

// An admin inspection in flight must not make the autoscaler skip a round,
// and vice versa; each is only guarded against its own pile-up.
func TestInspectorsDoNotBlockEachOther(t *testing.T) {
	m := newTestManager(t)
	job := testJobs()[2] // pay-a
	server := broker.Key(job.ServerName, job.Server)

	admin := inspectKey{by: inspectAdmin, server: server}
	if !m.beginInspect(admin) {
		t.Fatal("beginInspect admin = false")
	}
	got := m.readyCounts(t.Context(), []config.ResolvedJob{job})
	if got[0].Error == "" || got[0].Error == errInspectBusy.Error() {
		t.Fatalf("autoscale query during admin inspection: %+v, want a broker error", got[0])
	}
	m.endInspect(admin)

	auto := inspectKey{by: inspectAutoscale, server: server}
	if !m.beginInspect(auto) {
		t.Fatal("beginInspect autoscale = false")
	}
	defer m.endInspect(auto)
	if got := m.readyCounts(t.Context(), []config.ResolvedJob{job}); got[0].Error != errInspectBusy.Error() {
		t.Fatalf("second autoscale query = %+v, want busy", got[0])
	}
	st, err := m.GetWithDepth(t.Context(), "pay-a")
	if err != nil {
		t.Fatal(err)
	}
	if st.Depth.Queue.Error == errInspectBusy.Error() {
		t.Fatal("admin query blocked by autoscale inspection")
	}
}
