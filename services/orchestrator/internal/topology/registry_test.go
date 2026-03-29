package topology_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

func newTopo(primary string, nodes ...models.NodeStatus) *models.ClusterTopology {
	return &models.ClusterTopology{
		Version:     "v1",
		PrimaryNode: primary,
		Nodes:       nodes,
		UpdatedAt:   time.Now(),
	}
}

func TestRegistry_GetReturnsNilWhenEmpty(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	if r.Get() != nil {
		t.Error("expected nil topology before first Update")
	}
}

func TestRegistry_PrimaryReturnsEmptyWhenNil(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	if r.Primary() != "" {
		t.Errorf("Primary() = %q, want empty string", r.Primary())
	}
}

func TestRegistry_UpdateAndGet(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	topo := newTopo("pg-primary",
		models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary},
		models.NodeStatus{NodeID: "pg-replica1", Role: models.RoleReplica},
	)
	r.Update(topo)

	got := r.Get()
	if got == nil {
		t.Fatal("Get() returned nil after Update")
	}
	if got.PrimaryNode != "pg-primary" {
		t.Errorf("PrimaryNode = %q, want %q", got.PrimaryNode, "pg-primary")
	}
	if len(got.Nodes) != 2 {
		t.Errorf("Nodes count = %d, want 2", len(got.Nodes))
	}
}

func TestRegistry_GetReturnsCopy(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.Update(newTopo("pg-primary",
		models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary},
	))

	got1 := r.Get()
	got1.PrimaryNode = "mutated"

	got2 := r.Get()
	if got2.PrimaryNode == "mutated" {
		t.Error("Get() should return a copy, not a pointer to internal state")
	}

	got1 = r.Get()
	got1.Nodes[0].NodeID = "mutated"
	got2 = r.Get()
	if got2.Nodes[0].NodeID == "mutated" {
		t.Error("Get() Nodes slice should be a deep copy")
	}
}

func TestRegistry_SetPrimary(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.Update(newTopo("pg-primary"))

	r.SetPrimary("pg-replica1")

	if r.Primary() != "pg-replica1" {
		t.Errorf("Primary() = %q after SetPrimary, want %q", r.Primary(), "pg-replica1")
	}
}

func TestRegistry_SetPrimaryNoopWhenNilTopology(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	// не паникуем при вызове SetPrimary без предварительного Update
	r.SetPrimary("pg-replica1")
	if r.Primary() != "" {
		t.Error("expected empty primary when topology was never set")
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.Update(newTopo("pg-primary"))

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			r.Get()
		}()
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				r.SetPrimary("pg-replica1")
			} else {
				r.Update(newTopo("pg-primary"))
			}
		}(i)
	}
	wg.Wait()
}

// ─────────────────────────────────────────
// UpsertNode
// ─────────────────────────────────────────

func TestRegistry_UpsertNode_AddsNewNode(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.UpsertNode(models.NodeStatus{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy})

	got := r.Get()
	if got == nil {
		t.Fatal("Get() returned nil after UpsertNode")
	}
	if len(got.Nodes) != 1 {
		t.Fatalf("Nodes count = %d, want 1", len(got.Nodes))
	}
	if got.Nodes[0].NodeID != "pg-replica1" {
		t.Errorf("NodeID = %q, want %q", got.Nodes[0].NodeID, "pg-replica1")
	}
	if got.PrimaryNode != "" {
		t.Errorf("PrimaryNode = %q, want empty (replica doesn't set primary)", got.PrimaryNode)
	}
}

func TestRegistry_UpsertNode_UpdatesExistingNode(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.UpsertNode(models.NodeStatus{NodeID: "pg-replica1", WALReplayLSN: 1000})
	r.UpsertNode(models.NodeStatus{NodeID: "pg-replica1", WALReplayLSN: 5000})

	got := r.Get()
	if len(got.Nodes) != 1 {
		t.Errorf("Nodes count = %d, want 1 (no duplicate)", len(got.Nodes))
	}
	if got.Nodes[0].WALReplayLSN != 5000 {
		t.Errorf("WALReplayLSN = %d, want 5000", got.Nodes[0].WALReplayLSN)
	}
}

func TestRegistry_UpsertNode_SetsPrimaryWhenRolePrimary(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy})

	if r.Primary() != "pg-primary" {
		t.Errorf("Primary() = %q, want %q", r.Primary(), "pg-primary")
	}
	if got := r.Get(); got.PrimaryNode != "pg-primary" {
		t.Errorf("PrimaryNode = %q, want %q", got.PrimaryNode, "pg-primary")
	}
}

func TestRegistry_UpdateNodeState_ChangesState(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy})

	r.UpdateNodeState("pg-primary", models.StateUnreachable)

	got := r.Get()
	if got.Nodes[0].State != models.StateUnreachable {
		t.Errorf("State = %q, want %q", got.Nodes[0].State, models.StateUnreachable)
	}
}

func TestRegistry_UpdateNodeState_NoopForUnknownNode(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.UpsertNode(models.NodeStatus{NodeID: "pg-primary", State: models.StateHealthy})

	// должно не паниковать и не менять существующие узлы
	r.UpdateNodeState("pg-unknown", models.StateUnreachable)

	got := r.Get()
	if got.Nodes[0].State != models.StateHealthy {
		t.Errorf("existing node state changed unexpectedly: %q", got.Nodes[0].State)
	}
}

func TestRegistry_Version_IncrementsOnMutation(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())

	r.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})
	v1 := r.Get().Version

	r.UpsertNode(models.NodeStatus{NodeID: "pg-replica1", Role: models.RoleReplica})
	v2 := r.Get().Version

	r.SetPrimary("pg-replica1")
	v3 := r.Get().Version

	if v1 == "" {
		t.Error("Version should not be empty after first UpsertNode")
	}
	if v1 == v2 {
		t.Errorf("Version did not change after second UpsertNode: %q", v1)
	}
	if v2 == v3 {
		t.Errorf("Version did not change after SetPrimary: %q", v2)
	}
}

func TestRegistry_AppendEvent_StoresEvent(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	evt := models.FailoverEvent{
		OldPrimary: "pg-primary",
		NewPrimary: "pg-replica1",
		Reason:     "automatic",
	}
	r.AppendEvent(evt)

	events := r.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].OldPrimary != "pg-primary" {
		t.Errorf("OldPrimary = %q, want %q", events[0].OldPrimary, "pg-primary")
	}
}

func TestRegistry_AppendEvent_MultipleEvents(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.AppendEvent(models.FailoverEvent{OldPrimary: "a", NewPrimary: "b", Reason: "auto"})
	r.AppendEvent(models.FailoverEvent{OldPrimary: "b", NewPrimary: "c", Reason: "manual"})

	events := r.Events()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestRegistry_Events_ReturnsCopy(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())
	r.AppendEvent(models.FailoverEvent{OldPrimary: "a", NewPrimary: "b"})

	ev1 := r.Events()
	ev1[0].OldPrimary = "mutated"

	ev2 := r.Events()
	if ev2[0].OldPrimary == "mutated" {
		t.Error("Events() should return a copy, not a shared slice")
	}
}

func TestRegistry_AppendEvent_CapsAtMaxEvents(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())

	const maxEvents = 1000
	const extra = 5
	for i := 0; i < maxEvents+extra; i++ {
		r.AppendEvent(models.FailoverEvent{
			OldPrimary: fmt.Sprintf("old-%d", i),
			NewPrimary: fmt.Sprintf("new-%d", i),
			Reason:     "test",
		})
	}

	events := r.Events()
	if len(events) != maxEvents {
		t.Errorf("expected %d events (capped), got %d", maxEvents, len(events))
	}
	// The last event should be the most recently appended.
	last := events[len(events)-1]
	wantOld := fmt.Sprintf("old-%d", maxEvents+extra-1)
	if last.OldPrimary != wantOld {
		t.Errorf("last event OldPrimary = %q, want %q", last.OldPrimary, wantOld)
	}
}

func TestUpsertNode_DoesNotOverridePrimaryAfterFailover(t *testing.T) {
	reg := topology.NewRegistry(zap.NewNop())
	reg.UpsertNode(models.NodeStatus{NodeID: "n1", Role: models.RolePrimary, State: models.StateHealthy})
	reg.UpsertNode(models.NodeStatus{NodeID: "n2", Role: models.RoleReplica, State: models.StateHealthy})
	// simulate failover: new primary is n2
	reg.SetPrimary("n2")
	// stale heartbeat from old primary
	reg.UpsertNode(models.NodeStatus{NodeID: "n1", Role: models.RolePrimary, State: models.StateHealthy})
	if reg.Primary() != "n2" {
		t.Fatalf("expected primary=n2, got %s", reg.Primary())
	}
}

func TestRegistry_UpsertNode_ConcurrentSafe(t *testing.T) {
	r := topology.NewRegistry(zap.NewNop())

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			r.UpsertNode(models.NodeStatus{NodeID: fmt.Sprintf("node-%d", i), Role: models.RoleReplica})
		}(i)
		go func() {
			defer wg.Done()
			r.Get()
		}()
	}
	wg.Wait()
}
