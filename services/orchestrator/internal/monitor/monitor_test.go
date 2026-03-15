package monitor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/monitor"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

// ─────────────────────────────────────────
// Тестовые зависимости
// ─────────────────────────────────────────

// mockFailoverNotifier записывает вызовы NotifyPrimaryFailure.
type mockFailoverNotifier struct {
	mu     sync.Mutex
	called []string
}

func (m *mockFailoverNotifier) NotifyPrimaryFailure(_ context.Context, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.called = append(m.called, nodeID)
}

func (m *mockFailoverNotifier) WasCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.called) > 0
}

func (m *mockFailoverNotifier) CalledWith() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.called...)
}

// controllableClock позволяет управлять «текущим временем» в тестах.
type controllableClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock(t time.Time) *controllableClock { return &controllableClock{now: t} }

func (c *controllableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *controllableClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// ─────────────────────────────────────────
// ReceiveHeartbeat
// ─────────────────────────────────────────

func TestMonitor_ReceiveHeartbeat_UpdatesTopology(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitor(monitor.Config{HeartbeatTimeout: 30}, fm, topo, zap.NewNop())

	m.ReceiveHeartbeat(&models.NodeStatus{
		NodeID: "pg-primary",
		Role:   models.RolePrimary,
		State:  models.StateHealthy,
	})

	got := topo.Get()
	if got == nil {
		t.Fatal("topology is nil after ReceiveHeartbeat")
	}
	if len(got.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(got.Nodes))
	}
	if got.Nodes[0].NodeID != "pg-primary" {
		t.Errorf("NodeID = %q, want %q", got.Nodes[0].NodeID, "pg-primary")
	}
	if got.PrimaryNode != "pg-primary" {
		t.Errorf("PrimaryNode = %q, want %q", got.PrimaryNode, "pg-primary")
	}
}

func TestMonitor_ReceiveHeartbeat_SetsTimestampFromClock(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	fixedTime := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	clk := newClock(fixedTime)

	m := monitor.NewMonitorWithClock(monitor.Config{HeartbeatTimeout: 30}, &mockFailoverNotifier{}, topo, clk, zap.NewNop())

	status := &models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary}
	m.ReceiveHeartbeat(status)

	if !status.LastHeartbeat.Equal(fixedTime) {
		t.Errorf("LastHeartbeat = %v, want %v", status.LastHeartbeat, fixedTime)
	}
}

// ─────────────────────────────────────────
// CheckNodes
// ─────────────────────────────────────────

func TestMonitor_CheckNodes_NoFailoverForHealthyPrimary(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	topo.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})

	clk := newClock(time.Now())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitorWithClock(monitor.Config{HeartbeatTimeout: 30}, fm, topo, clk, zap.NewNop())

	// Heartbeat «сейчас» — не превышает timeout
	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})

	// Продвигаем время вперёд, но меньше timeout (29 секунд)
	clk.Advance(29 * time.Second)
	m.CheckNodes(context.Background())

	if fm.WasCalled() {
		t.Errorf("failover was triggered unexpectedly, calls: %v", fm.CalledWith())
	}
}

func TestMonitor_CheckNodes_TriggerFailoverOnPrimaryTimeout(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	topo.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})

	clk := newClock(time.Now())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitorWithClock(monitor.Config{HeartbeatTimeout: 30}, fm, topo, clk, zap.NewNop())

	// Heartbeat в момент t=0
	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})

	// Продвигаем время: 31 секунда > 30-секундного timeout
	clk.Advance(31 * time.Second)
	m.CheckNodes(context.Background())

	if !fm.WasCalled() {
		t.Fatal("expected failover to be triggered for timed-out primary")
	}
	calls := fm.CalledWith()
	if calls[0] != "pg-primary" {
		t.Errorf("failover called with %q, want %q", calls[0], "pg-primary")
	}
}

func TestMonitor_CheckNodes_NoFailoverWhenReplicaTimeout(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	// Primary — отдельный узел
	topo.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})
	topo.UpsertNode(models.NodeStatus{NodeID: "pg-replica1", Role: models.RoleReplica})

	clk := newClock(time.Now())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitorWithClock(monitor.Config{HeartbeatTimeout: 30}, fm, topo, clk, zap.NewNop())

	// Только реплика шлёт heartbeat
	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-replica1", Role: models.RoleReplica})

	// Timeout реплики — failover не должен произойти
	clk.Advance(31 * time.Second)
	m.CheckNodes(context.Background())

	if fm.WasCalled() {
		t.Errorf("failover should NOT trigger on replica timeout, calls: %v", fm.CalledWith())
	}
}

func TestMonitor_CheckNodes_IgnoresEmptyNodeStatus(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitor(monitor.Config{HeartbeatTimeout: 30}, fm, topo, zap.NewNop())

	// Ни одного heartbeat не было — CheckNodes не должен паниковать
	m.CheckNodes(context.Background())

	if fm.WasCalled() {
		t.Error("failover should not trigger when no heartbeats received")
	}
}

func TestMonitor_CheckNodes_MultipleNodesOnlyPrimaryTriggersFailover(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	topo.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})

	clk := newClock(time.Now())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitorWithClock(monitor.Config{HeartbeatTimeout: 30}, fm, topo, clk, zap.NewNop())

	// Оба узла шлют heartbeat
	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})
	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-replica1", Role: models.RoleReplica})

	// Оба превышают timeout
	clk.Advance(31 * time.Second)
	m.CheckNodes(context.Background())

	// Failover должен быть вызван ровно 1 раз — для primary
	calls := fm.CalledWith()
	if len(calls) != 1 {
		t.Errorf("expected exactly 1 failover call, got %d: %v", len(calls), calls)
	}
	if calls[0] != "pg-primary" {
		t.Errorf("failover called with %q, want %q", calls[0], "pg-primary")
	}
}

// ─────────────────────────────────────────
// RejoinHandler
// ─────────────────────────────────────────

type mockRejoinHandler struct {
	mu    sync.Mutex
	calls []struct{ nodeID, addr string }
}

func (h *mockRejoinHandler) HandleOldPrimaryRejoin(_ context.Context, nodeID, addr string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, struct{ nodeID, addr string }{nodeID, addr})
	return nil
}

func (h *mockRejoinHandler) CalledWith() []struct{ nodeID, addr string } {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]struct{ nodeID, addr string }(nil), h.calls...)
}

func TestMonitor_ReceiveHeartbeat_CallsRejoinHandler(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	fm := &mockFailoverNotifier{}
	rh := &mockRejoinHandler{}

	m := monitor.NewMonitor(monitor.Config{HeartbeatTimeout: 30}, fm, topo, zap.NewNop())
	m.WithRejoinHandler(rh)

	m.ReceiveHeartbeat(&models.NodeStatus{
		NodeID:  "pg-old-primary",
		Role:    models.RolePrimary,
		Address: "old-primary:50052",
	})

	calls := rh.CalledWith()
	if len(calls) != 1 {
		t.Fatalf("expected 1 rejoin call, got %d", len(calls))
	}
	if calls[0].nodeID != "pg-old-primary" {
		t.Errorf("nodeID = %q, want %q", calls[0].nodeID, "pg-old-primary")
	}
	if calls[0].addr != "old-primary:50052" {
		t.Errorf("addr = %q, want %q", calls[0].addr, "old-primary:50052")
	}
}

func TestMonitor_CheckNodes_MarksTimedOutNodeUnreachable(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	topo.UpsertNode(models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy})

	clk := newClock(time.Now())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitorWithClock(monitor.Config{HeartbeatTimeout: 30}, fm, topo, clk, zap.NewNop())

	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy})

	clk.Advance(31 * time.Second)
	m.CheckNodes(context.Background())

	got := topo.Get()
	var found *models.NodeStatus
	for i := range got.Nodes {
		if got.Nodes[i].NodeID == "pg-primary" {
			found = &got.Nodes[i]
		}
	}
	if found == nil {
		t.Fatal("node not found in topology after CheckNodes")
	}
	if found.State != models.StateUnreachable {
		t.Errorf("State = %q after timeout, want %q", found.State, models.StateUnreachable)
	}
}

func TestMonitor_CheckNodes_HealthyNodeRemainsHealthy(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	clk := newClock(time.Now())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitorWithClock(monitor.Config{HeartbeatTimeout: 30}, fm, topo, clk, zap.NewNop())

	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy})

	clk.Advance(15 * time.Second) // not yet timed out
	m.CheckNodes(context.Background())

	got := topo.Get()
	if got.Nodes[0].State != models.StateHealthy {
		t.Errorf("State = %q, want StateHealthy for non-timed-out node", got.Nodes[0].State)
	}
}

func TestMonitor_ReceiveHeartbeat_NoopWhenNoRejoinHandler(t *testing.T) {
	topo := topology.NewRegistry(zap.NewNop())
	fm := &mockFailoverNotifier{}
	m := monitor.NewMonitor(monitor.Config{HeartbeatTimeout: 30}, fm, topo, zap.NewNop())

	// No WithRejoinHandler call — should not panic
	m.ReceiveHeartbeat(&models.NodeStatus{NodeID: "pg-primary", Role: models.RolePrimary})
}
