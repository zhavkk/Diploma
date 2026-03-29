package failover_test

// Интеграционный тест полного failover flow:
// Monitor (heartbeat timeout) → Manager.NotifyPrimaryFailure → ElectNewPrimary → PromoteNode gRPC

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/monitor"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

// ─────────────────────────────────────────
// Локальные моки (независимы от manager_test.go)
// ─────────────────────────────────────────

type integMockTopo struct {
	mu      sync.Mutex
	primary string
	topo    *models.ClusterTopology
}

func (m *integMockTopo) Get() *models.ClusterTopology {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.topo
}
func (m *integMockTopo) Primary() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.primary
}
func (m *integMockTopo) SetPrimary(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.primary = id
}

type integMockCoord struct{ isLeader bool }

func (m *integMockCoord) IsLeader(_ context.Context) (bool, error) { return m.isLeader, nil }
func (m *integMockCoord) PutClusterState(_ context.Context, _, _ string) error { return nil }

type integMockCaller struct {
	mu                 sync.Mutex
	promoteCalledAddr  string
	reconfigCalledAddr string
}

func (m *integMockCaller) PromoteNode(_ context.Context, addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.promoteCalledAddr = addr
	return nil
}
func (m *integMockCaller) ReconfigureReplication(_ context.Context, addr, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconfigCalledAddr = addr
	return nil
}
func (m *integMockCaller) RunPgRewind(_ context.Context, _, _ string) error    { return nil }
func (m *integMockCaller) RestartPostgres(_ context.Context, _ string) error   { return nil }

// controllableClock для интеграционных тестов
type integClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *integClock) Now() time.Time   { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *integClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// ─────────────────────────────────────────
// Интеграционный тест 1:
// Полный цикл: heartbeat timeout → election → PromoteNode
// ─────────────────────────────────────────

func TestFullFailoverFlow_PrimaryTimeout_TriggersElectionAndPromote(t *testing.T) {
	// Настройка топологии: primary + 1 реплика
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052"},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		&integMockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Прямой вызов NotifyPrimaryFailure (как это делает Monitor при timeout)
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Primary должен смениться на реплику с наибольшим WAL LSN
	if mockTopo.primary != "pg-replica1" {
		t.Errorf("primary after failover = %q, want %q", mockTopo.primary, "pg-replica1")
	}

	// PromoteNode должен быть вызван с адресом новой primary
	if caller.promoteCalledAddr != "replica1:50052" {
		t.Errorf("PromoteNode called with %q, want %q", caller.promoteCalledAddr, "replica1:50052")
	}
}

// ─────────────────────────────────────────
// Интеграционный тест 2:
// Monitor + Manager + Registry: реплика с timeout не вызывает failover
// ─────────────────────────────────────────

func TestFullFailoverFlow_ReplicaTimeout_DoesNotTriggerFailover(t *testing.T) {
	// Используем реальный topology.Registry — он теперь поддерживает UpsertNode
	topoReg := topology.NewRegistry(zap.NewNop())
	topoReg.UpsertNode(models.NodeStatus{
		NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052",
	})
	topoReg.UpsertNode(models.NodeStatus{
		NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052",
	})

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{},
		topoReg,
		&integMockCoord{isLeader: true},
		replication.NewConfigurator(topoReg, caller, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	clk := &integClock{now: time.Now()}
	mon := monitor.NewMonitorWithClock(
		monitor.Config{HeartbeatTimeout: 30},
		mgr,    // *failover.Manager реализует monitor.FailoverNotifier
		topoReg,
		clk,
		zap.NewNop(),
	)

	// Только реплика шлёт heartbeat — primary молчит
	mon.ReceiveHeartbeat(&models.NodeStatus{
		NodeID: "pg-replica1",
		Role:   models.RoleReplica,
		State:  models.StateHealthy,
	})

	// Продвигаем время за порог timeout
	clk.Advance(31 * time.Second)
	mon.CheckNodes(context.Background())

	// PromoteNode не должен быть вызван — только реплика в timeout, primary не мониторился
	if caller.promoteCalledAddr != "" {
		t.Errorf("PromoteNode should NOT be called when only replica times out, got addr %q", caller.promoteCalledAddr)
	}

	// Primary должен остаться прежним
	if topoReg.Primary() != "pg-primary" {
		t.Errorf("primary changed to %q, should remain %q", topoReg.Primary(), "pg-primary")
	}
}

// ─────────────────────────────────────────
// Интеграционный тест 3:
// Monitor детектирует потерю primary и тригерит failover через реальный Manager
// ─────────────────────────────────────────

func TestFullFailoverFlow_MonitorDetectsPrimaryLoss_AndElects(t *testing.T) {
	topoReg := topology.NewRegistry(zap.NewNop())
	topoReg.UpsertNode(models.NodeStatus{
		NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052",
	})
	topoReg.UpsertNode(models.NodeStatus{
		NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052",
	})

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		topoReg,
		&integMockCoord{isLeader: true},
		replication.NewConfigurator(topoReg, caller, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	clk := &integClock{now: time.Now()}
	mon := monitor.NewMonitorWithClock(
		monitor.Config{HeartbeatTimeout: 10},
		mgr,
		topoReg,
		clk,
		zap.NewNop(),
	)

	// Primary шлёт heartbeat
	mon.ReceiveHeartbeat(&models.NodeStatus{
		NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052",
	})
	// Реплика тоже шлёт heartbeat
	mon.ReceiveHeartbeat(&models.NodeStatus{
		NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052",
	})

	// Primary «падает» — продвигаем время за timeout
	clk.Advance(15 * time.Second)
	mon.CheckNodes(context.Background())

	// Failover должен был произойти: replica1 становится primary
	if topoReg.Primary() != "pg-replica1" {
		t.Errorf("primary after failover = %q, want %q", topoReg.Primary(), "pg-replica1")
	}
	if caller.promoteCalledAddr != "replica1:50052" {
		t.Errorf("PromoteNode called on %q, want %q", caller.promoteCalledAddr, "replica1:50052")
	}
}
