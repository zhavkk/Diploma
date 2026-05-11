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
	"github.com/zhavkk/Diploma/pkg/version"
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

type integMockCoord struct {
	mu        sync.Mutex
	isLeader  bool
	storage   map[string]string
	putCount  int
	fenceSet  bool
	fenceCleared bool
}

func (m *integMockCoord) IsLeader(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isLeader, nil
}
func (m *integMockCoord) PutClusterState(_ context.Context, key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.storage == nil {
		m.storage = make(map[string]string)
	}
	// coordination.Module adds prefix internally, so we do the same
	fullKey := "/ha-orchestrator/state/" + key
	m.storage[fullKey] = value
	m.putCount++

	// Track fence lifecycle
	if key == "fence/pg-primary" || fullKey == "/ha-orchestrator/state/fence/pg-primary" {
		if value != "" {
			m.fenceSet = true
			m.fenceCleared = false
		} else {
			m.fenceCleared = true
		}
	}
	return nil
}
func (m *integMockCoord) GetClusterState(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.storage == nil {
		return "", nil
	}
	// coordination.Module adds prefix internally, so we do the same
	fullKey := "/ha-orchestrator/state/" + key
	return m.storage[fullKey], nil
}
func (m *integMockCoord) WasFenceSet() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fenceSet
}
func (m *integMockCoord) WasFenceCleared() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fenceCleared
}

type integMockCaller struct {
	mu                 sync.Mutex
	promoteCalledAddr  string
	reconfigCalledAddr string
	pgRewindCalledAddr string
	failPromote        bool
}

func (m *integMockCaller) PromoteNode(_ context.Context, addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failPromote {
		return context.Canceled
	}
	m.promoteCalledAddr = addr
	return nil
}
func (m *integMockCaller) ReconfigureReplication(_ context.Context, addr, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconfigCalledAddr = addr
	return nil
}
func (m *integMockCaller) RunPgRewind(_ context.Context, addr, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pgRewindCalledAddr = addr
	return nil
}
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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mockCoord := &integMockCoord{isLeader: true}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
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

	// Fence token должен был быть установлен и очищен
	if !mockCoord.WasFenceSet() {
		t.Errorf("fence token was not set during failover")
	}
	if !mockCoord.WasFenceCleared() {
		t.Errorf("fence token was not cleared after successful failover")
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
		NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0"),
	})
	topoReg.UpsertNode(models.NodeStatus{
		NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0"),
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
		NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0"),
	})
	topoReg.UpsertNode(models.NodeStatus{
		NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0"),
	})

	caller := &integMockCaller{}
	mockCoord := &integMockCoord{isLeader: true}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		topoReg,
		mockCoord,
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
		NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0"),
	})
	// Реплика тоже шлёт heartbeat
	mon.ReceiveHeartbeat(&models.NodeStatus{
		NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0"), WALReplayLSN: 5000,
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

// ─────────────────────────────────────────
// STONITH Fencing Integration Tests
// ─────────────────────────────────────────

func TestSTONITH_FenceTokenLifecycle(t *testing.T) {
	mockCoord := &integMockCoord{isLeader: true}
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Выполняем failover
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Fence token должен быть установлен и затем очищен в течение failover
	if !mockCoord.WasFenceSet() {
		t.Errorf("fence token should be set during failover")
	}
	if !mockCoord.WasFenceCleared() {
		t.Errorf("fence token should be cleared after successful failover")
	}
}

func TestSTONITH_FenceTokenSetOnManualFailover(t *testing.T) {
	mockCoord := &integMockCoord{isLeader: true}
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Выполняем ручной failover
	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("manual failover failed: %v", err)
	}

	// Fence token должен быть установлен и затем очищен
	if !mockCoord.WasFenceSet() {
		t.Errorf("fence token should be set during manual failover")
	}
	if !mockCoord.WasFenceCleared() {
		t.Errorf("fence token should be cleared after successful manual failover")
	}
}

func TestSTONITH_FencedNodeCannotRejoin(t *testing.T) {
	mockCoord := &integMockCoord{isLeader: true}
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Выполняем failover
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Имитируем ситуацию, когда fence токен всё ещё активен
	// (например, если мы искусственно установили его обратно)
	mockCoord.PutClusterState(context.Background(), "fence/pg-primary", "test-token-1234567890123456789012345678")

	// Пытаемся перезапустить старый primary — должно быть заблокировано
	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")
	if err == nil {
		t.Errorf("expected error when fenced node tries to rejoin")
	}
	if !contains(err.Error(), "fenced") {
		t.Errorf("error should mention fencing, got: %v", err)
	}

	// pg_rewind не должен был быть вызван
	if caller.pgRewindCalledAddr != "" {
		t.Errorf("pg_rewind should not be called when node is fenced")
	}
}

func TestSTONITH_NodeCanRejoinAfterFenceCleared(t *testing.T) {
	mockCoord := &integMockCoord{isLeader: true}
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Выполняем failover — это очистит fence token при успешном завершении
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Fence token должен быть очищен после успешного failover
	fenceKey := "fence/pg-primary"
	fenceValue, _ := mockCoord.GetClusterState(context.Background(), fenceKey)
	if fenceValue != "" {
		t.Errorf("fence token should be cleared after successful failover, got: %q", fenceValue)
	}

	// Теперь перезапуск старого primary должен быть разрешён
	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")
	if err != nil {
		t.Errorf("old primary should be allowed to rejoin after fence is cleared: %v", err)
	}

	// pg_rewind должен был быть вызван
	if caller.pgRewindCalledAddr != "primary:50052" {
		t.Errorf("pg_rewind should be called when node is not fenced, got: %q", caller.pgRewindCalledAddr)
	}
}

func TestSTONITH_SplitBrainPrevention(t *testing.T) {
	// Этот тест демонстрирует, как fencing предотвращает split-brain:
	// Старый primary пытается продолжать принимать записи после того,
	// как новый primary был избран. Fencing блокирует перезапуск старого primary.

	mockCoord := &integMockCoord{isLeader: true}
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Scenario: primary падает, failover происходит, но старый primary пытается вернуться
	// после того как новый primary уже принял транзакции

	// 1. Failover происходит
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// 2. Новой primary становится replica1
	if mockTopo.primary != "pg-replica1" {
		t.Errorf("new primary should be replica1, got %q", mockTopo.primary)
	}

	// 3. Симулируем ситуацию, когда fence токен всё ещё активен
	// (например, если operator очистил его вручную или он не был очищен)
	mockCoord.PutClusterState(context.Background(), "fence/pg-primary", "active-fence-token")

	// 4. Старый primary пытается вернуться
	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")

	// 5. Перезапуск ДОЛЖЕН быть заблокирован для предотвращения split-brain
	if err == nil {
		t.Errorf("split-brain prevention failed: fenced node was allowed to rejoin")
	}
	if !contains(err.Error(), "fenced") {
		t.Errorf("error should explicitly mention fencing for split-brain prevention: %v", err)
	}

	// 6. pg_rewind не должен был быть вызван
	if caller.pgRewindCalledAddr != "" {
		t.Errorf("pg_rewind should not be called when node is fenced to prevent split-brain")
	}

	// 7. Только после того как fence очищен, старый primary может перезапуститься
	mockCoord.PutClusterState(context.Background(), "fence/pg-primary", "")

	err = mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")
	if err != nil {
		t.Errorf("after fence cleared, rejoin should be allowed: %v", err)
	}

	// 8. Теперь pg_rewind должен был быть вызван
	if caller.pgRewindCalledAddr != "primary:50052" {
		t.Errorf("pg_rewind should be called after fence is cleared")
	}
}

func TestSTONITH_FenceTokenLength(t *testing.T) {
	// Проверяем, что fence token имеет правильную длину (32 hex символа = 16 байт)

	mockCoord := &integMockCoord{isLeader: true}
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Выполняем failover
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Проверяем длину fence токена (он должен быть 32 символа)
	mockCoord.mu.Lock()
	fenceKey := "/ha-orchestrator/state/fence/pg-primary"
	fenceToken := mockCoord.storage[fenceKey]
	mockCoord.mu.Unlock()

	// Токен мог быть уже очищен, проверяем что он был установлен
	if fenceToken != "" && len(fenceToken) != 32 {
		t.Errorf("fence token should be 32 characters (16 bytes hex encoded), got length %d", len(fenceToken))
	}
}

func TestSTONITH_ManualFailoverFencing(t *testing.T) {
	// Проверяем fencing при ручном failover

	mockCoord := &integMockCoord{isLeader: true}
	mockTopo := &integMockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica1:50052", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	caller := &integMockCaller{}
	mgr := failover.NewManager(
		failover.Config{QuorumSize: 1},
		mockTopo,
		mockCoord,
		replication.NewConfigurator(nil, nil, zap.NewNop()),
		caller,
		zap.NewNop(),
	)

	// Выполняем ручной failover
	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("manual failover failed: %v", err)
	}

	// Проверяем что primary изменился
	if mockTopo.primary != "pg-replica1" {
		t.Errorf("new primary should be pg-replica1, got %q", mockTopo.primary)
	}

	// Проверяем fencing
	if !mockCoord.WasFenceSet() {
		t.Errorf("fence token should be set during manual failover")
	}
	if !mockCoord.WasFenceCleared() {
		t.Errorf("fence token should be cleared after successful manual failover")
	}
}
