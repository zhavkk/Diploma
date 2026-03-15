package replication_test

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
)

// ─────────────────────────────────────────
// Моки
// ─────────────────────────────────────────

type mockTopoSource struct {
	topo *models.ClusterTopology
}

func (m *mockTopoSource) Get() *models.ClusterTopology { return m.topo }

type mockNodeAgentCaller struct {
	calls      []reconfigCall
	promoteErr error
	reconfErr  error
}

type reconfigCall struct {
	addr            string
	primaryConnInfo string
	timeline        string
}

func (m *mockNodeAgentCaller) PromoteNode(_ context.Context, addr string) error {
	return m.promoteErr
}

func (m *mockNodeAgentCaller) ReconfigureReplication(_ context.Context, addr, primaryConnInfo, timeline string) error {
	m.calls = append(m.calls, reconfigCall{addr: addr, primaryConnInfo: primaryConnInfo, timeline: timeline})
	return m.reconfErr
}

// ─────────────────────────────────────────
// Apply
// ─────────────────────────────────────────

func TestConfigurator_Apply_CallsGRPCForEachNode(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Address: "primary:50052", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
				{NodeID: "pg-replica2", Address: "replica2:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	cfg := models.ReplicationConfig{SynchronousStandbyNames: "pg-replica1", EnableSyncReplication: true}
	targetNodes := []string{"pg-replica1", "pg-replica2"}

	if err := c.Apply(context.Background(), cfg, targetNodes); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(caller.calls) != 2 {
		t.Errorf("expected 2 gRPC calls, got %d", len(caller.calls))
	}
}

func TestConfigurator_Apply_ErrorFromOneNodeAborts(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{reconfErr: errors.New("connection refused")}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	err := c.Apply(context.Background(), models.ReplicationConfig{}, []string{"pg-replica1"})
	if err == nil {
		t.Error("expected error when gRPC call fails")
	}
}

func TestConfigurator_Apply_LooksUpAddressByNodeID(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "192.168.1.11:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	_ = c.Apply(context.Background(), models.ReplicationConfig{}, []string{"pg-replica1"})

	if len(caller.calls) == 0 {
		t.Fatal("expected at least one gRPC call")
	}
	if caller.calls[0].addr != "192.168.1.11:50052" {
		t.Errorf("gRPC called with addr %q, want %q", caller.calls[0].addr, "192.168.1.11:50052")
	}
}

func TestConfigurator_Apply_SkipsNodeWithNoAddress(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	// Не должно быть ошибки — просто пропускаем узел без адреса
	if err := c.Apply(context.Background(), models.ReplicationConfig{}, []string{"pg-replica1"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(caller.calls) != 0 {
		t.Errorf("expected no gRPC calls for node without address, got %d", len(caller.calls))
	}
}

// ─────────────────────────────────────────
// ReconfigureAfterFailover
// ─────────────────────────────────────────

func TestConfigurator_ReconfigureAfterFailover_CallsReplicasOnly(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-replica1", // новый primary после failover
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
				{NodeID: "pg-replica2", Address: "replica2:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	if err := c.ReconfigureAfterFailover(context.Background(), "pg-replica1"); err != nil {
		t.Fatalf("ReconfigureAfterFailover: %v", err)
	}
	// pg-replica2 должен получить reconfig, pg-replica1 (новый primary) — нет
	if len(caller.calls) != 1 {
		t.Errorf("expected 1 reconfig call (for replica2 only), got %d", len(caller.calls))
	}
	if caller.calls[0].addr != "replica2:50052" {
		t.Errorf("reconfig called on %q, want replica2:50052", caller.calls[0].addr)
	}
}

func TestConfigurator_ReconfigureAfterFailover_PassesPrimaryConnInfo(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
				{NodeID: "pg-replica2", Address: "replica2:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	_ = c.ReconfigureAfterFailover(context.Background(), "pg-replica1")

	if len(caller.calls) == 0 {
		t.Fatal("no reconfig calls made")
	}
	// primary_conninfo должен указывать на нового primary
	if caller.calls[0].primaryConnInfo == "" {
		t.Error("expected non-empty primaryConnInfo")
	}
}

func TestConfigurator_ReconfigureAfterFailover_NoopWhenTopologyNil(t *testing.T) {
	topo := &mockTopoSource{topo: nil}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	if err := c.ReconfigureAfterFailover(context.Background(), "pg-replica1"); err != nil {
		t.Errorf("unexpected error with nil topology: %v", err)
	}
	if len(caller.calls) != 0 {
		t.Error("expected no calls with nil topology")
	}
}

// ─────────────────────────────────────────
// Apply — передача PrimaryConnInfo
// ─────────────────────────────────────────

func TestConfigurator_Apply_PassesPrimaryConnInfoFromConfig(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	cfg := models.ReplicationConfig{
		PrimaryConnInfo: "host=pg-primary port=5432 user=replicator",
	}
	if err := c.Apply(context.Background(), cfg, []string{"pg-replica1"}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(caller.calls) == 0 {
		t.Fatal("expected at least one gRPC call")
	}
	if caller.calls[0].primaryConnInfo != cfg.PrimaryConnInfo {
		t.Errorf("primaryConnInfo = %q, want %q", caller.calls[0].primaryConnInfo, cfg.PrimaryConnInfo)
	}
}

func TestConfigurator_Apply_EmptyPrimaryConnInfoAllowed(t *testing.T) {
	// Оператор обновляет только sync settings — primaryConnInfo пуст и это ок.
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	if err := c.Apply(context.Background(), models.ReplicationConfig{}, []string{"pg-replica1"}); err != nil {
		t.Fatalf("Apply with empty PrimaryConnInfo: %v", err)
	}
	if len(caller.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(caller.calls))
	}
	if caller.calls[0].primaryConnInfo != "" {
		t.Errorf("expected empty primaryConnInfo, got %q", caller.calls[0].primaryConnInfo)
	}
}

// ─────────────────────────────────────────
// ReconfigureAfterFailover — сбор ошибок
// ─────────────────────────────────────────

func TestConfigurator_ReconfigureAfterFailover_ReturnsErrorWhenAnyReplicaFails(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
				{NodeID: "pg-replica2", Address: "replica2:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{reconfErr: errors.New("connection refused")}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	// Раньше: ошибки проглатывались, возвращался nil — это баг.
	err := c.ReconfigureAfterFailover(context.Background(), "pg-new-primary")
	if err == nil {
		t.Error("expected error when replica reconfiguration fails, got nil")
	}
}

func TestConfigurator_ReconfigureAfterFailover_CollectsAllErrors(t *testing.T) {
	topo := &mockTopoSource{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Address: "replica1:50052", Role: models.RoleReplica, State: models.StateHealthy},
				{NodeID: "pg-replica2", Address: "replica2:50052", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	caller := &mockNodeAgentCaller{reconfErr: errors.New("timeout")}
	c := replication.NewConfigurator(topo, caller, zap.NewNop())

	// Оба узла провалились — оба должны присутствовать в итоговой ошибке.
	err := c.ReconfigureAfterFailover(context.Background(), "pg-new-primary")
	if err == nil {
		t.Fatal("expected aggregated error")
	}
	// Продолжает обработку даже после первой ошибки (не fail-fast).
	if len(caller.calls) != 2 {
		t.Errorf("expected 2 reconfig attempts (both replicas), got %d", len(caller.calls))
	}
}
