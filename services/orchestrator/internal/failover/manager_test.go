package failover_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	nodeagentv1 "github.com/zhavkk/Diploma/api/proto/gen/nodeagent/v1"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
)

// ─────────────────────────────────────────
// Моки топологии и координации
// ─────────────────────────────────────────

type mockTopo struct {
	topo    *models.ClusterTopology
	primary string
}

func (m *mockTopo) Get() *models.ClusterTopology { return m.topo }
func (m *mockTopo) Primary() string               { return m.primary }
func (m *mockTopo) SetPrimary(id string)          { m.primary = id }

type mockCoord struct {
	isLeader bool
	err      error

	putCalls []mockPutCall
	putErr   error
}

type mockPutCall struct {
	Key   string
	Value string
}

func (m *mockCoord) IsLeader(_ context.Context) (bool, error) { return m.isLeader, m.err }
func (m *mockCoord) PutClusterState(_ context.Context, key, value string) error {
	m.putCalls = append(m.putCalls, mockPutCall{Key: key, Value: value})
	return m.putErr
}

// ─────────────────────────────────────────
// Мок NodeAgentCaller
// ─────────────────────────────────────────

type mockNodeAgentCaller struct {
	promoteCalledAddr  string
	promoteErr         error
	reconfigCalledAddr string
	reconfigErr        error
	pgRewindCalledAddr string
	pgRewindErr        error
	restartCalledAddr  string
	restartErr         error
}

func (m *mockNodeAgentCaller) PromoteNode(_ context.Context, addr string) error {
	m.promoteCalledAddr = addr
	return m.promoteErr
}

func (m *mockNodeAgentCaller) ReconfigureReplication(_ context.Context, addr, _, _ string) error {
	m.reconfigCalledAddr = addr
	return m.reconfigErr
}

func (m *mockNodeAgentCaller) RunPgRewind(_ context.Context, addr, _ string) error {
	m.pgRewindCalledAddr = addr
	return m.pgRewindErr
}

func (m *mockNodeAgentCaller) RestartPostgres(_ context.Context, addr string) error {
	m.restartCalledAddr = addr
	return m.restartErr
}

// ─────────────────────────────────────────
// ElectNewPrimary
// ─────────────────────────────────────────

func TestElectNewPrimary_SelectsHighestWALLSN(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000},
			},
		},
	}
	coord := &mockCoord{isLeader: true}
	rc := replication.NewConfigurator(nil, nil, zap.NewNop())
	mgr := failover.NewManager(failover.Config{QuorumSize: 2}, topo, coord, rc, nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")
	if elected != "pg-replica1" {
		t.Errorf("elected = %q, want %q (highest WAL LSN)", elected, "pg-replica1")
	}
}

func TestElectNewPrimary_ExcludesFailedNode(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")
	if elected == "pg-primary" {
		t.Error("elected failed node as primary")
	}
	if elected != "pg-replica1" {
		t.Errorf("elected = %q, want %q", elected, "pg-replica1")
	}
}

func TestElectNewPrimary_ReturnsEmptyWhenNoHealthyReplicas(t *testing.T) {
	topo := &mockTopo{
		topo: &models.ClusterTopology{
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateUnreachable},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")
	if elected != "" {
		t.Errorf("expected empty string, got %q", elected)
	}
}

func TestElectNewPrimary_ReturnsEmptyWhenTopologyNil(t *testing.T) {
	topo := &mockTopo{topo: nil}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")
	if elected != "" {
		t.Errorf("expected empty string when topology nil, got %q", elected)
	}
}

// ─────────────────────────────────────────
// NotifyPrimaryFailure
// ─────────────────────────────────────────

func TestNotifyPrimaryFailure_AbortsWhenNotLeader(t *testing.T) {
	coord := &mockCoord{isLeader: false}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-primary" {
		t.Errorf("primary changed to %q despite not being leader", topo.primary)
	}
}

func TestNotifyPrimaryFailure_SkipsWhenAlreadyInProgress(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	done := make(chan struct{})
	go func() {
		mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")
	<-done
}

func TestNotifyPrimaryFailure_UpdatesTopologyOnSuccess(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary after failover = %q, want %q", topo.primary, "pg-replica1")
	}
}

// ─────────────────────────────────────────
// Cluster state persistence to etcd
// ─────────────────────────────────────────

func TestNotifyPrimaryFailure_PersistsPrimaryToEtcd(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if len(coord.putCalls) != 1 {
		t.Fatalf("PutClusterState called %d times, want 1", len(coord.putCalls))
	}
	if coord.putCalls[0].Key != "primary" {
		t.Errorf("PutClusterState key = %q, want %q", coord.putCalls[0].Key, "primary")
	}
	if coord.putCalls[0].Value != "pg-replica1" {
		t.Errorf("PutClusterState value = %q, want %q", coord.putCalls[0].Value, "pg-replica1")
	}
}

func TestTriggerManualFailover_PersistsPrimaryToEtcd(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052"},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord,
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("TriggerManualFailover: %v", err)
	}

	if len(coord.putCalls) != 1 {
		t.Fatalf("PutClusterState called %d times, want 1", len(coord.putCalls))
	}
	if coord.putCalls[0].Key != "primary" {
		t.Errorf("PutClusterState key = %q, want %q", coord.putCalls[0].Key, "primary")
	}
	if coord.putCalls[0].Value != "pg-replica1" {
		t.Errorf("PutClusterState value = %q, want %q", coord.putCalls[0].Value, "pg-replica1")
	}
}

func TestNotifyPrimaryFailure_ContinuesWhenPutClusterStateFails(t *testing.T) {
	coord := &mockCoord{isLeader: true, putErr: errors.New("etcd: connection refused")}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Failover should still complete even if etcd persistence fails.
	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q despite PutClusterState error", topo.primary, "pg-replica1")
	}
}

// ─────────────────────────────────────────
// PromoteNode gRPC call
// ─────────────────────────────────────────

func TestNotifyPrimaryFailure_CallsPromoteOnNewPrimary(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052"},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if caller.promoteCalledAddr != "replica1:50052" {
		t.Errorf("PromoteNode called with %q, want %q", caller.promoteCalledAddr, "replica1:50052")
	}
}

func TestNotifyPrimaryFailure_ContinuesAfterPromoteError(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052"},
			},
		},
	}
	caller := &mockNodeAgentCaller{promoteErr: errors.New("pg_ctl: timeout")}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	// Даже при ошибке promote — topology должна обновиться
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary after failover = %q, want %q even with promote error", topo.primary, "pg-replica1")
	}
}

// ─────────────────────────────────────────
// GRPCNodeAgentCaller — интеграционный тест через bufconn
// ─────────────────────────────────────────

type minimalNodeAgent struct {
	nodeagentv1.UnimplementedNodeAgentServiceServer
	promoteCalled bool
	promoteErr    error
}

func (s *minimalNodeAgent) PromoteNode(_ context.Context, _ *nodeagentv1.PromoteNodeRequest) (*nodeagentv1.PromoteNodeResponse, error) {
	s.promoteCalled = true
	if s.promoteErr != nil {
		return &nodeagentv1.PromoteNodeResponse{Success: false, Message: s.promoteErr.Error()}, nil
	}
	return &nodeagentv1.PromoteNodeResponse{Success: true}, nil
}

func (s *minimalNodeAgent) ReconfigureReplication(_ context.Context, req *nodeagentv1.ReconfigureReplicationRequest) (*nodeagentv1.ReconfigureReplicationResponse, error) {
	return &nodeagentv1.ReconfigureReplicationResponse{Success: true}, nil
}

func (s *minimalNodeAgent) RunPgRewind(_ context.Context, _ *nodeagentv1.RunPgRewindRequest) (*nodeagentv1.RunPgRewindResponse, error) {
	return &nodeagentv1.RunPgRewindResponse{Success: true}, nil
}


func TestGRPCNodeAgentCaller_PromoteNode_Success(t *testing.T) {
	srv := &minimalNodeAgent{}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.PromoteNode(context.Background(), "passthrough://bufnet")
	if err != nil {
		t.Fatalf("PromoteNode: %v", err)
	}
	if !srv.promoteCalled {
		t.Error("expected PromoteNode to be called on server")
	}
}

func TestGRPCNodeAgentCaller_PromoteNode_ServerReportsFailure(t *testing.T) {
	srv := &minimalNodeAgent{promoteErr: errors.New("not a replica")}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.PromoteNode(context.Background(), "passthrough://bufnet")
	if err == nil {
		t.Error("expected error when server reports failure")
	}
}

func TestGRPCNodeAgentCaller_ReconfigureReplication_Success(t *testing.T) {
	srv := &minimalNodeAgent{}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.ReconfigureReplication(context.Background(), "passthrough://bufnet", "host=pg-primary port=5432 user=replicator", "latest")
	if err != nil {
		t.Fatalf("ReconfigureReplication: %v", err)
	}
}

// ─────────────────────────────────────────
// TriggerManualFailover — targeted failover
// ─────────────────────────────────────────

func TestTriggerManualFailover_PromotesSpecificTarget(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052"},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica2:50052"},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	// Явно указываем replica1, хотя replica2 имеет больший WAL LSN
	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("TriggerManualFailover: %v", err)
	}
	if caller.promoteCalledAddr != "replica1:50052" {
		t.Errorf("PromoteNode called with %q, want %q", caller.promoteCalledAddr, "replica1:50052")
	}
	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q", topo.primary, "pg-replica1")
	}
}

func TestTriggerManualFailover_RejectsUnknownTarget(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-unknown")
	if err == nil {
		t.Error("expected error for unknown target node")
	}
}

func TestTriggerManualFailover_RejectsUnhealthyTarget(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateUnreachable},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err == nil {
		t.Error("expected error for unhealthy target node")
	}
}

// ─────────────────────────────────────────
// QuorumSize enforcement
// ─────────────────────────────────────────

func TestNotifyPrimaryFailure_AbortsWhenBelowQuorum(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052"},
			},
		},
	}
	// QuorumSize=2, но живых реплик только 1 → failover не должен произойти
	mgr := failover.NewManager(failover.Config{QuorumSize: 2}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if caller.promoteCalledAddr != "" {
		t.Errorf("PromoteNode should NOT be called when below quorum, got addr %q", caller.promoteCalledAddr)
	}
	if topo.primary != "pg-primary" {
		t.Errorf("primary changed to %q despite quorum not met", topo.primary)
	}
}

func TestNotifyPrimaryFailure_ProceedsAtExactQuorum(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052"},
			},
		},
	}
	// QuorumSize=1, живых реплик ровно 1 → failover должен произойти
	mgr := failover.NewManager(failover.Config{QuorumSize: 1}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q", topo.primary, "pg-replica1")
	}
}

func TestNotifyPrimaryFailure_ProceedsWhenQuorumIsZero(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052"},
			},
		},
	}
	// QuorumSize=0 (не задан) → не блокирует failover
	mgr := failover.NewManager(failover.Config{QuorumSize: 0}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q (QuorumSize=0 should not block)", topo.primary, "pg-replica1")
	}
}

func TestTriggerManualFailover_RejectsPrimaryTarget(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-primary")
	if err == nil {
		t.Error("expected error when target is already primary")
	}
}

// ─────────────────────────────────────────
// TriggerManualFailover — quorum enforcement
// ─────────────────────────────────────────

func TestTriggerManualFailover_RejectsWhenBelowQuorum(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052"},
			},
		},
	}
	// QuorumSize=2, но живых реплик только 1 → manual failover должен быть отклонён
	mgr := failover.NewManager(failover.Config{QuorumSize: 2}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err == nil {
		t.Error("expected error when below quorum for manual failover")
	}
	if caller.promoteCalledAddr != "" {
		t.Errorf("PromoteNode should NOT be called when below quorum, got addr %q", caller.promoteCalledAddr)
	}
}

func TestTriggerManualFailover_ProceedsAtExactQuorum(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052"},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 500, Address: "replica2:50052"},
			},
		},
	}
	// QuorumSize=2, живых реплик ровно 2 → manual failover разрешён
	mgr := failover.NewManager(failover.Config{QuorumSize: 2}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("TriggerManualFailover: %v", err)
	}
	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q", topo.primary, "pg-replica1")
	}
}

func TestTriggerManualFailover_ProceedsWhenQuorumIsZero(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052"},
			},
		},
	}
	// QuorumSize=0 → не блокирует ручной failover
	mgr := failover.NewManager(failover.Config{QuorumSize: 0}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("TriggerManualFailover: %v", err)
	}
	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q", topo.primary, "pg-replica1")
	}
}

// ─────────────────────────────────────────
// HandleOldPrimaryRejoin — pg_rewind integration
// ─────────────────────────────────────────

func TestManager_HandleOldPrimaryRejoin_CallsPgRewindOnOldPrimary(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052"},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	// Запускаем failover: pg-primary падает, replica1 становится primary
	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Старый primary «возвращается» → должен быть вызван pg_rewind
	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")
	if err != nil {
		t.Fatalf("HandleOldPrimaryRejoin: %v", err)
	}
	if caller.pgRewindCalledAddr != "primary:50052" {
		t.Errorf("RunPgRewind called on %q, want %q", caller.pgRewindCalledAddr, "primary:50052")
	}
}

func TestManager_HandleOldPrimaryRejoin_NoopWhenNotOldPrimary(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052"},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	// Нет failover → нет oldPrimaryID → HandleOldPrimaryRejoin должен быть no-op
	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-replica2", "replica2:50052")
	if err != nil {
		t.Fatalf("HandleOldPrimaryRejoin should be no-op: %v", err)
	}
	if caller.pgRewindCalledAddr != "" {
		t.Errorf("RunPgRewind should NOT be called for non-old-primary, got addr %q", caller.pgRewindCalledAddr)
	}
}

func TestManager_HandleOldPrimaryRejoin_ClearsAfterRejoin(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052"},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052"},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	// Первый вызов — pg_rewind выполняется
	_ = mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")

	// Второй вызов — oldPrimaryID уже сброшен, должен быть no-op
	caller.pgRewindCalledAddr = ""
	_ = mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")
	if caller.pgRewindCalledAddr != "" {
		t.Errorf("RunPgRewind called twice — second call should be no-op")
	}
}

// ─────────────────────────────────────────
// GRPCNodeAgentCaller — RunPgRewind
// ─────────────────────────────────────────

func TestGRPCNodeAgentCaller_RunPgRewind_Success(t *testing.T) {
	srv := &minimalNodeAgent{}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.RunPgRewind(context.Background(), "passthrough://bufnet", "host=new-primary port=5432 user=replicator")
	if err != nil {
		t.Fatalf("RunPgRewind: %v", err)
	}
}

// ─────────────────────────────────────────
// GRPCNodeAgentCaller — retry logic
// ─────────────────────────────────────────

type flakeyNodeAgent struct {
	nodeagentv1.UnimplementedNodeAgentServiceServer
	calls     int
	failFirst int // fail the first N calls, succeed after
}

func (s *flakeyNodeAgent) PromoteNode(_ context.Context, _ *nodeagentv1.PromoteNodeRequest) (*nodeagentv1.PromoteNodeResponse, error) {
	s.calls++
	if s.calls <= s.failFirst {
		return &nodeagentv1.PromoteNodeResponse{Success: false, Message: "not ready yet"}, nil
	}
	return &nodeagentv1.PromoteNodeResponse{Success: true}, nil
}

func (s *flakeyNodeAgent) RunPgRewind(_ context.Context, _ *nodeagentv1.RunPgRewindRequest) (*nodeagentv1.RunPgRewindResponse, error) {
	return &nodeagentv1.RunPgRewindResponse{Success: true}, nil
}

func (s *flakeyNodeAgent) ReconfigureReplication(_ context.Context, _ *nodeagentv1.ReconfigureReplicationRequest) (*nodeagentv1.ReconfigureReplicationResponse, error) {
	return &nodeagentv1.ReconfigureReplicationResponse{Success: true}, nil
}

func TestGRPCNodeAgentCaller_PromoteNode_RetriesOnFailure(t *testing.T) {
	srv := &flakeyNodeAgent{failFirst: 2}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.PromoteNode(context.Background(), "passthrough://bufnet")
	if err != nil {
		t.Fatalf("PromoteNode with retry: %v", err)
	}
	if srv.calls < 3 {
		t.Errorf("expected at least 3 calls (2 failures + 1 success), got %d", srv.calls)
	}
}

func TestGRPCNodeAgentCaller_PromoteNode_FailsAfterMaxRetries(t *testing.T) {
	srv := &flakeyNodeAgent{failFirst: 100} // always fails
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.PromoteNode(context.Background(), "passthrough://bufnet")
	if err == nil {
		t.Error("expected error after max retries exceeded")
	}
}
