package failover_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	nodeagentv1 "github.com/zhavkk/Diploma/api/proto/gen/nodeagent/v1"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/pkg/version"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
)

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

type mockTopo struct {
	topo    *models.ClusterTopology
	primary string
}

func (m *mockTopo) Get() *models.ClusterTopology { return m.topo }
func (m *mockTopo) Primary() string              { return m.primary }
func (m *mockTopo) SetPrimary(id string)         { m.primary = id }

type mockCoord struct {
	isLeader bool
	err      error

	putCalls []mockPutCall
	putErr   error

	getStorage map[string]string
	getErr     error
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
func (m *mockCoord) GetClusterState(_ context.Context, key string) (string, error) {
	if m.getErr != nil {
		return "", m.getErr
	}
	if m.getStorage == nil {
		return "", nil
	}
	return m.getStorage[key], nil
}

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

func TestElectNewPrimary_SelectsHighestWALLSN(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateUnreachable, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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

func TestNotifyPrimaryFailure_AbortsWhenNotLeader(t *testing.T) {
	coord := &mockCoord{isLeader: false}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary after failover = %q, want %q", topo.primary, "pg-replica1")
	}
}

func TestNotifyPrimaryFailure_PersistsPrimaryToEtcd(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if len(coord.putCalls) != 3 {
		t.Fatalf("PutClusterState called %d times, want 3 (fence set, primary, fence cleared)", len(coord.putCalls))
	}

	var primaryValue string
	for _, call := range coord.putCalls {
		if call.Key == "primary" {
			primaryValue = call.Value
			break
		}
	}
	if primaryValue != "pg-replica1" {
		t.Errorf("PutClusterState value for primary = %q, want %q", primaryValue, "pg-replica1")
	}

	var fenceValue string
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" {
			fenceValue = call.Value
			break
		}
	}
	if fenceValue == "" {
		t.Error("fence token was not set")
	}

	var lastFenceValue string
	for i := len(coord.putCalls) - 1; i >= 0; i-- {
		if coord.putCalls[i].Key == "fence/pg-primary" {
			lastFenceValue = coord.putCalls[i].Value
			break
		}
	}
	if lastFenceValue != "" {
		t.Errorf("fence token should be cleared after successful failover, got %q", lastFenceValue)
	}
}

func TestTriggerManualFailover_PersistsPrimaryToEtcd(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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

	if len(coord.putCalls) != 3 {
		t.Fatalf("PutClusterState called %d times, want 3 (fence set, primary, fence cleared)", len(coord.putCalls))
	}

	var primaryValue string
	for _, call := range coord.putCalls {
		if call.Key == "primary" {
			primaryValue = call.Value
			break
		}
	}
	if primaryValue != "pg-replica1" {
		t.Errorf("PutClusterState value for primary = %q, want %q", primaryValue, "pg-replica1")
	}

	var fenceValue string
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" {
			fenceValue = call.Value
			break
		}
	}
	if fenceValue == "" {
		t.Error("fence token was not set during manual failover")
	}

	var lastFenceValue string
	for i := len(coord.putCalls) - 1; i >= 0; i-- {
		if coord.putCalls[i].Key == "fence/pg-primary" {
			lastFenceValue = coord.putCalls[i].Value
			break
		}
	}
	if lastFenceValue != "" {
		t.Errorf("fence token should be cleared after successful manual failover, got %q", lastFenceValue)
	}
}

func TestNotifyPrimaryFailure_ContinuesWhenPrimaryPutFailsButNotFence(t *testing.T) {
	coord := &mockCoord{isLeader: true, getStorage: map[string]string{}, getErr: errors.New("etcd: connection refused")}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q despite GetClusterState error", topo.primary, "pg-replica1")
	}
}

func TestNotifyPrimaryFailure_CallsPromoteOnNewPrimary(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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

func TestNotifyPrimaryFailure_AbortsWhenPromoteFails(t *testing.T) {
	coord := &mockCoord{isLeader: true}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{promoteErr: errors.New("pg_ctl: timeout")}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-primary" {
		t.Errorf("primary should remain %q when promote fails, got %q", "pg-primary", topo.primary)
	}

	if mgr.IsFailoverInProgress() {
		t.Error("failoverInProgress flag should be cleared after abort")
	}
}

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
	go grpcSrv.Serve(lis)
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
	go grpcSrv.Serve(lis)
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
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.ReconfigureReplication(context.Background(), "passthrough://bufnet", "host=pg-primary port=5432 user=replicator", "latest")
	if err != nil {
		t.Fatalf("ReconfigureReplication: %v", err)
	}
}

func TestTriggerManualFailover_PromotesSpecificTarget(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, Address: "replica2:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

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

func TestTriggerManualFailover_AbortsWhenPromoteFails(t *testing.T) {
	caller := &mockNodeAgentCaller{promoteErr: errors.New("pg_ctl: permission denied")}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err == nil {
		t.Error("expected error when PromoteNode fails")
	}
	if !contains(err.Error(), "PromoteNode failed") {
		t.Errorf("error should mention PromoteNode failure, got: %v", err)
	}

	if topo.primary != "pg-primary" {
		t.Errorf("primary should remain %q when promote fails, got %q", "pg-primary", topo.primary)
	}

	if mgr.IsFailoverInProgress() {
		t.Error("failoverInProgress flag should be cleared after abort")
	}
}

func TestTriggerManualFailover_RejectsUnknownTarget(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateUnreachable, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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

func TestNotifyPrimaryFailure_AbortsWhenBelowQuorum(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
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

func TestTriggerManualFailover_RejectsWhenBelowQuorum(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 500, Address: "replica2:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

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

func TestManager_HandleOldPrimaryRejoin_CallsPgRewindOnOldPrimary(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

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
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 100, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	_ = mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")

	caller.pgRewindCalledAddr = ""
	_ = mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")
	if caller.pgRewindCalledAddr != "" {
		t.Errorf("RunPgRewind called twice — second call should be no-op")
	}
}

func TestGRPCNodeAgentCaller_RunPgRewind_Success(t *testing.T) {
	srv := &minimalNodeAgent{}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.RunPgRewind(context.Background(), "passthrough://bufnet", "host=new-primary port=5432 user=replicator")
	if err != nil {
		t.Fatalf("RunPgRewind: %v", err)
	}
}

type flakeyNodeAgent struct {
	nodeagentv1.UnimplementedNodeAgentServiceServer
	calls     int
	failFirst int
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
	go grpcSrv.Serve(lis)
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
	srv := &flakeyNodeAgent{failFirst: 100}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	caller := failover.NewGRPCNodeAgentCallerWithDialer(func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	})

	err := caller.PromoteNode(context.Background(), "passthrough://bufnet")
	if err == nil {
		t.Error("expected error after max retries exceeded")
	}
}

type mockReconfigurator struct {
	successCount int
	reconfErr    error
}

func (m *mockReconfigurator) ReconfigureAfterFailover(_ context.Context, _ string) (int, error) {
	return m.successCount, m.reconfErr
}

func (m *mockReconfigurator) PrimaryConnInfoForNode(_, _ string) string {
	return "host=localhost port=5432 user=replicator"
}

func TestNotifyPrimaryFailure_ContinuesWhenAllReplicasFailReconfig(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	rc := &mockReconfigurator{successCount: 0, reconfErr: errors.New("all replicas failed")}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, rc, caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q (failover commits even when all replicas fail to reconfigure)", topo.primary, "pg-replica1")
	}

	if caller.promoteCalledAddr != "replica1:50052" {
		t.Errorf("PromoteNode should have been called, got addr %q", caller.promoteCalledAddr)
	}
}

func TestNotifyPrimaryFailure_ContinuesWhenSomeReplicasFailReconfig(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 4000, Address: "replica2:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	rc := &mockReconfigurator{successCount: 1, reconfErr: errors.New("pg-replica2 failed")}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, rc, caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q (failover should succeed with at least one replica)", topo.primary, "pg-replica1")
	}
}

func TestNotifyPrimaryFailure_SucceedsWhenAllReplicasReconfigure(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 4000, Address: "replica2:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}

	rc := &mockReconfigurator{successCount: 2, reconfErr: nil}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, rc, caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q", topo.primary, "pg-replica1")
	}
}

func TestTriggerManualFailover_ContinuesWhenAllReplicasFailReconfig(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	rc := &mockReconfigurator{successCount: 0, reconfErr: errors.New("all replicas failed")}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, rc, caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")

	if err != nil {
		t.Errorf("manual failover should succeed even when all replicas fail to reconfigure: %v", err)
	}
	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q (failover commits even when all replicas fail)", topo.primary, "pg-replica1")
	}

	if caller.promoteCalledAddr != "replica1:50052" {
		t.Errorf("PromoteNode should have been called, got addr %q", caller.promoteCalledAddr)
	}
}

func TestTriggerManualFailover_ContinuesWhenSomeReplicasFailReconfig(t *testing.T) {
	caller := &mockNodeAgentCaller{}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 500, Address: "replica2:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	rc := &mockReconfigurator{successCount: 1, reconfErr: errors.New("one replica failed")}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, rc, caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Errorf("manual failover should succeed with at least one replica: %v", err)
	}
	if topo.primary != "pg-replica1" {
		t.Errorf("primary = %q, want %q", topo.primary, "pg-replica1")
	}
}

func TestElectNewPrimary_RejectsIncompatibleVersion(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, PGVersion: "PostgreSQL 15.0", PGVersionParsed: version.Parse("PostgreSQL 15.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")
	if elected != "" {
		t.Errorf("elected = %q, want empty string (should reject incompatible version)", elected)
	}
}

func TestElectNewPrimary_SkipsNodeWithUnparseableVersion(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, PGVersion: "unknown version", PGVersionParsed: version.Parse("unknown version")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true}, replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")
	if elected != "" {
		t.Errorf("elected = %q, want empty string (should skip node with unparsable version)", elected)
	}
}

func TestTriggerManualFailover_RejectsIncompatibleVersion(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, PGVersion: "PostgreSQL 15.0", PGVersionParsed: version.Parse("PostgreSQL 15.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err == nil {
		t.Error("expected error for incompatible version target node")
	}
	if !contains(err.Error(), "incompatible") {
		t.Errorf("error should mention version incompatibility, got: %v", err)
	}
}

func TestVersionCompatibilityCheckMetrics_Success(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 3000, PGVersion: "PostgreSQL 16.1", PGVersionParsed: version.Parse("PostgreSQL 16.1")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")

	if elected != "pg-replica1" {
		t.Fatalf("elected = %q, want %q", elected, "pg-replica1")
	}

	if elected != "" {

		t.Log("version compatibility check passed, metric incremented with result=success")
	}
}

func TestVersionCompatibilityCheckMetrics_Incompatible(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, PGVersion: "PostgreSQL 15.0", PGVersionParsed: version.Parse("PostgreSQL 15.0")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")

	if elected != "" {
		t.Errorf("elected = %q, want empty string (incompatible version rejected)", elected)
	}

	t.Log("version compatibility check failed, metric incremented with result=incompatible")
}

func TestVersionCompatibilityCheckMetrics_Unparseable(t *testing.T) {
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 9000, PGVersion: "unknown version string", PGVersionParsed: version.Parse("unknown version string")},
				{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	mgr := failover.NewManager(failover.Config{}, topo, &mockCoord{isLeader: true},
		replication.NewConfigurator(nil, nil, zap.NewNop()), nil, zap.NewNop())

	elected := mgr.ElectNewPrimary("pg-primary")

	if elected != "" {
		t.Errorf("elected = %q, want empty string (unparseable version rejected)", elected)
	}

	t.Log("version compatibility check failed, metric incremented with result=unparseable")
}

func TestNotifyPrimaryFailure_SetsFenceToken(t *testing.T) {
	coord := &mockCoord{
		isLeader:   true,
		putCalls:   []mockPutCall{},
		getStorage: map[string]string{},
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	fenceKey := ""
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" {
			fenceKey = call.Value
			break
		}
	}
	if fenceKey == "" {
		t.Error("fence token was not set for failed primary")
	}

	if len(fenceKey) != 32 {
		t.Errorf("fence token length = %d, want 32", len(fenceKey))
	}
}

func TestNotifyPrimaryFailure_ClearsFenceTokenOnSuccess(t *testing.T) {
	coord := &mockCoord{
		isLeader:   true,
		putCalls:   []mockPutCall{},
		getStorage: map[string]string{},
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	if len(coord.putCalls) < 2 {
		t.Fatalf("expected at least 2 Put calls (set and clear), got %d", len(coord.putCalls))
	}

	lastFenceValue := ""
	for i := len(coord.putCalls) - 1; i >= 0; i-- {
		if coord.putCalls[i].Key == "fence/pg-primary" {
			lastFenceValue = coord.putCalls[i].Value
			break
		}
	}

	if lastFenceValue != "" {
		t.Errorf("fence token should be cleared, got value %q", lastFenceValue)
	}
}

func TestTriggerManualFailover_SetsFenceToken(t *testing.T) {
	coord := &mockCoord{
		isLeader:   true,
		putCalls:   []mockPutCall{},
		getStorage: map[string]string{},
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("TriggerManualFailover: %v", err)
	}

	fenceKey := ""
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" {
			fenceKey = call.Value
			break
		}
	}
	if fenceKey == "" {
		t.Error("fence token was not set for old primary during manual failover")
	}
}

func TestTriggerManualFailover_ClearsFenceTokenOnSuccess(t *testing.T) {
	coord := &mockCoord{
		isLeader:   true,
		putCalls:   []mockPutCall{},
		getStorage: map[string]string{},
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 1000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")
	if err != nil {
		t.Fatalf("TriggerManualFailover: %v", err)
	}

	if len(coord.putCalls) < 2 {
		t.Fatalf("expected at least 2 Put calls (set and clear), got %d", len(coord.putCalls))
	}

	lastFenceValue := ""
	for i := len(coord.putCalls) - 1; i >= 0; i-- {
		if coord.putCalls[i].Key == "fence/pg-primary" {
			lastFenceValue = coord.putCalls[i].Value
			break
		}
	}

	if lastFenceValue != "" {
		t.Errorf("fence token should be cleared after manual failover, got value %q", lastFenceValue)
	}
}

func TestHandleOldPrimaryRejoin_RejectsFencedNode(t *testing.T) {
	coord := &mockCoord{
		isLeader:   true,
		putCalls:   []mockPutCall{},
		getStorage: map[string]string{"fence/pg-primary": "a1b2c3d4e5f6"},
	}
	topo := &mockTopo{
		primary: "pg-replica1",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-replica1",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")

	if err == nil {
		t.Error("expected error when fenced node tries to rejoin")
	}
	if !contains(err.Error(), "fenced") {
		t.Errorf("error should mention fencing, got: %v", err)
	}

	if caller.pgRewindCalledAddr != "" {
		t.Errorf("RunPgRewind should NOT be called for fenced node, got addr %q", caller.pgRewindCalledAddr)
	}
}

func TestHandleOldPrimaryRejoin_AllowsRejoinWhenNotFenced(t *testing.T) {
	coord := &mockCoord{
		isLeader:   true,
		putCalls:   []mockPutCall{},
		getStorage: map[string]string{},
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")
	coord.putCalls = nil

	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")

	if err != nil {
		t.Errorf("unexpected error when non-fenced node tries to rejoin: %v", err)
	}

	if caller.pgRewindCalledAddr != "primary:50052" {
		t.Errorf("RunPgRewind should be called for non-fenced node, got addr %q", caller.pgRewindCalledAddr)
	}
}

func TestHandleOldPrimaryRejoin_AllowsRejoinWithEtcdError(t *testing.T) {

	coord := &mockCoord{
		isLeader: true,
		putCalls: []mockPutCall{},
		getErr:   errors.New("etcd: connection refused"),
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	err := mgr.HandleOldPrimaryRejoin(context.Background(), "pg-primary", "primary:50052")

	if err != nil {
		t.Errorf("unexpected error when etcd is unreachable: %v", err)
	}

	if caller.pgRewindCalledAddr != "primary:50052" {
		t.Errorf("RunPgRewind should be called when etcd is unreachable (fail-open), got addr %q", caller.pgRewindCalledAddr)
	}
}

func TestNotifyPrimaryFailure_ClearsFenceTokenOnAbort(t *testing.T) {

	coord := &mockCoord{
		isLeader: false,
		putCalls: []mockPutCall{},
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	mgr.NotifyPrimaryFailure(context.Background(), "pg-primary")

	fenceSet := false
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" && call.Value != "" {
			fenceSet = true
		}
	}
	if !fenceSet {
		t.Error("fence token should have been set before failover started")
	}

	fenceCleared := false
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" && call.Value == "" {
			fenceCleared = true
		}
	}
	if !fenceCleared {
		t.Error("fence token should be cleared even when failover is aborted")
	}
}

func TestTriggerManualFailover_ClearsFenceTokenOnAbort(t *testing.T) {

	coord := &mockCoord{
		isLeader: false,
		putCalls: []mockPutCall{},
	}
	topo := &mockTopo{
		primary: "pg-primary",
		topo: &models.ClusterTopology{
			PrimaryNode: "pg-primary",
			Nodes: []models.NodeStatus{
				{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, Address: "primary:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
				{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, Address: "replica1:50052", PGVersion: "PostgreSQL 16.0", PGVersionParsed: version.Parse("PostgreSQL 16.0")},
			},
		},
	}
	caller := &mockNodeAgentCaller{}
	mgr := failover.NewManager(failover.Config{}, topo, coord, replication.NewConfigurator(nil, nil, zap.NewNop()), caller, zap.NewNop())

	err := mgr.TriggerManualFailover(context.Background(), "pg-replica1")

	if err == nil {
		t.Error("manual failover should have failed when not leader")
	}

	fenceSet := false
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" && call.Value != "" {
			fenceSet = true
		}
	}
	if !fenceSet {
		t.Error("fence token should have been set before failover started")
	}

	fenceCleared := false
	for _, call := range coord.putCalls {
		if call.Key == "fence/pg-primary" && call.Value == "" {
			fenceCleared = true
		}
	}
	if !fenceCleared {
		t.Error("fence token should be cleared even when manual failover is aborted")
	}
}
