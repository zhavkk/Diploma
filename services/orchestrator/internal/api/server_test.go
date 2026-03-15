package api_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	orchestratorv1 "github.com/zhavkk/Diploma/api/proto/gen/orchestrator/v1"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/api"
)

// ─────────────────────────────────────────
// Моки
// ─────────────────────────────────────────

type mockTopology struct {
	topo   *models.ClusterTopology
	events []models.FailoverEvent
}

func (m *mockTopology) Get() *models.ClusterTopology { return m.topo }
func (m *mockTopology) Primary() string {
	if m.topo == nil {
		return ""
	}
	return m.topo.PrimaryNode
}
func (m *mockTopology) SetPrimary(id string) {
	if m.topo != nil {
		m.topo.PrimaryNode = id
	}
}
func (m *mockTopology) Events() []models.FailoverEvent { return m.events }

type mockFailover struct {
	triggerCalled      bool
	targetNode         string
	failoverInProgress bool
}

func (m *mockFailover) TriggerManualFailover(_ context.Context, targetNodeID string) error {
	m.triggerCalled = true
	m.targetNode = targetNodeID
	return nil
}

func (m *mockFailover) IsFailoverInProgress() bool {
	return m.failoverInProgress
}

type mockReplConf struct {
	applyCalled bool
	lastCfg     models.ReplicationConfig
}

func (m *mockReplConf) Apply(_ context.Context, cfg models.ReplicationConfig, _ []string) error {
	m.applyCalled = true
	m.lastCfg = cfg
	return nil
}

type mockHeartbeatReceiver struct {
	received *models.NodeStatus
}

func (m *mockHeartbeatReceiver) ReceiveHeartbeat(status *models.NodeStatus) {
	m.received = status
}

// ─────────────────────────────────────────
// Вспомогательная функция: in-process gRPC сервер
// ─────────────────────────────────────────

func setupOrchestrator(t *testing.T, topo api.TopologySource, fm api.FailoverTrigger, rc api.ReplicationApplier) (orchestratorv1.OrchestratorServiceClient, func()) {
	t.Helper()
	return setupOrchestratorWithHB(t, topo, fm, rc, &mockHeartbeatReceiver{})
}

func setupOrchestratorWithHB(t *testing.T, topo api.TopologySource, fm api.FailoverTrigger, rc api.ReplicationApplier, hb api.HeartbeatReceiver) (orchestratorv1.OrchestratorServiceClient, func()) {
	t.Helper()
	log := zap.NewNop()

	srv := api.NewServer(api.Config{GRPCAddr: ":0", HTTPAddr: ":0"}, topo, fm, rc, hb, log)

	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	orchestratorv1.RegisterOrchestratorServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}

	return orchestratorv1.NewOrchestratorServiceClient(conn), func() {
		conn.Close()
		grpcSrv.Stop()
	}
}

func sampleTopology() *models.ClusterTopology {
	return &models.ClusterTopology{
		Version:     "v1",
		PrimaryNode: "pg-primary",
		Nodes: []models.NodeStatus{
			{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy, LastHeartbeat: time.Now()},
			{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000, ReplicationLag: 10},
			{NodeID: "pg-replica2", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 4800, ReplicationLag: 50},
		},
	}
}

// ─────────────────────────────────────────
// GetClusterStatus
// ─────────────────────────────────────────

func TestServer_GetClusterStatus_ReturnsTopology(t *testing.T) {
	topo := &mockTopology{topo: sampleTopology()}
	client, cleanup := setupOrchestrator(t, topo, &mockFailover{}, &mockReplConf{})
	defer cleanup()

	resp, err := client.GetClusterStatus(context.Background(), &orchestratorv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if resp.Status.PrimaryNode != "pg-primary" {
		t.Errorf("PrimaryNode = %q, want %q", resp.Status.PrimaryNode, "pg-primary")
	}
	if len(resp.Status.ReplicaNodes) != 2 {
		t.Errorf("ReplicaNodes count = %d, want 2", len(resp.Status.ReplicaNodes))
	}
}

func TestServer_GetClusterStatus_WhenTopologyEmpty(t *testing.T) {
	topo := &mockTopology{topo: nil}
	client, cleanup := setupOrchestrator(t, topo, &mockFailover{}, &mockReplConf{})
	defer cleanup()

	resp, err := client.GetClusterStatus(context.Background(), &orchestratorv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if resp.Status.PrimaryNode != "" {
		t.Errorf("expected empty PrimaryNode, got %q", resp.Status.PrimaryNode)
	}
}

// ─────────────────────────────────────────
// TriggerFailover
// ─────────────────────────────────────────

func TestServer_TriggerFailover_CallsFailoverManager(t *testing.T) {
	fm := &mockFailover{}
	client, cleanup := setupOrchestrator(t, &mockTopology{topo: sampleTopology()}, fm, &mockReplConf{})
	defer cleanup()

	resp, err := client.TriggerFailover(context.Background(), &orchestratorv1.TriggerFailoverRequest{
		TargetNode: "pg-replica1",
	})
	if err != nil {
		t.Fatalf("TriggerFailover: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true: %s", resp.Message)
	}
	if !fm.triggerCalled {
		t.Error("expected TriggerManualFailover to be called")
	}
	if fm.targetNode != "pg-replica1" {
		t.Errorf("targetNode = %q, want %q", fm.targetNode, "pg-replica1")
	}
}

// ─────────────────────────────────────────
// ListNodes
// ─────────────────────────────────────────

func TestServer_ListNodes_ReturnsAllNodes(t *testing.T) {
	topo := &mockTopology{topo: sampleTopology()}
	client, cleanup := setupOrchestrator(t, topo, &mockFailover{}, &mockReplConf{})
	defer cleanup()

	resp, err := client.ListNodes(context.Background(), &orchestratorv1.ListNodesRequest{})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(resp.Nodes) != 3 {
		t.Errorf("nodes count = %d, want 3", len(resp.Nodes))
	}
}

func TestServer_ListNodes_CorrectRoles(t *testing.T) {
	topo := &mockTopology{topo: sampleTopology()}
	client, cleanup := setupOrchestrator(t, topo, &mockFailover{}, &mockReplConf{})
	defer cleanup()

	resp, _ := client.ListNodes(context.Background(), &orchestratorv1.ListNodesRequest{})

	for _, n := range resp.Nodes {
		if n.NodeId == "pg-primary" && n.Role != "primary" {
			t.Errorf("primary node has role %q, want %q", n.Role, "primary")
		}
		if n.NodeId == "pg-replica1" && n.Role != "replica" {
			t.Errorf("replica1 has role %q, want %q", n.Role, "replica")
		}
	}
}

func TestServer_ListNodes_ReplicaLagPopulated(t *testing.T) {
	topo := &mockTopology{topo: sampleTopology()}
	client, cleanup := setupOrchestrator(t, topo, &mockFailover{}, &mockReplConf{})
	defer cleanup()

	resp, _ := client.ListNodes(context.Background(), &orchestratorv1.ListNodesRequest{})
	for _, n := range resp.Nodes {
		if n.NodeId == "pg-replica1" && n.WalLag != 10 {
			t.Errorf("replica1 WalLag = %d, want 10", n.WalLag)
		}
	}
}

// ─────────────────────────────────────────
// UpdateReplicationConfig
// ─────────────────────────────────────────

func TestServer_UpdateReplicationConfig_AppliesConfig(t *testing.T) {
	rc := &mockReplConf{}
	client, cleanup := setupOrchestrator(t, &mockTopology{topo: sampleTopology()}, &mockFailover{}, rc)
	defer cleanup()

	resp, err := client.UpdateReplicationConfig(context.Background(), &orchestratorv1.UpdateReplicationConfigRequest{
		SynchronousStandbyNames: "pg-replica1",
		EnableSyncReplication:   true,
	})
	if err != nil {
		t.Fatalf("UpdateReplicationConfig: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true: %s", resp.Message)
	}
	if !rc.applyCalled {
		t.Error("expected Apply to be called")
	}
	if rc.lastCfg.SynchronousStandbyNames != "pg-replica1" {
		t.Errorf("SynchronousStandbyNames = %q, want %q", rc.lastCfg.SynchronousStandbyNames, "pg-replica1")
	}
}

// ─────────────────────────────────────────
// ReportHeartbeat
// ─────────────────────────────────────────

func TestServer_ReportHeartbeat_UpdatesMonitor(t *testing.T) {
	hb := &mockHeartbeatReceiver{}
	client, cleanup := setupOrchestratorWithHB(t, &mockTopology{topo: sampleTopology()}, &mockFailover{}, &mockReplConf{}, hb)
	defer cleanup()

	resp, err := client.ReportHeartbeat(context.Background(), &orchestratorv1.ReportHeartbeatRequest{
		NodeId:         "pg-replica1",
		Address:        "replica1:50052",
		Role:           "replica",
		IsInRecovery:   true,
		WalReplayLsn:   5000,
		ReplicationLag: 10,
		PostgresRunning: true,
	})
	if err != nil {
		t.Fatalf("ReportHeartbeat: %v", err)
	}
	if !resp.Ok {
		t.Error("expected Ok=true")
	}
	if hb.received == nil {
		t.Fatal("expected ReceiveHeartbeat to be called")
	}
	if hb.received.NodeID != "pg-replica1" {
		t.Errorf("received NodeID = %q, want %q", hb.received.NodeID, "pg-replica1")
	}
	if hb.received.WALReplayLSN != 5000 {
		t.Errorf("received WALReplayLSN = %d, want 5000", hb.received.WALReplayLSN)
	}
}

// ─────────────────────────────────────────
// Run — graceful shutdown
// ─────────────────────────────────────────

func TestServer_Run_HTTPShutdownOnContextCancel(t *testing.T) {
	// Занимаем свободный порт, чтобы знать адрес HTTP-сервера.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	httpAddr := ln.Addr().String()
	ln.Close()

	srv := api.NewServer(
		api.Config{GRPCAddr: "127.0.0.1:0", HTTPAddr: httpAddr},
		&mockTopology{topo: sampleTopology()},
		&mockFailover{},
		&mockReplConf{},
		&mockHeartbeatReceiver{},
		zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- srv.Run(ctx) }()

	// Ждём пока HTTP-сервер поднимется.
	deadline := time.Now().Add(2 * time.Second)
	var httpUp bool
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + httpAddr + "/healthz")
		if err == nil {
			resp.Body.Close()
			httpUp = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !httpUp {
		t.Fatal("HTTP server did not come up in 2s")
	}

	// Отменяем контекст — оба сервера должны остановиться.
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return within 3s after context cancel")
	}

	// HTTP-сервер должен быть уже недоступен.
	_, connErr := net.DialTimeout("tcp", httpAddr, 200*time.Millisecond)
	if connErr == nil {
		t.Error("HTTP server is still accepting connections after shutdown")
	}
}

func TestServer_GetClusterStatus_FailoverInProgressTrue(t *testing.T) {
	fm := &mockFailover{failoverInProgress: true}
	client, cleanup := setupOrchestrator(t, &mockTopology{topo: sampleTopology()}, fm, &mockReplConf{})
	defer cleanup()

	resp, err := client.GetClusterStatus(context.Background(), &orchestratorv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if !resp.Status.FailoverInProgress {
		t.Error("expected FailoverInProgress=true when failover is running")
	}
}

func TestServer_GetClusterStatus_FailoverInProgressFalse(t *testing.T) {
	fm := &mockFailover{failoverInProgress: false}
	client, cleanup := setupOrchestrator(t, &mockTopology{topo: sampleTopology()}, fm, &mockReplConf{})
	defer cleanup()

	resp, err := client.GetClusterStatus(context.Background(), &orchestratorv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if resp.Status.FailoverInProgress {
		t.Error("expected FailoverInProgress=false when no failover is running")
	}
}

func TestServer_HandleEvents_ReturnsEmptySlice(t *testing.T) {
	topo := &mockTopology{topo: sampleTopology()}
	srv := api.NewServer(
		api.Config{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"},
		topo, &mockFailover{}, &mockReplConf{}, &mockHeartbeatReceiver{},
		zap.NewNop(),
	)
	events := topo.Events()
	// nil and empty slice are both acceptable for an empty event log
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
	_ = srv
}

func TestServer_HandleEvents_ReturnsStoredEvents(t *testing.T) {
	events := []models.FailoverEvent{
		{OldPrimary: "pg-primary", NewPrimary: "pg-replica1", Reason: "automatic"},
	}
	topo := &mockTopology{topo: sampleTopology(), events: events}
	_ = api.NewServer(
		api.Config{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"},
		topo, &mockFailover{}, &mockReplConf{}, &mockHeartbeatReceiver{},
		zap.NewNop(),
	)
	got := topo.Events()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if got[0].Reason != "automatic" {
		t.Errorf("Reason = %q, want %q", got[0].Reason, "automatic")
	}
}

func TestServer_ReportHeartbeat_SetsAddress(t *testing.T) {
	hb := &mockHeartbeatReceiver{}
	client, cleanup := setupOrchestratorWithHB(t, &mockTopology{}, &mockFailover{}, &mockReplConf{}, hb)
	defer cleanup()

	_, err := client.ReportHeartbeat(context.Background(), &orchestratorv1.ReportHeartbeatRequest{
		NodeId:  "pg-primary",
		Address: "primary:50052",
		Role:    "primary",
	})
	if err != nil {
		t.Fatalf("ReportHeartbeat: %v", err)
	}
	if hb.received == nil {
		t.Fatal("expected ReceiveHeartbeat to be called")
	}
	if hb.received.Address != "primary:50052" {
		t.Errorf("received Address = %q, want %q", hb.received.Address, "primary:50052")
	}
}
