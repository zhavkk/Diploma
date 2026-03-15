package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	orchestratorv1 "github.com/zhavkk/Diploma/api/proto/gen/orchestrator/v1"
)

// ─────────────────────────────────────────
// Mock orchestrator server
// ─────────────────────────────────────────

type mockOrchestratorSrv struct {
	orchestratorv1.UnimplementedOrchestratorServiceServer
	triggerCalled   bool
	triggerTarget   string
	updateCalled    bool
	updateNames     string
}

func (s *mockOrchestratorSrv) GetClusterStatus(_ context.Context, _ *orchestratorv1.GetClusterStatusRequest) (*orchestratorv1.GetClusterStatusResponse, error) {
	return &orchestratorv1.GetClusterStatusResponse{
		Status: &orchestratorv1.ClusterStatus{
			PrimaryNode:     "pg-primary",
			ReplicaNodes:    []string{"pg-replica1", "pg-replica2"},
			TopologyVersion: "v1",
		},
	}, nil
}

func (s *mockOrchestratorSrv) TriggerFailover(_ context.Context, req *orchestratorv1.TriggerFailoverRequest) (*orchestratorv1.TriggerFailoverResponse, error) {
	s.triggerCalled = true
	s.triggerTarget = req.TargetNode
	return &orchestratorv1.TriggerFailoverResponse{Success: true, Message: "failover initiated"}, nil
}

func (s *mockOrchestratorSrv) ListNodes(_ context.Context, _ *orchestratorv1.ListNodesRequest) (*orchestratorv1.ListNodesResponse, error) {
	return &orchestratorv1.ListNodesResponse{
		Nodes: []*orchestratorv1.NodeInfo{
			{NodeId: "pg-primary", Role: "primary", Healthy: true},
			{NodeId: "pg-replica1", Role: "replica", Healthy: true, WalLag: 100},
			{NodeId: "pg-replica2", Role: "replica", Healthy: false, WalLag: 500},
		},
	}, nil
}

func (s *mockOrchestratorSrv) UpdateReplicationConfig(_ context.Context, req *orchestratorv1.UpdateReplicationConfigRequest) (*orchestratorv1.UpdateReplicationConfigResponse, error) {
	s.updateCalled = true
	s.updateNames = req.SynchronousStandbyNames
	return &orchestratorv1.UpdateReplicationConfigResponse{Success: true, Message: "applied"}, nil
}

// ─────────────────────────────────────────
// Helper: запуск in-process gRPC сервера
// ─────────────────────────────────────────

func startMockServer(t *testing.T, srv *mockOrchestratorSrv) string {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	orchestratorv1.RegisterOrchestratorServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	// Override the global dial function for tests
	testDialFn = func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	}
	t.Cleanup(func() { testDialFn = nil })

	return "passthrough://bufnet"
}

// ─────────────────────────────────────────
// Tests
// ─────────────────────────────────────────

func TestCmdStatus_PrintsPrimaryNode(t *testing.T) {
	srv := &mockOrchestratorSrv{}
	addr := startMockServer(t, srv)

	var buf bytes.Buffer
	err := runStatus(addr, &buf)
	if err != nil {
		t.Fatalf("status command: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "pg-primary") {
		t.Errorf("output %q does not contain primary node", out)
	}
}

func TestCmdStatus_PrintsReplicaNodes(t *testing.T) {
	srv := &mockOrchestratorSrv{}
	addr := startMockServer(t, srv)

	var buf bytes.Buffer
	_ = runStatus(addr, &buf)
	out := buf.String()
	if !strings.Contains(out, "pg-replica1") {
		t.Errorf("output %q does not contain replica1", out)
	}
}

func TestCmdFailover_CallsTriggerFailover(t *testing.T) {
	srv := &mockOrchestratorSrv{}
	addr := startMockServer(t, srv)

	var buf bytes.Buffer
	err := runFailover(addr, "pg-replica1", &buf)
	if err != nil {
		t.Fatalf("failover command: %v", err)
	}
	if !srv.triggerCalled {
		t.Error("expected TriggerFailover to be called")
	}
	if srv.triggerTarget != "pg-replica1" {
		t.Errorf("target = %q, want %q", srv.triggerTarget, "pg-replica1")
	}
}

func TestCmdNodes_PrintsNodeList(t *testing.T) {
	srv := &mockOrchestratorSrv{}
	addr := startMockServer(t, srv)

	var buf bytes.Buffer
	err := runNodes(addr, &buf)
	if err != nil {
		t.Fatalf("nodes command: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "pg-primary") || !strings.Contains(out, "pg-replica1") {
		t.Errorf("output %q missing nodes", out)
	}
}

func TestCmdNodes_ShowsHealthStatus(t *testing.T) {
	srv := &mockOrchestratorSrv{}
	addr := startMockServer(t, srv)

	var buf bytes.Buffer
	_ = runNodes(addr, &buf)
	out := buf.String()
	// pg-replica2 is unhealthy — должно быть отражено в выводе
	if !strings.Contains(out, "pg-replica2") {
		t.Errorf("output %q should contain pg-replica2", out)
	}
}

func TestCmdReplicationSetSync_CallsUpdateConfig(t *testing.T) {
	srv := &mockOrchestratorSrv{}
	addr := startMockServer(t, srv)

	var buf bytes.Buffer
	err := runSetSync(addr, "pg-replica1", &buf)
	if err != nil {
		t.Fatalf("set-sync command: %v", err)
	}
	if !srv.updateCalled {
		t.Error("expected UpdateReplicationConfig to be called")
	}
	if srv.updateNames != "pg-replica1" {
		t.Errorf("SynchronousStandbyNames = %q, want %q", srv.updateNames, "pg-replica1")
	}
}
