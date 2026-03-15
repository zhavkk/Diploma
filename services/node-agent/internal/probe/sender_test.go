package probe_test

import (
	"context"
	"net"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	orchestratorv1 "github.com/zhavkk/Diploma/api/proto/gen/orchestrator/v1"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/node-agent/internal/probe"
)

// ─────────────────────────────────────────
// Минимальный mock оркестратора (только ReportHeartbeat)
// ─────────────────────────────────────────

type mockOrchestratorServer struct {
	orchestratorv1.UnimplementedOrchestratorServiceServer
	received *orchestratorv1.ReportHeartbeatRequest
}

func (s *mockOrchestratorServer) ReportHeartbeat(_ context.Context, req *orchestratorv1.ReportHeartbeatRequest) (*orchestratorv1.ReportHeartbeatResponse, error) {
	s.received = req
	return &orchestratorv1.ReportHeartbeatResponse{Ok: true}, nil
}

func setupMockOrchestrator(t *testing.T, srv *mockOrchestratorServer) func(ctx context.Context, addr string) (net.Conn, error) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	orchestratorv1.RegisterOrchestratorServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	return func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	}
}

// ─────────────────────────────────────────
// GRPCSender тесты
// ─────────────────────────────────────────

func TestGRPCSender_SendsHeartbeat(t *testing.T) {
	srv := &mockOrchestratorServer{}
	dialFn := setupMockOrchestrator(t, srv)
	sender := probe.NewGRPCSenderWithDialer(dialFn, "passthrough://bufnet")

	status := &models.NodeStatus{
		NodeID:          "pg-replica1",
		Address:         "pg-replica1:50052",
		Role:            models.RoleReplica,
		IsInRecovery:    true,
		WALReplayLSN:    9000,
		ReplicationLag:  15,
		PostgresRunning: true,
	}

	if err := sender.Send(context.Background(), status); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if srv.received == nil {
		t.Fatal("expected ReportHeartbeat to be called")
	}
	if srv.received.NodeId != "pg-replica1" {
		t.Errorf("NodeId = %q, want %q", srv.received.NodeId, "pg-replica1")
	}
	if srv.received.WalReplayLsn != 9000 {
		t.Errorf("WalReplayLsn = %d, want 9000", srv.received.WalReplayLsn)
	}
	if !srv.received.IsInRecovery {
		t.Error("IsInRecovery should be true for replica")
	}
}

func TestGRPCSender_SendsAllFields(t *testing.T) {
	srv := &mockOrchestratorServer{}
	dialFn := setupMockOrchestrator(t, srv)
	sender := probe.NewGRPCSenderWithDialer(dialFn, "passthrough://bufnet")

	status := &models.NodeStatus{
		NodeID:          "pg-primary",
		Address:         "pg-primary:50052",
		Role:            models.RolePrimary,
		IsInRecovery:    false,
		WALReplayLSN:    50000,
		ReplicationLag:  0,
		PostgresRunning: true,
	}

	_ = sender.Send(context.Background(), status)

	if srv.received.Role != "primary" {
		t.Errorf("Role = %q, want %q", srv.received.Role, "primary")
	}
	if srv.received.PostgresRunning != true {
		t.Error("PostgresRunning should be true")
	}
	if srv.received.ReplicationLag != 0 {
		t.Errorf("ReplicationLag = %d, want 0 for primary", srv.received.ReplicationLag)
	}
}

// ─────────────────────────────────────────
// Probe интеграционный тест: вызывает sender после collect
// (используем mock sender без реального PostgreSQL)
// ─────────────────────────────────────────

type mockSender struct {
	sent []*models.NodeStatus
}

func (m *mockSender) Send(_ context.Context, s *models.NodeStatus) error {
	m.sent = append(m.sent, s)
	return nil
}

func TestProbe_Latest_NilBeforeFirstCollect(t *testing.T) {
	p := probe.New(probe.Config{NodeID: "test-node", NodeAddr: "node:50052", PollInterval: 1}, nil, zap.NewNop())
	if p.Latest() != nil {
		t.Error("Latest() should be nil before first collect")
	}
}
