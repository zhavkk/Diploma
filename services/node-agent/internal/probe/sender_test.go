package probe_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	orchestratorv1 "github.com/zhavkk/Diploma/api/proto/gen/orchestrator/v1"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/node-agent/internal/probe"
)

// ─────────────────────────────────────────
// Mock orchestrator gRPC server
// ─────────────────────────────────────────

type mockOrchestratorServer struct {
	orchestratorv1.UnimplementedOrchestratorServiceServer
	hits    atomic.Int32
	lastReq atomic.Value // *orchestratorv1.ReportHeartbeatRequest
	failErr error
}

func (s *mockOrchestratorServer) ReportHeartbeat(_ context.Context, req *orchestratorv1.ReportHeartbeatRequest) (*orchestratorv1.ReportHeartbeatResponse, error) {
	s.hits.Add(1)
	s.lastReq.Store(req)
	if s.failErr != nil {
		return nil, s.failErr
	}
	return &orchestratorv1.ReportHeartbeatResponse{Ok: true}, nil
}

func newSenderTestServer(t *testing.T, srv orchestratorv1.OrchestratorServiceServer) (sender *probe.GRPCSender, cleanup func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	orchestratorv1.RegisterOrchestratorServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck

	dialFn := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	s := probe.NewGRPCSenderWithDialer(dialFn, "passthrough://bufnet")
	return s, func() {
		s.Close()
		grpcSrv.Stop()
		lis.Close()
	}
}

// ─────────────────────────────────────────
// GRPCSender тесты
// ─────────────────────────────────────────

func TestGRPCSender_Send_Success(t *testing.T) {
	srv := &mockOrchestratorServer{}
	sender, cleanup := newSenderTestServer(t, srv)
	defer cleanup()

	status := &models.NodeStatus{
		NodeID:          "pg-replica1",
		Address:         "replica1:50052",
		Role:            models.RoleReplica,
		IsInRecovery:    true,
		WALReplayLSN:    9000,
		ReplicationLag:  1000,
		PostgresRunning: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sender.Send(ctx, status); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if srv.hits.Load() != 1 {
		t.Errorf("expected 1 ReportHeartbeat call, got %d", srv.hits.Load())
	}

	got, ok := srv.lastReq.Load().(*orchestratorv1.ReportHeartbeatRequest)
	if !ok || got == nil {
		t.Fatal("expected lastReq to be stored")
	}
	if got.NodeId != "pg-replica1" {
		t.Errorf("NodeId = %q, want %q", got.NodeId, "pg-replica1")
	}
	if got.Role != string(models.RoleReplica) {
		t.Errorf("Role = %q, want %q", got.Role, string(models.RoleReplica))
	}
	if got.WalReplayLsn != 9000 {
		t.Errorf("WalReplayLsn = %d, want 9000", got.WalReplayLsn)
	}
	if got.ReplicationLag != 1000 {
		t.Errorf("ReplicationLag = %d, want 1000", got.ReplicationLag)
	}
	if !got.IsInRecovery {
		t.Error("expected IsInRecovery=true")
	}
	if !got.PostgresRunning {
		t.Error("expected PostgresRunning=true")
	}
}

func TestGRPCSender_Send_ReuseConnection(t *testing.T) {
	srv := &mockOrchestratorServer{}
	sender, cleanup := newSenderTestServer(t, srv)
	defer cleanup()

	status := &models.NodeStatus{
		NodeID:          "pg-primary",
		Address:         "primary:50052",
		Role:            models.RolePrimary,
		PostgresRunning: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		if err := sender.Send(ctx, status); err != nil {
			t.Fatalf("Send #%d failed: %v", i+1, err)
		}
	}

	if srv.hits.Load() != 3 {
		t.Errorf("expected 3 ReportHeartbeat calls, got %d", srv.hits.Load())
	}
}

func TestGRPCSender_Close_Idempotent(t *testing.T) {
	srv := &mockOrchestratorServer{}
	sender, cleanup := newSenderTestServer(t, srv)
	defer cleanup()

	// Send once to establish connection.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status := &models.NodeStatus{
		NodeID:          "pg-primary",
		Address:         "primary:50052",
		Role:            models.RolePrimary,
		PostgresRunning: true,
	}
	if err := sender.Send(ctx, status); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Close twice — must not panic.
	if err := sender.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	// Second Close should be safe (conn already nil or closed).
	sender.Close()
}

func TestGRPCSender_Send_PropagatesError(t *testing.T) {
	srv := &mockOrchestratorServer{failErr: errors.New("internal error")}
	sender, cleanup := newSenderTestServer(t, srv)
	defer cleanup()

	status := &models.NodeStatus{
		NodeID:          "pg-primary",
		Address:         "primary:50052",
		Role:            models.RolePrimary,
		PostgresRunning: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := sender.Send(ctx, status)
	if err == nil {
		t.Fatal("expected error from Send when server returns error")
	}

	if srv.hits.Load() != 1 {
		t.Errorf("expected 1 ReportHeartbeat call even on error, got %d", srv.hits.Load())
	}
}
