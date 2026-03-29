package failover_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	nodeagentv1 "github.com/zhavkk/Diploma/api/proto/gen/nodeagent/v1"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
)

const callerBufSize = 1024 * 1024

// failingNodeAgentServer — фиктивный gRPC-сервер, возвращающий ошибку первые N раз.
type failingNodeAgentServer struct {
	nodeagentv1.UnimplementedNodeAgentServiceServer
	failCount    int32 // сколько раз ещё вернуть ошибку
	promoteHits  atomic.Int32
	reconfigHits atomic.Int32
	pgRewindHits atomic.Int32
	restartHits  atomic.Int32
}

func (s *failingNodeAgentServer) PromoteNode(_ context.Context, _ *nodeagentv1.PromoteNodeRequest) (*nodeagentv1.PromoteNodeResponse, error) {
	s.promoteHits.Add(1)
	if s.failCount > 0 {
		s.failCount--
		return nil, errors.New("transient error")
	}
	return &nodeagentv1.PromoteNodeResponse{Success: true}, nil
}

func (s *failingNodeAgentServer) ReconfigureReplication(_ context.Context, _ *nodeagentv1.ReconfigureReplicationRequest) (*nodeagentv1.ReconfigureReplicationResponse, error) {
	s.reconfigHits.Add(1)
	if s.failCount > 0 {
		s.failCount--
		return nil, errors.New("transient error")
	}
	return &nodeagentv1.ReconfigureReplicationResponse{Success: true}, nil
}

func (s *failingNodeAgentServer) RunPgRewind(_ context.Context, _ *nodeagentv1.RunPgRewindRequest) (*nodeagentv1.RunPgRewindResponse, error) {
	s.pgRewindHits.Add(1)
	if s.failCount > 0 {
		s.failCount--
		return nil, errors.New("transient error")
	}
	return &nodeagentv1.RunPgRewindResponse{Success: true}, nil
}

func (s *failingNodeAgentServer) RestartPostgres(_ context.Context, _ *nodeagentv1.RestartPostgresRequest) (*nodeagentv1.RestartPostgresResponse, error) {
	s.restartHits.Add(1)
	if s.failCount > 0 {
		s.failCount--
		return nil, errors.New("transient error")
	}
	return &nodeagentv1.RestartPostgresResponse{Success: true}, nil
}

func newCallerTestServer(t *testing.T, srv nodeagentv1.NodeAgentServiceServer) (caller *failover.GRPCNodeAgentCaller, cleanup func()) {
	t.Helper()
	lis := bufconn.Listen(callerBufSize)
	grpcSrv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck

	dialFn := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	c := failover.NewGRPCNodeAgentCallerWithDialer(dialFn)
	return c, func() { grpcSrv.Stop(); lis.Close() }
}

// ─────────────────────────────────────────
// PromoteNode — уже имеет retry, проверяем
// ─────────────────────────────────────────

func TestGRPCCaller_PromoteNode_RetriesOnTransientError(t *testing.T) {
	srv := &failingNodeAgentServer{failCount: 2} // первые 2 — ошибка, 3-й — успех
	caller, cleanup := newCallerTestServer(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := caller.PromoteNode(ctx, "passthrough://bufnet"); err != nil {
		t.Fatalf("PromoteNode failed after retries: %v", err)
	}
	if srv.promoteHits.Load() < 3 {
		t.Errorf("expected at least 3 PromoteNode calls (2 failures + 1 success), got %d", srv.promoteHits.Load())
	}
}

// ─────────────────────────────────────────
// ReconfigureReplication — должен иметь retry
// ─────────────────────────────────────────

func TestGRPCCaller_ReconfigureReplication_RetriesOnTransientError(t *testing.T) {
	srv := &failingNodeAgentServer{failCount: 2}
	caller, cleanup := newCallerTestServer(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := caller.ReconfigureReplication(ctx, "passthrough://bufnet", "host=pg-primary", "latest"); err != nil {
		t.Fatalf("ReconfigureReplication failed after retries: %v", err)
	}
	if srv.reconfigHits.Load() < 3 {
		t.Errorf("expected at least 3 ReconfigureReplication calls, got %d", srv.reconfigHits.Load())
	}
}

func TestGRPCCaller_ReconfigureReplication_SucceedsOnFirstAttempt(t *testing.T) {
	srv := &failingNodeAgentServer{failCount: 0}
	caller, cleanup := newCallerTestServer(t, srv)
	defer cleanup()

	ctx := context.Background()
	if err := caller.ReconfigureReplication(ctx, "passthrough://bufnet", "host=pg-primary", "latest"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.reconfigHits.Load() != 1 {
		t.Errorf("expected exactly 1 call on success, got %d", srv.reconfigHits.Load())
	}
}

// ─────────────────────────────────────────
// RunPgRewind — должен иметь retry
// ─────────────────────────────────────────

func TestGRPCCaller_RunPgRewind_RetriesOnTransientError(t *testing.T) {
	srv := &failingNodeAgentServer{failCount: 2}
	caller, cleanup := newCallerTestServer(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := caller.RunPgRewind(ctx, "passthrough://bufnet", "host=pg-new-primary"); err != nil {
		t.Fatalf("RunPgRewind failed after retries: %v", err)
	}
	if srv.pgRewindHits.Load() < 3 {
		t.Errorf("expected at least 3 RunPgRewind calls, got %d", srv.pgRewindHits.Load())
	}
}

func TestGRPCCaller_RunPgRewind_SucceedsOnFirstAttempt(t *testing.T) {
	srv := &failingNodeAgentServer{failCount: 0}
	caller, cleanup := newCallerTestServer(t, srv)
	defer cleanup()

	ctx := context.Background()
	if err := caller.RunPgRewind(ctx, "passthrough://bufnet", "host=pg-new-primary"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.pgRewindHits.Load() != 1 {
		t.Errorf("expected exactly 1 call on success, got %d", srv.pgRewindHits.Load())
	}
}

// -----------------------------------------
// RestartPostgres
// -----------------------------------------

func TestGRPCCaller_RestartPostgres_RetriesOnTransientError(t *testing.T) {
	srv := &failingNodeAgentServer{failCount: 2}
	caller, cleanup := newCallerTestServer(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := caller.RestartPostgres(ctx, "passthrough://bufnet"); err != nil {
		t.Fatalf("RestartPostgres failed after retries: %v", err)
	}
	if srv.restartHits.Load() < 3 {
		t.Errorf("expected at least 3 RestartPostgres calls, got %d", srv.restartHits.Load())
	}
}

func TestGRPCCaller_RestartPostgres_SucceedsOnFirstAttempt(t *testing.T) {
	srv := &failingNodeAgentServer{failCount: 0}
	caller, cleanup := newCallerTestServer(t, srv)
	defer cleanup()

	ctx := context.Background()
	if err := caller.RestartPostgres(ctx, "passthrough://bufnet"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.restartHits.Load() != 1 {
		t.Errorf("expected exactly 1 call on success, got %d", srv.restartHits.Load())
	}
}
