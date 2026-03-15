package controller_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	nodeagentv1 "github.com/zhavkk/Diploma/api/proto/gen/nodeagent/v1"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/node-agent/internal/controller"
)

// ─────────────────────────────────────────
// Моки
// ─────────────────────────────────────────

type mockCommander struct {
	promoteErr    error
	pgRewindErr   error
	restartErr    error
	reconfigErr   error
	promoteCalled bool
	rewindCalled  bool
	restartCalled bool
	reconfigArgs  struct{ connInfo, timeline string }
}

func (m *mockCommander) Promote(_ context.Context) error {
	m.promoteCalled = true
	return m.promoteErr
}
func (m *mockCommander) PgRewind(_ context.Context, sourceConnInfo string) error {
	m.rewindCalled = true
	m.reconfigArgs.connInfo = sourceConnInfo
	return m.pgRewindErr
}
func (m *mockCommander) Restart(_ context.Context) error {
	m.restartCalled = true
	return m.restartErr
}
func (m *mockCommander) Reconfigure(_ context.Context, primaryConnInfo, timeline string) error {
	m.reconfigArgs.connInfo = primaryConnInfo
	m.reconfigArgs.timeline = timeline
	return m.reconfigErr
}

type mockStatusProvider struct {
	status *models.NodeStatus
}

func (m *mockStatusProvider) Latest() *models.NodeStatus { return m.status }

// ─────────────────────────────────────────
// Вспомогательная функция: in-process gRPC сервер через bufconn
// ─────────────────────────────────────────

func setupTestServer(t *testing.T, cmd controller.PGCommander, sp controller.NodeStatusProvider) (nodeagentv1.NodeAgentServiceClient, func()) {
	t.Helper()
	log := zap.NewNop()

	ctrl := controller.New(controller.Config{
		NodeID:   "test-node",
		PGData:   "/tmp/pgdata",
		GRPCAddr: ":0",
	}, cmd, sp, log)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	nodeagentv1.RegisterNodeAgentServiceServer(srv, ctrl)
	go srv.Serve(lis) //nolint:errcheck

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}

	client := nodeagentv1.NewNodeAgentServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return client, cleanup
}

// ─────────────────────────────────────────
// PromoteNode
// ─────────────────────────────────────────

func TestController_PromoteNode_Success(t *testing.T) {
	cmd := &mockCommander{}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.PromoteNode(context.Background(), &nodeagentv1.PromoteNodeRequest{})
	if err != nil {
		t.Fatalf("PromoteNode rpc error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true, got false: %s", resp.Message)
	}
	if !cmd.promoteCalled {
		t.Error("expected Promote() to be called")
	}
}

func TestController_PromoteNode_CommanderError(t *testing.T) {
	cmd := &mockCommander{promoteErr: errors.New("pg_ctl: no such file")}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.PromoteNode(context.Background(), &nodeagentv1.PromoteNodeRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false")
	}
	if resp.Message == "" {
		t.Error("expected non-empty error message")
	}
}

// ─────────────────────────────────────────
// RunPgRewind
// ─────────────────────────────────────────

func TestController_RunPgRewind_Success(t *testing.T) {
	cmd := &mockCommander{}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.RunPgRewind(context.Background(), &nodeagentv1.RunPgRewindRequest{
		SourceConninfo: "host=192.168.1.10 port=5432 user=replicator",
	})
	if err != nil {
		t.Fatalf("RunPgRewind rpc error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true: %s", resp.Message)
	}
	if !cmd.rewindCalled {
		t.Error("expected PgRewind() to be called")
	}
}

func TestController_RunPgRewind_PassesConnInfo(t *testing.T) {
	cmd := &mockCommander{}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	connInfo := "host=pg-primary port=5432 user=replicator"
	client.RunPgRewind(context.Background(), &nodeagentv1.RunPgRewindRequest{ //nolint:errcheck
		SourceConninfo: connInfo,
	})

	if cmd.reconfigArgs.connInfo != connInfo {
		t.Errorf("PgRewind called with %q, want %q", cmd.reconfigArgs.connInfo, connInfo)
	}
}

func TestController_RunPgRewind_CommanderError(t *testing.T) {
	cmd := &mockCommander{pgRewindErr: errors.New("diverged timeline")}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.RunPgRewind(context.Background(), &nodeagentv1.RunPgRewindRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false on rewind error")
	}
}

// ─────────────────────────────────────────
// ReconfigureReplication
// ─────────────────────────────────────────

func TestController_ReconfigureReplication_Success(t *testing.T) {
	cmd := &mockCommander{}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.ReconfigureReplication(context.Background(), &nodeagentv1.ReconfigureReplicationRequest{
		PrimaryConninfo:        "host=pg-primary port=5432 user=replicator",
		RecoveryTargetTimeline: "latest",
	})
	if err != nil {
		t.Fatalf("ReconfigureReplication rpc error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true: %s", resp.Message)
	}
	if cmd.reconfigArgs.connInfo != "host=pg-primary port=5432 user=replicator" {
		t.Errorf("wrong connInfo passed: %q", cmd.reconfigArgs.connInfo)
	}
	if cmd.reconfigArgs.timeline != "latest" {
		t.Errorf("wrong timeline passed: %q", cmd.reconfigArgs.timeline)
	}
}

func TestController_ReconfigureReplication_CommanderError(t *testing.T) {
	cmd := &mockCommander{reconfigErr: errors.New("permission denied")}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.ReconfigureReplication(context.Background(), &nodeagentv1.ReconfigureReplicationRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false")
	}
}

// ─────────────────────────────────────────
// RestartPostgres
// ─────────────────────────────────────────

func TestController_RestartPostgres_Success(t *testing.T) {
	cmd := &mockCommander{}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.RestartPostgres(context.Background(), &nodeagentv1.RestartPostgresRequest{})
	if err != nil {
		t.Fatalf("RestartPostgres rpc error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true: %s", resp.Message)
	}
	if !cmd.restartCalled {
		t.Error("expected Restart() to be called")
	}
}

// ─────────────────────────────────────────
// GetNodeStatus
// ─────────────────────────────────────────

func TestController_GetNodeStatus_ReturnsCurrentStatus(t *testing.T) {
	sp := &mockStatusProvider{
		status: &models.NodeStatus{
			NodeID:         "test-node",
			Role:           models.RolePrimary,
			IsInRecovery:   false,
			WALReplayLSN:   12345,
			ReplicationLag: 0,
			PGVersion:      "PostgreSQL 16.0",
			PostgresRunning: true,
		},
	}
	client, cleanup := setupTestServer(t, &mockCommander{}, sp)
	defer cleanup()

	resp, err := client.GetNodeStatus(context.Background(), &nodeagentv1.GetNodeStatusRequest{})
	if err != nil {
		t.Fatalf("GetNodeStatus rpc error: %v", err)
	}
	if resp.Status.NodeId != "test-node" {
		t.Errorf("NodeId = %q, want %q", resp.Status.NodeId, "test-node")
	}
	if resp.Status.IsInRecovery {
		t.Error("IsInRecovery should be false for primary")
	}
	if resp.Status.WalReplayLsn != 12345 {
		t.Errorf("WalReplayLsn = %d, want 12345", resp.Status.WalReplayLsn)
	}
}

func TestController_GetNodeStatus_WhenProbeNotReady(t *testing.T) {
	sp := &mockStatusProvider{status: nil} // probe ещё не собрал данные
	client, cleanup := setupTestServer(t, &mockCommander{}, sp)
	defer cleanup()

	resp, err := client.GetNodeStatus(context.Background(), &nodeagentv1.GetNodeStatusRequest{})
	if err != nil {
		t.Fatalf("GetNodeStatus rpc error: %v", err)
	}
	// При nil статусе возвращаем пустой ответ, не ошибку транспорта
	if resp.Status == nil {
		t.Error("expected non-nil Status even when probe not ready")
	}
}
