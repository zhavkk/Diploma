package controller_test

import (
	"context"
	"errors"
	"net"
	"sync"
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
// Mocks
// ─────────────────────────────────────────

// mockCommander implements PGCommander, recording calls and returning
// configurable errors. All fields are guarded by mu for -race safety.
type mockCommander struct {
	mu sync.Mutex

	promoteErr    error
	promoteCalled bool

	pgRewindErr    error
	rewindCalled   bool
	rewindSource   string

	restartErr    error
	restartCalled bool

	reconfigErr      error
	reconfigCalled   bool
	reconfigConnInfo string
	reconfigTimeline string
}

func (m *mockCommander) Promote(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.promoteCalled = true
	return m.promoteErr
}

func (m *mockCommander) PgRewind(_ context.Context, sourceConnInfo string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rewindCalled = true
	m.rewindSource = sourceConnInfo
	return m.pgRewindErr
}

func (m *mockCommander) Restart(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.restartCalled = true
	return m.restartErr
}

func (m *mockCommander) Reconfigure(_ context.Context, primaryConnInfo, timeline string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconfigCalled = true
	m.reconfigConnInfo = primaryConnInfo
	m.reconfigTimeline = timeline
	return m.reconfigErr
}

// mockStatusProvider implements NodeStatusProvider, returning a configurable
// NodeStatus. Access is guarded by mu for -race safety.
type mockStatusProvider struct {
	mu     sync.RWMutex
	status *models.NodeStatus
}

func (m *mockStatusProvider) Latest() *models.NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

// ─────────────────────────────────────────
// Test helper: in-process gRPC server via bufconn
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
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
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
		lis.Close()
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
	if resp.Message != "promoted" {
		t.Errorf("expected message %q, got %q", "promoted", resp.Message)
	}

	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	if !cmd.promoteCalled {
		t.Error("expected Promote() to be called")
	}
}

func TestController_PromoteNode_Failure(t *testing.T) {
	cmd := &mockCommander{promoteErr: errors.New("pg_ctl: no such file")}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.PromoteNode(context.Background(), &nodeagentv1.PromoteNodeRequest{})
	if err != nil {
		// Controller returns the error inside the response, not as a gRPC status error.
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false when commander returns error")
	}
	if resp.Message == "" {
		t.Error("expected non-empty error message in response")
	}
	if resp.Message != "pg_ctl: no such file" {
		t.Errorf("expected error message %q, got %q", "pg_ctl: no such file", resp.Message)
	}
}

// ─────────────────────────────────────────
// ReconfigureReplication
// ─────────────────────────────────────────

func TestController_ReconfigureReplication_Success(t *testing.T) {
	cmd := &mockCommander{}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	wantConn := "host=pg-primary port=5432 user=replicator"
	wantTimeline := "latest"

	resp, err := client.ReconfigureReplication(context.Background(), &nodeagentv1.ReconfigureReplicationRequest{
		PrimaryConninfo:        wantConn,
		RecoveryTargetTimeline: wantTimeline,
	})
	if err != nil {
		t.Fatalf("ReconfigureReplication rpc error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true: %s", resp.Message)
	}
	if resp.Message != "reconfigured" {
		t.Errorf("expected message %q, got %q", "reconfigured", resp.Message)
	}

	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	if !cmd.reconfigCalled {
		t.Error("expected Reconfigure() to be called")
	}
	if cmd.reconfigConnInfo != wantConn {
		t.Errorf("primary conninfo: want %q, got %q", wantConn, cmd.reconfigConnInfo)
	}
	if cmd.reconfigTimeline != wantTimeline {
		t.Errorf("timeline: want %q, got %q", wantTimeline, cmd.reconfigTimeline)
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
	if resp.Message != "permission denied" {
		t.Errorf("expected error message %q, got %q", "permission denied", resp.Message)
	}
}

// ─────────────────────────────────────────
// RunPgRewind
// ─────────────────────────────────────────

func TestController_RunPgRewind_Success(t *testing.T) {
	cmd := &mockCommander{}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	wantSource := "host=192.168.1.10 port=5432 user=replicator"

	resp, err := client.RunPgRewind(context.Background(), &nodeagentv1.RunPgRewindRequest{
		SourceConninfo: wantSource,
	})
	if err != nil {
		t.Fatalf("RunPgRewind rpc error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true: %s", resp.Message)
	}
	if resp.Message != "pg_rewind completed" {
		t.Errorf("expected message %q, got %q", "pg_rewind completed", resp.Message)
	}

	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	if !cmd.rewindCalled {
		t.Error("expected PgRewind() to be called")
	}
	if cmd.rewindSource != wantSource {
		t.Errorf("source conninfo: want %q, got %q", wantSource, cmd.rewindSource)
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
	if resp.Message != "diverged timeline" {
		t.Errorf("expected error message %q, got %q", "diverged timeline", resp.Message)
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
	if resp.Message != "restarted" {
		t.Errorf("expected message %q, got %q", "restarted", resp.Message)
	}

	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	if !cmd.restartCalled {
		t.Error("expected Restart() to be called")
	}
}

func TestController_RestartPostgres_CommanderError(t *testing.T) {
	cmd := &mockCommander{restartErr: errors.New("pg_ctl restart failed")}
	client, cleanup := setupTestServer(t, cmd, &mockStatusProvider{})
	defer cleanup()

	resp, err := client.RestartPostgres(context.Background(), &nodeagentv1.RestartPostgresRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false")
	}
	if resp.Message != "pg_ctl restart failed" {
		t.Errorf("expected error message %q, got %q", "pg_ctl restart failed", resp.Message)
	}
}

// ─────────────────────────────────────────
// GetNodeStatus
// ─────────────────────────────────────────

func TestController_GetNodeStatus_Success(t *testing.T) {
	sp := &mockStatusProvider{
		status: &models.NodeStatus{
			NodeID:          "test-node",
			Role:            models.RolePrimary,
			IsInRecovery:    false,
			WALReceiveLSN:   123456,
			WALReplayLSN:    123400,
			ReplicationLag:  56,
			PGVersion:       "PostgreSQL 16.0",
			PostgresRunning: true,
		},
	}
	client, cleanup := setupTestServer(t, &mockCommander{}, sp)
	defer cleanup()

	resp, err := client.GetNodeStatus(context.Background(), &nodeagentv1.GetNodeStatusRequest{})
	if err != nil {
		t.Fatalf("GetNodeStatus rpc error: %v", err)
	}

	s := resp.GetStatus()
	if s == nil {
		t.Fatal("expected non-nil Status in response")
	}
	if s.NodeId != "test-node" {
		t.Errorf("NodeId: want %q, got %q", "test-node", s.NodeId)
	}
	if s.Role != "primary" {
		t.Errorf("Role: want %q, got %q", "primary", s.Role)
	}
	if s.IsInRecovery {
		t.Error("IsInRecovery: want false, got true")
	}
	if s.WalReceiveLsn != 123456 {
		t.Errorf("WalReceiveLsn: want 123456, got %d", s.WalReceiveLsn)
	}
	if s.WalReplayLsn != 123400 {
		t.Errorf("WalReplayLsn: want 123400, got %d", s.WalReplayLsn)
	}
	if s.ReplicationLag != 56 {
		t.Errorf("ReplicationLag: want 56, got %d", s.ReplicationLag)
	}
	if s.PgVersion != "PostgreSQL 16.0" {
		t.Errorf("PgVersion: want %q, got %q", "PostgreSQL 16.0", s.PgVersion)
	}
	if !s.PostgresRunning {
		t.Error("PostgresRunning: want true, got false")
	}
}

func TestController_GetNodeStatus_WhenProbeNotReady(t *testing.T) {
	sp := &mockStatusProvider{status: nil}
	client, cleanup := setupTestServer(t, &mockCommander{}, sp)
	defer cleanup()

	resp, err := client.GetNodeStatus(context.Background(), &nodeagentv1.GetNodeStatusRequest{})
	if err != nil {
		t.Fatalf("GetNodeStatus rpc error: %v", err)
	}

	s := resp.GetStatus()
	if s == nil {
		t.Fatal("expected non-nil Status even when probe not ready")
	}
	// NodeId is always set from controller config, even when Latest() returns nil.
	if s.NodeId != "test-node" {
		t.Errorf("NodeId: want %q, got %q", "test-node", s.NodeId)
	}
	// All other fields should be zero values when Latest() returns nil.
	if s.Role != "" {
		t.Errorf("Role: want empty, got %q", s.Role)
	}
	if s.IsInRecovery {
		t.Error("IsInRecovery: want false, got true")
	}
	if s.WalReceiveLsn != 0 {
		t.Errorf("WalReceiveLsn: want 0, got %d", s.WalReceiveLsn)
	}
	if s.WalReplayLsn != 0 {
		t.Errorf("WalReplayLsn: want 0, got %d", s.WalReplayLsn)
	}
	if s.ReplicationLag != 0 {
		t.Errorf("ReplicationLag: want 0, got %d", s.ReplicationLag)
	}
	if s.PgVersion != "" {
		t.Errorf("PgVersion: want empty, got %q", s.PgVersion)
	}
	if s.PostgresRunning {
		t.Error("PostgresRunning: want false, got true")
	}
}
