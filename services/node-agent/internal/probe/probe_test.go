package probe_test

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/node-agent/internal/probe"
)

// ─────────────────────────────────────────
// Mock PGStatusClient
// ─────────────────────────────────────────

type mockPGClient struct {
	inRecovery bool
	replayLSN  int64
	receiveLSN int64
	version    string
	err        error
}

func (m *mockPGClient) IsInRecovery(_ context.Context) (bool, error) {
	return m.inRecovery, m.err
}
func (m *mockPGClient) WALReplayLSN(_ context.Context) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.replayLSN, nil
}
func (m *mockPGClient) WALReceiveLSN(_ context.Context) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.receiveLSN, nil
}
func (m *mockPGClient) Version(_ context.Context) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.version, nil
}

// ─────────────────────────────────────────
// probe.Collect() тесты
// ─────────────────────────────────────────

func TestProbe_Collect_SetsOwnAddress(t *testing.T) {
	pg := &mockPGClient{inRecovery: false, version: "PostgreSQL 15.2"}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "pg-primary:50052", PollInterval: 5}, pg, zap.NewNop())

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.Address != "pg-primary:50052" {
		t.Errorf("Address = %q, want %q", status.Address, "pg-primary:50052")
	}
}

func TestProbe_Collect_DetectsRoleAsPrimary(t *testing.T) {
	pg := &mockPGClient{inRecovery: false}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.Role != models.RolePrimary {
		t.Errorf("Role = %q, want %q", status.Role, models.RolePrimary)
	}
	if status.IsInRecovery {
		t.Error("IsInRecovery should be false for primary")
	}
}

func TestProbe_Collect_DetectsRoleAsReplica(t *testing.T) {
	pg := &mockPGClient{inRecovery: true, receiveLSN: 1000, replayLSN: 900}
	p := probe.New(probe.Config{NodeID: "pg-replica1", NodeAddr: "replica1:50052", PollInterval: 5}, pg, zap.NewNop())

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.Role != models.RoleReplica {
		t.Errorf("Role = %q, want %q", status.Role, models.RoleReplica)
	}
	if !status.IsInRecovery {
		t.Error("IsInRecovery should be true for replica")
	}
}

func TestProbe_Collect_ComputesWALLagForReplica(t *testing.T) {
	pg := &mockPGClient{inRecovery: true, receiveLSN: 10000, replayLSN: 9000}
	p := probe.New(probe.Config{NodeID: "pg-replica1", NodeAddr: "replica1:50052", PollInterval: 5}, pg, zap.NewNop())

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.ReplicationLag != 1000 {
		t.Errorf("ReplicationLag = %d, want 1000", status.ReplicationLag)
	}
}

func TestProbe_Collect_ZeroLagForPrimary(t *testing.T) {
	pg := &mockPGClient{inRecovery: false, receiveLSN: 50000, replayLSN: 50000}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.ReplicationLag != 0 {
		t.Errorf("ReplicationLag = %d, want 0 for primary", status.ReplicationLag)
	}
}

func TestProbe_Collect_ErrorFromPGReturnsError(t *testing.T) {
	pg := &mockPGClient{err: errors.New("connection refused")}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	_, err := p.Collect(context.Background())
	if err == nil {
		t.Error("expected error when pg query fails")
	}
}

func TestProbe_Run_SetsPostgresRunningFalseOnError(t *testing.T) {
	// First call succeeds, then fails — Latest() should reflect PostgresRunning=false.
	successPG := &mockPGClient{inRecovery: false, version: "PG15"}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, successPG, zap.NewNop())

	// Collect seeds p.latest internally on success.
	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("initial Collect: %v", err)
	}
	if !status.PostgresRunning {
		t.Fatal("expected PostgresRunning=true on success")
	}

	// Simulate a subsequent failure.
	p.MarkPostgresDown()

	latest := p.Latest()
	if latest == nil {
		t.Fatal("Latest() should not be nil after MarkPostgresDown")
	}
	if latest.PostgresRunning {
		t.Error("expected PostgresRunning=false after MarkPostgresDown")
	}
	if latest.State != models.StateDegraded {
		t.Errorf("State = %q, want %q", latest.State, models.StateDegraded)
	}
}
