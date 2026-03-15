package coordination_test

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/services/orchestrator/internal/coordination"
)

// ─────────────────────────────────────────
// Mock EtcdBackend
// ─────────────────────────────────────────

type mockEtcd struct {
	store map[string]string
	err   error
}

func newMockEtcd() *mockEtcd {
	return &mockEtcd{store: make(map[string]string)}
}

func (m *mockEtcd) Put(_ context.Context, key, value string) error {
	if m.err != nil {
		return m.err
	}
	m.store[key] = value
	return nil
}

func (m *mockEtcd) Get(_ context.Context, key string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.store[key], nil
}

func (m *mockEtcd) Campaign(_ context.Context, key, val string) (func(context.Context) error, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.store[key] = val
	return func(_ context.Context) error { return nil }, nil
}

// ─────────────────────────────────────────
// Тесты
// ─────────────────────────────────────────

func TestCoordModule_IsLeader_ReturnsTrueWhenLeader(t *testing.T) {
	backend := newMockEtcd()
	backend.store["/ha-orchestrator/leader"] = "node-1"

	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	leader, err := mod.IsLeader(context.Background())
	if err != nil {
		t.Fatalf("IsLeader: %v", err)
	}
	if !leader {
		t.Error("IsLeader = false, want true when stored value matches NodeID")
	}
}

func TestCoordModule_IsLeader_ReturnsFalseWhenNotLeader(t *testing.T) {
	backend := newMockEtcd()
	backend.store["/ha-orchestrator/leader"] = "node-2"

	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	leader, err := mod.IsLeader(context.Background())
	if err != nil {
		t.Fatalf("IsLeader: %v", err)
	}
	if leader {
		t.Error("IsLeader = true, want false when stored value is different NodeID")
	}
}

func TestCoordModule_IsLeader_ReturnsFalseWhenKeyAbsent(t *testing.T) {
	backend := newMockEtcd() // пустой store

	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	leader, err := mod.IsLeader(context.Background())
	if err != nil {
		t.Fatalf("IsLeader: %v", err)
	}
	if leader {
		t.Error("IsLeader = true, want false when key is absent")
	}
}

func TestCoordModule_PutAndGetClusterState_RoundTrip(t *testing.T) {
	backend := newMockEtcd()
	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	if err := mod.PutClusterState(context.Background(), "primary", "pg-primary"); err != nil {
		t.Fatalf("PutClusterState: %v", err)
	}

	val, err := mod.GetClusterState(context.Background(), "primary")
	if err != nil {
		t.Fatalf("GetClusterState: %v", err)
	}
	if val != "pg-primary" {
		t.Errorf("GetClusterState = %q, want %q", val, "pg-primary")
	}
}

func TestCoordModule_GetClusterState_ReturnsEmptyWhenNotSet(t *testing.T) {
	backend := newMockEtcd()
	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	val, err := mod.GetClusterState(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("GetClusterState: %v", err)
	}
	if val != "" {
		t.Errorf("GetClusterState = %q, want empty string for missing key", val)
	}
}

func TestCoordModule_IsLeader_ReturnsErrorOnBackendFailure(t *testing.T) {
	backend := newMockEtcd()
	backend.err = errors.New("etcd: connection refused")

	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	_, err := mod.IsLeader(context.Background())
	if err == nil {
		t.Error("expected error when backend fails")
	}
}
