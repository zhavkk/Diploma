package coordination_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/services/orchestrator/internal/coordination"
)

type mockEtcd struct {
	store             map[string]string
	err               error
	campaignCallCount int32

	failFirstN  int32
	sessionDone chan struct{}
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

func (m *mockEtcd) Campaign(ctx context.Context, key, val string) (func(context.Context) error, <-chan struct{}, error) {
	n := atomic.AddInt32(&m.campaignCallCount, 1)
	if m.err != nil {
		return nil, nil, m.err
	}

	if n <= atomic.LoadInt32(&m.failFirstN) {
		return nil, nil, errors.New("campaign: simulated transient error")
	}
	m.store[key] = val

	sessionDone := m.sessionDone
	if sessionDone == nil {
		sessionDone = make(chan struct{})
	}
	resign := func(_ context.Context) error { return nil }
	return resign, sessionDone, nil
}

func TestCoordModule_IsLeader_ReturnsFalseBeforeCampaign(t *testing.T) {
	backend := newMockEtcd()
	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	leader, err := mod.IsLeader(context.Background())
	if err != nil {
		t.Fatalf("IsLeader: %v", err)
	}
	if leader {
		t.Error("IsLeader = true, want false before Campaign has been called")
	}
}

func TestCoordModule_IsLeader_ReturnsTrueAfterCampaignSucceeds(t *testing.T) {
	backend := newMockEtcd()
	mod := coordination.NewModuleWithBackend(
		coordination.Config{NodeID: "node-1", BackoffMin: time.Millisecond},
		backend, zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		mod.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&backend.campaignCallCount) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	leader, err := mod.IsLeader(context.Background())
	if err != nil {
		t.Fatalf("IsLeader: %v", err)
	}
	if !leader {
		t.Error("IsLeader = false, want true after Campaign succeeds")
	}

	cancel()
	<-done
}

func TestCoordModule_IsLeader_ReturnsFalseAfterSessionExpires(t *testing.T) {
	sessionDone := make(chan struct{})
	backend := newMockEtcd()
	backend.sessionDone = sessionDone

	mod := coordination.NewModuleWithBackend(
		coordination.Config{NodeID: "node-1", BackoffMin: time.Millisecond},
		backend, zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		mod.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&backend.campaignCallCount) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	leader, _ := mod.IsLeader(context.Background())
	if !leader {
		t.Fatal("expected IsLeader = true after first Campaign succeeds")
	}

	close(sessionDone)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&backend.campaignCallCount) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if atomic.LoadInt32(&backend.campaignCallCount) < 2 {
		t.Fatal("expected Campaign to be called at least twice after session expiry")
	}

	cancel()
	<-done
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

func TestCoordModule_IsLeader_NeverReturnsError(t *testing.T) {
	backend := newMockEtcd()
	backend.err = errors.New("etcd: connection refused")

	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	_, err := mod.IsLeader(context.Background())
	if err != nil {
		t.Errorf("IsLeader should never return error with atomic implementation, got: %v", err)
	}
}

func TestModule_Run_BecomesLeaderAndResignsOnCancel(t *testing.T) {
	backend := newMockEtcd()
	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		mod.Run(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&backend.campaignCallCount) < 1 {
		t.Error("expected Campaign to be called at least once")
	}

	cancel()

	select {
	case <-done:

	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestCoordModule_PutClusterState_ReturnsBackendError(t *testing.T) {
	backendErr := errors.New("etcd: connection refused")
	backend := newMockEtcd()
	backend.err = backendErr
	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	err := mod.PutClusterState(context.Background(), "primary", "pg-primary")
	if err == nil {
		t.Fatal("expected error from PutClusterState when backend fails")
	}
	if !errors.Is(err, backendErr) {
		t.Errorf("PutClusterState error = %v, want it to wrap %v", err, backendErr)
	}
}

func TestCoordModule_GetClusterState_ReturnsBackendError(t *testing.T) {
	backendErr := errors.New("etcd: connection refused")
	backend := newMockEtcd()
	backend.err = backendErr
	mod := coordination.NewModuleWithBackend(coordination.Config{NodeID: "node-1"}, backend, zap.NewNop())

	_, err := mod.GetClusterState(context.Background(), "primary")
	if err == nil {
		t.Fatal("expected error from GetClusterState when backend fails")
	}
	if !errors.Is(err, backendErr) {
		t.Errorf("GetClusterState error = %v, want it to wrap %v", err, backendErr)
	}
}

func TestModule_Run_RetriesCampaignOnError(t *testing.T) {
	backend := newMockEtcd()

	atomic.StoreInt32(&backend.failFirstN, 2)

	mod := coordination.NewModuleWithBackend(
		coordination.Config{NodeID: "node-1", BackoffMin: time.Millisecond},
		backend, zap.NewNop(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go mod.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&backend.campaignCallCount) >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	count := atomic.LoadInt32(&backend.campaignCallCount)
	if count < 3 {
		t.Errorf("Campaign called %d times, want at least 3 (2 failures + 1 success)", count)
	}
}
