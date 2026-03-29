package probe_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
// Mock HeartbeatSender
// ─────────────────────────────────────────

type mockSender struct {
	mu    sync.Mutex
	calls int
}

func (m *mockSender) Send(_ context.Context, _ *models.NodeStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return nil
}

func (m *mockSender) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
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

// ─────────────────────────────────────────
// Mock ReplicationWatcher
// ─────────────────────────────────────────

type mockReplicationWatcher struct {
	stats []probe.ReplicationStat
}

func (m *mockReplicationWatcher) Latest() []probe.ReplicationStat {
	return m.stats
}

// ─────────────────────────────────────────
// probe.Collect() with watcher tests
// ─────────────────────────────────────────

func TestProbe_Collect_IncludesReplicationStatsFromWatcher(t *testing.T) {
	pg := &mockPGClient{inRecovery: false, version: "PostgreSQL 15.2", replayLSN: 5000, receiveLSN: 5000}
	w := &mockReplicationWatcher{
		stats: []probe.ReplicationStat{
			{ApplicationName: "pg-replica1", State: "streaming", ReplayLag: 100},
			{ApplicationName: "pg-replica2", State: "streaming", ReplayLag: 200},
		},
	}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())
	p.WithWatcher(w)

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.ReplicationStats == nil {
		t.Fatal("expected ReplicationStats to be populated when watcher has stats")
	}
	if status.ReplicationStats.State != "streaming" {
		t.Errorf("ReplicationStats.State = %q, want %q", status.ReplicationStats.State, "streaming")
	}
	if status.ReplicationStats.LagBytes != 300 {
		t.Errorf("ReplicationStats.LagBytes = %d, want 300 (100+200)", status.ReplicationStats.LagBytes)
	}
	if status.ReplicationStats.WALLSN != "5000" {
		t.Errorf("ReplicationStats.WALLSN = %q, want %q", status.ReplicationStats.WALLSN, "5000")
	}
}

func TestProbe_Collect_NilReplicationStatsWhenNoWatcher(t *testing.T) {
	pg := &mockPGClient{inRecovery: false, version: "PostgreSQL 15.2"}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.ReplicationStats != nil {
		t.Errorf("expected nil ReplicationStats when no watcher is set, got %+v", status.ReplicationStats)
	}
}

func TestProbe_Collect_NilReplicationStatsWhenWatcherEmpty(t *testing.T) {
	pg := &mockPGClient{inRecovery: false, version: "PostgreSQL 15.2"}
	w := &mockReplicationWatcher{stats: nil}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())
	p.WithWatcher(w)

	status, err := p.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if status.ReplicationStats != nil {
		t.Errorf("expected nil ReplicationStats when watcher returns empty, got %+v", status.ReplicationStats)
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

func TestProbe_ConcurrentLatestAndCollect_NoRace(t *testing.T) {
	pg := &mockPGClient{inRecovery: false, version: "PostgreSQL 15.2", replayLSN: 1000, receiveLSN: 1000}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	// Seed an initial status so MarkPostgresDown has something to work with.
	_, _ = p.Collect(context.Background())

	const goroutines = 30
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Writers: Collect and MarkPostgresDown
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if j%2 == 0 {
					_, _ = p.Collect(context.Background())
				} else {
					p.MarkPostgresDown()
				}
			}
		}()
	}

	// Readers: Latest
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = p.Latest()
			}
		}()
	}

	wg.Wait()
}

// ─────────────────────────────────────────
// probe.Run() тесты
// ─────────────────────────────────────────

func TestProbe_Run_TickerCallsSend(t *testing.T) {
	pg := &mockPGClient{inRecovery: false, version: "PostgreSQL 15.2", replayLSN: 1000, receiveLSN: 1000}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 1}, pg, zap.NewNop())

	sender := &mockSender{}
	p.WithSender(sender)

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	<-done

	if sender.callCount() < 2 {
		t.Errorf("expected at least 2 Send calls in 2.5s with 1s interval, got %d", sender.callCount())
	}
}

func TestProbe_Run_SkipsSendWhenCollectFails(t *testing.T) {
	pg := &mockPGClient{err: errors.New("connection refused")}
	p := probe.New(probe.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 1}, pg, zap.NewNop())

	sender := &mockSender{}
	p.WithSender(sender)

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	<-done

	if sender.callCount() != 0 {
		t.Errorf("expected 0 Send calls when Collect always fails, got %d", sender.callCount())
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
