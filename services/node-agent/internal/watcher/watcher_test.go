package watcher_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/pgclient"
	"github.com/zhavkk/Diploma/services/node-agent/internal/watcher"
)

// ─────────────────────────────────────────
// Моки
// ─────────────────────────────────────────

type mockPGStats struct {
	stats []pgclient.ReplicationStat
	err   error
}

func (m *mockPGStats) ReplicationStats(_ context.Context) ([]pgclient.ReplicationStat, error) {
	return m.stats, m.err
}

// ─────────────────────────────────────────
// SendMetrics тесты
// ─────────────────────────────────────────

// TestWatcher_SendMetrics_ReturnsErrorOnPGFailure verifies that SendMetrics
// propagates errors returned by the PostgreSQL stats source.
func TestWatcher_SendMetrics_ReturnsErrorOnPGFailure(t *testing.T) {
	pg := &mockPGStats{err: errors.New("connection refused")}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	err := w.SendMetrics(context.Background())
	if err == nil {
		t.Error("expected error when pg query fails")
	}
}

// TestWatcher_SendMetrics_LogsReplicationStats verifies that SendMetrics returns
// nil and does not crash when the pg source returns one replication stat.
func TestWatcher_SendMetrics_LogsReplicationStats(t *testing.T) {
	pg := &mockPGStats{
		stats: []pgclient.ReplicationStat{
			{
				ApplicationName: "pg-replica1",
				ClientAddr:      "10.0.0.2",
				State:           "streaming",
				WriteLag:        10,
				FlushLag:        20,
				ReplayLag:       30,
			},
		},
	}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("SendMetrics returned unexpected error: %v", err)
	}
}

// ─────────────────────────────────────────
// Run тесты
// ─────────────────────────────────────────

// countingPGStats wraps another StatsSource and atomically counts calls.
type countingPGStats struct {
	inner *mockPGStats
	calls atomic.Int32
}

func (c *countingPGStats) ReplicationStats(ctx context.Context) ([]pgclient.ReplicationStat, error) {
	c.calls.Add(1)
	return c.inner.ReplicationStats(ctx)
}

// TestWatcher_Run_TickerCallsSendMetrics verifies that Run calls SendMetrics on
// each ticker tick and stops cleanly when the context is cancelled.
func TestWatcher_Run_TickerCallsSendMetrics(t *testing.T) {
	pg := &mockPGStats{
		stats: []pgclient.ReplicationStat{
			{ApplicationName: "pg-replica1", ReplayLag: 50},
		},
	}
	counting := &countingPGStats{inner: pg}

	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 1}, counting, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	<-done

	if counting.calls.Load() < 2 {
		t.Errorf("expected at least 2 SendMetrics calls in 2.5s with 1s interval, got %d", counting.calls.Load())
	}
}

// ─────────────────────────────────────────
// Latest тесты
// ─────────────────────────────────────────

// TestWatcher_Latest_NilBeforeSend verifies that Latest returns nil
// before any SendMetrics call has been made.
func TestWatcher_Latest_NilBeforeSend(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{{ApplicationName: "r1"}}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	got := w.Latest()
	if got != nil {
		t.Fatalf("expected nil before SendMetrics, got %v", got)
	}
}

// TestWatcher_Latest_ReturnsStatsAfterSend verifies that Latest returns
// stats that were stored during a successful SendMetrics call.
func TestWatcher_Latest_ReturnsStatsAfterSend(t *testing.T) {
	expected := []pgclient.ReplicationStat{
		{ApplicationName: "r1", State: "streaming", WriteLag: 5},
		{ApplicationName: "r2", State: "streaming", WriteLag: 10},
	}
	pg := &mockPGStats{stats: expected}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := w.Latest()
	if len(got) != len(expected) {
		t.Fatalf("expected %d stats, got %d", len(expected), len(got))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("stat[%d] mismatch: got %+v, want %+v", i, got[i], expected[i])
		}
	}
}

// TestWatcher_Latest_ReturnsCopy verifies that the slice returned by Latest
// is a defensive copy and mutations do not affect the internal state.
func TestWatcher_Latest_ReturnsCopy(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{
		{ApplicationName: "r1", WriteLag: 1},
	}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mutate the returned slice.
	first := w.Latest()
	first[0].ApplicationName = "mutated"

	// The internal state should be unchanged.
	second := w.Latest()
	if second[0].ApplicationName != "r1" {
		t.Fatalf("Latest returned a reference, not a copy: got %q", second[0].ApplicationName)
	}
}

// TestWatcher_Latest_DoesNotUpdateOnError verifies that Latest keeps the
// previous stats when SendMetrics fails.
func TestWatcher_Latest_DoesNotUpdateOnError(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{
		{ApplicationName: "r1"},
	}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	// Successful call.
	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Now make the source fail.
	pg.err = errors.New("db down")
	_ = w.SendMetrics(context.Background())

	got := w.Latest()
	if len(got) != 1 || got[0].ApplicationName != "r1" {
		t.Fatalf("expected previous stats to be preserved, got %+v", got)
	}
}

// TestWatcher_Latest_ConcurrentAccess verifies that Latest and SendMetrics
// can be called concurrently without data races.
func TestWatcher_Latest_ConcurrentAccess(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{
		{ApplicationName: "r1", WriteLag: 1},
	}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup

	// Writer goroutine: repeatedly calls SendMetrics.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = w.SendMetrics(context.Background())
			}
		}
	}()

	// Multiple reader goroutines: repeatedly call Latest.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					stats := w.Latest()
					// Just access the data to trigger any race detector finding.
					_ = len(stats)
				}
			}
		}()
	}

	wg.Wait()
}
