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

type mockPGStats struct {
	stats []pgclient.ReplicationStat
	err   error
}

func (m *mockPGStats) ReplicationStats(_ context.Context) ([]pgclient.ReplicationStat, error) {
	return m.stats, m.err
}

func TestWatcher_SendMetrics_ReturnsErrorOnPGFailure(t *testing.T) {
	pg := &mockPGStats{err: errors.New("connection refused")}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	err := w.SendMetrics(context.Background())
	if err == nil {
		t.Error("expected error when pg query fails")
	}
}

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

type countingPGStats struct {
	inner *mockPGStats
	calls atomic.Int32
}

func (c *countingPGStats) ReplicationStats(ctx context.Context) ([]pgclient.ReplicationStat, error) {
	c.calls.Add(1)
	return c.inner.ReplicationStats(ctx)
}

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

func TestWatcher_Latest_NilBeforeSend(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{{ApplicationName: "r1"}}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	got := w.Latest()
	if got != nil {
		t.Fatalf("expected nil before SendMetrics, got %v", got)
	}
}

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

func TestWatcher_Latest_ReturnsCopy(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{
		{ApplicationName: "r1", WriteLag: 1},
	}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first := w.Latest()
	first[0].ApplicationName = "mutated"

	second := w.Latest()
	if second[0].ApplicationName != "r1" {
		t.Fatalf("Latest returned a reference, not a copy: got %q", second[0].ApplicationName)
	}
}

func TestWatcher_Latest_DoesNotUpdateOnError(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{
		{ApplicationName: "r1"},
	}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pg.err = errors.New("db down")
	_ = w.SendMetrics(context.Background())

	got := w.Latest()
	if len(got) != 1 || got[0].ApplicationName != "r1" {
		t.Fatalf("expected previous stats to be preserved, got %+v", got)
	}
}

func TestWatcher_Latest_ConcurrentAccess(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{
		{ApplicationName: "r1", WriteLag: 1},
	}}
	w := watcher.New(watcher.Config{NodeID: "n1", PollInterval: 5}, pg, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup

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

					_ = len(stats)
				}
			}
		}()
	}

	wg.Wait()
}
