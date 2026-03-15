package watcher_test

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
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

type mockMetricsSender struct {
	sent []*models.NodeStatus
	err  error
}

func (m *mockMetricsSender) Send(_ context.Context, s *models.NodeStatus) error {
	m.sent = append(m.sent, s)
	return m.err
}

// ─────────────────────────────────────────
// SendMetrics тесты
// ─────────────────────────────────────────

func TestWatcher_SendMetrics_SendsPerReplica(t *testing.T) {
	pg := &mockPGStats{
		stats: []pgclient.ReplicationStat{
			{ApplicationName: "pg-replica1", State: "streaming", WriteLag: 50, FlushLag: 40, ReplayLag: 100},
			{ApplicationName: "pg-replica2", State: "streaming", WriteLag: 200, FlushLag: 190, ReplayLag: 500},
		},
	}
	sender := &mockMetricsSender{}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())
	w.WithSender(sender)

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("SendMetrics: %v", err)
	}
	if len(sender.sent) != 2 {
		t.Errorf("expected 2 sends, got %d", len(sender.sent))
	}
}

func TestWatcher_SendMetrics_SkipsWhenNoMetrics(t *testing.T) {
	pg := &mockPGStats{stats: []pgclient.ReplicationStat{}}
	sender := &mockMetricsSender{}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())
	w.WithSender(sender)

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("SendMetrics: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Errorf("expected 0 sends for empty stats, got %d", len(sender.sent))
	}
}

func TestWatcher_SendMetrics_NoSenderIsNoop(t *testing.T) {
	pg := &mockPGStats{
		stats: []pgclient.ReplicationStat{
			{ApplicationName: "pg-replica1", ReplayLag: 100},
		},
	}
	// Sender не установлен — не должно быть паники
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	if err := w.SendMetrics(context.Background()); err != nil {
		t.Fatalf("SendMetrics without sender: %v", err)
	}
}

func TestWatcher_SendMetrics_ReturnsErrorOnPGFailure(t *testing.T) {
	pg := &mockPGStats{err: errors.New("connection refused")}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())

	err := w.SendMetrics(context.Background())
	if err == nil {
		t.Error("expected error when pg query fails")
	}
}

func TestWatcher_SendMetrics_PopulatesReplicationLag(t *testing.T) {
	pg := &mockPGStats{
		stats: []pgclient.ReplicationStat{
			{ApplicationName: "pg-replica1", ReplayLag: 999},
		},
	}
	sender := &mockMetricsSender{}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())
	w.WithSender(sender)

	_ = w.SendMetrics(context.Background())

	if len(sender.sent) == 0 {
		t.Fatal("no data sent")
	}
	if sender.sent[0].ReplicationLag != 999 {
		t.Errorf("ReplicationLag = %d, want 999", sender.sent[0].ReplicationLag)
	}
}

func TestWatcher_SendMetrics_SetsOwnAddress(t *testing.T) {
	pg := &mockPGStats{
		stats: []pgclient.ReplicationStat{
			{ApplicationName: "pg-replica1", ReplayLag: 10},
		},
	}
	sender := &mockMetricsSender{}
	w := watcher.New(watcher.Config{NodeID: "pg-primary", NodeAddr: "primary:50052", PollInterval: 5}, pg, zap.NewNop())
	w.WithSender(sender)

	_ = w.SendMetrics(context.Background())

	if len(sender.sent) == 0 {
		t.Fatal("no data sent")
	}
	if sender.sent[0].Address != "primary:50052" {
		t.Errorf("Address = %q, want %q", sender.sent[0].Address, "primary:50052")
	}
}
