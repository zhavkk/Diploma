package watcher

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/pgclient"
)

// Config holds watcher settings including node identity and polling interval.
type Config struct {
	NodeID       string
	NodeAddr     string
	PollInterval int
}

// ReplicationStatsSource queries pg_stat_replication for downstream replica statistics.
type ReplicationStatsSource interface {
	ReplicationStats(ctx context.Context) ([]pgclient.ReplicationStat, error)
}

// Watcher periodically polls replication statistics from PostgreSQL. It is safe for concurrent use.
type Watcher struct {
	cfg    Config
	pg     ReplicationStatsSource
	log    *zap.Logger
	mu     sync.RWMutex
	latest []pgclient.ReplicationStat
}

// New creates a Watcher that polls replication stats from the given PostgreSQL client.
func New(cfg Config, pg ReplicationStatsSource, log *zap.Logger) *Watcher {
	w := &Watcher{cfg: cfg, pg: pg, log: log}
	if w.cfg.PollInterval <= 0 {
		w.cfg.PollInterval = 5
	}
	return w
}

// Run starts the watcher loop, polling replication stats until the context is cancelled.
func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.cfg.PollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickCtx, tickCancel := context.WithTimeout(ctx, time.Duration(w.cfg.PollInterval)*time.Second)
			if err := w.SendMetrics(tickCtx); err != nil {
				w.log.Warn("replication watcher error", zap.Error(err))
			}
			tickCancel()
		}
	}
}

// Latest returns a copy of the most recently collected replication statistics.
func (w *Watcher) Latest() []pgclient.ReplicationStat {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.latest == nil {
		return nil
	}
	out := make([]pgclient.ReplicationStat, len(w.latest))
	copy(out, w.latest)
	return out
}

// SendMetrics fetches replication stats from PostgreSQL and stores them for later retrieval.
func (w *Watcher) SendMetrics(ctx context.Context) error {
	stats, err := w.pg.ReplicationStats(ctx)
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.latest = make([]pgclient.ReplicationStat, len(stats))
	copy(w.latest, stats)
	w.mu.Unlock()

	for _, s := range stats {
		w.log.Info("replication stat",
			zap.String("app", s.ApplicationName),
			zap.String("addr", s.ClientAddr),
			zap.String("state", s.State),
			zap.Int64("write_lag_ms", s.WriteLag),
			zap.Int64("flush_lag_ms", s.FlushLag),
			zap.Int64("replay_lag_ms", s.ReplayLag),
		)
	}
	return nil
}
