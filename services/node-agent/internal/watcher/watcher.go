package watcher

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/pgclient"
)

type Config struct {
	NodeID       string
	NodeAddr     string
	PollInterval int
}

type ReplicationStatsSource interface {
	ReplicationStats(ctx context.Context) ([]pgclient.ReplicationStat, error)
}

type Watcher struct {
	cfg    Config
	pg     ReplicationStatsSource
	log    *zap.Logger
	mu     sync.RWMutex
	latest []pgclient.ReplicationStat
}

func New(cfg Config, pg ReplicationStatsSource, log *zap.Logger) *Watcher {
	w := &Watcher{cfg: cfg, pg: pg, log: log}
	if w.cfg.PollInterval <= 0 {
		w.cfg.PollInterval = 5
	}
	return w
}

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
