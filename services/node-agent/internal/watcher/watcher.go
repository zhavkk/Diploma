package watcher

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/pgclient"
)

type Config struct {
	NodeID       string
	PollInterval int
}

type ReplicationMetrics struct {
	NodeID          string
	ApplicationName string
	State           string
	WriteLag        int64
	FlushLag        int64
	ReplayLag       int64
	SlotActive      bool
}

type Watcher struct {
	cfg Config
	pg  *pgclient.Client
	log *zap.Logger
}

func New(cfg Config, pg *pgclient.Client, log *zap.Logger) *Watcher {
	return &Watcher{cfg: cfg, pg: pg, log: log}
}

func (w *Watcher) Run(ctx context.Context, orchestratorAddr string) {
	ticker := time.NewTicker(time.Duration(w.cfg.PollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics, err := w.collect(ctx)
			if err != nil {
				w.log.Warn("replication watcher error", zap.Error(err))
				continue
			}
			for _, m := range metrics {
				w.log.Debug("replication metrics",
					zap.String("app", m.ApplicationName),
					zap.String("state", m.State),
					zap.Int64("write_lag_ms", m.WriteLag),
					zap.Int64("replay_lag_ms", m.ReplayLag),
				)
			}
			// TODO: gRPC вызов HealthMonitor.ReceiveReplicationMetrics(orchestratorAddr, metrics)
		}
	}
}

func (w *Watcher) collect(ctx context.Context) ([]ReplicationMetrics, error) {
	stats, err := w.pg.ReplicationStats(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]ReplicationMetrics, 0, len(stats))
	for _, s := range stats {
		result = append(result, ReplicationMetrics{
			NodeID:          w.cfg.NodeID,
			ApplicationName: s.ApplicationName,
			State:           s.State,
			WriteLag:        s.WriteLag,
			FlushLag:        s.FlushLag,
			ReplayLag:       s.ReplayLag,
		})
	}
	return result, nil
}
