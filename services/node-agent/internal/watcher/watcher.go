package watcher

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
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

type MetricsSender interface {
	Send(ctx context.Context, status *models.NodeStatus) error
}

type Watcher struct {
	cfg    Config
	pg     ReplicationStatsSource
	sender MetricsSender
	log    *zap.Logger
}

func New(cfg Config, pg ReplicationStatsSource, log *zap.Logger) *Watcher {
	return &Watcher{cfg: cfg, pg: pg, log: log}
}

func (w *Watcher) WithSender(s MetricsSender) {
	w.sender = s
}

func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.cfg.PollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.SendMetrics(ctx); err != nil {
				w.log.Warn("replication watcher error", zap.Error(err))
			}
		}
	}
}

func (w *Watcher) SendMetrics(ctx context.Context) error {
	stats, err := w.pg.ReplicationStats(ctx)
	if err != nil {
		return err
	}

	if w.sender == nil || len(stats) == 0 {
		return nil
	}

	for _, s := range stats {
		w.log.Debug("replication metrics",
			zap.String("app", s.ApplicationName),
			zap.String("state", s.State),
			zap.Int64("write_lag_ms", s.WriteLag),
			zap.Int64("replay_lag_ms", s.ReplayLag),
		)
		status := &models.NodeStatus{
			NodeID:         w.cfg.NodeID,
			Address:        w.cfg.NodeAddr,
			Role:           models.RolePrimary,
			State:          models.StateHealthy,
			ReplicationLag: s.ReplayLag,
		}
		if err := w.sender.Send(ctx, status); err != nil {
			w.log.Warn("send replication metrics failed",
				zap.String("app", s.ApplicationName),
				zap.Error(err),
			)
		}
	}
	return nil
}
