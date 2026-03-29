package probe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/metrics"
	"github.com/zhavkk/Diploma/pkg/models"
)

type PGStatusClient interface {
	IsInRecovery(ctx context.Context) (bool, error)
	WALReplayLSN(ctx context.Context) (int64, error)
	WALReceiveLSN(ctx context.Context) (int64, error)
	Version(ctx context.Context) (string, error)
}

// ReplicationWatcher provides the latest replication stats collected by the watcher.
type ReplicationWatcher interface {
	Latest() []ReplicationStat
}

// ReplicationStat mirrors pgclient.ReplicationStat to avoid a direct dependency.
type ReplicationStat struct {
	ApplicationName string
	ClientAddr      string
	State           string
	WriteLag        int64
	FlushLag        int64
	ReplayLag       int64
}

type Config struct {
	NodeID       string
	NodeAddr     string
	PollInterval int
}

type Probe struct {
	cfg     Config
	pg      PGStatusClient
	sender  HeartbeatSender
	watcher ReplicationWatcher
	log     *zap.Logger
	mu      sync.RWMutex
	latest  *models.NodeStatus
}

func New(cfg Config, pg PGStatusClient, log *zap.Logger) *Probe {
	p := &Probe{cfg: cfg, pg: pg, log: log}
	if p.cfg.PollInterval <= 0 {
		p.cfg.PollInterval = 5
	}
	return p
}

func (p *Probe) WithSender(s HeartbeatSender) {
	p.sender = s
}

func (p *Probe) WithWatcher(w ReplicationWatcher) {
	p.watcher = w
}

func (p *Probe) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.cfg.PollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collectCtx, collectCancel := context.WithTimeout(ctx, time.Duration(p.cfg.PollInterval)*time.Second)
			status, err := p.Collect(collectCtx)
			collectCancel()
			if err != nil {
				p.log.Warn("probe collect error", zap.Error(err))
				p.MarkPostgresDown()
				continue
			}
			p.log.Debug("probe collected",
				zap.String("node", status.NodeID),
				zap.String("role", string(status.Role)),
				zap.Int64("lag", status.ReplicationLag),
			)
			if p.sender != nil {
				sendCtx, sendCancel := context.WithTimeout(ctx, time.Duration(p.cfg.PollInterval)*time.Second)
				if err := p.sender.Send(sendCtx, status); err != nil {
					p.log.Warn("heartbeat send failed", zap.Error(err))
				}
				sendCancel()
			}
		}
	}
}

func (p *Probe) MarkPostgresDown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.latest == nil {
		return
	}
	down := *p.latest
	down.PostgresRunning = false
	down.State = models.StateDegraded
	p.latest = &down
}

func (p *Probe) Latest() *models.NodeStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.latest
}

func (p *Probe) Collect(ctx context.Context) (*models.NodeStatus, error) {
	start := time.Now()
	defer func() {
		metrics.ProbeCollectDurationSeconds.WithLabelValues(p.cfg.NodeID).Observe(time.Since(start).Seconds())
	}()

	inRecovery, err := p.pg.IsInRecovery(ctx)
	if err != nil {
		return nil, err
	}

	replayLSN, err := p.pg.WALReplayLSN(ctx)
	if err != nil {
		return nil, err
	}

	receiveLSN, err := p.pg.WALReceiveLSN(ctx)
	if err != nil {
		return nil, err
	}

	version, err := p.pg.Version(ctx)
	if err != nil {
		return nil, err
	}

	role := models.RolePrimary
	if inRecovery {
		role = models.RoleReplica
	}

	var lag int64
	if inRecovery && receiveLSN > replayLSN {
		lag = receiveLSN - replayLSN
	}

	status := &models.NodeStatus{
		NodeID:          p.cfg.NodeID,
		Address:         p.cfg.NodeAddr,
		Role:            role,
		State:           models.StateHealthy,
		IsInRecovery:    inRecovery,
		WALReceiveLSN:   receiveLSN,
		WALReplayLSN:    replayLSN,
		ReplicationLag:  lag,
		PGVersion:       version,
		PostgresRunning: true,
		LastHeartbeat:   time.Now(),
	}

	// Enrich with watcher replication stats when available.
	if p.watcher != nil {
		if stats := p.watcher.Latest(); len(stats) > 0 {
			// For a primary node, aggregate downstream replica stats.
			// Use the first replica's state as representative and sum lag.
			var totalLag int64
			for _, s := range stats {
				totalLag += s.ReplayLag
			}
			status.ReplicationStats = &models.ReplicationStats{
				State:    stats[0].State,
				WALLSN:   fmt.Sprintf("%d", replayLSN),
				LagBytes: totalLag,
			}
		}
	}

	p.mu.Lock()
	p.latest = status
	p.mu.Unlock()
	return status, nil
}
