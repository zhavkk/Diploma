package probe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/metrics"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/pkg/version"
)

type PGStatusClient interface {
	IsInRecovery(ctx context.Context) (bool, error)
	WALReplayLSN(ctx context.Context) (int64, error)
	WALReceiveLSN(ctx context.Context) (int64, error)
	Version(ctx context.Context) (string, error)
}

type ReplicationWatcher interface {
	Latest() []ReplicationStat
}

type ReplicationStat struct {
	ApplicationName string
	ClientAddr      string
	State           string
	WriteLag        int64
	FlushLag        int64
	ReplayLag       int64
}

type Config struct {
	NodeID               string
	NodeAddr             string
	PollInterval         int
	PollIntervalDuration time.Duration
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
	if p.cfg.PollIntervalDuration <= 0 {
		p.cfg.PollIntervalDuration = time.Duration(p.cfg.PollInterval) * time.Second
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
	interval := p.interval()
	p.log.Info("probe loop starting", zap.Duration("poll_interval", interval))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.log.Info("probe loop stopping")
			return
		case <-ticker.C:
			collectCtx, collectCancel := context.WithTimeout(ctx, interval)
			status, err := p.Collect(collectCtx)
			collectCancel()
			if err != nil {
				p.log.Error("probe collect error", zap.Error(err))
				p.MarkPostgresDown()
				continue
			}
			p.log.Info("probe collected status",
				zap.String("node", status.NodeID),
				zap.String("role", string(status.Role)),
				zap.Bool("postgres_running", status.PostgresRunning),
				zap.Int64("lag", status.ReplicationLag),
				zap.String("pg_version", status.PGVersion),
			)
			if p.sender != nil {
				sendCtx, sendCancel := context.WithTimeout(ctx, interval)
				if err := p.sender.Send(sendCtx, status); err != nil {
					p.log.Error("heartbeat send failed", zap.Error(err))
				} else {
					p.log.Debug("heartbeat sent successfully")
				}
				sendCancel()
			} else {
				p.log.Warn("no heartbeat sender configured, heartbeat not sent")
			}
		}
	}
}

func (p *Probe) interval() time.Duration {
	if p.cfg.PollIntervalDuration > 0 {
		return p.cfg.PollIntervalDuration
	}
	return time.Duration(p.cfg.PollInterval) * time.Second
}

func (p *Probe) Refresh(ctx context.Context) (*models.NodeStatus, error) {
	return p.Collect(ctx)
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

	versionStr, err := p.pg.Version(ctx)
	if err != nil {
		return nil, err
	}

	parsedVersion := version.Parse(versionStr)
	if parsedVersion.IsZero() {
		p.log.Warn("failed to parse PostgreSQL version", zap.String("raw_version", versionStr))
	} else {
		p.log.Info("successfully parsed PostgreSQL version",
			zap.String("raw_version", versionStr),
			zap.Int("major", parsedVersion.Major),
			zap.Int("minor", parsedVersion.Minor),
			zap.Int("patch", parsedVersion.Patch))
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
		PGVersion:       versionStr,
		PGVersionParsed: parsedVersion,
		PostgresRunning: true,
		LastHeartbeat:   time.Now(),
	}

	if p.watcher != nil {
		if stats := p.watcher.Latest(); len(stats) > 0 {

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
