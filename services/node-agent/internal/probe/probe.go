package probe

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

type PGStatusClient interface {
	IsInRecovery(ctx context.Context) (bool, error)
	WALReplayLSN(ctx context.Context) (int64, error)
	WALReceiveLSN(ctx context.Context) (int64, error)
	Version(ctx context.Context) (string, error)
}

type Config struct {
	NodeID       string
	NodeAddr     string
	PollInterval int
}

type Probe struct {
	cfg    Config
	pg     PGStatusClient
	sender HeartbeatSender
	log    *zap.Logger
	latest *models.NodeStatus
}

func New(cfg Config, pg PGStatusClient, log *zap.Logger) *Probe {
	return &Probe{cfg: cfg, pg: pg, log: log}
}

func (p *Probe) WithSender(s HeartbeatSender) {
	p.sender = s
}

func (p *Probe) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.cfg.PollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status, err := p.Collect(ctx)
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
				if err := p.sender.Send(ctx, status); err != nil {
					p.log.Warn("heartbeat send failed", zap.Error(err))
				}
			}
		}
	}
}

func (p *Probe) MarkPostgresDown() {
	if p.latest == nil {
		return
	}
	down := *p.latest
	down.PostgresRunning = false
	down.State = models.StateDegraded
	p.latest = &down
}

func (p *Probe) Latest() *models.NodeStatus {
	return p.latest
}

func (p *Probe) Collect(ctx context.Context) (*models.NodeStatus, error) {
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
	p.latest = status
	return status, nil
}
