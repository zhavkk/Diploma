package probe

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/pkg/pgclient"
)

type Config struct {
	NodeID       string
	PollInterval int
}

type Probe struct {
	cfg    Config
	pg     *pgclient.Client
	log    *zap.Logger
	latest *models.NodeStatus
}

func New(cfg Config, pg *pgclient.Client, log *zap.Logger) *Probe {
	return &Probe{cfg: cfg, pg: pg, log: log}
}

func (p *Probe) Run(ctx context.Context, orchestratorAddr string) {
	ticker := time.NewTicker(time.Duration(p.cfg.PollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status, err := p.collect(ctx)
			if err != nil {
				p.log.Warn("probe collect error", zap.Error(err))
				continue
			}
			p.latest = status
			p.log.Debug("probe collected",
				zap.String("node", status.NodeID),
				zap.String("role", string(status.Role)),
				zap.Int64("lag", status.ReplicationLag),
			)
			// TODO: gRPC вызов HealthMonitor.ReceiveHeartbeat(orchestratorAddr, status)
		}
	}
}

func (p *Probe) Latest() *models.NodeStatus {
	return p.latest
}

func (p *Probe) collect(ctx context.Context) (*models.NodeStatus, error) {
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

	return &models.NodeStatus{
		NodeID:          p.cfg.NodeID,
		Role:            role,
		State:           models.StateHealthy,
		IsInRecovery:    inRecovery,
		WALReceiveLSN:   receiveLSN,
		WALReplayLSN:    replayLSN,
		ReplicationLag:  lag,
		PGVersion:       version,
		PostgresRunning: true,
		LastHeartbeat:   time.Now(),
	}, nil
}
