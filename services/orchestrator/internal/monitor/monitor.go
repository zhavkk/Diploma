package monitor

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

type Config struct {
	HeartbeatTimeout int
	PollInterval     int
}

type Monitor struct {
	cfg      Config
	failover *failover.Manager
	topo     *topology.Registry
	log      *zap.Logger

	nodeStatus map[string]*models.NodeStatus
}

func NewMonitor(cfg Config, fm *failover.Manager, tr *topology.Registry, log *zap.Logger) *Monitor {
	return &Monitor{
		cfg:        cfg,
		failover:   fm,
		topo:       tr,
		log:        log,
		nodeStatus: make(map[string]*models.NodeStatus),
	}
}

func (m *Monitor) ReceiveHeartbeat(status *models.NodeStatus) {
	status.LastHeartbeat = time.Now()
	m.nodeStatus[status.NodeID] = status
	m.log.Debug("heartbeat received",
		zap.String("node", status.NodeID),
		zap.String("role", string(status.Role)),
		zap.Int64("lag", status.ReplicationLag),
	)
}

func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cfg.HeartbeatTimeout) * time.Second / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkNodes(ctx)
		}
	}
}

func (m *Monitor) checkNodes(ctx context.Context) {
	threshold := time.Duration(m.cfg.HeartbeatTimeout) * time.Second
	primary := m.topo.Primary()

	for nodeID, status := range m.nodeStatus {
		if time.Since(status.LastHeartbeat) > threshold {
			m.log.Warn("node heartbeat timeout",
				zap.String("node", nodeID),
				zap.String("role", string(status.Role)),
			)

			if nodeID == primary {
				m.log.Error("primary node unreachable — triggering failover", zap.String("node", nodeID))
				m.failover.NotifyPrimaryFailure(ctx, nodeID)
			}
		}
	}
}
