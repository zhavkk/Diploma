package monitor

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/metrics"
	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

type FailoverNotifier interface {
	NotifyPrimaryFailure(ctx context.Context, failedNodeID string)
}

type RejoinHandler interface {
	HandleOldPrimaryRejoin(ctx context.Context, nodeID, nodeAddr string) error
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Config struct {
	HeartbeatTimeout    int
	PollInterval        int
	StartupGracePeriod  time.Duration
}

type Monitor struct {
	cfg              Config
	failover         FailoverNotifier
	topo             *topology.Registry
	clock            Clock
	log              *zap.Logger
	nodeStatus       map[string]*models.NodeStatus
	rejoinHandler    RejoinHandler
	notifiedPrimary  string
	startedAt        time.Time
}

func (m *Monitor) WithRejoinHandler(h RejoinHandler) {
	m.rejoinHandler = h
}

func NewMonitor(cfg Config, fm FailoverNotifier, tr *topology.Registry, log *zap.Logger) *Monitor {
	return NewMonitorWithClock(cfg, fm, tr, realClock{}, log)
}

func NewMonitorWithClock(cfg Config, fm FailoverNotifier, tr *topology.Registry, clock Clock, log *zap.Logger) *Monitor {
	return &Monitor{
		cfg:        cfg,
		failover:   fm,
		topo:       tr,
		clock:      clock,
		log:        log,
		nodeStatus: make(map[string]*models.NodeStatus),
	}
}

func (m *Monitor) ReceiveHeartbeat(status *models.NodeStatus) {
	status.LastHeartbeat = m.clock.Now()
	m.nodeStatus[status.NodeID] = status

	metrics.HeartbeatsReceived.WithLabelValues(status.NodeID).Inc()

	if m.notifiedPrimary == status.NodeID {
		m.notifiedPrimary = ""
	}

	m.topo.UpsertNode(*status)
	m.log.Debug("heartbeat received",
		zap.String("node", status.NodeID),
		zap.String("role", string(status.Role)),
		zap.Int64("lag", status.ReplicationLag),
	)
	if m.rejoinHandler != nil {
		if err := m.rejoinHandler.HandleOldPrimaryRejoin(context.Background(), status.NodeID, status.Address); err != nil {
			m.log.Warn("pg_rewind rejoin failed", zap.String("node", status.NodeID), zap.Error(err))
		}
	}
}

func (m *Monitor) Run(ctx context.Context) {
	m.startedAt = m.clock.Now()

	ticker := time.NewTicker(time.Duration(m.cfg.HeartbeatTimeout) * time.Second / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.CheckNodes(ctx)
		}
	}
}

func (m *Monitor) startupGracePeriod() time.Duration {
	if m.cfg.StartupGracePeriod > 0 {
		return m.cfg.StartupGracePeriod
	}
	return 2 * time.Duration(m.cfg.HeartbeatTimeout) * time.Second
}

func (m *Monitor) CheckNodes(ctx context.Context) {
	threshold := time.Duration(m.cfg.HeartbeatTimeout) * time.Second
	primary := m.topo.Primary()
	now := m.clock.Now()

	// During startup grace period, skip failover triggers.
	inGracePeriod := !m.startedAt.IsZero() && now.Sub(m.startedAt) < m.startupGracePeriod()

	var timedOut []string
	var primaryFailed bool

	for nodeID, status := range m.nodeStatus {
		if now.Sub(status.LastHeartbeat) > threshold {
			m.log.Warn("node heartbeat timeout",
				zap.String("node", nodeID),
				zap.String("role", string(status.Role)),
			)
			timedOut = append(timedOut, nodeID)
			if nodeID == primary {
				primaryFailed = true
			}
		}
	}

	if primaryFailed && !inGracePeriod && m.notifiedPrimary != primary {
		m.log.Error("primary node unreachable — triggering failover", zap.String("node", primary))
		m.notifiedPrimary = primary
		m.failover.NotifyPrimaryFailure(ctx, primary)
	}

	for _, nodeID := range timedOut {
		m.topo.UpdateNodeState(nodeID, models.StateUnreachable)
	}
}
