package monitor

import (
	"context"
	"sync"
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
	HeartbeatTimeout   int
	PollInterval       int
	CheckInterval      time.Duration
	StartupGracePeriod time.Duration
}

type RejoinChecker interface {
	NeedsRejoin(nodeID string) bool
}

type Monitor struct {
	cfg      Config
	failover FailoverNotifier
	topo     *topology.Registry
	clock    Clock
	log      *zap.Logger

	mu              sync.Mutex
	nodeStatus      map[string]*models.NodeStatus
	rejoinHandler   RejoinHandler
	rejoinChecker   RejoinChecker
	notifiedPrimary string
	startedAt       time.Time
}

func (m *Monitor) WithRejoinHandler(h RejoinHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejoinHandler = h
	if rc, ok := h.(RejoinChecker); ok {
		m.rejoinChecker = rc
	}
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

	var needsRejoin bool
	var rejoinHandler RejoinHandler

	m.mu.Lock()
	m.nodeStatus[status.NodeID] = status
	if m.notifiedPrimary == status.NodeID {
		m.notifiedPrimary = ""
	}

	if m.rejoinHandler != nil {
		rejoinHandler = m.rejoinHandler
		needsRejoin = true
		if m.rejoinChecker != nil {
			needsRejoin = m.rejoinChecker.NeedsRejoin(status.NodeID)
		}
	}

	nodeID := status.NodeID
	nodeAddr := status.Address

	m.mu.Unlock()

	metrics.HeartbeatsReceived.WithLabelValues(status.NodeID).Inc()

	m.topo.UpsertNode(*status)
	m.log.Debug("heartbeat received",
		zap.String("node", status.NodeID),
		zap.String("role", string(status.Role)),
		zap.Int64("lag", status.ReplicationLag),
	)

	if needsRejoin && rejoinHandler != nil {
		if err := rejoinHandler.HandleOldPrimaryRejoin(context.Background(), nodeID, nodeAddr); err != nil {
			m.log.Warn("pg_rewind rejoin failed", zap.String("node", nodeID), zap.Error(err))
		}
	}
}

func (m *Monitor) Run(ctx context.Context) {
	m.mu.Lock()
	m.startedAt = m.clock.Now()
	m.mu.Unlock()

	ticker := time.NewTicker(m.checkInterval())
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

func (m *Monitor) checkInterval() time.Duration {
	if m.cfg.CheckInterval > 0 {
		return m.cfg.CheckInterval
	}
	if m.cfg.PollInterval > 0 {
		return time.Duration(m.cfg.PollInterval) * time.Second
	}
	interval := time.Duration(m.cfg.HeartbeatTimeout) * time.Second / 2
	if interval <= 0 {
		return time.Second
	}
	return interval
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

	m.mu.Lock()
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

	shouldNotify := primaryFailed && !inGracePeriod && m.notifiedPrimary != primary
	if shouldNotify {
		m.notifiedPrimary = primary
	}
	m.mu.Unlock()

	if shouldNotify {
		m.log.Error("primary node unreachable — triggering failover", zap.String("node", primary))
		m.failover.NotifyPrimaryFailure(ctx, primary)
	}

	for _, nodeID := range timedOut {
		m.topo.UpdateNodeState(nodeID, models.StateUnreachable)
	}
}
