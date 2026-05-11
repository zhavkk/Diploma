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

// FailoverNotifier is called when the monitor detects that the primary node has failed.
type FailoverNotifier interface {
	NotifyPrimaryFailure(ctx context.Context, failedNodeID string)
}

// RejoinHandler reintegrates a former primary node back into the cluster as a replica.
type RejoinHandler interface {
	HandleOldPrimaryRejoin(ctx context.Context, nodeID, nodeAddr string) error
}

// Clock abstracts time for testing purposes.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config holds monitor settings including heartbeat timeout and startup grace period.
type Config struct {
	HeartbeatTimeout    int
	PollInterval        int
	StartupGracePeriod  time.Duration
}

// RejoinChecker allows the monitor to check whether a node needs rejoin before
// calling the heavier HandleOldPrimaryRejoin method.
type RejoinChecker interface {
	NeedsRejoin(nodeID string) bool
}

// Monitor tracks node heartbeats and triggers failover when the primary becomes unreachable.
type Monitor struct {
	cfg              Config
	failover         FailoverNotifier
	topo             *topology.Registry
	clock            Clock
	log              *zap.Logger

	mu               sync.Mutex
	nodeStatus       map[string]*models.NodeStatus
	rejoinHandler    RejoinHandler
	rejoinChecker    RejoinChecker
	notifiedPrimary  string
	startedAt        time.Time
}

// WithRejoinHandler sets the handler used to reintegrate former primaries on heartbeat.
// If h also implements RejoinChecker, the monitor will call NeedsRejoin before
// invoking the heavier HandleOldPrimaryRejoin.
func (m *Monitor) WithRejoinHandler(h RejoinHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejoinHandler = h
	if rc, ok := h.(RejoinChecker); ok {
		m.rejoinChecker = rc
	}
}

// NewMonitor creates a Monitor that uses the real system clock.
func NewMonitor(cfg Config, fm FailoverNotifier, tr *topology.Registry, log *zap.Logger) *Monitor {
	return NewMonitorWithClock(cfg, fm, tr, realClock{}, log)
}

// NewMonitorWithClock creates a Monitor with an injectable clock for testing.
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

// ReceiveHeartbeat processes an incoming heartbeat from a node agent and updates the topology.
func (m *Monitor) ReceiveHeartbeat(status *models.NodeStatus) {
	status.LastHeartbeat = m.clock.Now()

	var needsRejoin bool
	var rejoinHandler RejoinHandler

	m.mu.Lock()
	m.nodeStatus[status.NodeID] = status
	if m.notifiedPrimary == status.NodeID {
		m.notifiedPrimary = ""
	}

	// Capture rejoin handler and check if rejoin is needed while holding the lock.
	// This prevents data races if WithRejoinHandler is called concurrently.
	if m.rejoinHandler != nil {
		rejoinHandler = m.rejoinHandler
		needsRejoin = true
		if m.rejoinChecker != nil {
			needsRejoin = m.rejoinChecker.NeedsRejoin(status.NodeID)
		}
	}

	// Capture node info before releasing lock to use in rejoin call
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

	// Handle rejoin outside the lock - this can involve slow gRPC operations.
	if needsRejoin && rejoinHandler != nil {
		if err := rejoinHandler.HandleOldPrimaryRejoin(context.Background(), nodeID, nodeAddr); err != nil {
			m.log.Warn("pg_rewind rejoin failed", zap.String("node", nodeID), zap.Error(err))
		}
	}
}

// Run starts the monitor loop that periodically checks node liveness until the context is cancelled.
func (m *Monitor) Run(ctx context.Context) {
	m.mu.Lock()
	m.startedAt = m.clock.Now()
	m.mu.Unlock()

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

// CheckNodes evaluates heartbeat freshness for all known nodes and triggers failover if the primary is unreachable.
func (m *Monitor) CheckNodes(ctx context.Context) {
	threshold := time.Duration(m.cfg.HeartbeatTimeout) * time.Second
	primary := m.topo.Primary()
	now := m.clock.Now()

	// During startup grace period, skip failover triggers.
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
