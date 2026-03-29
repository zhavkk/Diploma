package failover

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/metrics"
	"github.com/zhavkk/Diploma/pkg/models"
)

// ReplicationConfigurator reconfigures replication on cluster nodes after a failover.
type ReplicationConfigurator interface {
	ReconfigureAfterFailover(ctx context.Context, newPrimaryNodeID string) error
	PrimaryConnInfo(addr string) string
}

// Config holds failover manager settings such as the required quorum size.
type Config struct {
	QuorumSize int
}

// TopologyRegistry provides access to the cluster topology for failover decisions.
type TopologyRegistry interface {
	Primary() string
	SetPrimary(nodeID string)
	Get() *models.ClusterTopology
}

// CoordinationModule abstracts etcd-based leader election and cluster state persistence.
type CoordinationModule interface {
	IsLeader(ctx context.Context) (bool, error)
	PutClusterState(ctx context.Context, key, value string) error
}

// EventAppender records failover events for audit and observability.
type EventAppender interface {
	AppendEvent(evt models.FailoverEvent)
}

// Manager orchestrates automatic and manual failover of the PostgreSQL primary.
// It is safe for concurrent use.
type Manager struct {
	cfg        Config
	topo       TopologyRegistry
	coord      CoordinationModule
	replConf   ReplicationConfigurator
	caller     NodeAgentCaller
	eventStore EventAppender
	log        *zap.Logger

	mu                 sync.Mutex
	failoverInProgress bool
	rewindInProgress   bool

	oldPrimaryID       string
	newPrimaryConnInfo string
}

// NewManager creates a new failover Manager with the given dependencies.
func NewManager(
	cfg Config,
	topo TopologyRegistry,
	coord CoordinationModule,
	rc ReplicationConfigurator,
	caller NodeAgentCaller,
	log *zap.Logger,
) *Manager {
	return &Manager{
		cfg:      cfg,
		topo:     topo,
		coord:    coord,
		replConf: rc,
		caller:   caller,
		log:      log,
	}
}

// WithEventStore sets the event store used to record failover events.
func (m *Manager) WithEventStore(e EventAppender) {
	m.eventStore = e
}

// Run blocks until the context is cancelled. The Manager reacts to failover notifications rather than polling.
func (m *Manager) Run(ctx context.Context) {
	<-ctx.Done()
}

// IsFailoverInProgress reports whether a failover operation is currently running.
func (m *Manager) IsFailoverInProgress() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failoverInProgress
}

// NotifyPrimaryFailure triggers an automatic failover after the given primary node is detected as failed.
func (m *Manager) NotifyPrimaryFailure(ctx context.Context, failedNodeID string) {
	m.mu.Lock()
	if m.failoverInProgress {
		m.log.Warn("failover already in progress, skipping", zap.String("node", failedNodeID))
		m.mu.Unlock()
		return
	}
	m.failoverInProgress = true
	m.mu.Unlock()

	start := time.Now()
	result := "error"
	defer func() {
		metrics.FailoverTotal.WithLabelValues("automatic", result).Inc()
		metrics.FailoverDurationSeconds.Observe(time.Since(start).Seconds())
		m.mu.Lock()
		m.failoverInProgress = false
		m.mu.Unlock()
	}()

	m.log.Info("starting failover", zap.String("failed_primary", failedNodeID))

	isLeader, err := m.coord.IsLeader(ctx)
	if err != nil {
		m.log.Warn("coordinator check failed, aborting failover", zap.Error(err))
		return
	}
	if !isLeader {
		m.log.Warn("not the leader, aborting failover")
		return
	}

	if m.cfg.QuorumSize > 0 {
		healthyReplicas := m.countHealthyReplicas(failedNodeID)
		if healthyReplicas < m.cfg.QuorumSize {
			m.log.Warn("quorum not met, aborting failover",
				zap.Int("healthy_replicas", healthyReplicas),
				zap.Int("quorum_size", m.cfg.QuorumSize),
			)
			return
		}
	}

	newPrimary := m.ElectNewPrimary(failedNodeID)
	if newPrimary == "" {
		m.log.Error("failed to elect new primary: no healthy replica available")
		return
	}

	m.log.Info("elected new primary", zap.String("node", newPrimary))

	var newPrimaryAddr string
	if topo := m.topo.Get(); topo != nil {
		for _, n := range topo.Nodes {
			if n.NodeID == newPrimary {
				newPrimaryAddr = n.Address
				break
			}
		}
	}

	if m.caller != nil {
		if newPrimaryAddr != "" {
			if err := m.caller.PromoteNode(ctx, newPrimaryAddr); err != nil {
				m.log.Error("PromoteNode failed, continuing failover", zap.Error(err))
			}
		} else {
			m.log.Warn("new primary has no address, skipping PromoteNode", zap.String("node", newPrimary))
		}
	}

	m.topo.SetPrimary(newPrimary)

	if err := m.coord.PutClusterState(ctx, "primary", newPrimary); err != nil {
		m.log.Warn("failed to persist primary to etcd", zap.Error(err))
	}

	if err := m.replConf.ReconfigureAfterFailover(ctx, newPrimary); err != nil {
		m.log.Error("replication reconfiguration failed", zap.Error(err))
	}

	if newPrimaryAddr != "" {
		m.mu.Lock()
		m.oldPrimaryID = failedNodeID
		m.newPrimaryConnInfo = m.replConf.PrimaryConnInfo(newPrimaryAddr)
		m.mu.Unlock()
	} else {
		m.log.Warn("new primary address unknown, old primary cannot be auto-rewound",
			zap.String("old_primary", failedNodeID),
			zap.String("new_primary", newPrimary),
		)
	}

	result = "success"
	now := time.Now()
	m.log.Info("failover completed",
		zap.String("old_primary", failedNodeID),
		zap.String("new_primary", newPrimary),
		zap.Time("at", now),
	)
	if m.eventStore != nil {
		m.eventStore.AppendEvent(models.FailoverEvent{
			OldPrimary: failedNodeID,
			NewPrimary: newPrimary,
			Reason:     "automatic",
			OccurredAt: now,
		})
	}
}

// NeedsRejoin reports whether the given node is a former primary that needs pg_rewind to rejoin the cluster.
func (m *Manager) NeedsRejoin(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.oldPrimaryID == nodeID && !m.rewindInProgress
}

// HandleOldPrimaryRejoin runs pg_rewind and reconfigures the old primary to rejoin the cluster as a replica.
func (m *Manager) HandleOldPrimaryRejoin(ctx context.Context, nodeID, nodeAddr string) error {
	m.mu.Lock()
	if m.oldPrimaryID != nodeID {
		m.mu.Unlock()
		return nil
	}
	connInfo := m.newPrimaryConnInfo
	m.rewindInProgress = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.rewindInProgress = false
		m.mu.Unlock()
	}()

	m.log.Info("old primary rejoining, running pg_rewind",
		zap.String("node", nodeID),
		zap.String("addr", nodeAddr),
	)

	if m.caller == nil {
		return nil
	}
	if err := m.caller.RunPgRewind(ctx, nodeAddr, connInfo); err != nil {
		return fmt.Errorf("pg_rewind for %s: %w", nodeID, err)
	}

	// Reconfigure the old primary as a replica of the new primary.
	if err := m.caller.ReconfigureReplication(ctx, nodeAddr, connInfo, "latest"); err != nil {
		return fmt.Errorf("reconfigure replication for %s: %w", nodeID, err)
	}

	// Restart PostgreSQL so it comes up as a standby.
	if err := m.caller.RestartPostgres(ctx, nodeAddr); err != nil {
		return fmt.Errorf("restart postgres for %s: %w", nodeID, err)
	}

	m.mu.Lock()
	m.oldPrimaryID = ""
	m.newPrimaryConnInfo = ""
	m.mu.Unlock()

	m.log.Info("old primary rejoined as replica", zap.String("node", nodeID))
	return nil
}

func (m *Manager) countHealthyReplicas(excludeNodeID string) int {
	topo := m.topo.Get()
	if topo == nil {
		return 0
	}
	count := 0
	for _, n := range topo.Nodes {
		if n.NodeID == excludeNodeID {
			continue
		}
		if n.Role == models.RoleReplica && n.State == models.StateHealthy {
			count++
		}
	}
	return count
}

// ElectNewPrimary selects the healthy replica with the highest WAL replay LSN, excluding the given node.
func (m *Manager) ElectNewPrimary(excludeNodeID string) string {
	topo := m.topo.Get()
	if topo == nil {
		return ""
	}

	var best *models.NodeStatus
	for i := range topo.Nodes {
		node := &topo.Nodes[i]
		if node.NodeID == excludeNodeID {
			continue
		}
		if node.State != models.StateHealthy {
			continue
		}
		if node.Role != models.RoleReplica {
			continue
		}
		// Use the furthest-ahead LSN: received bytes may be ahead of replayed bytes.
		nodeLSN := node.WALReplayLSN
		if node.WALReceiveLSN > nodeLSN {
			nodeLSN = node.WALReceiveLSN
		}
		bestLSN := int64(0)
		if best != nil {
			bestLSN = best.WALReplayLSN
			if best.WALReceiveLSN > bestLSN {
				bestLSN = best.WALReceiveLSN
			}
		}
		if best == nil || nodeLSN > bestLSN {
			best = node
		}
	}

	if best == nil {
		return ""
	}
	return best.NodeID
}

// TriggerManualFailover promotes the specified target node to primary, demoting the current primary.
func (m *Manager) TriggerManualFailover(ctx context.Context, targetNodeID string) error {
	m.log.Info("manual failover requested", zap.String("target", targetNodeID))

	topo := m.topo.Get()
	if topo == nil {
		return fmt.Errorf("failover: topology not available")
	}

	var target *models.NodeStatus
	for i := range topo.Nodes {
		if topo.Nodes[i].NodeID == targetNodeID {
			target = &topo.Nodes[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("failover: target node %q not found in topology", targetNodeID)
	}
	if target.NodeID == m.topo.Primary() {
		return fmt.Errorf("failover: target %q is already the primary", targetNodeID)
	}
	if target.State != models.StateHealthy {
		return fmt.Errorf("failover: target %q is not healthy (state=%s)", targetNodeID, target.State)
	}

	m.mu.Lock()
	if m.failoverInProgress {
		m.mu.Unlock()
		return fmt.Errorf("failover: already in progress")
	}
	// Re-validate: check target hasn't become primary due to concurrent failover.
	if m.topo.Primary() == targetNodeID {
		m.mu.Unlock()
		return fmt.Errorf("failover: target %q is already the primary (concurrent failover occurred)", targetNodeID)
	}
	m.failoverInProgress = true
	m.mu.Unlock()

	start := time.Now()
	manualResult := "error"
	defer func() {
		metrics.FailoverTotal.WithLabelValues("manual", manualResult).Inc()
		metrics.FailoverDurationSeconds.Observe(time.Since(start).Seconds())
		m.mu.Lock()
		m.failoverInProgress = false
		m.mu.Unlock()
	}()

	isLeader, err := m.coord.IsLeader(ctx)
	if err != nil {
		return fmt.Errorf("failover: coordinator check failed: %w", err)
	}
	if !isLeader {
		return fmt.Errorf("failover: not the leader")
	}

	oldPrimary := m.topo.Primary()

	if m.cfg.QuorumSize > 0 {
		healthyReplicas := m.countHealthyReplicas(oldPrimary)
		if healthyReplicas < m.cfg.QuorumSize {
			return fmt.Errorf("failover: quorum not met (healthy replicas: %d, required: %d)",
				healthyReplicas, m.cfg.QuorumSize)
		}
	}
	m.log.Info("promoting target node", zap.String("node", targetNodeID))

	if m.caller != nil && target.Address != "" {
		if err := m.caller.PromoteNode(ctx, target.Address); err != nil {
			m.log.Error("PromoteNode failed, continuing", zap.Error(err))
		}
	}

	m.topo.SetPrimary(targetNodeID)

	if err := m.coord.PutClusterState(ctx, "primary", targetNodeID); err != nil {
		m.log.Warn("failed to persist primary to etcd", zap.Error(err))
	}

	if err := m.replConf.ReconfigureAfterFailover(ctx, targetNodeID); err != nil {
		m.log.Error("replication reconfiguration failed", zap.Error(err))
	}

	if target.Address != "" {
		m.mu.Lock()
		m.oldPrimaryID = oldPrimary
		m.newPrimaryConnInfo = m.replConf.PrimaryConnInfo(target.Address)
		m.mu.Unlock()
	} else {
		m.log.Warn("target address unknown, old primary cannot be auto-rewound",
			zap.String("old_primary", oldPrimary),
			zap.String("new_primary", targetNodeID),
		)
	}

	manualResult = "success"
	now := time.Now()
	m.log.Info("manual failover completed",
		zap.String("old_primary", oldPrimary),
		zap.String("new_primary", targetNodeID),
		zap.Time("at", now),
	)
	if m.eventStore != nil {
		m.eventStore.AppendEvent(models.FailoverEvent{
			OldPrimary: oldPrimary,
			NewPrimary: targetNodeID,
			Reason:     "manual",
			OccurredAt: now,
		})
	}
	return nil
}
