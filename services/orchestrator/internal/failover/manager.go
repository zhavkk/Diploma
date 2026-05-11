package failover

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/metrics"
	"github.com/zhavkk/Diploma/pkg/models"
)

// ReplicationConfigurator reconfigures replication on cluster nodes after a failover.
type ReplicationConfigurator interface {
	ReconfigureAfterFailover(ctx context.Context, newPrimaryNodeID string) (int, error)
	PrimaryConnInfoForNode(nodeID, addr string) string
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
	GetClusterState(ctx context.Context, key string) (string, error)
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
	fenceToken         string // STONITH fence token for split-brain prevention
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

// generateFenceToken creates a cryptographically random fence token.
func generateFenceToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate fence token: %w", err)
	}
	return hex.EncodeToString(b), nil
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

		// Always clear fence token on exit to allow old primary to rejoin
		// after operator intervention, regardless of failover outcome
		if m.fenceToken != "" {
			m.clearFenceToken(ctx, failedNodeID)
		}
	}()

	m.log.Info("starting failover", zap.String("failed_primary", failedNodeID))

	// Set STONITH fence token before proceeding with failover
	if err := m.setFenceToken(ctx, failedNodeID); err != nil {
		m.log.Error("failed to set fence token, aborting failover", zap.Error(err))
		return
	}

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
				// CRITICAL: abort failover if promotion fails to prevent split-brain
				m.log.Error("PromoteNode failed, aborting failover to prevent split-brain",
					zap.String("new_primary", newPrimary),
					zap.String("new_primary_addr", newPrimaryAddr),
					zap.Error(err),
				)
				result = "error"
				return
			}
		} else {
			m.log.Error("new primary has no address, aborting failover", zap.String("node", newPrimary))
			result = "error"
			return
		}
	} else {
		m.log.Error("no node agent caller available, aborting failover", zap.String("node", newPrimary))
		result = "error"
		return
	}

	m.topo.SetPrimary(newPrimary)

	if err := m.coord.PutClusterState(ctx, "primary", newPrimary); err != nil {
		m.log.Warn("failed to persist primary to etcd", zap.Error(err))
	}

	successCount, reconfigErr := m.replConf.ReconfigureAfterFailover(ctx, newPrimary)
	if reconfigErr != nil {
		if successCount == 0 {
			// CRITICAL: no replicas can be reconfigured, but we've already promoted the new primary
			// The failover is committed but replication is broken - operator intervention required
			m.log.Error("replication reconfiguration failed for all replicas - failover committed but replication broken",
				zap.Int("success_count", successCount),
				zap.String("new_primary", newPrimary),
				zap.Error(reconfigErr),
			)
			metrics.ReplicationReconfigureTotal.WithLabelValues("failure").Inc()
		} else {
			m.log.Warn("some replicas failed to reconfigure, continuing failover",
				zap.Int("success_count", successCount),
				zap.Error(reconfigErr),
			)
			// Count partial failures
			topo := m.topo.Get()
			if topo != nil {
				expectedCount := 0
				for _, n := range topo.Nodes {
					if n.NodeID != newPrimary && n.State == models.StateHealthy {
						expectedCount++
					}
				}
				failureCount := expectedCount - successCount
				if failureCount > 0 {
					metrics.ReplicationReconfigureTotal.WithLabelValues("failure").Add(float64(failureCount))
				}
			}
		}
	}
	if successCount > 0 {
		metrics.ReplicationReconfigureTotal.WithLabelValues("success").Add(float64(successCount))
	}

	if newPrimaryAddr != "" {
		m.mu.Lock()
		m.oldPrimaryID = failedNodeID
		m.newPrimaryConnInfo = m.replConf.PrimaryConnInfoForNode(newPrimary, newPrimaryAddr)
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

// isTimelineDivergenceError checks if an error represents a timeline divergence failure.
// Timeline divergence indicates the old primary has WAL history that cannot be reconciled
// with the new primary and requires manual intervention (e.g., rebuilding from backup).
func isTimelineDivergenceError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "timeline divergence") ||
		strings.Contains(errStr, "could not find common ancestor of the source and target cluster's timelines")
}

// HandleOldPrimaryRejoin runs pg_rewind and reconfigures the old primary to rejoin the cluster as a replica.
func (m *Manager) HandleOldPrimaryRejoin(ctx context.Context, nodeID, nodeAddr string) error {
	// Check STONITH fence - prevent split-brain by blocking fenced nodes from rejoining
	if isFenced, err := m.checkFenced(ctx, nodeID); err != nil {
		m.log.Warn("failed to check fence status, allowing rejoin with caution", zap.String("node", nodeID), zap.Error(err))
	} else if isFenced {
		m.log.Error("node is fenced, rejecting rejoin to prevent split-brain",
			zap.String("node", nodeID),
			zap.String("addr", nodeAddr),
		)
		return fmt.Errorf("node %q is fenced: rejoin blocked to prevent split-brain", nodeID)
	}

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
		if isTimelineDivergenceError(err) {
			// CRITICAL: Timeline divergence is a fatal error that requires manual intervention.
			// The old primary has WAL history that cannot be reconciled with the new primary.
			// Operator must either rebuild the node from a base backup or restore from a backup
			// taken before the divergence point. Do not clear oldPrimaryID so this node
			// remains flagged for manual rejoin.
			m.log.Error("timeline divergence detected during pg_rewind - manual intervention required",
				zap.String("node", nodeID),
				zap.String("addr", nodeAddr),
				zap.Error(err),
			)
			return fmt.Errorf("timeline divergence for %s: node requires manual rebuild, automatic rejoin aborted: %w", nodeID, err)
		}
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

// validateVersionCompatibility checks if the target node has a compatible PostgreSQL version
// with the rest of the cluster. Returns true if compatible, false otherwise.
// Logs warnings for version mismatches that are within the compatibility range.
func (m *Manager) validateVersionCompatibility(targetNodeID string) (bool, error) {
	topo := m.topo.Get()
	if topo == nil {
		return false, fmt.Errorf("topology not available")
	}

	var target *models.NodeStatus
	for i := range topo.Nodes {
		if topo.Nodes[i].NodeID == targetNodeID {
			target = &topo.Nodes[i]
			break
		}
	}
	if target == nil {
		return false, fmt.Errorf("target node %q not found", targetNodeID)
	}

	// If target version is zero (could not be parsed), reject failover
	if target.PGVersionParsed.IsZero() {
		m.log.Error("cannot elect node as primary: failed to parse PostgreSQL version",
			zap.String("node", targetNodeID),
			zap.String("raw_version", target.PGVersion),
		)
		metrics.VersionCompatibilityCheckTotal.WithLabelValues("unparseable").Inc()
		return false, nil
	}

	// Check compatibility with all other healthy nodes
	for _, node := range topo.Nodes {
		if node.NodeID == targetNodeID {
			continue
		}

		// Skip nodes with unparsable versions - log warning but don't fail
		if node.PGVersionParsed.IsZero() {
			m.log.Warn("node has unparsable PostgreSQL version, skipping compatibility check",
				zap.String("node", node.NodeID),
				zap.String("raw_version", node.PGVersion),
			)
			continue
		}

		// Skip degraded/unreachable nodes from compatibility check
		if node.State != models.StateHealthy {
			continue
		}

		compatible := target.PGVersionParsed.CompatibleWith(node.PGVersionParsed)
		if !compatible {
			m.log.Error("version incompatibility detected, rejecting failover",
				zap.String("target_node", targetNodeID),
				zap.String("target_version", target.PGVersionParsed.StringMajorMinor()),
				zap.String("other_node", node.NodeID),
				zap.String("other_version", node.PGVersionParsed.StringMajorMinor()),
			)
			metrics.VersionCompatibilityCheckTotal.WithLabelValues("incompatible").Inc()
			return false, nil
		}

		// Log warning for minor version mismatches that are still compatible
		if target.PGVersionParsed.Minor != node.PGVersionParsed.Minor {
			m.log.Warn("minor version mismatch detected within compatibility range",
				zap.String("target_node", targetNodeID),
				zap.String("target_version", target.PGVersionParsed.StringMajorMinor()),
				zap.String("other_node", node.NodeID),
				zap.String("other_version", node.PGVersionParsed.StringMajorMinor()),
			)
		}
	}

	metrics.VersionCompatibilityCheckTotal.WithLabelValues("success").Inc()
	return true, nil
}

// ElectNewPrimary selects the healthy replica with the highest WAL replay LSN, excluding the given node.
// Also validates PostgreSQL version compatibility before electing the new primary.
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

	// Validate version compatibility before returning the elected node
	compatible, err := m.validateVersionCompatibility(best.NodeID)
	if err != nil {
		m.log.Error("version compatibility check failed",
			zap.String("node", best.NodeID),
			zap.Error(err),
		)
		return ""
	}
	if !compatible {
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

	// Validate PostgreSQL version compatibility before proceeding with manual failover
	compatible, err := m.validateVersionCompatibility(targetNodeID)
	if err != nil {
		return fmt.Errorf("failover: version compatibility check failed: %w", err)
	}
	if !compatible {
		return fmt.Errorf("failover: target %q has incompatible PostgreSQL version with cluster", targetNodeID)
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
	oldPrimary := m.topo.Primary()

	defer func() {
		metrics.FailoverTotal.WithLabelValues("manual", manualResult).Inc()
		metrics.FailoverDurationSeconds.Observe(time.Since(start).Seconds())
		m.mu.Lock()
		m.failoverInProgress = false
		m.mu.Unlock()

		// Always clear fence token on exit to allow old primary to rejoin
		// after operator intervention, regardless of failover outcome
		if m.fenceToken != "" {
			m.clearFenceToken(ctx, oldPrimary)
		}
	}()

	// Set STONITH fence token before proceeding with failover
	if err := m.setFenceToken(ctx, oldPrimary); err != nil {
		return fmt.Errorf("failover: failed to set fence token: %w", err)
	}

	isLeader, err := m.coord.IsLeader(ctx)
	if err != nil {
		return fmt.Errorf("failover: coordinator check failed: %w", err)
	}
	if !isLeader {
		return fmt.Errorf("failover: not the leader")
	}

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
			// CRITICAL: abort failover if promotion fails to prevent split-brain
			m.log.Error("PromoteNode failed, aborting manual failover to prevent split-brain",
				zap.String("target", targetNodeID),
				zap.String("target_addr", target.Address),
				zap.Error(err),
			)
			manualResult = "error"
			return fmt.Errorf("failover: PromoteNode failed for %s: %w", targetNodeID, err)
		}
	} else {
		m.log.Error("cannot promote target node: no caller or missing address", zap.String("target", targetNodeID))
		manualResult = "error"
		return fmt.Errorf("failover: cannot promote %s: no caller or missing address", targetNodeID)
	}

	m.topo.SetPrimary(targetNodeID)

	if err := m.coord.PutClusterState(ctx, "primary", targetNodeID); err != nil {
		m.log.Warn("failed to persist primary to etcd", zap.Error(err))
	}

	successCount, reconfigErr := m.replConf.ReconfigureAfterFailover(ctx, targetNodeID)
	if reconfigErr != nil {
		if successCount == 0 {
			// CRITICAL: no replicas can be reconfigured, but we've already promoted the target node
			// The failover is committed but replication is broken - operator intervention required
			m.log.Error("replication reconfiguration failed for all replicas - failover committed but replication broken",
				zap.Int("success_count", successCount),
				zap.String("new_primary", targetNodeID),
				zap.Error(reconfigErr),
			)
			metrics.ReplicationReconfigureTotal.WithLabelValues("failure").Inc()
		} else {
			m.log.Warn("some replicas failed to reconfigure, continuing manual failover",
				zap.Int("success_count", successCount),
				zap.Error(reconfigErr),
			)
			// Count partial failures
			topo := m.topo.Get()
			if topo != nil {
				expectedCount := 0
				for _, n := range topo.Nodes {
					if n.NodeID != targetNodeID && n.State == models.StateHealthy {
						expectedCount++
					}
				}
				failureCount := expectedCount - successCount
				if failureCount > 0 {
					metrics.ReplicationReconfigureTotal.WithLabelValues("failure").Add(float64(failureCount))
				}
			}
		}
	}
	if successCount > 0 {
		metrics.ReplicationReconfigureTotal.WithLabelValues("success").Add(float64(successCount))
	}

	if target.Address != "" {
		m.mu.Lock()
		m.oldPrimaryID = oldPrimary
		m.newPrimaryConnInfo = m.replConf.PrimaryConnInfoForNode(targetNodeID, target.Address)
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

// setFenceToken writes a fence token to etcd for the given node.
// This prevents the fenced node from rejoining the cluster, protecting against split-brain.
func (m *Manager) setFenceToken(ctx context.Context, nodeID string) error {
	token, err := generateFenceToken()
	if err != nil {
		return fmt.Errorf("failed to generate fence token: %w", err)
	}

	key := fmt.Sprintf("fence/%s", nodeID)
	if err := m.coord.PutClusterState(ctx, key, token); err != nil {
		return fmt.Errorf("failed to write fence token to etcd: %w", err)
	}

	m.mu.Lock()
	m.fenceToken = token
	m.mu.Unlock()

	m.log.Info("fence token set for node",
		zap.String("node", nodeID),
		zap.String("token", token),
	)
	return nil
}

// clearFenceToken removes the fence token from etcd for the given node.
// This allows the node to rejoin the cluster after successful failover.
func (m *Manager) clearFenceToken(ctx context.Context, nodeID string) {
	key := fmt.Sprintf("fence/%s", nodeID)
	if err := m.coord.PutClusterState(ctx, key, ""); err != nil {
		m.log.Warn("failed to clear fence token from etcd",
			zap.String("node", nodeID),
			zap.Error(err),
		)
		return
	}

	m.mu.Lock()
	m.fenceToken = ""
	m.mu.Unlock()

	m.log.Info("fence token cleared for node", zap.String("node", nodeID))
}

// checkFenced determines whether a node is currently fenced.
// A fenced node has an active fence token in etcd and cannot rejoin the cluster.
func (m *Manager) checkFenced(ctx context.Context, nodeID string) (bool, error) {
	key := fmt.Sprintf("fence/%s", nodeID)
	value, err := m.coord.GetClusterState(ctx, key)
	if err != nil {
		// If we can't read from etcd, assume not fenced but log warning
		m.log.Warn("failed to read fence token from etcd, assuming not fenced",
			zap.String("node", nodeID),
			zap.Error(err),
		)
		return false, err
	}

	// Empty value means no fence token exists
	return value != "", nil
}
