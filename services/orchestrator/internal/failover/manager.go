package failover

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
)

type Config struct {
	QuorumSize int
}

type TopologyRegistry interface {
	Primary() string
	SetPrimary(nodeID string)
	Get() *models.ClusterTopology
}

type CoordinationModule interface {
	IsLeader(ctx context.Context) (bool, error)
}

type EventAppender interface {
	AppendEvent(evt models.FailoverEvent)
}

type Manager struct {
	cfg        Config
	topo       TopologyRegistry
	coord      CoordinationModule
	replConf   *replication.Configurator
	caller     NodeAgentCaller
	eventStore EventAppender
	log        *zap.Logger

	mu                 sync.Mutex
	failoverInProgress bool

	oldPrimaryID       string
	newPrimaryConnInfo string
}

func NewManager(
	cfg Config,
	topo TopologyRegistry,
	coord CoordinationModule,
	rc *replication.Configurator,
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

func (m *Manager) WithEventStore(e EventAppender) {
	m.eventStore = e
}

func (m *Manager) Run(ctx context.Context) {
	<-ctx.Done()
}

func (m *Manager) IsFailoverInProgress() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failoverInProgress
}

func (m *Manager) NotifyPrimaryFailure(ctx context.Context, failedNodeID string) {
	m.mu.Lock()
	if m.failoverInProgress {
		m.log.Warn("failover already in progress, skipping", zap.String("node", failedNodeID))
		m.mu.Unlock()
		return
	}
	m.failoverInProgress = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.failoverInProgress = false
		m.mu.Unlock()
	}()

	m.log.Info("starting failover", zap.String("failed_primary", failedNodeID))

	isLeader, err := m.coord.IsLeader(ctx)
	if err != nil || !isLeader {
		m.log.Warn("not the leader, aborting failover", zap.Error(err))
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

	if m.caller != nil {
		var addr string
		if topo := m.topo.Get(); topo != nil {
			for _, n := range topo.Nodes {
				if n.NodeID == newPrimary {
					addr = n.Address
					break
				}
			}
		}
		if addr != "" {
			if err := m.caller.PromoteNode(ctx, addr); err != nil {
				m.log.Error("PromoteNode failed, continuing failover", zap.Error(err))
			}
		} else {
			m.log.Warn("new primary has no address, skipping PromoteNode", zap.String("node", newPrimary))
		}
	}

	if err := m.replConf.ReconfigureAfterFailover(ctx, newPrimary); err != nil {
		m.log.Error("replication reconfiguration failed", zap.Error(err))
	}

	m.topo.SetPrimary(newPrimary)

	var newPrimaryAddr string
	if topo := m.topo.Get(); topo != nil {
		for _, n := range topo.Nodes {
			if n.NodeID == newPrimary {
				newPrimaryAddr = n.Address
				break
			}
		}
	}
	m.mu.Lock()
	m.oldPrimaryID = failedNodeID
	m.newPrimaryConnInfo = fmt.Sprintf("host=%s user=replicator application_name=%s", newPrimaryAddr, failedNodeID)
	m.mu.Unlock()

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

func (m *Manager) HandleOldPrimaryRejoin(ctx context.Context, nodeID, nodeAddr string) error {
	m.mu.Lock()
	if m.oldPrimaryID != nodeID {
		m.mu.Unlock()
		return nil
	}
	connInfo := m.newPrimaryConnInfo

	m.oldPrimaryID = ""
	m.newPrimaryConnInfo = ""
	m.mu.Unlock()

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
	m.log.Info("pg_rewind completed, node can rejoin as replica", zap.String("node", nodeID))
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
		if best == nil || node.WALReplayLSN > best.WALReplayLSN {
			best = node
		}
	}

	if best == nil {
		return ""
	}
	return best.NodeID
}

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
	m.failoverInProgress = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.failoverInProgress = false
		m.mu.Unlock()
	}()

	isLeader, err := m.coord.IsLeader(ctx)
	if err != nil || !isLeader {
		return fmt.Errorf("failover: not the leader")
	}

	if m.cfg.QuorumSize > 0 {
		currentPrimary := m.topo.Primary()
		healthyReplicas := m.countHealthyReplicas(currentPrimary)
		if healthyReplicas < m.cfg.QuorumSize {
			return fmt.Errorf("failover: quorum not met (healthy replicas: %d, required: %d)",
				healthyReplicas, m.cfg.QuorumSize)
		}
	}

	oldPrimary := m.topo.Primary()
	m.log.Info("promoting target node", zap.String("node", targetNodeID))

	if m.caller != nil && target.Address != "" {
		if err := m.caller.PromoteNode(ctx, target.Address); err != nil {
			m.log.Error("PromoteNode failed, continuing", zap.Error(err))
		}
	}

	if err := m.replConf.ReconfigureAfterFailover(ctx, targetNodeID); err != nil {
		m.log.Error("replication reconfiguration failed", zap.Error(err))
	}

	m.topo.SetPrimary(targetNodeID)

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
