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

type Manager struct {
	cfg      Config
	topo     TopologyRegistry
	coord    CoordinationModule
	replConf *replication.Configurator
	log      *zap.Logger

	mu                 sync.Mutex
	failoverInProgress bool
}

func NewManager(
	cfg Config,
	topo TopologyRegistry,
	coord CoordinationModule,
	rc *replication.Configurator,
	log *zap.Logger,
) *Manager {
	return &Manager{
		cfg:      cfg,
		topo:     topo,
		coord:    coord,
		replConf: rc,
		log:      log,
	}
}

func (m *Manager) Run(ctx context.Context) {
	<-ctx.Done()
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

	newPrimary, err := m.electNewPrimary(failedNodeID)
	if err != nil {
		m.log.Error("failed to elect new primary", zap.Error(err))
		return
	}

	m.log.Info("elected new primary", zap.String("node", newPrimary))

	// TODO: вызов gRPC NodeAgentService.PromoteNode(newPrimary)

	if err := m.replConf.ReconfigureAfterFailover(ctx, newPrimary); err != nil {
		m.log.Error("replication reconfiguration failed", zap.Error(err))
	}

	m.topo.SetPrimary(newPrimary)

	// TODO: HAProxy reload / API вызов

	m.log.Info("failover completed",
		zap.String("old_primary", failedNodeID),
		zap.String("new_primary", newPrimary),
		zap.Time("at", time.Now()),
	)
}

func (m *Manager) electNewPrimary(excludeNodeID string) (string, error) {
	topo := m.topo.Get()
	if topo == nil {
		return "", fmt.Errorf("topology not available")
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
		return "", fmt.Errorf("no healthy replica available for promotion")
	}
	return best.NodeID, nil
}

func (m *Manager) TriggerManualFailover(ctx context.Context, targetNodeID string) error {
	m.log.Info("manual failover requested", zap.String("target", targetNodeID))
	m.NotifyPrimaryFailure(ctx, m.topo.Primary())
	return nil
}
