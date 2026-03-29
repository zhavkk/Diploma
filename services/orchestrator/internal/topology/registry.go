package topology

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

// Registry is the in-memory store for cluster topology and failover events.
// It is safe for concurrent use.
type Registry struct {
	mu      sync.RWMutex
	topo    *models.ClusterTopology
	events  []models.FailoverEvent
	log     *zap.Logger
	version atomic.Int64
}

func (r *Registry) nextVersion() string {
	return fmt.Sprintf("v%d", r.version.Add(1))
}

// NewRegistry creates a new empty topology registry.
func NewRegistry(log *zap.Logger) *Registry {
	return &Registry{log: log}
}

// Get returns a deep copy of the current cluster topology, or nil if not yet initialized.
func (r *Registry) Get() *models.ClusterTopology {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.topo == nil {
		return nil
	}
	cp := *r.topo
	cp.Nodes = make([]models.NodeStatus, len(r.topo.Nodes))
	copy(cp.Nodes, r.topo.Nodes)
	return &cp
}

// Update replaces the entire cluster topology and increments the version.
func (r *Registry) Update(topo *models.ClusterTopology) {
	r.mu.Lock()
	defer r.mu.Unlock()
	topo.Version = r.nextVersion()
	r.topo = topo
	r.log.Info("topology updated",
		zap.String("primary", topo.PrimaryNode),
		zap.String("version", topo.Version),
		zap.Int("nodes", len(topo.Nodes)),
	)
}

// Primary returns the node ID of the current primary, or an empty string if unknown.
func (r *Registry) Primary() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.topo == nil {
		return ""
	}
	return r.topo.PrimaryNode
}

// SetPrimary updates the primary node ID in the topology.
func (r *Registry) SetPrimary(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.topo != nil {
		r.topo.PrimaryNode = nodeID
		r.topo.Version = r.nextVersion()
		r.log.Info("primary updated", zap.String("node_id", nodeID))
	}
}

// UpsertNode inserts or updates a node's status in the topology.
func (r *Registry) UpsertNode(status models.NodeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.topo == nil {
		r.topo = &models.ClusterTopology{UpdatedAt: time.Now()}
	}

	for i, n := range r.topo.Nodes {
		if n.NodeID == status.NodeID {
			r.topo.Nodes[i] = status
			if status.Role == models.RolePrimary && r.topo.PrimaryNode == "" {
				r.topo.PrimaryNode = status.NodeID
			}
			r.topo.UpdatedAt = time.Now()
			r.topo.Version = r.nextVersion()
			r.log.Debug("node updated in topology",
				zap.String("node_id", status.NodeID),
				zap.String("role", string(status.Role)),
			)
			return
		}
	}

	r.topo.Nodes = append(r.topo.Nodes, status)
	if status.Role == models.RolePrimary && r.topo.PrimaryNode == "" {
		r.topo.PrimaryNode = status.NodeID
	}
	r.topo.UpdatedAt = time.Now()
	r.topo.Version = r.nextVersion()
	r.log.Debug("node added to topology",
		zap.String("node_id", status.NodeID),
		zap.String("role", string(status.Role)),
	)
}

const maxEvents = 1000

// AppendEvent records a failover event, retaining at most the last 1000 events.
func (r *Registry) AppendEvent(evt models.FailoverEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
	if len(r.events) > maxEvents {
		r.events = r.events[len(r.events)-maxEvents:]
	}
	r.log.Info("failover event recorded",
		zap.String("old_primary", evt.OldPrimary),
		zap.String("new_primary", evt.NewPrimary),
		zap.String("reason", evt.Reason),
		zap.Time("at", evt.OccurredAt),
	)
}

// Events returns a copy of all recorded failover events.
func (r *Registry) Events() []models.FailoverEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make([]models.FailoverEvent, len(r.events))
	copy(cp, r.events)
	return cp
}

// UpdateNodeState changes the state of a specific node in the topology.
func (r *Registry) UpdateNodeState(nodeID string, state models.NodeState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.topo == nil {
		return
	}
	for i := range r.topo.Nodes {
		if r.topo.Nodes[i].NodeID == nodeID {
			r.topo.Nodes[i].State = state
			r.topo.UpdatedAt = time.Now()
			r.topo.Version = r.nextVersion()
			r.log.Info("node state updated",
				zap.String("node_id", nodeID),
				zap.String("state", string(state)),
			)
			return
		}
	}
}
