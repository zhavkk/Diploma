package topology

import (
	"sync"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

type Registry struct {
	mu   sync.RWMutex
	topo *models.ClusterTopology
	log  *zap.Logger
}

func NewRegistry(log *zap.Logger) *Registry {
	return &Registry{log: log}
}

func (r *Registry) Get() *models.ClusterTopology {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.topo == nil {
		return nil
	}
	copy := *r.topo
	return &copy
}

func (r *Registry) Update(topo *models.ClusterTopology) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topo = topo
	r.log.Info("topology updated",
		zap.String("primary", topo.PrimaryNode),
		zap.String("version", topo.Version),
		zap.Int("nodes", len(topo.Nodes)),
	)
}

func (r *Registry) Primary() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.topo == nil {
		return ""
	}
	return r.topo.PrimaryNode
}

func (r *Registry) SetPrimary(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.topo != nil {
		r.topo.PrimaryNode = nodeID
		r.log.Info("primary updated", zap.String("node_id", nodeID))
	}
}
