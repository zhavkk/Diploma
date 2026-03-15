package replication

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

type TopologySource interface {
	Get() *models.ClusterTopology
}

type NodeAgentCaller interface {
	ReconfigureReplication(ctx context.Context, nodeAddr, primaryConnInfo, timeline string) error
}

type Configurator struct {
	topo   TopologySource
	caller NodeAgentCaller
	log    *zap.Logger
}

func NewConfigurator(topo TopologySource, caller NodeAgentCaller, log *zap.Logger) *Configurator {
	return &Configurator{topo: topo, caller: caller, log: log}
}

func (c *Configurator) Apply(ctx context.Context, cfg models.ReplicationConfig, targetNodes []string) error {
	c.log.Info("applying replication config",
		zap.String("synchronous_standby_names", cfg.SynchronousStandbyNames),
		zap.Bool("sync", cfg.EnableSyncReplication),
		zap.Strings("nodes", targetNodes),
	)

	addrMap := c.buildAddrMap()

	for _, nodeID := range targetNodes {
		addr, ok := addrMap[nodeID]
		if !ok || addr == "" {
			c.log.Warn("node has no address, skipping reconfig", zap.String("node_id", nodeID))
			continue
		}
		if err := c.caller.ReconfigureReplication(ctx, addr, cfg.PrimaryConnInfo, "latest"); err != nil {
			return fmt.Errorf("configurator: apply to node %q: %w", nodeID, err)
		}
	}
	return nil
}

func (c *Configurator) ReconfigureAfterFailover(ctx context.Context, newPrimaryNodeID string) error {
	c.log.Info("reconfiguring replication after failover", zap.String("new_primary", newPrimaryNodeID))

	if c.topo == nil {
		return nil
	}
	topo := c.topo.Get()
	if topo == nil {
		return nil
	}

	addrMap := c.buildAddrMap()
	newPrimaryAddr := addrMap[newPrimaryNodeID]
	primaryConnInfo := fmt.Sprintf("host=%s port=5432 user=replicator", newPrimaryAddr)

	var errs []error
	for _, node := range topo.Nodes {
		if node.NodeID == newPrimaryNodeID {
			continue
		}
		addr, ok := addrMap[node.NodeID]
		if !ok || addr == "" {
			c.log.Warn("replica has no address, skipping reconfig", zap.String("node_id", node.NodeID))
			continue
		}
		if err := c.caller.ReconfigureReplication(ctx, addr, primaryConnInfo, "latest"); err != nil {
			c.log.Error("reconfig failed for replica",
				zap.String("node_id", node.NodeID),
				zap.Error(err),
			)
			errs = append(errs, fmt.Errorf("node %q: %w", node.NodeID, err))
		}
	}
	return errors.Join(errs...)
}

func (c *Configurator) buildAddrMap() map[string]string {
	if c.topo == nil {
		return map[string]string{}
	}
	topo := c.topo.Get()
	if topo == nil {
		return map[string]string{}
	}
	m := make(map[string]string, len(topo.Nodes))
	for _, n := range topo.Nodes {
		m[n.NodeID] = n.Address
	}
	return m
}
