package replication

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

type Configurator struct {
	log *zap.Logger
}

func NewConfigurator(log *zap.Logger) *Configurator {
	return &Configurator{log: log}
}

func (c *Configurator) Apply(ctx context.Context, cfg models.ReplicationConfig, targetNodes []string) error {
	c.log.Info("applying replication config",
		zap.String("synchronous_standby_names", cfg.SynchronousStandbyNames),
		zap.Bool("sync", cfg.EnableSyncReplication),
		zap.Strings("nodes", targetNodes),
	)

	for _, nodeID := range targetNodes {
		if err := c.applyToNode(ctx, nodeID, cfg); err != nil {
			return fmt.Errorf("configurator: apply to node %q: %w", nodeID, err)
		}
	}
	return nil
}

func (c *Configurator) ReconfigureAfterFailover(ctx context.Context, newPrimaryNodeID string) error {
	c.log.Info("reconfiguring replication after failover",
		zap.String("new_primary", newPrimaryNodeID),
	)
	// TODO: получить список реплик из topology.Registry
	// Для каждой реплики: вызов NodeAgentService.ReconfigureReplication(primary_conninfo)
	return nil
}

func (c *Configurator) applyToNode(ctx context.Context, nodeID string, cfg models.ReplicationConfig) error {
	// TODO: gRPC вызов NodeAgentService.ReconfigureReplication на nodeID
	c.log.Debug("applying replication config to node",
		zap.String("node_id", nodeID),
		zap.String("synchronous_standby_names", cfg.SynchronousStandbyNames),
	)
	return nil
}
