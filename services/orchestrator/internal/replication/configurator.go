package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

// TopologySource provides read access to the current cluster topology.
type TopologySource interface {
	Get() *models.ClusterTopology
}

// NodeAgentCaller sends replication reconfiguration commands to node agents via gRPC.
type NodeAgentCaller interface {
	ReconfigureReplication(ctx context.Context, nodeAddr, primaryConnInfo, timeline string) error
}

// Config holds PostgreSQL replication connection parameters.
type Config struct {
	ReplicationPassword string
	ReplicationUser     string // defaults to "replicator"
	SSLMode             string // defaults to "disable"
	PGPort              int    // defaults to 5432
	PGHosts             map[string]string
}

func connInfoQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// Configurator manages replication settings across cluster replicas after topology changes.
type Configurator struct {
	cfg    Config
	topo   TopologySource
	caller NodeAgentCaller
	log    *zap.Logger
}

// NewConfigurator creates a Configurator with default replication connection settings.
func NewConfigurator(topo TopologySource, caller NodeAgentCaller, log *zap.Logger) *Configurator {
	return &Configurator{
		cfg:    Config{SSLMode: "disable", ReplicationUser: "replicator"},
		topo:   topo,
		caller: caller,
		log:    log,
	}
}

// NewConfiguratorWithConfig creates a Configurator with explicit replication connection settings.
func NewConfiguratorWithConfig(cfg Config, topo TopologySource, caller NodeAgentCaller, log *zap.Logger) *Configurator {
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	if cfg.ReplicationUser == "" {
		cfg.ReplicationUser = "replicator"
	}
	return &Configurator{cfg: cfg, topo: topo, caller: caller, log: log}
}

// Apply sends the given replication configuration to each of the specified target nodes.
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

// ReconfigureAfterFailover updates all replicas to stream from the new primary after a failover.
// Returns the number of successfully reconfigured replicas and an error if any replica failed.
func (c *Configurator) ReconfigureAfterFailover(ctx context.Context, newPrimaryNodeID string) (int, error) {
	c.log.Info("reconfiguring replication after failover", zap.String("new_primary", newPrimaryNodeID))

	if c.topo == nil {
		return 0, nil
	}
	topo := c.topo.Get()
	if topo == nil {
		return 0, nil
	}

	addrMap := c.buildAddrMap()
	newPrimaryAddr := addrMap[newPrimaryNodeID]
	primaryConnInfo := c.PrimaryConnInfoForNode(newPrimaryNodeID, newPrimaryAddr)

	var errs []error
	successCount := 0
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
		} else {
			successCount++
		}
	}
	return successCount, errors.Join(errs...)
}

// PrimaryConnInfo builds a libpq-compatible connection string for the primary at the given address.
func (c *Configurator) PrimaryConnInfo(addr string) string {
	return c.PrimaryConnInfoForNode("", addr)
}

// PrimaryConnInfoForNode builds a libpq-compatible connection string for a node.
// PGHosts can override the node-agent host when PostgreSQL is reachable through
// a different hostname, which is common in Docker Compose.
func (c *Configurator) PrimaryConnInfoForNode(nodeID, addr string) string {
	host := addr
	if nodeID != "" && c.cfg.PGHosts != nil {
		if configuredHost := c.cfg.PGHosts[nodeID]; configuredHost != "" {
			host = configuredHost
		}
	}
	if h, _, err := net.SplitHostPort(addr); err == nil {
		if host == addr {
			host = h
		}
	}
	sslMode := c.cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	port := c.cfg.PGPort
	if port == 0 {
		port = 5432
	}
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s sslmode=%s",
		host, port, connInfoQuote(c.cfg.ReplicationUser), connInfoQuote(c.cfg.ReplicationPassword), sslMode,
	)
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
