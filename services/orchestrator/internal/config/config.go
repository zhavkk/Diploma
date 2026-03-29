package config

import (
	"fmt"

	envutil "github.com/zhavkk/Diploma/pkg/config"
)

type OrchestratorConfig struct {
	NodeID               string
	GRPCAddr             string
	HTTPAddr             string
	HeartbeatTimeout     int
	QuorumSize           int
	EtcdEndpoints        []string
	ReplicationPassword  string
	ReplicationUser      string
	ReplicationPGPort    int
}

func LoadOrchestrator() (*OrchestratorConfig, error) {
	nodeID, err := envutil.RequireEnv("NODE_ID")
	if err != nil {
		return nil, err
	}

	cfg := &OrchestratorConfig{
		NodeID:              nodeID,
		GRPCAddr:            envutil.EnvOr("GRPC_ADDR", ":50051"),
		HTTPAddr:            envutil.EnvOr("HTTP_ADDR", ":8080"),
		HeartbeatTimeout:    envutil.EnvInt("HEARTBEAT_TIMEOUT", 10),
		QuorumSize:          envutil.EnvInt("QUORUM_SIZE", 1),
		EtcdEndpoints:       envutil.EnvStringSlice("ETCD_ENDPOINTS", []string{"etcd:2379"}),
		ReplicationPassword: envutil.EnvOr("REPLICATION_PASSWORD", "replicator"),
		ReplicationUser:     envutil.EnvOr("REPLICATION_USER", "replicator"),
		ReplicationPGPort:   envutil.EnvInt("REPLICATION_PG_PORT", 5432),
	}
	if cfg.HeartbeatTimeout <= 0 {
		return nil, fmt.Errorf("config: HEARTBEAT_TIMEOUT must be > 0, got %d", cfg.HeartbeatTimeout)
	}
	if cfg.QuorumSize < 0 {
		return nil, fmt.Errorf("config: QUORUM_SIZE must be >= 0, got %d", cfg.QuorumSize)
	}
	if cfg.ReplicationPGPort <= 0 || cfg.ReplicationPGPort > 65535 {
		return nil, fmt.Errorf("config: REPLICATION_PG_PORT must be 1-65535, got %d", cfg.ReplicationPGPort)
	}
	return cfg, nil
}
