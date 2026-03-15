package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type OrchestratorConfig struct {
	NodeID           string
	GRPCAddr         string
	HTTPAddr         string
	HeartbeatTimeout int
	QuorumSize       int
	EtcdEndpoints    []string
}

func LoadOrchestrator() (*OrchestratorConfig, error) {
	nodeID, err := requireEnv("NODE_ID")
	if err != nil {
		return nil, err
	}

	cfg := &OrchestratorConfig{
		NodeID:           nodeID,
		GRPCAddr:         envOr("GRPC_ADDR", ":50051"),
		HTTPAddr:         envOr("HTTP_ADDR", ":8080"),
		HeartbeatTimeout: envInt("HEARTBEAT_TIMEOUT", 10),
		QuorumSize:       envInt("QUORUM_SIZE", 1),
		EtcdEndpoints:    envStringSlice("ETCD_ENDPOINTS", []string{"etcd:2379"}),
	}
	return cfg, nil
}

func requireEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("config: required env var %q is not set", key)
	}
	return v, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envStringSlice(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			result = append(result, s)
		}
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}
