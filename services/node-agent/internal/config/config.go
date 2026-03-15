package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type NodeAgentConfig struct {
	NodeID           string
	NodeAddr         string
	OrchestratorAddr string

	PGData     string
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGSSLMode  string

	PollInterval int
	GRPCAddr     string
	HealthAddr   string
}

func LoadNodeAgent() (*NodeAgentConfig, error) {
	nodeID, err := requireEnv("NODE_ID")
	if err != nil {
		return nil, err
	}
	nodeAddr, err := requireEnv("NODE_ADDR")
	if err != nil {
		return nil, err
	}
	orchAddr, err := requireEnv("ORCHESTRATOR_ADDR")
	if err != nil {
		return nil, err
	}
	pgdata, err := requireEnv("PGDATA")
	if err != nil {
		return nil, err
	}
	pgUser, err := requireEnv("PG_USER")
	if err != nil {
		return nil, err
	}
	pgPassword, err := requireEnv("PG_PASSWORD")
	if err != nil {
		return nil, err
	}

	cfg := &NodeAgentConfig{
		NodeID:           nodeID,
		NodeAddr:         nodeAddr,
		OrchestratorAddr: orchAddr,
		PGData:           pgdata,
		PGUser:           pgUser,
		PGPassword:       pgPassword,
		PGHost:           envOr("PG_HOST", "localhost"),
		PGPort:           envInt("PG_PORT", 5432),
		PGSSLMode:        envOr("PG_SSL_MODE", "disable"),
		PollInterval:     envInt("POLL_INTERVAL", 5),
		GRPCAddr:         envOr("GRPC_ADDR", ":50052"),
		HealthAddr:       envOr("HEALTH_ADDR", ":8081"),
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
