package config

import (
	"fmt"
	"time"

	envutil "github.com/zhavkk/Diploma/pkg/config"
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

	PollInterval  int
	GRPCAddr      string
	HealthAddr    string
	GRPCTLSCert   string
	GRPCTLSKey    string
	GRPCTLSCACert string

	PgRewindRetryDelay time.Duration // delay between pg_rewind retries

	// Connection pool settings for pgclient
	PGMaxOpenConns    int
	PGMaxIdleConns    int
	PGConnMaxLifetime time.Duration
}

func LoadNodeAgent() (*NodeAgentConfig, error) {
	nodeID, err := envutil.RequireEnv("NODE_ID")
	if err != nil {
		return nil, err
	}
	nodeAddr, err := envutil.RequireEnv("NODE_ADDR")
	if err != nil {
		return nil, err
	}
	orchAddr, err := envutil.RequireEnv("ORCHESTRATOR_ADDR")
	if err != nil {
		return nil, err
	}
	pgdata, err := envutil.RequireEnv("PGDATA")
	if err != nil {
		return nil, err
	}
	pgUser, err := envutil.RequireEnv("PG_USER")
	if err != nil {
		return nil, err
	}
	pgPassword, err := envutil.RequireEnv("PG_PASSWORD")
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
		PGHost:           envutil.EnvOr("PG_HOST", "localhost"),
		PGPort:           envutil.EnvInt("PG_PORT", 5432),
		PGSSLMode:        envutil.EnvOr("PG_SSL_MODE", "disable"),
		PollInterval:     envutil.EnvInt("POLL_INTERVAL", 5),
		GRPCAddr:         envutil.EnvOr("GRPC_ADDR", ":50052"),
		HealthAddr:       envutil.EnvOr("HEALTH_ADDR", ":8081"),
		GRPCTLSCert:      envutil.EnvOr("GRPC_TLS_CERT", ""),
		GRPCTLSKey:       envutil.EnvOr("GRPC_TLS_KEY", ""),
		GRPCTLSCACert:    envutil.EnvOr("GRPC_TLS_CA", ""),
		PGMaxOpenConns:     envutil.EnvInt("PG_MAX_OPEN_CONNS", 25),
		PGMaxIdleConns:     envutil.EnvInt("PG_MAX_IDLE_CONNS", 5),
		PGConnMaxLifetime:  envutil.EnvDuration("PG_CONN_MAX_LIFETIME", 5*time.Minute),
		PgRewindRetryDelay: envutil.EnvDuration("PG_REWIND_RETRY_DELAY", 5*time.Second),
	}
	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("config: POLL_INTERVAL must be > 0, got %d", cfg.PollInterval)
	}
	if cfg.PGPort <= 0 || cfg.PGPort > 65535 {
		return nil, fmt.Errorf("config: PG_PORT must be 1-65535, got %d", cfg.PGPort)
	}
	return cfg, nil
}
