package config_test

import (
	"testing"

	"github.com/zhavkk/Diploma/services/node-agent/internal/config"
)

func TestNodeAgentConfig_Defaults(t *testing.T) {
	t.Setenv("NODE_ID", "pg-primary")
	t.Setenv("NODE_ADDR", "pg-primary:50052")
	t.Setenv("ORCHESTRATOR_ADDR", "orchestrator:50051")
	t.Setenv("PGDATA", "/var/lib/postgresql/data")
	t.Setenv("PG_USER", "postgres")
	t.Setenv("PG_PASSWORD", "secret")

	cfg, err := config.LoadNodeAgent()
	if err != nil {
		t.Fatalf("LoadNodeAgent: %v", err)
	}

	if cfg.PGHost != "localhost" {
		t.Errorf("PGHost = %q, want localhost", cfg.PGHost)
	}
	if cfg.PGPort != 5432 {
		t.Errorf("PGPort = %d, want 5432", cfg.PGPort)
	}
	if cfg.PGSSLMode != "disable" {
		t.Errorf("PGSSLMode = %q, want disable", cfg.PGSSLMode)
	}
	if cfg.GRPCAddr != ":50052" {
		t.Errorf("GRPCAddr = %q, want :50052", cfg.GRPCAddr)
	}
	if cfg.HealthAddr != ":8081" {
		t.Errorf("HealthAddr = %q, want :8081", cfg.HealthAddr)
	}
	if cfg.PollInterval != 5 {
		t.Errorf("PollInterval = %d, want 5", cfg.PollInterval)
	}
	// Verify default connection pool settings
	if cfg.PGMaxOpenConns != 25 {
		t.Errorf("PGMaxOpenConns = %d, want 25", cfg.PGMaxOpenConns)
	}
	if cfg.PGMaxIdleConns != 5 {
		t.Errorf("PGMaxIdleConns = %d, want 5", cfg.PGMaxIdleConns)
	}
	if cfg.PGConnMaxLifetime.Minutes() != 5 {
		t.Errorf("PGConnMaxLifetime = %v, want 5m", cfg.PGConnMaxLifetime)
	}
}

func TestNodeAgentConfig_ReadsFromEnv(t *testing.T) {
	t.Setenv("NODE_ID", "pg-replica1")
	t.Setenv("NODE_ADDR", "10.0.0.1:50052")
	t.Setenv("ORCHESTRATOR_ADDR", "10.0.0.100:50051")
	t.Setenv("PGDATA", "/data/pg")
	t.Setenv("PG_USER", "replicator")
	t.Setenv("PG_PASSWORD", "pass")
	t.Setenv("PG_HOST", "10.0.0.1")
	t.Setenv("PG_PORT", "5433")
	t.Setenv("PG_SSL_MODE", "require")
	t.Setenv("POLL_INTERVAL", "10")
	t.Setenv("GRPC_ADDR", ":50053")
	t.Setenv("HEALTH_ADDR", ":8082")

	cfg, err := config.LoadNodeAgent()
	if err != nil {
		t.Fatalf("LoadNodeAgent: %v", err)
	}

	if cfg.PGHost != "10.0.0.1" {
		t.Errorf("PGHost = %q, want 10.0.0.1", cfg.PGHost)
	}
	if cfg.PGPort != 5433 {
		t.Errorf("PGPort = %d, want 5433", cfg.PGPort)
	}
	if cfg.PGSSLMode != "require" {
		t.Errorf("PGSSLMode = %q, want require", cfg.PGSSLMode)
	}
	if cfg.PollInterval != 10 {
		t.Errorf("PollInterval = %d, want 10", cfg.PollInterval)
	}
	if cfg.GRPCAddr != ":50053" {
		t.Errorf("GRPCAddr = %q, want :50053", cfg.GRPCAddr)
	}
	// Verify connection pool settings can be customized
	if cfg.PGMaxOpenConns != 25 {
		t.Errorf("PGMaxOpenConns = %d, want 25 (default)", cfg.PGMaxOpenConns)
	}
	if cfg.PGMaxIdleConns != 5 {
		t.Errorf("PGMaxIdleConns = %d, want 5 (default)", cfg.PGMaxIdleConns)
	}
	if cfg.PGConnMaxLifetime.Minutes() != 5 {
		t.Errorf("PGConnMaxLifetime = %v, want 5m (default)", cfg.PGConnMaxLifetime)
	}
}

func TestNodeAgentConfig_ErrorWhenPollIntervalZero(t *testing.T) {
	t.Setenv("NODE_ID", "pg-primary")
	t.Setenv("NODE_ADDR", "pg-primary:50052")
	t.Setenv("ORCHESTRATOR_ADDR", "orchestrator:50051")
	t.Setenv("PGDATA", "/var/lib/postgresql/data")
	t.Setenv("PG_USER", "postgres")
	t.Setenv("PG_PASSWORD", "secret")
	t.Setenv("POLL_INTERVAL", "0")

	_, err := config.LoadNodeAgent()
	if err == nil {
		t.Error("expected error when POLL_INTERVAL=0")
	}
}

func TestNodeAgentConfig_ErrorWhenPGPortInvalid(t *testing.T) {
	t.Setenv("NODE_ID", "pg-primary")
	t.Setenv("NODE_ADDR", "pg-primary:50052")
	t.Setenv("ORCHESTRATOR_ADDR", "orchestrator:50051")
	t.Setenv("PGDATA", "/var/lib/postgresql/data")
	t.Setenv("PG_USER", "postgres")
	t.Setenv("PG_PASSWORD", "secret")
	t.Setenv("PG_PORT", "99999")

	_, err := config.LoadNodeAgent()
	if err == nil {
		t.Error("expected error when PG_PORT=99999 (out of range)")
	}
}

func TestNodeAgentConfig_ErrorOnMissingRequired(t *testing.T) {
	cases := []struct {
		name    string
		unset   string
		setEnvs map[string]string
	}{
		{
			name:  "missing NODE_ID",
			unset: "NODE_ID",
			setEnvs: map[string]string{
				"NODE_ADDR": "node:50052", "ORCHESTRATOR_ADDR": "orch:50051",
				"PGDATA": "/data", "PG_USER": "u", "PG_PASSWORD": "p",
			},
		},
		{
			name:  "missing PG_USER",
			unset: "PG_USER",
			setEnvs: map[string]string{
				"NODE_ID": "n1", "NODE_ADDR": "node:50052", "ORCHESTRATOR_ADDR": "orch:50051",
				"PGDATA": "/data", "PG_PASSWORD": "p",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.setEnvs {
				t.Setenv(k, v)
			}
			t.Setenv(tc.unset, "")

			_, err := config.LoadNodeAgent()
			if err == nil {
				t.Errorf("expected error when %s is missing", tc.unset)
			}
		})
	}
}

func TestNodeAgentConfig_ConnectionPoolSettings(t *testing.T) {
	t.Setenv("NODE_ID", "pg-replica2")
	t.Setenv("NODE_ADDR", "10.0.0.2:50052")
	t.Setenv("ORCHESTRATOR_ADDR", "10.0.0.100:50051")
	t.Setenv("PGDATA", "/data/pg")
	t.Setenv("PG_USER", "replicator")
	t.Setenv("PG_PASSWORD", "pass")
	t.Setenv("PG_MAX_OPEN_CONNS", "50")
	t.Setenv("PG_MAX_IDLE_CONNS", "10")
	t.Setenv("PG_CONN_MAX_LIFETIME", "10m")

	cfg, err := config.LoadNodeAgent()
	if err != nil {
		t.Fatalf("LoadNodeAgent: %v", err)
	}

	if cfg.PGMaxOpenConns != 50 {
		t.Errorf("PGMaxOpenConns = %d, want 50", cfg.PGMaxOpenConns)
	}
	if cfg.PGMaxIdleConns != 10 {
		t.Errorf("PGMaxIdleConns = %d, want 10", cfg.PGMaxIdleConns)
	}
	if cfg.PGConnMaxLifetime.Minutes() != 10 {
		t.Errorf("PGConnMaxLifetime = %v, want 10m", cfg.PGConnMaxLifetime)
	}
}

func TestNodeAgentConfig_PgRewindRetryDelay(t *testing.T) {
	t.Setenv("NODE_ID", "pg-replica3")
	t.Setenv("NODE_ADDR", "10.0.0.3:50052")
	t.Setenv("ORCHESTRATOR_ADDR", "10.0.0.100:50051")
	t.Setenv("PGDATA", "/data/pg")
	t.Setenv("PG_USER", "replicator")
	t.Setenv("PG_PASSWORD", "pass")
	t.Setenv("PG_REWIND_RETRY_DELAY", "10s")

	cfg, err := config.LoadNodeAgent()
	if err != nil {
		t.Fatalf("LoadNodeAgent: %v", err)
	}

	if cfg.PgRewindRetryDelay.Seconds() != 10 {
		t.Errorf("PgRewindRetryDelay = %v, want 10s", cfg.PgRewindRetryDelay)
	}
}
