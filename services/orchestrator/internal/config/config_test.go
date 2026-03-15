package config_test

import (
	"testing"

	"github.com/zhavkk/Diploma/services/orchestrator/internal/config"
)

func TestOrchestratorConfig_Defaults(t *testing.T) {
	t.Setenv("NODE_ID", "orchestrator-1")

	cfg, err := config.LoadOrchestrator()
	if err != nil {
		t.Fatalf("LoadOrchestrator: %v", err)
	}

	if cfg.GRPCAddr != ":50051" {
		t.Errorf("GRPCAddr = %q, want :50051", cfg.GRPCAddr)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.HeartbeatTimeout != 10 {
		t.Errorf("HeartbeatTimeout = %d, want 10", cfg.HeartbeatTimeout)
	}
	if cfg.QuorumSize != 1 {
		t.Errorf("QuorumSize = %d, want 1", cfg.QuorumSize)
	}
	if len(cfg.EtcdEndpoints) != 1 || cfg.EtcdEndpoints[0] != "etcd:2379" {
		t.Errorf("EtcdEndpoints = %v, want [etcd:2379]", cfg.EtcdEndpoints)
	}
}

func TestOrchestratorConfig_ReadsFromEnv(t *testing.T) {
	t.Setenv("NODE_ID", "orch-node-42")
	t.Setenv("GRPC_ADDR", ":9090")
	t.Setenv("HTTP_ADDR", ":9091")
	t.Setenv("HEARTBEAT_TIMEOUT", "30")
	t.Setenv("QUORUM_SIZE", "2")
	t.Setenv("ETCD_ENDPOINTS", "etcd1:2379,etcd2:2379")

	cfg, err := config.LoadOrchestrator()
	if err != nil {
		t.Fatalf("LoadOrchestrator: %v", err)
	}

	if cfg.NodeID != "orch-node-42" {
		t.Errorf("NodeID = %q, want orch-node-42", cfg.NodeID)
	}
	if cfg.GRPCAddr != ":9090" {
		t.Errorf("GRPCAddr = %q, want :9090", cfg.GRPCAddr)
	}
	if cfg.HeartbeatTimeout != 30 {
		t.Errorf("HeartbeatTimeout = %d, want 30", cfg.HeartbeatTimeout)
	}
	if cfg.QuorumSize != 2 {
		t.Errorf("QuorumSize = %d, want 2", cfg.QuorumSize)
	}
	if len(cfg.EtcdEndpoints) != 2 {
		t.Errorf("EtcdEndpoints = %v, want 2 entries", cfg.EtcdEndpoints)
	}
}

func TestOrchestratorConfig_ErrorWhenNodeIDMissing(t *testing.T) {
	t.Setenv("NODE_ID", "") // явно пусто

	_, err := config.LoadOrchestrator()
	if err == nil {
		t.Error("expected error when NODE_ID is empty")
	}
}
