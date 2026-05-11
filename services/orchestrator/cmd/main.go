package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/zhavkk/Diploma/pkg/tlsconfig"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/api"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/config"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/coordination"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/monitor"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer log.Sync()

	cfg, err := config.LoadOrchestrator()
	if err != nil {
		log.Fatal("config load failed", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Validate TLS certificates on startup
	if cfg.GRPCTLSCert != "" || cfg.GRPCTLSCACert != "" {
		result, err := tlsconfig.ValidateCertificates(cfg.GRPCTLSCert, cfg.GRPCTLSKey, cfg.GRPCTLSCACert)
		if err != nil {
			log.Fatal("TLS certificate validation failed", zap.Error(err))
		}
		tlsconfig.LogValidationResults(log, result)

		// Reject startup if any certificate is expired
		if len(result.Expired) > 0 {
			log.Fatal("cannot start: TLS certificates are expired",
				zap.Int("expired_count", len(result.Expired)),
			)
		}
	}

	serverTLSOpt, err := tlsconfig.ServerOption(cfg.GRPCTLSCert, cfg.GRPCTLSKey)
	if err != nil {
		log.Fatal("TLS server credentials", zap.Error(err))
	}

	clientTLSOpt, err := tlsconfig.ClientDialOption(cfg.GRPCTLSCACert)
	if err != nil {
		log.Fatal("TLS client credentials", zap.Error(err))
	}

	topoRegistry := topology.NewRegistry(log)

	coordModule, err := coordination.NewModule(coordination.Config{
		EtcdEndpoints: cfg.EtcdEndpoints,
		NodeID:        cfg.NodeID,
	}, log)
	if err != nil {
		log.Fatal("coordination module init", zap.Error(err))
	}
	defer coordModule.Close()

	// Seed topology with last known primary from etcd.
	if primary, err := coordModule.GetClusterState(ctx, "primary"); err != nil {
		log.Warn("failed to read last primary from etcd", zap.Error(err))
	} else if primary != "" {
		log.Info("restoring primary from etcd", zap.String("primary", primary))
		topoRegistry.SetPrimary(primary)
	}

	nodeAgentCaller := failover.NewGRPCNodeAgentCaller(clientTLSOpt)
	replConfigurator := replication.NewConfiguratorWithConfig(replication.Config{
		ReplicationPassword: cfg.ReplicationPassword,
		ReplicationUser:     cfg.ReplicationUser,
		SSLMode:             "disable",
		PGPort:              cfg.ReplicationPGPort,
		PGHosts:             cfg.ReplicationPGHosts,
	}, topoRegistry, nodeAgentCaller, log)

	failoverMgr := failover.NewManager(failover.Config{
		QuorumSize: cfg.QuorumSize,
	}, topoRegistry, coordModule, replConfigurator, nodeAgentCaller, log)

	healthMon := monitor.NewMonitor(monitor.Config{
		HeartbeatTimeout: cfg.HeartbeatTimeout,
	}, failoverMgr, topoRegistry, log)

	server := api.NewServer(api.Config{
		GRPCAddr:    cfg.GRPCAddr,
		HTTPAddr:    cfg.HTTPAddr,
		GRPCOptions: []grpc.ServerOption{serverTLSOpt},
	}, topoRegistry, failoverMgr, replConfigurator, healthMon, log)

	healthMon.WithRejoinHandler(failoverMgr)
	failoverMgr.WithEventStore(topoRegistry)

	go healthMon.Run(ctx)
	go failoverMgr.Run(ctx)
	go coordModule.Run(ctx)

	// Start periodic certificate validation monitoring
	tlsconfig.StartPeriodicValidation(ctx, log, cfg.GRPCTLSCert, cfg.GRPCTLSKey, cfg.GRPCTLSCACert)

	log.Info("orchestrator started", zap.String("node_id", cfg.NodeID))

	if err := server.Run(ctx); err != nil {
		log.Error("server stopped", zap.Error(err))
	}

	log.Info("orchestrator shutdown complete")
}
