package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/services/orchestrator/internal/api"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/coordination"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/monitor"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	topoRegistry := topology.NewRegistry(log)

	coordModule, err := coordination.NewModule(coordination.Config{
		EtcdEndpoints: []string{"etcd:2379"},
		NodeID:        mustEnv("NODE_ID"),
	}, log)
	if err != nil {
		log.Fatal("coordination module init", zap.Error(err))
	}
	defer coordModule.Close()

	replConfigurator := replication.NewConfigurator(log)

	failoverMgr := failover.NewManager(failover.Config{
		QuorumSize: 2,
	}, topoRegistry, coordModule, replConfigurator, log)

	healthMon := monitor.NewMonitor(monitor.Config{
		HeartbeatTimeout: 10,
	}, failoverMgr, topoRegistry, log)

	server := api.NewServer(api.Config{
		GRPCAddr: ":50051",
		HTTPAddr: ":8080",
	}, topoRegistry, failoverMgr, replConfigurator, log)

	go healthMon.Run(ctx)
	go failoverMgr.Run(ctx)
	go coordModule.Run(ctx)

	log.Info("orchestrator started")

	if err := server.Run(ctx); err != nil {
		log.Error("server stopped", zap.Error(err))
	}

	log.Info("orchestrator shutdown complete")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("required env var not set: " + key)
	}
	return v
}
