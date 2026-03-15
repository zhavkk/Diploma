package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/pgclient"
	"github.com/zhavkk/Diploma/services/node-agent/internal/controller"
	"github.com/zhavkk/Diploma/services/node-agent/internal/health"
	"github.com/zhavkk/Diploma/services/node-agent/internal/probe"
	"github.com/zhavkk/Diploma/services/node-agent/internal/watcher"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pg, err := pgclient.New(pgclient.Config{
		Host:     "localhost",
		Port:     5432,
		User:     mustEnv("PG_USER"),
		Password: mustEnv("PG_PASSWORD"),
		DBName:   "postgres",
		SSLMode:  "disable",
	})
	if err != nil {
		log.Fatal("postgres connect failed", zap.Error(err))
	}
	defer pg.Close()

	nodeID := mustEnv("NODE_ID")

	// --- DB Probe ---
	dbProbe := probe.New(probe.Config{
		NodeID:       nodeID,
		PollInterval: 5,
	}, pg, log)

	replWatcher := watcher.New(watcher.Config{
		NodeID:       nodeID,
		PollInterval: 5,
	}, pg, log)

	nodeController := controller.New(controller.Config{
		NodeID:   nodeID,
		PGData:   mustEnv("PGDATA"),
		GRPCAddr: ":50052",
	}, log)

	healthSrv := health.NewServer(health.Config{
		Addr:   ":8081",
		NodeID: nodeID,
	}, dbProbe, log)

	go dbProbe.Run(ctx, mustEnv("ORCHESTRATOR_ADDR"))
	go replWatcher.Run(ctx, mustEnv("ORCHESTRATOR_ADDR"))

	log.Info("node-agent started", zap.String("node_id", nodeID))

	go nodeController.Run(ctx)
	healthSrv.Run(ctx)

	log.Info("node-agent shutdown complete")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("required env var not set: " + key)
	}
	return v
}
