package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/pgclient"
	"github.com/zhavkk/Diploma/services/node-agent/internal/config"
	"github.com/zhavkk/Diploma/services/node-agent/internal/controller"
	"github.com/zhavkk/Diploma/services/node-agent/internal/health"
	"github.com/zhavkk/Diploma/services/node-agent/internal/probe"
	"github.com/zhavkk/Diploma/services/node-agent/internal/watcher"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg, err := config.LoadNodeAgent()
	if err != nil {
		log.Fatal("config load failed", zap.Error(err))
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pg, err := pgclient.New(pgclient.Config{
		Host:     cfg.PGHost,
		Port:     cfg.PGPort,
		User:     cfg.PGUser,
		Password: cfg.PGPassword,
		DBName:   "postgres",
		SSLMode:  cfg.PGSSLMode,
	})
	if err != nil {
		log.Fatal("postgres connect failed", zap.Error(err))
	}
	defer pg.Close()

	dbProbe := probe.New(probe.Config{
		NodeID:       cfg.NodeID,
		NodeAddr:     cfg.NodeAddr,
		PollInterval: cfg.PollInterval,
	}, pg, log)
	dbProbe.WithSender(probe.NewGRPCSender(cfg.OrchestratorAddr))

	replWatcher := watcher.New(watcher.Config{
		NodeID:       cfg.NodeID,
		NodeAddr:     cfg.NodeAddr,
		PollInterval: cfg.PollInterval,
	}, pg, log)

	nodeController := controller.New(controller.Config{
		NodeID:   cfg.NodeID,
		PGData:   cfg.PGData,
		GRPCAddr: cfg.GRPCAddr,
	}, controller.NewExecCommander(cfg.PGData), dbProbe, log)

	healthSrv := health.NewServer(health.Config{
		Addr:   cfg.HealthAddr,
		NodeID: cfg.NodeID,
	}, dbProbe, log)

	go dbProbe.Run(ctx)
	go replWatcher.Run(ctx)

	log.Info("node-agent started", zap.String("node_id", cfg.NodeID))

	go nodeController.Run(ctx)
	healthSrv.Run(ctx)

	log.Info("node-agent shutdown complete")
}
