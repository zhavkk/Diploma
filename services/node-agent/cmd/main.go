package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/zhavkk/Diploma/pkg/pgclient"
	"github.com/zhavkk/Diploma/pkg/tlsconfig"
	"github.com/zhavkk/Diploma/services/node-agent/internal/config"
	"github.com/zhavkk/Diploma/services/node-agent/internal/controller"
	"github.com/zhavkk/Diploma/services/node-agent/internal/health"
	"github.com/zhavkk/Diploma/services/node-agent/internal/probe"
	"github.com/zhavkk/Diploma/services/node-agent/internal/watcher"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer log.Sync()

	cfg, err := config.LoadNodeAgent()
	if err != nil {
		log.Fatal("config load failed", zap.Error(err))
	}

	serverTLSOpt, err := tlsconfig.ServerOption(cfg.GRPCTLSCert, cfg.GRPCTLSKey)
	if err != nil {
		log.Fatal("TLS server credentials", zap.Error(err))
	}

	clientTLSOpt, err := tlsconfig.ClientDialOption(cfg.GRPCTLSCACert)
	if err != nil {
		log.Fatal("TLS client credentials", zap.Error(err))
	}

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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pg, err := pgclient.New(ctx, pgclient.Config{
		Host:            cfg.PGHost,
		Port:            cfg.PGPort,
		User:            cfg.PGUser,
		Password:        cfg.PGPassword,
		DBName:          "postgres",
		SSLMode:         cfg.PGSSLMode,
		MaxOpenConns:    cfg.PGMaxOpenConns,
		MaxIdleConns:    cfg.PGMaxIdleConns,
		ConnMaxLifetime: cfg.PGConnMaxLifetime,
	}, log)
	if err != nil {
		log.Fatal("postgres connect failed", zap.Error(err))
	}
	defer pg.Close()

	sender := probe.NewGRPCSender(cfg.OrchestratorAddr, clientTLSOpt).WithLogger(log)
	defer sender.Close()

	replWatcher := watcher.New(watcher.Config{
		NodeID:       cfg.NodeID,
		NodeAddr:     cfg.NodeAddr,
		PollInterval: cfg.PollInterval,
	}, pg, log)

	dbProbe := probe.New(probe.Config{
		NodeID:       cfg.NodeID,
		NodeAddr:     cfg.NodeAddr,
		PollInterval: cfg.PollInterval,
	}, pg, log)
	dbProbe.WithSender(sender)
	dbProbe.WithWatcher(&watcherAdapter{w: replWatcher})

	nodeController := controller.New(controller.Config{
		NodeID:             cfg.NodeID,
		PGData:             cfg.PGData,
		GRPCAddr:           cfg.GRPCAddr,
		GRPCOptions:        []grpc.ServerOption{serverTLSOpt},
		PgRewindRetryDelay: cfg.PgRewindRetryDelay,
	}, controller.NewExecCommanderWithRetryDelay(cfg.PGData, cfg.PgRewindRetryDelay), dbProbe, log)

	healthSrv := health.NewServer(health.Config{
		Addr:   cfg.HealthAddr,
		NodeID: cfg.NodeID,
	}, dbProbe, log)

	// Start periodic certificate validation monitoring
	tlsconfig.StartPeriodicValidation(ctx, log, cfg.GRPCTLSCert, cfg.GRPCTLSKey, cfg.GRPCTLSCACert)

	log.Info("node-agent started", zap.String("node_id", cfg.NodeID))
	log.Info("preparing to start goroutines")

	g, gCtx := errgroup.WithContext(ctx)

	log.Info("creating probe goroutine")
	g.Go(func() error {
		log.Info("starting probe loop")
		defer log.Info("probe loop exited")
		dbProbe.Run(gCtx)
		return nil
	})

	log.Info("creating watcher goroutine")
	g.Go(func() error {
		log.Info("starting replication watcher")
		defer log.Info("replication watcher exited")
		replWatcher.Run(gCtx)
		return nil
	})

	log.Info("creating controller goroutine")
	g.Go(func() error {
		log.Info("starting node controller")
		defer log.Info("node controller exited")
		return nodeController.Run(gCtx)
	})

	log.Info("creating health server goroutine")
	g.Go(func() error {
		log.Info("starting health server")
		defer log.Info("health server exited")
		healthSrv.Run(gCtx)
		return nil
	})

	log.Info("all goroutines created, waiting for context")

	if err := g.Wait(); err != nil {
		log.Error("node-agent component failed", zap.Error(err))
	}

	log.Info("node-agent shutdown complete")
}

// watcherAdapter adapts the watcher's []pgclient.ReplicationStat to the
// probe.ReplicationWatcher interface, which uses probe.ReplicationStat
// to avoid a direct dependency on pgclient in the probe package.
type watcherAdapter struct {
	w *watcher.Watcher
}

func (a *watcherAdapter) Latest() []probe.ReplicationStat {
	pgStats := a.w.Latest()
	if pgStats == nil {
		return nil
	}
	out := make([]probe.ReplicationStat, len(pgStats))
	for i, s := range pgStats {
		out[i] = probe.ReplicationStat{
			ApplicationName: s.ApplicationName,
			ClientAddr:      s.ClientAddr,
			State:           s.State,
			WriteLag:        s.WriteLag,
			FlushLag:        s.FlushLag,
			ReplayLag:       s.ReplayLag,
		}
	}
	return out
}
