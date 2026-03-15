package controller

import (
	"context"
	"fmt"
	"net"
	"os/exec"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Config struct {
	NodeID   string
	PGData   string // путь к PGDATA
	GRPCAddr string // адрес gRPC сервера (для получения команд от оркестратора)
}

// Controller — выполняет команды управления PostgreSQL-узлом по указанию Failover Manager.
// Реализует gRPC NodeAgentService (promote, pg_rewind, reconfigure, restart).
type Controller struct {
	cfg     Config
	log     *zap.Logger
	grpcSrv *grpc.Server
}

func New(cfg Config, log *zap.Logger) *Controller {
	return &Controller{
		cfg:     cfg,
		log:     log,
		grpcSrv: grpc.NewServer(),
	}
}

// Run запускает gRPC сервер для получения команд от оркестратора.
func (c *Controller) Run(ctx context.Context) {
	lis, err := net.Listen("tcp", c.cfg.GRPCAddr)
	if err != nil {
		c.log.Fatal("node controller listen failed", zap.Error(err))
	}

	// TODO: зарегистрировать nodeagentv1.RegisterNodeAgentServiceServer(c.grpcSrv, c)

	go func() {
		c.log.Info("node controller gRPC listening", zap.String("addr", c.cfg.GRPCAddr))
		if err := c.grpcSrv.Serve(lis); err != nil {
			c.log.Error("gRPC serve error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	c.grpcSrv.GracefulStop()
}

// Promote выполняет pg_ctl promote для перевода реплики в primary.
func (c *Controller) Promote(ctx context.Context) error {
	c.log.Info("executing pg_ctl promote", zap.String("pgdata", c.cfg.PGData))
	cmd := exec.CommandContext(ctx, "pg_ctl", "promote", "-D", c.cfg.PGData)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("promote: %w, output: %s", err, out)
	}
	c.log.Info("promote successful")
	return nil
}

// PgRewind ресинхронизирует бывший primary с новым через pg_rewind.
// Вызывается после failover, когда старый primary возвращается в кластер.
func (c *Controller) PgRewind(ctx context.Context, sourceConnInfo string) error {
	c.log.Info("executing pg_rewind",
		zap.String("pgdata", c.cfg.PGData),
		zap.String("source_conninfo", sourceConnInfo),
	)
	cmd := exec.CommandContext(ctx,
		"pg_rewind",
		"--target-pgdata="+c.cfg.PGData,
		"--source-server="+sourceConnInfo,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pg_rewind: %w, output: %s", err, out)
	}
	c.log.Info("pg_rewind successful")
	return nil
}

// Restart перезапускает PostgreSQL.
func (c *Controller) Restart(ctx context.Context) error {
	c.log.Info("restarting postgres", zap.String("pgdata", c.cfg.PGData))
	cmd := exec.CommandContext(ctx, "pg_ctl", "restart", "-D", c.cfg.PGData, "-w")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("restart: %w, output: %s", err, out)
	}
	return nil
}

// ReconfigureReplication обновляет primary_conninfo и перезапускает репликацию.
func (c *Controller) ReconfigureReplication(ctx context.Context, primaryConnInfo, timeline string) error {
	c.log.Info("reconfiguring replication",
		zap.String("primary_conninfo", primaryConnInfo),
		zap.String("timeline", timeline),
	)
	// TODO: перезаписать postgresql.auto.conf / recovery.conf с новым primary_conninfo
	// и вызвать pg_ctl reload или SELECT pg_reload_conf()
	return nil
}
