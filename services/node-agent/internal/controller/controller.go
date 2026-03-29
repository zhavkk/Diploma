package controller

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	nodeagentv1 "github.com/zhavkk/Diploma/api/proto/gen/nodeagent/v1"
	"github.com/zhavkk/Diploma/pkg/models"
)

type NodeStatusProvider interface {
	Latest() *models.NodeStatus
}

type Config struct {
	NodeID   string
	PGData   string
	GRPCAddr string
}

type Controller struct {
	nodeagentv1.UnimplementedNodeAgentServiceServer

	cfg        Config
	log        *zap.Logger
	grpcSrv    *grpc.Server
	commander  PGCommander
	statusProv NodeStatusProvider
}

func New(cfg Config, commander PGCommander, statusProv NodeStatusProvider, log *zap.Logger) *Controller {
	return &Controller{
		cfg:        cfg,
		log:        log,
		grpcSrv:    grpc.NewServer(),
		commander:  commander,
		statusProv: statusProv,
	}
}

func (c *Controller) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", c.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("node controller listen: %w", err)
	}

	nodeagentv1.RegisterNodeAgentServiceServer(c.grpcSrv, c)

	go func() {
		c.log.Info("node controller gRPC listening", zap.String("addr", c.cfg.GRPCAddr))
		if err := c.grpcSrv.Serve(lis); err != nil {
			c.log.Error("gRPC serve error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	c.grpcSrv.GracefulStop()
	return nil
}

func (c *Controller) GetNodeStatus(_ context.Context, _ *nodeagentv1.GetNodeStatusRequest) (*nodeagentv1.GetNodeStatusResponse, error) {
	proto := &nodeagentv1.NodeStatus{NodeId: c.cfg.NodeID}
	if s := c.statusProv.Latest(); s != nil {
		proto.Role = string(s.Role)
		proto.IsInRecovery = s.IsInRecovery
		proto.WalReceiveLsn = s.WALReceiveLSN
		proto.WalReplayLsn = s.WALReplayLSN
		proto.ReplicationLag = s.ReplicationLag
		proto.PgVersion = s.PGVersion
		proto.PostgresRunning = s.PostgresRunning
	}
	return &nodeagentv1.GetNodeStatusResponse{Status: proto}, nil
}

func (c *Controller) PromoteNode(ctx context.Context, _ *nodeagentv1.PromoteNodeRequest) (*nodeagentv1.PromoteNodeResponse, error) {
	c.log.Info("promote requested", zap.String("node", c.cfg.NodeID))
	if err := c.commander.Promote(ctx); err != nil {
		c.log.Error("promote failed", zap.Error(err))
		return &nodeagentv1.PromoteNodeResponse{Success: false, Message: err.Error()}, nil
	}
	c.log.Info("promote successful")
	return &nodeagentv1.PromoteNodeResponse{Success: true, Message: "promoted"}, nil
}

func (c *Controller) ReconfigureReplication(ctx context.Context, req *nodeagentv1.ReconfigureReplicationRequest) (*nodeagentv1.ReconfigureReplicationResponse, error) {
	c.log.Info("reconfigure replication requested",
		zap.String("primary_conninfo", req.PrimaryConninfo),
		zap.String("timeline", req.RecoveryTargetTimeline),
	)
	if err := c.commander.Reconfigure(ctx, req.PrimaryConninfo, req.RecoveryTargetTimeline); err != nil {
		c.log.Error("reconfigure failed", zap.Error(err))
		return &nodeagentv1.ReconfigureReplicationResponse{Success: false, Message: err.Error()}, nil
	}
	return &nodeagentv1.ReconfigureReplicationResponse{Success: true, Message: "reconfigured"}, nil
}

func (c *Controller) RunPgRewind(ctx context.Context, req *nodeagentv1.RunPgRewindRequest) (*nodeagentv1.RunPgRewindResponse, error) {
	c.log.Info("pg_rewind requested", zap.String("source_conninfo", req.SourceConninfo))
	if err := c.commander.PgRewind(ctx, req.SourceConninfo); err != nil {
		c.log.Error("pg_rewind failed", zap.Error(err))
		return &nodeagentv1.RunPgRewindResponse{Success: false, Message: err.Error()}, nil
	}
	c.log.Info("pg_rewind successful")
	return &nodeagentv1.RunPgRewindResponse{Success: true, Message: "pg_rewind completed"}, nil
}

func (c *Controller) RestartPostgres(ctx context.Context, _ *nodeagentv1.RestartPostgresRequest) (*nodeagentv1.RestartPostgresResponse, error) {
	c.log.Info("restart postgres requested", zap.String("node", c.cfg.NodeID))
	if err := c.commander.Restart(ctx); err != nil {
		c.log.Error("restart failed", zap.Error(err))
		return &nodeagentv1.RestartPostgresResponse{Success: false, Message: err.Error()}, nil
	}
	return &nodeagentv1.RestartPostgresResponse{Success: true, Message: "restarted"}, nil
}
