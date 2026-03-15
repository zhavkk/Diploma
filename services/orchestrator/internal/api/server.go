package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	orchestratorv1 "github.com/zhavkk/Diploma/api/proto/gen/orchestrator/v1"
	"github.com/zhavkk/Diploma/pkg/models"
)

type TopologySource interface {
	Get() *models.ClusterTopology
	Primary() string
	Events() []models.FailoverEvent
}

type FailoverTrigger interface {
	TriggerManualFailover(ctx context.Context, targetNodeID string) error
	IsFailoverInProgress() bool
}

type ReplicationApplier interface {
	Apply(ctx context.Context, cfg models.ReplicationConfig, targetNodes []string) error
}

type HeartbeatReceiver interface {
	ReceiveHeartbeat(status *models.NodeStatus)
}

type Config struct {
	GRPCAddr string
	HTTPAddr string
}

type Server struct {
	orchestratorv1.UnimplementedOrchestratorServiceServer

	cfg       Config
	topo      TopologySource
	failover  FailoverTrigger
	replConf  ReplicationApplier
	heartbeat HeartbeatReceiver
	log       *zap.Logger
}

func NewServer(cfg Config, topo TopologySource, fm FailoverTrigger, rc ReplicationApplier, hb HeartbeatReceiver, log *zap.Logger) *Server {
	return &Server{cfg: cfg, topo: topo, failover: fm, replConf: rc, heartbeat: hb, log: log}
}

func (s *Server) Run(ctx context.Context) error {
	grpcLis, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("api: grpc listen %s: %w", s.cfg.GRPCAddr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/events", s.handleEvents)
	httpSrv := &http.Server{Addr: s.cfg.HTTPAddr, Handler: mux}

	grpcSrv := grpc.NewServer()
	orchestratorv1.RegisterOrchestratorServiceServer(grpcSrv, s)

	errCh := make(chan error, 2)

	go func() {
		s.log.Info("gRPC server listening", zap.String("addr", s.cfg.GRPCAddr))
		errCh <- grpcSrv.Serve(grpcLis)
	}()

	go func() {
		s.log.Info("HTTP server listening", zap.String("addr", s.cfg.HTTPAddr))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():

		grpcSrv.GracefulStop()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutCtx); err != nil {
			s.log.Warn("HTTP server shutdown error", zap.Error(err))
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Server) GetClusterStatus(_ context.Context, _ *orchestratorv1.GetClusterStatusRequest) (*orchestratorv1.GetClusterStatusResponse, error) {
	status := &orchestratorv1.ClusterStatus{
		FailoverInProgress: s.failover.IsFailoverInProgress(),
	}
	if topo := s.topo.Get(); topo != nil {
		status.PrimaryNode = topo.PrimaryNode
		status.TopologyVersion = topo.Version
		for _, n := range topo.Nodes {
			if n.Role == models.RoleReplica {
				status.ReplicaNodes = append(status.ReplicaNodes, n.NodeID)
			}
		}
	}
	return &orchestratorv1.GetClusterStatusResponse{Status: status}, nil
}

func (s *Server) TriggerFailover(ctx context.Context, req *orchestratorv1.TriggerFailoverRequest) (*orchestratorv1.TriggerFailoverResponse, error) {
	s.log.Info("manual failover via API", zap.String("target", req.TargetNode))
	if err := s.failover.TriggerManualFailover(ctx, req.TargetNode); err != nil {
		return &orchestratorv1.TriggerFailoverResponse{Success: false, Message: err.Error()}, nil
	}
	return &orchestratorv1.TriggerFailoverResponse{Success: true, Message: "failover initiated"}, nil
}

func (s *Server) ListNodes(_ context.Context, _ *orchestratorv1.ListNodesRequest) (*orchestratorv1.ListNodesResponse, error) {
	topo := s.topo.Get()
	if topo == nil {
		return &orchestratorv1.ListNodesResponse{}, nil
	}
	nodes := make([]*orchestratorv1.NodeInfo, 0, len(topo.Nodes))
	for _, n := range topo.Nodes {
		nodes = append(nodes, &orchestratorv1.NodeInfo{
			NodeId:  n.NodeID,
			Address: n.Address,
			Role:    string(n.Role),
			Healthy: n.State == models.StateHealthy,
			WalLag:  n.ReplicationLag,
		})
	}
	return &orchestratorv1.ListNodesResponse{Nodes: nodes}, nil
}

func (s *Server) UpdateReplicationConfig(ctx context.Context, req *orchestratorv1.UpdateReplicationConfigRequest) (*orchestratorv1.UpdateReplicationConfigResponse, error) {
	topo := s.topo.Get()
	var targetNodes []string
	if topo != nil {
		for _, n := range topo.Nodes {
			targetNodes = append(targetNodes, n.NodeID)
		}
	}
	cfg := models.ReplicationConfig{
		SynchronousStandbyNames: req.SynchronousStandbyNames,
		EnableSyncReplication:   req.EnableSyncReplication,
	}
	if err := s.replConf.Apply(ctx, cfg, targetNodes); err != nil {
		return &orchestratorv1.UpdateReplicationConfigResponse{Success: false, Message: err.Error()}, nil
	}
	return &orchestratorv1.UpdateReplicationConfigResponse{Success: true, Message: "applied"}, nil
}

func (s *Server) ReportHeartbeat(_ context.Context, req *orchestratorv1.ReportHeartbeatRequest) (*orchestratorv1.ReportHeartbeatResponse, error) {
	status := &models.NodeStatus{
		NodeID:          req.NodeId,
		Address:         req.Address,
		Role:            models.NodeRole(req.Role),
		IsInRecovery:    req.IsInRecovery,
		WALReplayLSN:    req.WalReplayLsn,
		ReplicationLag:  req.ReplicationLag,
		PostgresRunning: req.PostgresRunning,
		State:           models.StateHealthy,
	}
	s.heartbeat.ReceiveHeartbeat(status)
	return &orchestratorv1.ReportHeartbeatResponse{Ok: true}, nil
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	topo := s.topo.Get()
	if topo == nil {
		http.Error(w, "topology not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topo)
}

func (s *Server) handleEvents(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.topo.Events())
}
