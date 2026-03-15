package api

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/zhavkk/Diploma/services/orchestrator/internal/failover"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/replication"
	"github.com/zhavkk/Diploma/services/orchestrator/internal/topology"
)

type Config struct {
	GRPCAddr string
	HTTPAddr string
}

type Server struct {
	cfg      Config
	topo     *topology.Registry
	failover *failover.Manager
	replConf *replication.Configurator
	log      *zap.Logger
	grpcSrv  *grpc.Server
}

func NewServer(
	cfg Config,
	topo *topology.Registry,
	fm *failover.Manager,
	rc *replication.Configurator,
	log *zap.Logger,
) *Server {
	return &Server{
		cfg:      cfg,
		topo:     topo,
		failover: fm,
		replConf: rc,
		log:      log,
		grpcSrv:  grpc.NewServer(),
	}
}

func (s *Server) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("api: listen %s: %w", s.cfg.GRPCAddr, err)
	}

	// TODO: зарегистрировать сгенерированный gRPC-сервер (protoc-gen-go-grpc)
	// orchestratorv1.RegisterOrchestratorServiceServer(s.grpcSrv, s)

	errCh := make(chan error, 2)

	go func() {
		s.log.Info("gRPC server listening", zap.String("addr", s.cfg.GRPCAddr))
		errCh <- s.grpcSrv.Serve(lis)
	}()

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/api/v1/status", s.handleStatus)
		srv := &http.Server{Addr: s.cfg.HTTPAddr, Handler: mux}
		s.log.Info("HTTP server listening", zap.String("addr", s.cfg.HTTPAddr))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		s.grpcSrv.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// handleStatus отдаёт текущую топологию кластера в JSON.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	topo := s.topo.Get()
	if topo == nil {
		http.Error(w, "topology not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// TODO: json.NewEncoder(w).Encode(topo)
}
