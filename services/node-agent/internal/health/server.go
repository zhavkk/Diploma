package health

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

type Config struct {
	Addr   string
	NodeID string
}

type StatusProvider interface {
	Latest() *models.NodeStatus
}

type Server struct {
	cfg   Config
	probe StatusProvider
	log   *zap.Logger
}

func NewServer(cfg Config, p StatusProvider, log *zap.Logger) *Server {
	return &Server{cfg: cfg, probe: p, log: log}
}

func (s *Server) Run(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/primary", s.HandlePrimary)
	mux.HandleFunc("/health/replica", s.HandleReplica)
	mux.HandleFunc("/health/alive", s.HandleAlive)
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{Addr: s.cfg.Addr, Handler: mux}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	s.log.Info("health HTTP server listening", zap.String("addr", s.cfg.Addr))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.log.Error("health server error", zap.Error(err))
	}
}

func (s *Server) HandlePrimary(w http.ResponseWriter, _ *http.Request) {
	status := s.probe.Latest()
	if status == nil || !status.PostgresRunning || status.IsInRecovery {
		http.Error(w, "not primary", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("primary"))
}

func (s *Server) HandleReplica(w http.ResponseWriter, _ *http.Request) {
	status := s.probe.Latest()
	if status == nil || !status.PostgresRunning || !status.IsInRecovery {
		http.Error(w, "not replica", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("replica"))
}

func (s *Server) HandleAlive(w http.ResponseWriter, _ *http.Request) {
	status := s.probe.Latest()
	if status == nil || !status.PostgresRunning {
		http.Error(w, "postgres not running", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
