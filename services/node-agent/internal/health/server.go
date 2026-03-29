package health

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
)

// Config holds the HTTP listen address and node identity for the health server.
type Config struct {
	Addr   string
	NodeID string
}

// StatusProvider returns the latest known status of the local PostgreSQL instance.
type StatusProvider interface {
	Latest() *models.NodeStatus
}

// Server exposes HTTP health endpoints used by HAProxy for backend selection.
type Server struct {
	cfg   Config
	probe StatusProvider
	log   *zap.Logger
}

// NewServer creates a new health HTTP server.
func NewServer(cfg Config, p StatusProvider, log *zap.Logger) *Server {
	return &Server{cfg: cfg, probe: p, log: log}
}

// Run starts the HTTP server and blocks until the context is cancelled.
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

// HandlePrimary returns 200 if the local PostgreSQL is running as primary, 503 otherwise.
func (s *Server) HandlePrimary(w http.ResponseWriter, _ *http.Request) {
	status := s.probe.Latest()
	if status == nil || !status.PostgresRunning || status.IsInRecovery {
		http.Error(w, "not primary", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("primary"))
}

// HandleReplica returns 200 if the local PostgreSQL is running as a replica, 503 otherwise.
func (s *Server) HandleReplica(w http.ResponseWriter, _ *http.Request) {
	status := s.probe.Latest()
	if status == nil || !status.PostgresRunning || !status.IsInRecovery {
		http.Error(w, "not replica", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("replica"))
}

// HandleAlive returns 200 if PostgreSQL is running regardless of role, 503 otherwise.
func (s *Server) HandleAlive(w http.ResponseWriter, _ *http.Request) {
	status := s.probe.Latest()
	if status == nil || !status.PostgresRunning {
		http.Error(w, "postgres not running", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
