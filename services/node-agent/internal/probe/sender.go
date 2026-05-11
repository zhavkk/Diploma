package probe

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orchestratorv1 "github.com/zhavkk/Diploma/api/proto/gen/orchestrator/v1"
	"github.com/zhavkk/Diploma/pkg/models"
)

type HeartbeatSender interface {
	Send(ctx context.Context, status *models.NodeStatus) error
}

type GRPCSender struct {
	orchestratorAddr string
	dialFn           func(ctx context.Context, addr string) (net.Conn, error)
	dialOpts         []grpc.DialOption
	mu               sync.Mutex
	conn             *grpc.ClientConn
	log              *zap.Logger
}

func NewGRPCSender(orchestratorAddr string, dialOpts ...grpc.DialOption) *GRPCSender {
	return &GRPCSender{orchestratorAddr: orchestratorAddr, dialOpts: dialOpts, log: zap.NewNop()}
}

func NewGRPCSenderWithDialer(fn func(ctx context.Context, addr string) (net.Conn, error), orchestratorAddr string, dialOpts ...grpc.DialOption) *GRPCSender {
	return &GRPCSender{dialFn: fn, orchestratorAddr: orchestratorAddr, dialOpts: dialOpts, log: zap.NewNop()}
}

// WithLogger sets the logger for the GRPC sender
func (s *GRPCSender) WithLogger(log *zap.Logger) *GRPCSender {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = log
	return s
}

func (s *GRPCSender) dial() (*grpc.ClientConn, error) {
	s.log.Info("dialing orchestrator", zap.String("addr", s.orchestratorAddr))
	opts := make([]grpc.DialOption, 0, len(s.dialOpts)+2)
	if len(s.dialOpts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, s.dialOpts...)
	}
	if s.dialFn != nil {
		opts = append(opts, grpc.WithContextDialer(s.dialFn))
	}
	conn, err := grpc.NewClient(s.orchestratorAddr, opts...)
	if err != nil {
		s.log.Error("failed to dial orchestrator", zap.Error(err))
		return nil, err
	}
	s.log.Info("successfully connected to orchestrator")
	return conn, nil
}

func (s *GRPCSender) getConn() (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.conn, nil
	}
	conn, err := s.dial()
	if err != nil {
		return nil, err
	}
	s.conn = conn
	return s.conn, nil
}

func (s *GRPCSender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *GRPCSender) resetConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

func (s *GRPCSender) Send(ctx context.Context, status *models.NodeStatus) error {
	s.log.Debug("preparing to send heartbeat",
		zap.String("node", status.NodeID),
		zap.String("role", string(status.Role)),
		zap.Bool("postgres_running", status.PostgresRunning))

	conn, err := s.getConn()
	if err != nil {
		s.log.Error("failed to get orchestrator connection", zap.Error(err))
		return fmt.Errorf("heartbeat sender dial: %w", err)
	}

	client := orchestratorv1.NewOrchestratorServiceClient(conn)
	req := &orchestratorv1.ReportHeartbeatRequest{
		NodeId:          status.NodeID,
		Address:         status.Address,
		Role:            string(status.Role),
		IsInRecovery:    status.IsInRecovery,
		WalReplayLsn:    status.WALReplayLSN,
		WalReceiveLsn:   status.WALReceiveLSN,
		ReplicationLag:  status.ReplicationLag,
		PostgresRunning: status.PostgresRunning,
		PgVersion:       status.PGVersion,
	}

	s.log.Info("sending heartbeat to orchestrator",
		zap.String("node", status.NodeID),
		zap.String("orchestrator_addr", s.orchestratorAddr))

	resp, err := client.ReportHeartbeat(ctx, req)
	if err != nil {
		s.log.Error("heartbeat RPC call failed", zap.Error(err))
		s.resetConn()
		return err
	}

	if resp != nil && resp.Ok {
		s.log.Debug("heartbeat accepted by orchestrator")
	}

	return nil
}
