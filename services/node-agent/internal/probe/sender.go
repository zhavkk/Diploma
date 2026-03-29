package probe

import (
	"context"
	"fmt"
	"net"
	"sync"

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
}

func NewGRPCSender(orchestratorAddr string, dialOpts ...grpc.DialOption) *GRPCSender {
	return &GRPCSender{orchestratorAddr: orchestratorAddr, dialOpts: dialOpts}
}

func NewGRPCSenderWithDialer(fn func(ctx context.Context, addr string) (net.Conn, error), orchestratorAddr string, dialOpts ...grpc.DialOption) *GRPCSender {
	return &GRPCSender{dialFn: fn, orchestratorAddr: orchestratorAddr, dialOpts: dialOpts}
}

func (s *GRPCSender) dial() (*grpc.ClientConn, error) {
	opts := make([]grpc.DialOption, 0, len(s.dialOpts)+2)
	if len(s.dialOpts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, s.dialOpts...)
	}
	if s.dialFn != nil {
		opts = append(opts, grpc.WithContextDialer(s.dialFn))
	}
	return grpc.NewClient(s.orchestratorAddr, opts...)
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
	conn, err := s.getConn()
	if err != nil {
		return fmt.Errorf("heartbeat sender dial: %w", err)
	}

	_, err = orchestratorv1.NewOrchestratorServiceClient(conn).ReportHeartbeat(ctx, &orchestratorv1.ReportHeartbeatRequest{
		NodeId:          status.NodeID,
		Address:         status.Address,
		Role:            string(status.Role),
		IsInRecovery:    status.IsInRecovery,
		WalReplayLsn:    status.WALReplayLSN,
		WalReceiveLsn:   status.WALReceiveLSN,
		ReplicationLag:  status.ReplicationLag,
		PostgresRunning: status.PostgresRunning,
	})
	if err != nil {
		s.resetConn()
		return err
	}
	return nil
}
