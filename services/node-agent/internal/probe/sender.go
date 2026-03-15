package probe

import (
	"context"
	"fmt"
	"net"

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
}

func NewGRPCSender(orchestratorAddr string) *GRPCSender {
	return &GRPCSender{orchestratorAddr: orchestratorAddr}
}

func NewGRPCSenderWithDialer(fn func(ctx context.Context, addr string) (net.Conn, error), orchestratorAddr string) *GRPCSender {
	return &GRPCSender{dialFn: fn, orchestratorAddr: orchestratorAddr}
}

func (s *GRPCSender) dial() (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if s.dialFn != nil {
		opts = append(opts, grpc.WithContextDialer(s.dialFn))
	}
	return grpc.NewClient(s.orchestratorAddr, opts...)
}

func (s *GRPCSender) Send(ctx context.Context, status *models.NodeStatus) error {
	conn, err := s.dial()
	if err != nil {
		return fmt.Errorf("heartbeat sender dial: %w", err)
	}
	defer conn.Close()

	_, err = orchestratorv1.NewOrchestratorServiceClient(conn).ReportHeartbeat(ctx, &orchestratorv1.ReportHeartbeatRequest{
		NodeId:          status.NodeID,
		Address:         status.Address,
		Role:            string(status.Role),
		IsInRecovery:    status.IsInRecovery,
		WalReplayLsn:    status.WALReplayLSN,
		ReplicationLag:  status.ReplicationLag,
		PostgresRunning: status.PostgresRunning,
	})
	return err
}
