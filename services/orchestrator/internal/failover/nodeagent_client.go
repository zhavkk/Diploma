package failover

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodeagentv1 "github.com/zhavkk/Diploma/api/proto/gen/nodeagent/v1"
)

const (
	defaultMaxRetries    = 3
	defaultRetryBaseWait = 100 * time.Millisecond
)

type NodeAgentCaller interface {
	PromoteNode(ctx context.Context, nodeAddr string) error
	ReconfigureReplication(ctx context.Context, nodeAddr, primaryConnInfo, timeline string) error

	RunPgRewind(ctx context.Context, nodeAddr, sourceConnInfo string) error
}

type GRPCNodeAgentCaller struct {
	dialFn func(ctx context.Context, addr string) (net.Conn, error)
}

func NewGRPCNodeAgentCaller() *GRPCNodeAgentCaller {
	return &GRPCNodeAgentCaller{}
}

func NewGRPCNodeAgentCallerWithDialer(fn func(ctx context.Context, addr string) (net.Conn, error)) *GRPCNodeAgentCaller {
	return &GRPCNodeAgentCaller{dialFn: fn}
}

func (c *GRPCNodeAgentCaller) dial(addr string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if c.dialFn != nil {
		opts = append(opts, grpc.WithContextDialer(c.dialFn))
	}
	return grpc.NewClient(addr, opts...)
}

func withRetry(ctx context.Context, maxRetries int, base time.Duration, fn func() error) error {
	var lastErr error
	wait := base
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
				wait *= 2
			}
		}
		if lastErr = fn(); lastErr == nil {
			return nil
		}
	}
	return lastErr
}

func (c *GRPCNodeAgentCaller) PromoteNode(ctx context.Context, nodeAddr string) error {
	return withRetry(ctx, defaultMaxRetries, defaultRetryBaseWait, func() error {
		conn, err := c.dial(nodeAddr)
		if err != nil {
			return fmt.Errorf("nodeagent dial %s: %w", nodeAddr, err)
		}
		defer conn.Close()

		resp, err := nodeagentv1.NewNodeAgentServiceClient(conn).PromoteNode(ctx, &nodeagentv1.PromoteNodeRequest{})
		if err != nil {
			return fmt.Errorf("PromoteNode rpc %s: %w", nodeAddr, err)
		}
		if !resp.Success {
			return fmt.Errorf("PromoteNode %s: %s", nodeAddr, resp.Message)
		}
		return nil
	})
}

func (c *GRPCNodeAgentCaller) ReconfigureReplication(ctx context.Context, nodeAddr, primaryConnInfo, timeline string) error {
	return withRetry(ctx, defaultMaxRetries, defaultRetryBaseWait, func() error {
		conn, err := c.dial(nodeAddr)
		if err != nil {
			return fmt.Errorf("nodeagent dial %s: %w", nodeAddr, err)
		}
		defer conn.Close()

		resp, err := nodeagentv1.NewNodeAgentServiceClient(conn).ReconfigureReplication(ctx, &nodeagentv1.ReconfigureReplicationRequest{
			PrimaryConninfo:        primaryConnInfo,
			RecoveryTargetTimeline: timeline,
		})
		if err != nil {
			return fmt.Errorf("ReconfigureReplication rpc %s: %w", nodeAddr, err)
		}
		if !resp.Success {
			return fmt.Errorf("ReconfigureReplication %s: %s", nodeAddr, resp.Message)
		}
		return nil
	})
}

func (c *GRPCNodeAgentCaller) RunPgRewind(ctx context.Context, nodeAddr, sourceConnInfo string) error {
	return withRetry(ctx, defaultMaxRetries, defaultRetryBaseWait, func() error {
		conn, err := c.dial(nodeAddr)
		if err != nil {
			return fmt.Errorf("nodeagent dial %s: %w", nodeAddr, err)
		}
		defer conn.Close()

		resp, err := nodeagentv1.NewNodeAgentServiceClient(conn).RunPgRewind(ctx, &nodeagentv1.RunPgRewindRequest{
			SourceConninfo: sourceConnInfo,
		})
		if err != nil {
			return fmt.Errorf("RunPgRewind rpc %s: %w", nodeAddr, err)
		}
		if !resp.Success {
			return fmt.Errorf("RunPgRewind %s: %s", nodeAddr, resp.Message)
		}
		return nil
	})
}
