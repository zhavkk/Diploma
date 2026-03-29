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

// NodeAgentCaller sends gRPC commands to node agents for promotion, replication, rewind, and restart.
type NodeAgentCaller interface {
	PromoteNode(ctx context.Context, nodeAddr string) error
	ReconfigureReplication(ctx context.Context, nodeAddr, primaryConnInfo, timeline string) error
	RunPgRewind(ctx context.Context, nodeAddr, sourceConnInfo string) error
	RestartPostgres(ctx context.Context, nodeAddr string) error
}

// GRPCNodeAgentCaller implements NodeAgentCaller using gRPC with automatic retries.
type GRPCNodeAgentCaller struct {
	dialFn   func(ctx context.Context, addr string) (net.Conn, error)
	dialOpts []grpc.DialOption
}

// NewGRPCNodeAgentCaller creates a GRPCNodeAgentCaller with optional gRPC dial options.
func NewGRPCNodeAgentCaller(dialOpts ...grpc.DialOption) *GRPCNodeAgentCaller {
	return &GRPCNodeAgentCaller{dialOpts: dialOpts}
}

// NewGRPCNodeAgentCallerWithDialer creates a GRPCNodeAgentCaller with a custom dialer, useful for testing.
func NewGRPCNodeAgentCallerWithDialer(fn func(ctx context.Context, addr string) (net.Conn, error), dialOpts ...grpc.DialOption) *GRPCNodeAgentCaller {
	return &GRPCNodeAgentCaller{dialFn: fn, dialOpts: dialOpts}
}

func (c *GRPCNodeAgentCaller) dial(addr string) (*grpc.ClientConn, error) {
	opts := make([]grpc.DialOption, 0, len(c.dialOpts)+2)
	if len(c.dialOpts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, c.dialOpts...)
	}
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

// PromoteNode sends a promote request to the node agent at the given address.
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

// ReconfigureReplication sends a replication reconfiguration request to the node agent at the given address.
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

// RunPgRewind sends a pg_rewind request to the node agent at the given address.
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

// RestartPostgres sends a PostgreSQL restart request to the node agent at the given address.
func (c *GRPCNodeAgentCaller) RestartPostgres(ctx context.Context, nodeAddr string) error {
	return withRetry(ctx, defaultMaxRetries, defaultRetryBaseWait, func() error {
		conn, err := c.dial(nodeAddr)
		if err != nil {
			return fmt.Errorf("nodeagent dial %s: %w", nodeAddr, err)
		}
		defer conn.Close()

		resp, err := nodeagentv1.NewNodeAgentServiceClient(conn).RestartPostgres(ctx, &nodeagentv1.RestartPostgresRequest{})
		if err != nil {
			return fmt.Errorf("RestartPostgres rpc %s: %w", nodeAddr, err)
		}
		if !resp.Success {
			return fmt.Errorf("RestartPostgres %s: %s", nodeAddr, resp.Message)
		}
		return nil
	})
}
