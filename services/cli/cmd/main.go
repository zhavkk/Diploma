package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orchestratorv1 "github.com/zhavkk/Diploma/api/proto/gen/orchestrator/v1"
)

var orchestratorAddr string

var testDialFn func(ctx context.Context, addr string) (net.Conn, error)

func dial(addr string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if testDialFn != nil {
		opts = append(opts, grpc.WithContextDialer(testDialFn))
	}
	return grpc.NewClient(addr, opts...)
}

func main() {
	root := &cobra.Command{
		Use:   "ha-ctl",
		Short: "CLI для управления HA PostgreSQL кластером",
	}

	root.PersistentFlags().StringVar(&orchestratorAddr, "addr", "localhost:50051",
		"Адрес gRPC API оркестратора")

	root.AddCommand(
		newCmdStatus(),
		newCmdFailover(),
		newCmdNodes(),
		newCmdReplication(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newCmdStatus() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Показать текущий статус кластера",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStatus(orchestratorAddr, cmd.OutOrStdout())
		},
	}
}

func runStatus(addr string, w io.Writer) error {
	conn, err := dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		GetClusterStatus(context.Background(), &orchestratorv1.GetClusterStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetClusterStatus: %w", err)
	}

	fmt.Fprintf(w, "Primary:  %s\n", resp.Status.PrimaryNode)
	fmt.Fprintf(w, "Replicas: %v\n", resp.Status.ReplicaNodes)
	fmt.Fprintf(w, "Version:  %s\n", resp.Status.TopologyVersion)
	return nil
}

func newCmdFailover() *cobra.Command {
	var targetNode string
	cmd := &cobra.Command{
		Use:   "failover",
		Short: "Инициировать ручной failover на указанный узел",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if targetNode == "" {
				return fmt.Errorf("--target требует указания node-id")
			}
			return runFailover(orchestratorAddr, targetNode, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&targetNode, "target", "", "Node ID нового primary")
	return cmd
}

func runFailover(addr, targetNode string, w io.Writer) error {
	conn, err := dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		TriggerFailover(context.Background(), &orchestratorv1.TriggerFailoverRequest{
			TargetNode: targetNode,
		})
	if err != nil {
		return fmt.Errorf("TriggerFailover: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("failover rejected: %s", resp.Message)
	}
	fmt.Fprintf(w, "Failover to %q: %s\n", targetNode, resp.Message)
	return nil
}

func newCmdNodes() *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "Список узлов кластера с их статусом",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runNodes(orchestratorAddr, cmd.OutOrStdout())
		},
	}
}

func runNodes(addr string, w io.Writer) error {
	conn, err := dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		ListNodes(context.Background(), &orchestratorv1.ListNodesRequest{})
	if err != nil {
		return fmt.Errorf("ListNodes: %w", err)
	}

	fmt.Fprintf(w, "%-20s %-10s %-10s %s\n", "NODE", "ROLE", "HEALTHY", "WAL_LAG")
	for _, n := range resp.Nodes {
		healthy := "yes"
		if !n.Healthy {
			healthy = "no"
		}
		fmt.Fprintf(w, "%-20s %-10s %-10s %d\n", n.NodeId, n.Role, healthy, n.WalLag)
	}
	return nil
}

func newCmdReplication() *cobra.Command {
	repl := &cobra.Command{
		Use:   "replication",
		Short: "Управление параметрами репликации",
	}

	var names string
	setSync := &cobra.Command{
		Use:   "set-sync",
		Short: "Задать synchronous_standby_names",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSetSync(orchestratorAddr, names, cmd.OutOrStdout())
		},
	}
	setSync.Flags().StringVar(&names, "names", "", "Имена синхронных реплик")
	repl.AddCommand(setSync)

	return repl
}

func runSetSync(addr, names string, w io.Writer) error {
	conn, err := dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		UpdateReplicationConfig(context.Background(), &orchestratorv1.UpdateReplicationConfigRequest{
			SynchronousStandbyNames: names,
			EnableSyncReplication:   names != "",
		})
	if err != nil {
		return fmt.Errorf("UpdateReplicationConfig: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("update rejected: %s", resp.Message)
	}
	fmt.Fprintf(w, "synchronous_standby_names=%q: %s\n", names, resp.Message)
	return nil
}
