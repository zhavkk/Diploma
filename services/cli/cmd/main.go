package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

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
		"Адрес gRPC API оркестратора (HTTP-порт выводится автоматически)")

	root.AddCommand(
		newCmdStatus(),
		newCmdFailover(),
		newCmdNodes(),
		newCmdReplication(),
		newCmdEvents(),
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		GetClusterStatus(ctx, &orchestratorv1.GetClusterStatusRequest{})
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
			return runFailover(orchestratorAddr, targetNode, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&targetNode, "target", "", "Node ID нового primary")
	_ = cmd.MarkFlagRequired("target")
	return cmd
}

func runFailover(addr, targetNode string, w io.Writer) error {
	conn, err := dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		TriggerFailover(ctx, &orchestratorv1.TriggerFailoverRequest{
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		ListNodes(ctx, &orchestratorv1.ListNodesRequest{})
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

type eventRow struct {
	OldPrimary string    `json:"old_primary"`
	NewPrimary string    `json:"new_primary"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
}

// deriveHTTPAddr replaces the port in a gRPC address with the default HTTP port
// (8080). This lets users specify only --addr and have the events command
// automatically reach the orchestrator HTTP API.
func deriveHTTPAddr(grpcAddr string) string {
	host, _, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		// If we cannot parse, fall back to the raw address with default HTTP port.
		return grpcAddr
	}
	return net.JoinHostPort(host, "8080")
}

func newCmdEvents() *cobra.Command {
	return &cobra.Command{
		Use:   "events",
		Short: "Показать историю событий failover",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runEvents(deriveHTTPAddr(orchestratorAddr), cmd.OutOrStdout())
		},
	}
}

func runEvents(addr string, w io.Writer) error {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + addr + "/api/v1/events")
	if err != nil {
		return fmt.Errorf("http get events: %w", err)
	}
	defer resp.Body.Close()

	var rows []eventRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return fmt.Errorf("decode events: %w", err)
	}

	fmt.Fprintf(w, "%-20s %-20s %-10s %s\n", "OLD_PRIMARY", "NEW_PRIMARY", "REASON", "OCCURRED_AT")
	for _, r := range rows {
		fmt.Fprintf(w, "%-20s %-20s %-10s %s\n", r.OldPrimary, r.NewPrimary, r.Reason, r.OccurredAt.String())
	}
	return nil
}

func runSetSync(addr, names string, w io.Writer) error {
	conn, err := dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := orchestratorv1.NewOrchestratorServiceClient(conn).
		UpdateReplicationConfig(ctx, &orchestratorv1.UpdateReplicationConfigRequest{
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
