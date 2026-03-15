package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var orchestratorAddr string

func main() {
	root := &cobra.Command{
		Use:   "ha-ctl",
		Short: "CLI для управления HA PostgreSQL кластером",
	}

	root.PersistentFlags().StringVar(&orchestratorAddr, "addr", "localhost:50051",
		"Адрес gRPC API оркестратора")

	root.AddCommand(
		cmdStatus(),
		cmdFailover(),
		cmdNodes(),
		cmdReplication(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func cmdStatus() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Показать текущий статус кластера",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: gRPC вызов OrchestratorService.GetClusterStatus
			fmt.Printf("Connecting to orchestrator at %s...\n", orchestratorAddr)
			fmt.Println("Cluster status: OK (not yet implemented)")
			return nil
		},
	}
}

func cmdFailover() *cobra.Command {
	var targetNode string
	cmd := &cobra.Command{
		Use:   "failover",
		Short: "Инициировать ручной failover на указанный узел",
		RunE: func(cmd *cobra.Command, args []string) error {
			if targetNode == "" {
				return fmt.Errorf("--target требует указания node-id")
			}
			// TODO: gRPC вызов OrchestratorService.TriggerFailover
			fmt.Printf("Triggering failover to node %q via %s...\n", targetNode, orchestratorAddr)
			return nil
		},
	}
	cmd.Flags().StringVar(&targetNode, "target", "", "Node ID нового primary")
	return cmd
}

func cmdNodes() *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "Список узлов кластера с их статусом",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: gRPC вызов OrchestratorService.ListNodes
			fmt.Printf("Listing nodes from %s...\n", orchestratorAddr)
			return nil
		},
	}
}

func cmdReplication() *cobra.Command {
	repl := &cobra.Command{
		Use:   "replication",
		Short: "Управление параметрами репликации",
	}

	var names string
	setSync := &cobra.Command{
		Use:   "set-sync",
		Short: "Задать synchronous_standby_names",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: gRPC вызов OrchestratorService.UpdateReplicationConfig
			fmt.Printf("Setting synchronous_standby_names=%q via %s...\n", names, orchestratorAddr)
			return nil
		},
	}
	setSync.Flags().StringVar(&names, "names", "", "Имена синхронных реплик")
	repl.AddCommand(setSync)

	return repl
}
