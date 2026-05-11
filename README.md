# Diploma

A high-availability PostgreSQL cluster management system written in Go. The system automates failover, replication configuration, and health monitoring for a PostgreSQL primary-replica cluster.

## Architecture

The system consists of three main binaries:

- **`services/orchestrator`** — Control plane service that manages cluster topology, performs failover, and coordinates replication configuration.
- **`services/node-agent`** — Sidecar service running on each PostgreSQL host that monitors local state and executes commands.
- **`services/cli` (`ha-ctl`)** — CLI tool for interacting with the orchestrator.

### Components

- **`internal/topology`** — In-memory registry of cluster node states (thread-safe, versioned).
- **`internal/monitor`** — Polls heartbeats and detects primary failures.
- **`internal/failover`** — Failover manager that elects new primary and handles old primary rejoin.
- **`internal/coordination`** — etcd-backed leader election and cluster state persistence.
- **`internal/replication`** — Replication configurator that updates replica connection settings.
- **`internal/api`** — gRPC and HTTP API server.

### Communication

All inter-service communication uses gRPC:

- `OrchestratorService` — API for cluster status, failover triggers, node listing.
- `NodeAgentService` — API for node operations (promote, rewind, reconfigure, restart).

## Quick Start

```bash
# Build all binaries
make build

# Start the full cluster with Docker Compose
make up

# Check cluster status
./bin/ha-ctl status

# Trigger manual failover
./bin/ha-ctl failover --target node2
```

## Configuration

Both services are configured entirely via environment variables.

### Orchestrator Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | (required) | Unique identifier for this orchestrator instance |
| `GRPC_ADDR` | `:50051` | gRPC server address |
| `HTTP_ADDR` | `:8080` | HTTP API server address |
| `HEARTBEAT_TIMEOUT` | `10` | Heartbeat timeout in seconds |
| `QUORUM_SIZE` | `1` | Required number of healthy replicas for failover |
| `ETCD_ENDPOINTS` | `etcd:2379` | Comma-separated etcd endpoints |

### Node Agent Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | (required) | Unique identifier for this node |
| `NODE_ADDR` | (required) | Network address for gRPC connections |
| `ORCHESTRATOR_ADDR` | (required) | Orchestrator gRPC address |
| `PGDATA` | (required) | PostgreSQL data directory |
| `PG_USER` | (required) | PostgreSQL superuser username |
| `PG_PASSWORD` | (required) | PostgreSQL superuser password |
| `PG_HOST` | `localhost` | PostgreSQL host address |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_SSL_MODE` | `disable` | SSL mode for PostgreSQL connections |
| `POLL_INTERVAL` | `5` | Polling interval in seconds |
| `GRPC_ADDR` | `:50052` | gRPC server address |
| `HEALTH_ADDR` | `:8081` | HTTP health endpoint address |

## Failover Process

### Automatic Failover

When the orchestrator detects that the primary has failed:

1. Check that the orchestrator is the leader (via etcd).
2. Verify that quorum is met (sufficient healthy replicas).
3. Elect the new primary (replica with highest WAL LSN).
4. Validate PostgreSQL version compatibility.
5. Promote the selected replica to primary.
6. Reconfigure all other replicas to replicate from the new primary.
7. The old primary is marked for rejoin via pg_rewind.

### Manual Failover

Using the CLI, you can trigger a controlled failover:

```bash
./bin/ha-ctl failover --target node2
```

The process follows the same steps as automatic failover but with a specified target node.

### Old Primary Rejoin

When the old primary comes back online:

1. The node-agent detects the old primary needs rejoin.
2. Orchestrator runs `pg_rewind` to synchronize the WAL history.
3. Reconfigures the old primary as a replica.
4. Restarts PostgreSQL as a standby.

## Timeline Divergence

### What is Timeline Divergence?

Timeline divergence occurs when the old primary continued accepting writes after a failover (split-brain scenario) or when WAL history is lost, creating an unreconcilable difference between the old and new primary's WAL streams.

### Detection

The system detects timeline divergence by parsing `pg_rewind` output for the error message:

```
could not find common ancestor of the source and target cluster's timelines
```

When detected, the system returns `ErrTimelineDivergence` and treats this as a fatal error.

### Behavior

When timeline divergence is detected during old primary rejoin:

1. **Automatic retry is disabled** — Timeline divergence cannot be fixed by retrying.
2. **Old primary remains marked** — The node is flagged for manual intervention.
3. **Error is logged as fatal** — Operators are notified of the condition.
4. **No standby.signal is created** — Prevents the node from starting with inconsistent data.

### Recovery Procedure

Timeline divergence requires manual intervention. Follow these steps:

1. **Stop the old primary** (if running):
   ```bash
   pg_ctl stop -D /path/to/pgdata -m fast
   ```

2. **Identify the divergence point**:
   - Check both primary's WAL archives to find the last common LSN
   - Review the error logs for timeline IDs

3. **Choose a recovery method**:

   **Option A: Rebuild from base backup (recommended)**
   ```bash
   # Remove old data
   rm -rf /path/to/pgdata/*

   # Restore base backup from the new primary
   pg_basebackup -h new-primary -p 5432 -D /path/to/pgdata -P -U replicator

   # Start as replica
   pg_ctl start -D /path/to/pgdata
   ```

   **Option B: Restore from backup before divergence**
   ```bash
   # If you have a backup taken before the divergence
   # Restore it to a temporary location
   # Use pg_rewind if the backup is after the common ancestor
   pg_rewind --target-pgdata=/path/to/pgdata --source-server="host=new-primary port=5432"
   ```

   **Option C: Advanced WAL reconciliation (requires PostgreSQL expertise)**
   - Manually reconstruct WAL history from archives
   - This is complex and error-prone; consider rebuilding instead

4. **Verify replication**:
   ```bash
   # Check the node is in recovery
   psql -h old-primary -c "SELECT pg_is_in_recovery();"

   # Check replication lag
   ./bin/ha-ctl status
   ```

5. **Clear the rejoin flag**:
   - The orchestrator will automatically detect the node is healthy again
   - No manual intervention required if replication is working

### Prevention

To prevent timeline divergence:

1. **Implement proper fencing** — Use STONITH (Shoot The Other Node In The Head) to ensure failed primaries cannot accept writes.
2. **Monitor quorum** — Ensure sufficient replicas are healthy before failover.
3. **Archive WAL continuously** — Configure `archive_mode = on` with reliable WAL archiving.
4. **Use synchronous replication** — Consider `synchronous_commit = on` for critical data.

### Testing Timeline Divergence

The system includes tests for timeline divergence detection:

```bash
go test ./services/node-agent/internal/controller/... -v -run TestExecCommander_PgRewind_ErrTimelineDivergence
```

## Testing

```bash
# Run all tests
make test

# Run tests for a specific package
go test ./services/orchestrator/internal/failover/... -v -race -count=1

# Run linting
make lint
```

## Deployment

### Docker Compose

The `deployments/docker-compose.yml` file includes:

- 1 etcd instance
- 3 PostgreSQL instances (1 primary, 2 replicas)
- 3 node-agent instances
- 1 orchestrator instance
- 1 HAProxy instance

HAProxy routes:
- Port `5000` → Write traffic to primary
- Port `5001` → Read traffic to replicas

### Ansible

Bare-metal deployment playbooks are available in `deployments/ansible/`.

## Development

### Regenerating Protobuf Code

After modifying `.proto` files:

```bash
make proto
```

### Building Docker Images

```bash
make docker-build
```

## Contributing

- Follow existing code patterns and conventions
- Write tests for new functionality
- Use `go vet` and `golangci-lint` before committing
- Ensure all tests pass with `make test`

## License

This project is part of a Diploma thesis on high-availability database systems.
