package models

import "time"

// NodeRole represents the role of a PostgreSQL node in the cluster.
type NodeRole string

// Possible NodeRole values.
const (
	RolePrimary NodeRole = "primary"
	RoleReplica NodeRole = "replica"
	RoleUnknown NodeRole = "unknown"
)

// NodeState represents the health state of a cluster node.
type NodeState string

// Possible NodeState values.
const (
	StateHealthy     NodeState = "healthy"
	StateDegraded    NodeState = "degraded"
	StateUnreachable NodeState = "unreachable"
)

// ReplicationStats holds replication statistics reported by the watcher.
// For a primary node this contains downstream replica info from pg_stat_replication.
// For a replica node it may be nil.
type ReplicationStats struct {
	State    string `json:"state"`     // e.g. "streaming", "catchup"
	WALLSN   string `json:"wal_lsn"`   // current WAL LSN as string
	LagBytes int64  `json:"lag_bytes"` // replication lag in bytes
}

// NodeStatus describes the current state of a single PostgreSQL node in the cluster.
type NodeStatus struct {
	NodeID           string            `json:"node_id"`
	Address          string            `json:"address"`
	Role             NodeRole          `json:"role"`
	State            NodeState         `json:"state"`
	IsInRecovery     bool              `json:"is_in_recovery"`
	WALReceiveLSN    int64             `json:"wal_receive_lsn"`
	WALReplayLSN     int64             `json:"wal_replay_lsn"`
	ReplicationLag   int64             `json:"replication_lag"`
	PGVersion        string            `json:"pg_version"`
	PostgresRunning  bool              `json:"postgres_running"`
	LastHeartbeat    time.Time         `json:"last_heartbeat"`
	ReplicationStats *ReplicationStats `json:"replication_stats,omitempty"`
}

// ClusterTopology represents the full state of the PostgreSQL cluster including all nodes.
type ClusterTopology struct {
	Version     string       `json:"version"`
	PrimaryNode string       `json:"primary_node"`
	Nodes       []NodeStatus `json:"nodes"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

// FailoverEvent records a primary change that occurred in the cluster.
type FailoverEvent struct {
	OldPrimary string    `json:"old_primary"`
	NewPrimary string    `json:"new_primary"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
}

// ReplicationConfig holds the replication parameters to be applied to replica nodes.
type ReplicationConfig struct {
	PrimaryConnInfo         string `json:"primary_conn_info"`
	SynchronousStandbyNames string `json:"synchronous_standby_names"`
	EnableSyncReplication   bool   `json:"enable_sync_replication"`
}
