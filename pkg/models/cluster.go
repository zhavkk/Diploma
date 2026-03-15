package models

import "time"

type NodeRole string

const (
	RolePrimary NodeRole = "primary"
	RoleReplica NodeRole = "replica"
	RoleUnknown NodeRole = "unknown"
)

type NodeState string

const (
	StateHealthy     NodeState = "healthy"
	StateDegraded    NodeState = "degraded"
	StateUnreachable NodeState = "unreachable"
)

type NodeStatus struct {
	NodeID          string    `json:"node_id"`
	Address         string    `json:"address"`
	Role            NodeRole  `json:"role"`
	State           NodeState `json:"state"`
	IsInRecovery    bool      `json:"is_in_recovery"`
	WALReceiveLSN   int64     `json:"wal_receive_lsn"`
	WALReplayLSN    int64     `json:"wal_replay_lsn"`
	ReplicationLag  int64     `json:"replication_lag"`
	PGVersion       string    `json:"pg_version"`
	PostgresRunning bool      `json:"postgres_running"`
	LastHeartbeat   time.Time `json:"last_heartbeat"`
}

type ClusterTopology struct {
	Version     string       `json:"version"`
	PrimaryNode string       `json:"primary_node"`
	Nodes       []NodeStatus `json:"nodes"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

type FailoverEvent struct {
	OldPrimary string    `json:"old_primary"`
	NewPrimary string    `json:"new_primary"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
}

type ReplicationConfig struct {
	PrimaryConnInfo         string `json:"primary_conn_info"`
	SynchronousStandbyNames string `json:"synchronous_standby_names"`
	EnableSyncReplication   bool   `json:"enable_sync_replication"`
	MaxWALSenders           int    `json:"max_wal_senders"`
	WALLevel                string `json:"wal_level"`
}
