package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/zhavkk/Diploma/pkg/models"
)

func TestNodeStatus_JSONRoundTrip(t *testing.T) {
	original := models.NodeStatus{
		NodeID:          "pg-primary",
		Address:         "primary:50052",
		Role:            models.RolePrimary,
		State:           models.StateHealthy,
		IsInRecovery:    false,
		WALReceiveLSN:   1000,
		WALReplayLSN:    900,
		ReplicationLag:  100,
		PGVersion:       "14.5",
		PostgresRunning: true,
		LastHeartbeat:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got models.NodeStatus
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.NodeID != original.NodeID {
		t.Errorf("NodeID = %q, want %q", got.NodeID, original.NodeID)
	}
	if got.Address != original.Address {
		t.Errorf("Address = %q, want %q", got.Address, original.Address)
	}
	if got.Role != original.Role {
		t.Errorf("Role = %q, want %q", got.Role, original.Role)
	}
	if got.State != original.State {
		t.Errorf("State = %q, want %q", got.State, original.State)
	}
	if got.IsInRecovery != original.IsInRecovery {
		t.Errorf("IsInRecovery = %v, want %v", got.IsInRecovery, original.IsInRecovery)
	}
	if got.WALReceiveLSN != original.WALReceiveLSN {
		t.Errorf("WALReceiveLSN = %d, want %d", got.WALReceiveLSN, original.WALReceiveLSN)
	}
	if got.WALReplayLSN != original.WALReplayLSN {
		t.Errorf("WALReplayLSN = %d, want %d", got.WALReplayLSN, original.WALReplayLSN)
	}
	if got.ReplicationLag != original.ReplicationLag {
		t.Errorf("ReplicationLag = %d, want %d", got.ReplicationLag, original.ReplicationLag)
	}
	if got.PGVersion != original.PGVersion {
		t.Errorf("PGVersion = %q, want %q", got.PGVersion, original.PGVersion)
	}
	if got.PostgresRunning != original.PostgresRunning {
		t.Errorf("PostgresRunning = %v, want %v", got.PostgresRunning, original.PostgresRunning)
	}
	if !got.LastHeartbeat.Equal(original.LastHeartbeat) {
		t.Errorf("LastHeartbeat = %v, want %v", got.LastHeartbeat, original.LastHeartbeat)
	}
}

func TestClusterTopology_JSONRoundTrip(t *testing.T) {
	original := models.ClusterTopology{
		Version:     "v1",
		PrimaryNode: "pg-primary",
		Nodes: []models.NodeStatus{
			{NodeID: "pg-primary", Role: models.RolePrimary, State: models.StateHealthy},
			{NodeID: "pg-replica1", Role: models.RoleReplica, State: models.StateHealthy, WALReplayLSN: 5000},
		},
		UpdatedAt: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got models.ClusterTopology
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.Version != original.Version {
		t.Errorf("Version = %q, want %q", got.Version, original.Version)
	}
	if got.PrimaryNode != original.PrimaryNode {
		t.Errorf("PrimaryNode = %q, want %q", got.PrimaryNode, original.PrimaryNode)
	}
	if len(got.Nodes) != len(original.Nodes) {
		t.Fatalf("Nodes count = %d, want %d", len(got.Nodes), len(original.Nodes))
	}
	if got.Nodes[0].NodeID != original.Nodes[0].NodeID {
		t.Errorf("Nodes[0].NodeID = %q, want %q", got.Nodes[0].NodeID, original.Nodes[0].NodeID)
	}
	if got.Nodes[1].WALReplayLSN != original.Nodes[1].WALReplayLSN {
		t.Errorf("Nodes[1].WALReplayLSN = %d, want %d", got.Nodes[1].WALReplayLSN, original.Nodes[1].WALReplayLSN)
	}
	if !got.UpdatedAt.Equal(original.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, original.UpdatedAt)
	}
}

func TestFailoverEvent_JSONRoundTrip(t *testing.T) {
	original := models.FailoverEvent{
		OldPrimary: "pg-primary",
		NewPrimary: "pg-replica1",
		Reason:     "automatic",
		OccurredAt: time.Date(2024, 3, 15, 8, 30, 0, 0, time.UTC),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got models.FailoverEvent
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.OldPrimary != original.OldPrimary {
		t.Errorf("OldPrimary = %q, want %q", got.OldPrimary, original.OldPrimary)
	}
	if got.NewPrimary != original.NewPrimary {
		t.Errorf("NewPrimary = %q, want %q", got.NewPrimary, original.NewPrimary)
	}
	if got.Reason != original.Reason {
		t.Errorf("Reason = %q, want %q", got.Reason, original.Reason)
	}
	if !got.OccurredAt.Equal(original.OccurredAt) {
		t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, original.OccurredAt)
	}
}

func TestReplicationConfig_JSONRoundTrip(t *testing.T) {
	original := models.ReplicationConfig{
		PrimaryConnInfo:         "host=primary port=5432 user=replicator",
		SynchronousStandbyNames: "pg-replica1",
		EnableSyncReplication:   true,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got models.ReplicationConfig
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.PrimaryConnInfo != original.PrimaryConnInfo {
		t.Errorf("PrimaryConnInfo = %q, want %q", got.PrimaryConnInfo, original.PrimaryConnInfo)
	}
	if got.SynchronousStandbyNames != original.SynchronousStandbyNames {
		t.Errorf("SynchronousStandbyNames = %q, want %q", got.SynchronousStandbyNames, original.SynchronousStandbyNames)
	}
	if got.EnableSyncReplication != original.EnableSyncReplication {
		t.Errorf("EnableSyncReplication = %v, want %v", got.EnableSyncReplication, original.EnableSyncReplication)
	}
}

// TestNodeStatus_ZeroValueRoundTrip verifies that a zero-value NodeStatus
// marshals and unmarshals back to an identical zero-value struct.
func TestNodeStatus_ZeroValueRoundTrip(t *testing.T) {
	var original models.NodeStatus

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got models.NodeStatus
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.NodeID != "" {
		t.Errorf("NodeID = %q, want empty", got.NodeID)
	}
	if got.Address != "" {
		t.Errorf("Address = %q, want empty", got.Address)
	}
	if got.Role != "" {
		t.Errorf("Role = %q, want empty", got.Role)
	}
	if got.State != "" {
		t.Errorf("State = %q, want empty", got.State)
	}
	if got.IsInRecovery {
		t.Errorf("IsInRecovery = true, want false")
	}
	if got.WALReceiveLSN != 0 {
		t.Errorf("WALReceiveLSN = %d, want 0", got.WALReceiveLSN)
	}
	if got.WALReplayLSN != 0 {
		t.Errorf("WALReplayLSN = %d, want 0", got.WALReplayLSN)
	}
	if got.ReplicationLag != 0 {
		t.Errorf("ReplicationLag = %d, want 0", got.ReplicationLag)
	}
	if got.PGVersion != "" {
		t.Errorf("PGVersion = %q, want empty", got.PGVersion)
	}
	if got.PostgresRunning {
		t.Errorf("PostgresRunning = true, want false")
	}
	if !got.LastHeartbeat.IsZero() {
		t.Errorf("LastHeartbeat = %v, want zero time", got.LastHeartbeat)
	}
}

// TestClusterTopology_EmptyNodesSliceRoundTrip verifies that a ClusterTopology
// with an explicitly empty (non-nil) Nodes slice round-trips through JSON.
// Go's encoding/json marshals an empty non-nil slice as "[]" and unmarshals
// "[]" back to a non-nil empty slice, so we assert len == 0 either way.
func TestClusterTopology_EmptyNodesSliceRoundTrip(t *testing.T) {
	original := models.ClusterTopology{
		Version:     "v1",
		PrimaryNode: "pg-primary",
		Nodes:       []models.NodeStatus{}, // explicitly empty, not nil
		UpdatedAt:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got models.ClusterTopology
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// encoding/json unmarshals "[]" into a non-nil empty slice.
	// Either nil or length-0 is acceptable; the important thing is no nodes.
	if len(got.Nodes) != 0 {
		t.Errorf("Nodes length = %d, want 0", len(got.Nodes))
	}
	if got.Version != original.Version {
		t.Errorf("Version = %q, want %q", got.Version, original.Version)
	}
	if got.PrimaryNode != original.PrimaryNode {
		t.Errorf("PrimaryNode = %q, want %q", got.PrimaryNode, original.PrimaryNode)
	}
}

func TestNodeStatus_WithReplicationStats_JSONRoundTrip(t *testing.T) {
	original := models.NodeStatus{
		NodeID:          "pg-primary",
		Address:         "primary:50052",
		Role:            models.RolePrimary,
		State:           models.StateHealthy,
		PostgresRunning: true,
		ReplicationStats: &models.ReplicationStats{
			State:    "streaming",
			WALLSN:   "5000",
			LagBytes: 300,
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got models.NodeStatus
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.ReplicationStats == nil {
		t.Fatal("expected ReplicationStats to be present after round-trip")
	}
	if got.ReplicationStats.State != "streaming" {
		t.Errorf("ReplicationStats.State = %q, want %q", got.ReplicationStats.State, "streaming")
	}
	if got.ReplicationStats.WALLSN != "5000" {
		t.Errorf("ReplicationStats.WALLSN = %q, want %q", got.ReplicationStats.WALLSN, "5000")
	}
	if got.ReplicationStats.LagBytes != 300 {
		t.Errorf("ReplicationStats.LagBytes = %d, want 300", got.ReplicationStats.LagBytes)
	}
}

func TestNodeStatus_NilReplicationStats_OmittedInJSON(t *testing.T) {
	original := models.NodeStatus{
		NodeID: "pg-replica1",
		Role:   models.RoleReplica,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// The JSON should not contain "replication_stats" when it is nil.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal raw: %v", err)
	}
	if _, exists := raw["replication_stats"]; exists {
		t.Error("expected replication_stats to be omitted from JSON when nil")
	}
}

// TestNodeStatus_UnknownFieldIgnored verifies that Go's encoding/json silently
// ignores unknown fields when unmarshaling into NodeStatus, and that known
// fields are decoded correctly.
func TestNodeStatus_UnknownFieldIgnored(t *testing.T) {
	raw := `{"node_id":"x","unknown_field":"y"}`

	var got models.NodeStatus
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("Unmarshal returned unexpected error: %v", err)
	}

	if got.NodeID != "x" {
		t.Errorf("NodeID = %q, want %q", got.NodeID, "x")
	}
	// All other fields should be zero.
	if got.Address != "" {
		t.Errorf("Address = %q, want empty (unknown_field should be ignored)", got.Address)
	}
}
