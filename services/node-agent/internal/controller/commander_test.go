package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestExecCommander_Reconfigure_PreservesExistingKeys tests the file-parsing
// logic of parseAndMergeAutoConf directly without invoking pg_ctl.
func TestExecCommander_Reconfigure_PreservesExistingKeys(t *testing.T) {
	existing := strings.Join([]string{
		"# some prior comment",
		"max_connections = 100",
		"shared_buffers = 256MB",
	}, "\n") + "\n"

	result := parseAndMergeAutoConf(existing, "host=pg-primary port=5432 user=replicator", "latest")

	// Preserved settings must still be present.
	if !strings.Contains(result, "max_connections = 100") {
		t.Errorf("expected max_connections = 100 to be preserved, got:\n%s", result)
	}
	if !strings.Contains(result, "shared_buffers = 256MB") {
		t.Errorf("expected shared_buffers = 256MB to be preserved, got:\n%s", result)
	}

	// New managed keys must be set correctly.
	if !strings.Contains(result, "primary_conninfo = 'host=pg-primary port=5432 user=replicator'") {
		t.Errorf("expected primary_conninfo to be set, got:\n%s", result)
	}
	if !strings.Contains(result, "recovery_target_timeline = 'latest'") {
		t.Errorf("expected recovery_target_timeline to be set, got:\n%s", result)
	}

	// Header must be present.
	if !strings.Contains(result, "# Managed by HA node-agent") {
		t.Errorf("expected managed header comment, got:\n%s", result)
	}
}

func TestParseAndMergeAutoConf_ReplacesExistingManagedKeys(t *testing.T) {
	existing := strings.Join([]string{
		"# Managed by HA node-agent — do not edit manually",
		"primary_conninfo = 'host=old-primary port=5432 user=replicator'",
		"recovery_target_timeline = 'latest'",
		"max_connections = 200",
	}, "\n") + "\n"

	result := parseAndMergeAutoConf(existing, "host=new-primary port=5432 user=replicator", "latest")

	// Old primary_conninfo must be replaced.
	if strings.Contains(result, "old-primary") {
		t.Errorf("old primary_conninfo should have been replaced, got:\n%s", result)
	}
	if !strings.Contains(result, "host=new-primary") {
		t.Errorf("expected new primary_conninfo, got:\n%s", result)
	}

	// Other settings preserved.
	if !strings.Contains(result, "max_connections = 200") {
		t.Errorf("expected max_connections = 200 to be preserved, got:\n%s", result)
	}

	// Only one primary_conninfo line.
	count := strings.Count(result, "primary_conninfo")
	if count != 1 {
		t.Errorf("expected exactly 1 primary_conninfo line, got %d in:\n%s", count, result)
	}
}

func TestParseAndMergeAutoConf_EmptyExistingFile(t *testing.T) {
	result := parseAndMergeAutoConf("", "host=pg-primary port=5432 user=replicator", "latest")

	if !strings.Contains(result, "primary_conninfo = 'host=pg-primary port=5432 user=replicator'") {
		t.Errorf("expected primary_conninfo when existing is empty, got:\n%s", result)
	}
	if !strings.Contains(result, "recovery_target_timeline = 'latest'") {
		t.Errorf("expected recovery_target_timeline when existing is empty, got:\n%s", result)
	}
}

func TestParseAndMergeAutoConf_EscapesSingleQuotesInGUCValue(t *testing.T) {
	// connInfoQuote wraps the password in single quotes, e.g. password='s3cr3t'.
	// When embedded in a postgresql.conf single-quoted GUC value the inner quotes
	// must be escaped as '' to avoid premature string termination.
	connInfo := "host=pg-primary port=5432 user=replicator password='s3cr3t' sslmode=disable"
	result := parseAndMergeAutoConf("", connInfo, "latest")

	// The GUC value must contain '' (doubled) around the password, not bare '.
	if !strings.Contains(result, "password=''s3cr3t''") {
		t.Errorf("expected password single-quotes escaped as '' in GUC value, got:\n%s", result)
	}
	// The outer single quotes of the GUC value must be present.
	if !strings.Contains(result, "primary_conninfo = '") {
		t.Errorf("expected primary_conninfo to be single-quoted GUC value, got:\n%s", result)
	}
}

func TestParseAndMergeAutoConf_EmptyValuesNotWritten(t *testing.T) {
	existing := "max_connections = 100\n"

	result := parseAndMergeAutoConf(existing, "", "")

	// Empty values: no primary_conninfo or recovery_target_timeline should appear.
	if strings.Contains(result, "primary_conninfo") {
		t.Errorf("expected no primary_conninfo when value is empty, got:\n%s", result)
	}
	if strings.Contains(result, "recovery_target_timeline") {
		t.Errorf("expected no recovery_target_timeline when value is empty, got:\n%s", result)
	}

	// Preserved setting must still be there.
	if !strings.Contains(result, "max_connections = 100") {
		t.Errorf("expected max_connections = 100 to be preserved, got:\n%s", result)
	}
}

func TestExecCommander_PgRewind_CreatesStandbySignal(t *testing.T) {
	pgdata := t.TempDir()

	// Create mock pg_rewind that succeeds
	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
# Mock pg_rewind that succeeds
exit 0
`
	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create mock pg_ctl that succeeds
	mockPgCtl := filepath.Join(pgdata, "pg_ctl")
	pgCtlScript := `#!/bin/bash
exit 0
`
	if err := os.WriteFile(mockPgCtl, []byte(pgCtlScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := newTestExecCommander(pgdata, 5*time.Second, func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) > 0 {
			binName := filepath.Base(args[0])
			mockBin := filepath.Join(pgdata, binName)
			if _, err := os.Stat(mockBin); err == nil {
				cmdArgs := args[1:]
				output, err := exec.CommandContext(ctx, mockBin, cmdArgs...).CombinedOutput()
				if err != nil {
					return output, fmt.Errorf("%s: %w", binName, err)
				}
				return output, nil
			}
		}
		return nil, fmt.Errorf("command not found: %s", args[0])
	})

	if err := cmd.PgRewind(context.Background(), "host=new-primary port=5432"); err != nil {
		t.Fatalf("PgRewind: %v", err)
	}

	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, err := os.Stat(signalPath); os.IsNotExist(err) {
		t.Fatalf("standby.signal was not created in PGDATA (%s)", pgdata)
	}
}

// TestExecCommander_PgRewind_DivergentHistoryError tests the specific error case
// where pg_rewind fails due to divergent history, ensuring standby.signal is not created
// and the error is identified as ErrTimelineDivergence.
func TestExecCommander_PgRewind_DivergentHistoryError(t *testing.T) {
	pgdata := t.TempDir()

	// Create mock pg_rewind that returns divergent history error
	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
# Simulate pg_rewind output for divergent history
echo "could not find common ancestor of the source and target cluster's timelines" >&2
exit 1
`
	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create mock pg_ctl that succeeds
	mockPgCtl := filepath.Join(pgdata, "pg_ctl")
	pgCtlScript := `#!/bin/bash
exit 0
`
	if err := os.WriteFile(mockPgCtl, []byte(pgCtlScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := newTestExecCommander(pgdata, 5*time.Second, func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) > 0 {
			binName := filepath.Base(args[0])
			mockBin := filepath.Join(pgdata, binName)
			if _, err := os.Stat(mockBin); err == nil {
				cmdArgs := args[1:]
				output, err := exec.CommandContext(ctx, mockBin, cmdArgs...).CombinedOutput()
				if err != nil {
					return output, fmt.Errorf("%s: %w", binName, err)
				}
				return output, nil
			}
		}
		return nil, fmt.Errorf("command not found: %s", args[0])
	})

	err := cmd.PgRewind(context.Background(), "host=new-primary port=5432")

	if err == nil {
		t.Fatalf("expected error from PgRewind when pg_rewind fails due to divergent history, got nil")
	}

	// standby.signal must NOT be created
	signalPath := filepath.Join(pgdata, "standby.signal")
	_, statErr := os.Stat(signalPath)
	if statErr == nil {
		t.Fatalf("standby.signal should NOT be created when pg_rewind fails due to divergent history")
	}
	if !os.IsNotExist(statErr) {
		t.Fatalf("unexpected error checking standby.signal: %v", statErr)
	}

	// Error should be ErrTimelineDivergence (or wrap it)
	if !errors.Is(err, ErrTimelineDivergence) {
		t.Errorf("expected ErrTimelineDivergence (or wrapped error), got: %T: %v", err, err)
	}

	// Error should contain timeline divergence information
	if !strings.Contains(err.Error(), "could not find common ancestor") {
		t.Errorf("expected error to contain divergent history message, got: %v", err)
	}
}

// TestIsTimelineDivergenceError tests the timeline divergence detection logic.
func TestIsTimelineDivergenceError(t *testing.T) {
	tests := []struct {
		name     string
		output   []byte
		expected bool
	}{
		{
			name:     "exact match for timeline divergence",
			output:   []byte("could not find common ancestor of the source and target cluster's timelines"),
			expected: true,
		},
		{
			name:     "case insensitive match",
			output:   []byte("COULD NOT FIND COMMON ANCESTOR OF THE SOURCE AND TARGET CLUSTER'S TIMELINES"),
			expected: true,
		},
		{
			name:     "with apostrophe variation",
			output:   []byte("could not find common ancestor of the source and target clusters timelines"),
			expected: true,
		},
		{
			name:     "different error message",
			output:   []byte("connection refused"),
			expected: false,
		},
		{
			name:     "empty output",
			output:   []byte(""),
			expected: false,
		},
		{
			name:     "partial match on timeline",
			output:   []byte("timeline 2"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTimelineDivergenceError(tt.output)
			if result != tt.expected {
				t.Errorf("IsTimelineDivergenceError(%q) = %v, want %v", string(tt.output), result, tt.expected)
			}
		})
	}
}
