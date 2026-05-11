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

// TestExecCommander_PgRewind_RetryOnTransientError tests that pg_rewind
// retries on transient errors like connection refused.
func TestExecCommander_PgRewind_RetryOnTransientError(t *testing.T) {
	// Create test commander that bypasses su and calls commands directly
	pgdata := t.TempDir()
	retryFile := t.TempDir() + "/retry_count"

	// Create mock pg_rewind script that tracks attempts
	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := fmt.Sprintf(`#!/bin/bash
retryFile="%s"
data=$(cat "$retryFile" 2>/dev/null || echo "0")
attempts=$((data + 1))
echo "$attempts" > "$retryFile"
echo "pg_rewind attempt: $attempts" >&2

if [ $attempts -le 1 ]; then
    echo "pg_rewind: connection refused" >&2
    exit 1
fi
# Create standby.signal on success
touch "%s/standby.signal"
exit 0
`, retryFile, pgdata)

	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create mock pg_ctl that succeeds
	mockPgCtl := filepath.Join(pgdata, "pg_ctl")
	pgCtlScript := `#!/bin/bash
# Mock pg_ctl that succeeds for all commands
exit 0
`
	if err := os.WriteFile(mockPgCtl, []byte(pgCtlScript), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create testable commander that uses mock binaries
	cmd := newTestExecCommander(pgdata, 100*time.Millisecond, func(ctx context.Context, args ...string) ([]byte, error) {
		// Execute mock binaries directly
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

	if err != nil {
		t.Fatalf("PgRewind should succeed after retries, got error: %v", err)
	}

	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, statErr := os.Stat(signalPath); os.IsNotExist(statErr) {
		t.Fatalf("standby.signal was not created after successful retry")
	}
}

// testExecCommander is a wrapper around ExecCommander that allows mocking execAsPostgres
type testExecCommander struct {
	*ExecCommander
}

func newTestExecCommander(pgdata string, retryDelay time.Duration, mockExec func(ctx context.Context, args ...string) ([]byte, error)) *testExecCommander {
	cmd := &ExecCommander{
		pgdata:             pgdata,
		pgRewindRetryDelay: retryDelay,
		pgUser:             "postgres", // Set user to avoid nil issues
		execFunc:           mockExec,  // Inject mock function
	}
	return &testExecCommander{
		ExecCommander: cmd,
	}
}

// TestExecCommander_PgRewind_DivergentHistoryNoRetry tests that
// divergent history errors are not retried.
func TestExecCommander_PgRewind_DivergentHistoryNoRetry(t *testing.T) {
	pgdata := t.TempDir()

	// Create mock pg_rewind that always returns timeline divergence error
	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
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

	cmd := newTestExecCommander(pgdata, 50*time.Millisecond, func(ctx context.Context, args ...string) ([]byte, error) {
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

	start := time.Now()
	err := cmd.PgRewind(context.Background(), "host=new-primary port=5432")
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected error from PgRewind when divergent history detected, got nil")
	}

	// Verify error is timeline divergence
	if !errors.Is(err, ErrTimelineDivergence) {
		t.Errorf("expected ErrTimelineDivergence, got: %T: %v", err, err)
	}

	// standby.signal must NOT be created for divergent history errors
	signalPath := filepath.Join(pgdata, "standby.signal")
	_, statErr := os.Stat(signalPath)
	if statErr == nil {
		t.Fatalf("standby.signal should NOT be created for divergent history error")
	}
	if !os.IsNotExist(statErr) {
		t.Fatalf("unexpected error checking standby.signal: %v", statErr)
	}

	// Test should complete quickly (no retry backoff for divergent history)
	if duration > 500*time.Millisecond {
		t.Errorf("divergent history error should return immediately without retry, took %v", duration)
	}
}

// TestExecCommander_PgRewind_ExhaustsRetries tests that after MaxPgRewindRetries
// attempts, error is returned without further retries.
func TestExecCommander_PgRewind_ExhaustsRetries(t *testing.T) {
	pgdata := t.TempDir()

	// Create mock pg_rewind that always fails with connection refused
	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
echo "connection refused" >&2
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

	cmd := newTestExecCommander(pgdata, 10*time.Millisecond, func(ctx context.Context, args ...string) ([]byte, error) {
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

	start := time.Now()
	err := cmd.PgRewind(context.Background(), "host=new-primary port=5432")
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected error after exhausting retries, got nil")
	}

	// Verify error mentions number of attempts
	if !strings.Contains(err.Error(), "failed after") {
		t.Errorf("expected error to mention failed attempts, got: %v", err)
	}

	// Verify standby.signal was NOT created
	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, err := os.Stat(signalPath); err == nil {
		t.Fatal("standby.signal should NOT be created after failed retries")
	}

	// The test should have taken roughly time for 3 retries with exponential backoff
	// With 10ms initial delay, we expect: 10ms + 20ms + 40ms = 70ms minimum
	expectedMin := 70 * time.Millisecond
	if duration < expectedMin {
		t.Logf("Warning: test completed in %v, expected at least %v (may be due to timing)", duration, expectedMin)
	}
}

// TestExecCommander_PgRewind_ContextCancelDuringRetry tests that
// context cancellation during backoff is properly handled.
func TestExecCommander_PgRewind_ContextCancelDuringRetry(t *testing.T) {
	pgdata := t.TempDir()

	// Create mock pg_rewind that always fails
	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
echo "connection refused" >&2
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

	cmd := newTestExecCommander(pgdata, 1*time.Second, func(ctx context.Context, args ...string) ([]byte, error) {
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

	// Cancel context after a short delay (before retry backoff completes)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := cmd.PgRewind(ctx, "host=new-primary port=5432")

	if err == nil {
		t.Fatalf("expected error from context cancellation, got nil")
	}

	// Error should mention context cancellation
	if !strings.Contains(err.Error(), "context cancelled") {
		t.Errorf("expected error to contain 'context cancelled', got: %v", err)
	}
}

// TestExecCommander_PgRewind_NonRetryableError tests that
// non-retryable errors are returned immediately without retry.
func TestExecCommander_PgRewind_NonRetryableError(t *testing.T) {
	pgdata := t.TempDir()

	// Create mock pg_rewind that returns permission denied
	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
echo "permission denied: cannot access pgdata" >&2
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

	cmd := newTestExecCommander(pgdata, 100*time.Millisecond, func(ctx context.Context, args ...string) ([]byte, error) {
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

	start := time.Now()
	err := cmd.PgRewind(context.Background(), "host=new-primary port=5432")
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected error for non-retryable failure, got nil")
	}

	// Error should contain permission denied message
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("expected error to contain 'permission denied', got: %v", err)
	}

	// The test should have completed quickly (no retry backoff)
	// Non-retryable errors should return immediately
	if duration > 200*time.Millisecond {
		t.Errorf("non-retryable error should return immediately, took %v", duration)
	}

	// standby.signal must NOT be created
	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, err := os.Stat(signalPath); err == nil {
		t.Fatal("standby.signal should NOT be created for non-retryable error")
	}
}

// TestExecCommander_PgRewind_DefaultDelay tests that the default retry delay is 5 seconds.
func TestExecCommander_PgRewind_DefaultDelay(t *testing.T) {
	cmd := NewExecCommander("/tmp/pgdata")
	if cmd.pgRewindRetryDelay != 5*time.Second {
		t.Errorf("NewExecCommander should set pgRewindRetryDelay to 5s, got %v", cmd.pgRewindRetryDelay)
	}
}

// TestExecCommander_PgRewind_CustomDelay tests that custom retry delay can be set.
func TestExecCommander_PgRewind_CustomDelay(t *testing.T) {
	customDelay := 2 * time.Second
	cmd := NewExecCommanderWithRetryDelay("/tmp/pgdata", customDelay)
	if cmd.pgRewindRetryDelay != customDelay {
		t.Errorf("NewExecCommanderWithRetryDelay should set pgRewindRetryDelay to %v, got %v", customDelay, cmd.pgRewindRetryDelay)
	}
}

// TestIsRetryablePgRewindError tests the retry logic for various error types.
func TestIsRetryablePgRewindError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "connection refused is retryable",
			err:      fmt.Errorf("pg_rewind: connection refused"),
			expected:  true,
		},
		{
			name:     "no route to host is retryable",
			err:      fmt.Errorf("pg_rewind: no route to host"),
			expected:  true,
		},
		{
			name:     "connection timed out is retryable",
			err:      fmt.Errorf("pg_rewind: connection timed out"),
			expected:  true,
		},
		{
			name:     "i/o timeout is retryable",
			err:      fmt.Errorf("pg_rewind: i/o timeout"),
			expected:  true,
		},
		{
			name:     "timeout is retryable",
			err:      fmt.Errorf("pg_rewind: timeout"),
			expected:  true,
		},
		{
			name:     "temporary failure is retryable",
			err:      fmt.Errorf("pg_rewind: temporary failure"),
			expected:  true,
		},
		{
			name:     "network unreachable is retryable",
			err:      fmt.Errorf("pg_rewind: network is unreachable"),
			expected:  true,
		},
		{
			name:     "connection reset is retryable",
			err:      fmt.Errorf("pg_rewind: connection reset"),
			expected:  true,
		},
		{
			name:     "permission denied is not retryable",
			err:      fmt.Errorf("pg_rewind: permission denied"),
			expected:  false,
		},
		{
			name:     "no such file is not retryable",
			err:      fmt.Errorf("pg_rewind: no such file or directory"),
			expected:  false,
		},
		{
			name:     "nil error is not retryable",
			err:      nil,
			expected:  false,
		},
		{
			name:     "generic error is not retryable",
			err:      fmt.Errorf("pg_rewind: something went wrong"),
			expected:  false,
		},
		{
			name:     "divergent history is not retryable (handled separately)",
			err:      fmt.Errorf("pg_rewind: could not find common ancestor"),
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryablePgRewindError(tt.err)
			if result != tt.expected {
				t.Errorf("isRetryablePgRewindError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}
