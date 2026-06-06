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

func TestExecCommander_PgRewind_RetryOnTransientError(t *testing.T) {

	pgdata := t.TempDir()
	retryFile := t.TempDir() + "/retry_count"

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
touch "%s/standby.signal"
exit 0
`, retryFile, pgdata)

	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

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

	err := cmd.PgRewind(context.Background(), "host=new-primary port=5432")

	if err != nil {
		t.Fatalf("PgRewind should succeed after retries, got error: %v", err)
	}

	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, statErr := os.Stat(signalPath); os.IsNotExist(statErr) {
		t.Fatalf("standby.signal was not created after successful retry")
	}
}

type testExecCommander struct {
	*ExecCommander
}

func newTestExecCommander(pgdata string, retryDelay time.Duration, mockExec func(ctx context.Context, args ...string) ([]byte, error)) *testExecCommander {
	cmd := &ExecCommander{
		pgdata:             pgdata,
		pgRewindRetryDelay: retryDelay,
		pgUser:             "postgres",
		execFunc:           mockExec,
	}
	return &testExecCommander{
		ExecCommander: cmd,
	}
}

func TestExecCommander_PgRewind_DivergentHistoryNoRetry(t *testing.T) {
	pgdata := t.TempDir()

	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
echo "could not find common ancestor of the source and target cluster's timelines" >&2
exit 1
`
	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

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

	if !errors.Is(err, ErrTimelineDivergence) {
		t.Errorf("expected ErrTimelineDivergence, got: %T: %v", err, err)
	}

	signalPath := filepath.Join(pgdata, "standby.signal")
	_, statErr := os.Stat(signalPath)
	if statErr == nil {
		t.Fatalf("standby.signal should NOT be created for divergent history error")
	}
	if !os.IsNotExist(statErr) {
		t.Fatalf("unexpected error checking standby.signal: %v", statErr)
	}

	if duration > 500*time.Millisecond {
		t.Errorf("divergent history error should return immediately without retry, took %v", duration)
	}
}

func TestExecCommander_PgRewind_ExhaustsRetries(t *testing.T) {
	pgdata := t.TempDir()

	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
echo "connection refused" >&2
exit 1
`
	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

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

	if !strings.Contains(err.Error(), "failed after") {
		t.Errorf("expected error to mention failed attempts, got: %v", err)
	}

	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, err := os.Stat(signalPath); err == nil {
		t.Fatal("standby.signal should NOT be created after failed retries")
	}

	expectedMin := 70 * time.Millisecond
	if duration < expectedMin {
		t.Logf("Warning: test completed in %v, expected at least %v (may be due to timing)", duration, expectedMin)
	}
}

func TestExecCommander_PgRewind_ContextCancelDuringRetry(t *testing.T) {
	pgdata := t.TempDir()

	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
echo "connection refused" >&2
exit 1
`
	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := cmd.PgRewind(ctx, "host=new-primary port=5432")

	if err == nil {
		t.Fatalf("expected error from context cancellation, got nil")
	}

	if !strings.Contains(err.Error(), "context cancelled") {
		t.Errorf("expected error to contain 'context cancelled', got: %v", err)
	}
}

func TestExecCommander_PgRewind_NonRetryableError(t *testing.T) {
	pgdata := t.TempDir()

	mockPgRewind := filepath.Join(pgdata, "pg_rewind")
	script := `#!/bin/bash
echo "permission denied: cannot access pgdata" >&2
exit 1
`
	if err := os.WriteFile(mockPgRewind, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

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

	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("expected error to contain 'permission denied', got: %v", err)
	}

	if duration > 200*time.Millisecond {
		t.Errorf("non-retryable error should return immediately, took %v", duration)
	}

	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, err := os.Stat(signalPath); err == nil {
		t.Fatal("standby.signal should NOT be created for non-retryable error")
	}
}

func TestExecCommander_PgRewind_DefaultDelay(t *testing.T) {
	cmd := NewExecCommander("/tmp/pgdata")
	if cmd.pgRewindRetryDelay != 5*time.Second {
		t.Errorf("NewExecCommander should set pgRewindRetryDelay to 5s, got %v", cmd.pgRewindRetryDelay)
	}
}

func TestExecCommander_PgRewind_CustomDelay(t *testing.T) {
	customDelay := 2 * time.Second
	cmd := NewExecCommanderWithRetryDelay("/tmp/pgdata", customDelay)
	if cmd.pgRewindRetryDelay != customDelay {
		t.Errorf("NewExecCommanderWithRetryDelay should set pgRewindRetryDelay to %v, got %v", customDelay, cmd.pgRewindRetryDelay)
	}
}

func TestIsRetryablePgRewindError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "connection refused is retryable",
			err:      fmt.Errorf("pg_rewind: connection refused"),
			expected: true,
		},
		{
			name:     "no route to host is retryable",
			err:      fmt.Errorf("pg_rewind: no route to host"),
			expected: true,
		},
		{
			name:     "connection timed out is retryable",
			err:      fmt.Errorf("pg_rewind: connection timed out"),
			expected: true,
		},
		{
			name:     "i/o timeout is retryable",
			err:      fmt.Errorf("pg_rewind: i/o timeout"),
			expected: true,
		},
		{
			name:     "timeout is retryable",
			err:      fmt.Errorf("pg_rewind: timeout"),
			expected: true,
		},
		{
			name:     "temporary failure is retryable",
			err:      fmt.Errorf("pg_rewind: temporary failure"),
			expected: true,
		},
		{
			name:     "network unreachable is retryable",
			err:      fmt.Errorf("pg_rewind: network is unreachable"),
			expected: true,
		},
		{
			name:     "connection reset is retryable",
			err:      fmt.Errorf("pg_rewind: connection reset"),
			expected: true,
		},
		{
			name:     "permission denied is not retryable",
			err:      fmt.Errorf("pg_rewind: permission denied"),
			expected: false,
		},
		{
			name:     "no such file is not retryable",
			err:      fmt.Errorf("pg_rewind: no such file or directory"),
			expected: false,
		},
		{
			name:     "nil error is not retryable",
			err:      nil,
			expected: false,
		},
		{
			name:     "generic error is not retryable",
			err:      fmt.Errorf("pg_rewind: something went wrong"),
			expected: false,
		},
		{
			name:     "divergent history is not retryable (handled separately)",
			err:      fmt.Errorf("pg_rewind: could not find common ancestor"),
			expected: false,
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
