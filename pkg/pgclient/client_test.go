package pgclient

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNew_ContextCancelledDuringRetry(t *testing.T) {
	// Override retryInterval to be long so context cancellation fires during wait.
	old := retryInterval
	retryInterval = 5 * time.Second
	t.Cleanup(func() { retryInterval = old })

	// localhost:1 is an unreachable address (port 1 is reserved and not bound).
	cfg := Config{
		Host:    "localhost",
		Port:    1,
		User:    "nobody",
		DBName:  "nobody",
		SSLMode: "disable",
	}

	// Cancel the context after a short delay — it will fire while waiting in retryInterval.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := New(ctx, cfg, zap.NewNop())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from New with unreachable address")
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("expected error to contain 'context', got: %v", err)
	}
	// Should complete well within 2 seconds (context fires at ~200ms).
	if elapsed > 2*time.Second {
		t.Errorf("New took %v, expected < 2s", elapsed)
	}
}

func TestBuildDSN_NoExtraQuoting(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		contains string
		absent   string
	}{
		{
			name: "password with spaces",
			cfg: Config{
				Host: "localhost", Port: 5432,
				User: "admin", Password: "my secret pass", DBName: "mydb", SSLMode: "disable",
			},
			contains: "password=my secret pass",
			absent:   "'my secret pass'",
		},
		{
			name: "password with single quotes",
			cfg: Config{
				Host: "localhost", Port: 5432,
				User: "admin", Password: "p@ss'word", DBName: "mydb", SSLMode: "disable",
			},
			contains: "password=p@ss'word",
			absent:   "'p@ss",
		},
		{
			name: "user with special characters",
			cfg: Config{
				Host: "localhost", Port: 5432,
				User: "my user", Password: "pass", DBName: "my db", SSLMode: "disable",
			},
			contains: "user=my user",
			absent:   "'my user'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := buildDSN(tt.cfg)
			if !strings.Contains(dsn, tt.contains) {
				t.Errorf("DSN should contain %q, got: %s", tt.contains, dsn)
			}
			if strings.Contains(dsn, tt.absent) {
				t.Errorf("DSN should not contain %q (extra quoting), got: %s", tt.absent, dsn)
			}
		})
	}
}

func TestNew_FailsAfterMaxAttempts(t *testing.T) {
	// Override retryInterval so all 10 retries complete quickly.
	old := retryInterval
	retryInterval = 10 * time.Millisecond
	t.Cleanup(func() { retryInterval = old })

	cfg := Config{
		Host:    "localhost",
		Port:    1,
		User:    "nobody",
		DBName:  "nobody",
		SSLMode: "disable",
	}

	// Use a long-enough context so it doesn't cancel before all attempts finish.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := New(ctx, cfg, zap.NewNop())
	if err == nil {
		t.Fatal("expected error from New with unreachable address after max attempts")
	}
}
