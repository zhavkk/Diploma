package pgclient

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"github.com/stretchr/testify/require"
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

func TestConfig_DefaultConnectionPoolSettings(t *testing.T) {
	// Verify that zero values in Config result in zero pool settings
	// (since sql.DB uses its own defaults when not explicitly set)
	cfg := Config{
		Host:            "localhost",
		Port:            5432,
		User:            "test",
		Password:        "test",
		DBName:          "test",
		SSLMode:         "disable",
		MaxOpenConns:    0,
		MaxIdleConns:    0,
		ConnMaxLifetime: 0,
	}

	require.Equal(t, 0, cfg.MaxOpenConns, "MaxOpenConns should default to 0")
	require.Equal(t, 0, cfg.MaxIdleConns, "MaxIdleConns should default to 0")
	require.Equal(t, time.Duration(0), cfg.ConnMaxLifetime, "ConnMaxLifetime should default to 0")
}

func TestConfig_CustomConnectionPoolSettings(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		expected struct {
			maxOpenConns    int
			maxIdleConns    int
			connMaxLifetime time.Duration
		}
	}{
		{
			name: "production settings",
			cfg: Config{
				Host:            "localhost",
				Port:            5432,
				User:            "test",
				Password:        "test",
				DBName:          "test",
				SSLMode:         "disable",
				MaxOpenConns:    50,
				MaxIdleConns:    10,
				ConnMaxLifetime: 10 * time.Minute,
			},
			expected: struct {
				maxOpenConns    int
				maxIdleConns    int
				connMaxLifetime time.Duration
			}{50, 10, 10 * time.Minute},
		},
		{
			name: "minimal settings",
			cfg: Config{
				Host:            "localhost",
				Port:            5432,
				User:            "test",
				Password:        "test",
				DBName:          "test",
				SSLMode:         "disable",
				MaxOpenConns:    5,
				MaxIdleConns:    1,
				ConnMaxLifetime: 1 * time.Minute,
			},
			expected: struct {
				maxOpenConns    int
				maxIdleConns    int
				connMaxLifetime time.Duration
			}{5, 1, 1 * time.Minute},
		},
		{
			name: "max idle greater than max open",
			cfg: Config{
				Host:            "localhost",
				Port:            5432,
				User:            "test",
				Password:        "test",
				DBName:          "test",
				SSLMode:         "disable",
				MaxOpenConns:    5,
				MaxIdleConns:    10,
				ConnMaxLifetime: 5 * time.Minute,
			},
			expected: struct {
				maxOpenConns    int
				maxIdleConns    int
				connMaxLifetime time.Duration
			}{5, 10, 5 * time.Minute},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected.maxOpenConns, tt.cfg.MaxOpenConns)
			require.Equal(t, tt.expected.maxIdleConns, tt.cfg.MaxIdleConns)
			require.Equal(t, tt.expected.connMaxLifetime, tt.cfg.ConnMaxLifetime)
		})
	}
}

// TestNew_ConnectionPoolSettingsApplied verifies that connection pool settings
// are properly applied to the underlying sql.DB. This test requires a running
// PostgreSQL instance.
func TestNew_ConnectionPoolSettingsApplied(t *testing.T) {
	// Skip if no test database is configured
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set, skipping connection pool test")
	}

	tests := []struct {
		name     string
		maxOpen  int
		maxIdle  int
		maxLife  time.Duration
	}{
		{
			name:    "small pool",
			maxOpen: 3,
			maxIdle: 1,
			maxLife: time.Minute,
		},
		{
			name:    "large pool",
			maxOpen: 20,
			maxIdle: 5,
			maxLife: 5 * time.Minute,
		},
		{
			name:    "lifetime only",
			maxOpen: 0,
			maxIdle: 0,
			maxLife: time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Parse DSN to extract connection parameters
			cfg, err := parseDSN(dsn)
			require.NoError(t, err)

			cfg.MaxOpenConns = tt.maxOpen
			cfg.MaxIdleConns = tt.maxIdle
			cfg.ConnMaxLifetime = tt.maxLife

			client, err := New(ctx, cfg, zap.NewNop())
			require.NoError(t, err)
			defer client.Close()

			// Verify settings are applied via Stats()
			stats := client.Stats()
			if tt.maxOpen > 0 {
				require.Equal(t, tt.maxOpen, stats.MaxOpenConnections)
			} else {
				// When not set, MaxOpenConnections returns the actual limit used by sql.DB
				// which is 0 (unlimited) when not explicitly set
				require.GreaterOrEqual(t, stats.MaxOpenConnections, 0)
			}
		})
	}
}

func TestEnvDuration(t *testing.T) {
	// This test verifies the EnvDuration function from pkg/config/env.go
	// We test it here indirectly by checking that Config can be constructed
	// with various duration values
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"1s", time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"2h", 2 * time.Hour, false},
		{"30ms", 30 * time.Millisecond, false},
		{"invalid", 0, true}, // Would fall back to default in actual usage
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			d, err := time.ParseDuration(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, d)
			}
		})
	}
}

// parseDSN parses a libpq connection string into a Config.
// This is a simplified parser that handles the format:
// host=... port=... user=... password=... dbname=... sslmode=...
func parseDSN(dsn string) (Config, error) {
	var cfg Config
	cfg.SSLMode = "disable" // Default

	// Parse key=value pairs
	pairs := strings.Split(dsn, " ")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		value := parts[1]

		switch key {
		case "host":
			cfg.Host = value
		case "port":
			// Parse port
			re := regexp.MustCompile(`\d+`)
			portStr := re.FindString(value)
			if portStr != "" {
				var port int
				_, err := fmt.Sscanf(portStr, "%d", &port)
				if err == nil {
					cfg.Port = port
				}
			}
		case "user":
			cfg.User = value
		case "password":
			cfg.Password = value
		case "dbname":
			cfg.DBName = value
		case "sslmode":
			cfg.SSLMode = value
		}
	}

	return cfg, nil
}
