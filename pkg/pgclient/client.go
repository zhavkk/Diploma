package pgclient

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// retryInterval controls how long to wait between ping retries.
// Overridable in tests.
var retryInterval = 3 * time.Second

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

type Client struct {
	db *sql.DB
}

func buildDSN(cfg Config) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode,
	)
}

func New(ctx context.Context, cfg Config, log *zap.Logger) (*Client, error) {
	dsn := buildDSN(cfg)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("pgclient: open: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = db.Close()
		}
	}()

	const maxAttempts = 10
	var pingErr error
	for i := 1; i <= maxAttempts; i++ {
		pingErr = db.PingContext(ctx)
		if pingErr == nil {
			break
		}
		log.Warn("pgclient: ping failed", zap.Int("attempt", i), zap.Error(pingErr))
		if i == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("pgclient: context cancelled during startup: %w", ctx.Err())
		case <-time.After(retryInterval):
		}
	}
	if pingErr != nil {
		return nil, fmt.Errorf("pgclient: ping after %d attempts: %w", maxAttempts, pingErr)
	}
	ok = true
	return &Client{db: db}, nil
}

func (c *Client) IsInRecovery(ctx context.Context) (bool, error) {
	var inRecovery bool
	err := c.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		return false, fmt.Errorf("pgclient: is_in_recovery: %w", err)
	}
	return inRecovery, nil
}

func (c *Client) WALReplayLSN(ctx context.Context) (int64, error) {
	var lsn int64
	err := c.db.QueryRowContext(ctx,
		"SELECT COALESCE(pg_last_wal_replay_lsn() - '0/0'::pg_lsn, 0)",
	).Scan(&lsn)
	if err != nil {
		return 0, fmt.Errorf("pgclient: wal_replay_lsn: %w", err)
	}
	return lsn, nil
}

func (c *Client) WALReceiveLSN(ctx context.Context) (int64, error) {
	var lsn int64
	err := c.db.QueryRowContext(ctx,
		"SELECT COALESCE(pg_last_wal_receive_lsn() - '0/0'::pg_lsn, 0)",
	).Scan(&lsn)
	if err != nil {
		return 0, fmt.Errorf("pgclient: wal_receive_lsn: %w", err)
	}
	return lsn, nil
}

type ReplicationStat struct {
	ApplicationName string
	ClientAddr      string
	State           string
	WriteLag        int64
	FlushLag        int64
	ReplayLag       int64
}

func (c *Client) ReplicationStats(ctx context.Context) ([]ReplicationStat, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT
			application_name,
			client_addr::text,
			state,
			EXTRACT(EPOCH FROM write_lag)::bigint,
			EXTRACT(EPOCH FROM flush_lag)::bigint,
			EXTRACT(EPOCH FROM replay_lag)::bigint
		FROM pg_stat_replication
	`)
	if err != nil {
		return nil, fmt.Errorf("pgclient: replication_stats: %w", err)
	}
	defer rows.Close()

	var stats []ReplicationStat
	for rows.Next() {
		var s ReplicationStat
		var clientAddr sql.NullString
		if err := rows.Scan(
			&s.ApplicationName, &clientAddr, &s.State,
			&s.WriteLag, &s.FlushLag, &s.ReplayLag,
		); err != nil {
			return nil, fmt.Errorf("pgclient: replication_stats scan: %w", err)
		}
		s.ClientAddr = clientAddr.String // empty string when NULL
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

func (c *Client) Version(ctx context.Context) (string, error) {
	var version string
	err := c.db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("pgclient: version: %w", err)
	}
	return version, nil
}

func (c *Client) Close() error {
	return c.db.Close()
}
