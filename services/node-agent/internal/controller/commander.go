package controller

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var ErrTimelineDivergence = errors.New("timeline divergence detected: could not find common ancestor of the source and target cluster's timelines")

var timelineDivergencePattern = regexp.MustCompile(`(?i)could not find common ancestor of the source and target cluster'?s? timelines?`)

type PGCommander interface {
	Promote(ctx context.Context) error
	PgRewind(ctx context.Context, sourceConnInfo string) error
	Restart(ctx context.Context) error
	Reconfigure(ctx context.Context, primaryConnInfo, timeline string) error
}

type ExecCommander struct {
	pgdata             string
	pgRewindRetryDelay time.Duration
	pgUser             string

	execFunc func(ctx context.Context, args ...string) ([]byte, error)
}

func NewExecCommander(pgdata string) *ExecCommander {
	return &ExecCommander{
		pgdata:             pgdata,
		pgRewindRetryDelay: 5 * time.Second,
		pgUser:             "postgres",
	}
}

func NewExecCommanderWithRetryDelay(pgdata string, retryDelay time.Duration) *ExecCommander {
	return &ExecCommander{
		pgdata:             pgdata,
		pgRewindRetryDelay: retryDelay,
		pgUser:             "postgres",
	}
}

func (e *ExecCommander) execAsPostgres(ctx context.Context, args ...string) ([]byte, error) {
	if os.Geteuid() != 0 {
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		return cmd.CombinedOutput()
	}

	cmdArgs := append([]string{"-", e.pgUser, "-c"}, strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "su", cmdArgs...)
	return cmd.CombinedOutput()
}

func IsTimelineDivergenceError(output []byte) bool {
	return timelineDivergencePattern.Match(output)
}

func (e *ExecCommander) Promote(ctx context.Context) error {
	out, err := e.execAsPostgres(ctx, "pg_ctl", "promote", "-D", e.pgdata, "-w", "-t", "10")
	if err != nil {
		return fmt.Errorf("pg_ctl promote: %w, output: %s", err, out)
	}
	return nil
}

const (
	MaxPgRewindRetries   = 3
	defaultPgStopTimeout = 30 * time.Second
)

func (e *ExecCommander) PgRewind(ctx context.Context, sourceConnInfo string) error {

	stopCtx, stopCancel := context.WithTimeout(ctx, defaultPgStopTimeout)
	defer stopCancel()

	execFn := e.execFunc
	if execFn == nil {
		execFn = e.execAsPostgres
	}

	out, err := execFn(stopCtx, "pg_ctl", "stop", "-D", e.pgdata, "-m", "fast", "-w")
	if err != nil {

		if ctx.Err() != nil {
			return fmt.Errorf("pg_ctl stop before pg_rewind: %w (context cancelled)", ctx.Err())
		}

		if stopCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("pg_ctl stop timed out after %s: %w", defaultPgStopTimeout, err)
		}
		return fmt.Errorf("pg_ctl stop before pg_rewind: %w, output: %s", err, out)
	}

	const maxBackoff = 30 * time.Second
	backoff := e.pgRewindRetryDelay
	var lastErr error

	for attempt := 0; attempt <= MaxPgRewindRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("pg_rewind: context cancelled during backoff: %w", ctx.Err())
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		out, err := execFn(ctx,
			"pg_rewind",
			"--target-pgdata="+e.pgdata,
			"--source-server="+sourceConnInfo,
		)
		if err != nil {
			lastErr = fmt.Errorf("pg_rewind: %w, output: %s", err, out)
			if IsTimelineDivergenceError(out) {
				return fmt.Errorf("%w", ErrTimelineDivergence)
			}
			if isRetryablePgRewindError(lastErr) {
				continue
			}
			return lastErr
		}

		if err := os.WriteFile(filepath.Join(e.pgdata, "standby.signal"), nil, 0o600); err != nil {
			return fmt.Errorf("pg_rewind: create standby.signal: %w", err)
		}
		return nil
	}

	return fmt.Errorf("pg_rewind: failed after %d attempts, last error: %w", MaxPgRewindRetries+1, lastErr)
}

func isRetryablePgRewindError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	if strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "no route to host") ||
		strings.Contains(errStr, "connection timed out") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "temporary failure") ||
		strings.Contains(errStr, "network is unreachable") ||
		strings.Contains(errStr, "connection reset") {
		return true
	}

	return false
}

func (e *ExecCommander) Restart(ctx context.Context) error {
	out, err := e.execAsPostgres(ctx, "pg_ctl", "restart", "-D", e.pgdata, "-w")
	if err != nil {
		return fmt.Errorf("pg_ctl restart: %w, output: %s", err, out)
	}
	return nil
}

func parseAndMergeAutoConf(existing, primaryConnInfo, timeline string) string {
	const header = "# Managed by HA node-agent — do not edit manually"
	managedKeys := map[string]string{
		"primary_conninfo":         primaryConnInfo,
		"recovery_target_timeline": timeline,
	}
	seen := map[string]bool{}

	var outLines []string
	outLines = append(outLines, header)

	scanner := bufio.NewScanner(strings.NewReader(existing))
	for scanner.Scan() {
		line := scanner.Text()

		if strings.TrimSpace(line) == header {
			continue
		}

		replaced := false
		for key, val := range managedKeys {
			if strings.HasPrefix(strings.TrimSpace(line), key) {

				trimmed := strings.TrimSpace(line)
				rest := strings.TrimSpace(trimmed[len(key):])
				if strings.HasPrefix(rest, "=") {
					if val != "" {
						outLines = append(outLines, fmt.Sprintf("%s = '%s'", key, strings.ReplaceAll(val, "'", "''")))
					}
					seen[key] = true
					replaced = true
					break
				}
			}
		}
		if !replaced {
			outLines = append(outLines, line)
		}
	}

	for _, key := range []string{"primary_conninfo", "recovery_target_timeline"} {
		if !seen[key] {
			val := managedKeys[key]
			if val != "" {
				outLines = append(outLines, fmt.Sprintf("%s = '%s'", key, strings.ReplaceAll(val, "'", "''")))
			}
		}
	}

	return strings.Join(outLines, "\n") + "\n"
}

func (e *ExecCommander) Reconfigure(ctx context.Context, primaryConnInfo, timeline string) error {
	confPath := e.pgdata + "/postgresql.auto.conf"

	var existing string
	if data, err := os.ReadFile(confPath); err == nil {
		existing = string(data)
	}

	content := parseAndMergeAutoConf(existing, primaryConnInfo, timeline)

	if err := os.WriteFile(confPath, []byte(content), 0o600); err != nil {
		return fmt.Errorf("reconfigure: write %s: %w", confPath, err)
	}

	out, err := exec.CommandContext(ctx, "pg_ctl", "reload", "-D", e.pgdata).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl reload: %w, output: %s", err, out)
	}
	return nil
}
