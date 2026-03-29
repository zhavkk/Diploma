package controller

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type PGCommander interface {
	Promote(ctx context.Context) error
	PgRewind(ctx context.Context, sourceConnInfo string) error
	Restart(ctx context.Context) error
	Reconfigure(ctx context.Context, primaryConnInfo, timeline string) error
}

type ExecCommander struct {
	pgdata string
}

func NewExecCommander(pgdata string) *ExecCommander {
	return &ExecCommander{pgdata: pgdata}
}

func (e *ExecCommander) Promote(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "pg_ctl", "promote", "-D", e.pgdata).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl promote: %w, output: %s", err, out)
	}
	return nil
}

func (e *ExecCommander) PgRewind(ctx context.Context, sourceConnInfo string) error {
	out, err := exec.CommandContext(ctx,
		"pg_rewind",
		"--target-pgdata="+e.pgdata,
		"--source-server="+sourceConnInfo,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_rewind: %w, output: %s", err, out)
	}

	// Create standby.signal so PostgreSQL starts as a standby after rewind.
	if err := os.WriteFile(filepath.Join(e.pgdata, "standby.signal"), nil, 0o600); err != nil {
		return fmt.Errorf("pg_rewind: create standby.signal: %w", err)
	}
	return nil
}

func (e *ExecCommander) Restart(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "pg_ctl", "restart", "-D", e.pgdata, "-w").CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl restart: %w, output: %s", err, out)
	}
	return nil
}

// parseAndMergeAutoConf reads the existing postgresql.auto.conf content (if any),
// replaces or appends primary_conninfo and recovery_target_timeline lines,
// and preserves all other settings. The header comment is kept at the top.
func parseAndMergeAutoConf(existing, primaryConnInfo, timeline string) string {
	const header = "# Managed by HA node-agent — do not edit manually"
	managedKeys := map[string]string{
		"primary_conninfo":          primaryConnInfo,
		"recovery_target_timeline":  timeline,
	}
	seen := map[string]bool{}

	var outLines []string
	outLines = append(outLines, header)

	scanner := bufio.NewScanner(strings.NewReader(existing))
	for scanner.Scan() {
		line := scanner.Text()
		// Skip the header comment (we always re-add it at the top).
		if strings.TrimSpace(line) == header {
			continue
		}
		// Check if this line sets a managed key.
		replaced := false
		for key, val := range managedKeys {
			if strings.HasPrefix(strings.TrimSpace(line), key) {
				// Check it's actually an assignment (key followed by =).
				trimmed := strings.TrimSpace(line)
				rest := strings.TrimSpace(trimmed[len(key):])
				if strings.HasPrefix(rest, "=") {
					if val != "" {
						outLines = append(outLines, fmt.Sprintf("%s = '%s'", key, strings.ReplaceAll(val, "'", "''"))) //nolint:gocritic
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

	// Append any managed keys not yet seen.
	for _, key := range []string{"primary_conninfo", "recovery_target_timeline"} {
		if !seen[key] {
			val := managedKeys[key]
			if val != "" {
				outLines = append(outLines, fmt.Sprintf("%s = '%s'", key, strings.ReplaceAll(val, "'", "''"))) //nolint:gocritic
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
