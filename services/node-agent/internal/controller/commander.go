package controller

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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
	return nil
}

func (e *ExecCommander) Restart(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "pg_ctl", "restart", "-D", e.pgdata, "-w").CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl restart: %w, output: %s", err, out)
	}
	return nil
}

func (e *ExecCommander) Reconfigure(ctx context.Context, primaryConnInfo, timeline string) error {
	var lines []string
	lines = append(lines, "# Managed by HA node-agent — do not edit manually")
	if primaryConnInfo != "" {
		lines = append(lines, fmt.Sprintf("primary_conninfo = '%s'", primaryConnInfo))
	}
	if timeline != "" {
		lines = append(lines, fmt.Sprintf("recovery_target_timeline = '%s'", timeline))
	}
	content := strings.Join(lines, "\n") + "\n"

	confPath := e.pgdata + "/postgresql.auto.conf"
	if err := os.WriteFile(confPath, []byte(content), 0o600); err != nil {
		return fmt.Errorf("reconfigure: write %s: %w", confPath, err)
	}

	out, err := exec.CommandContext(ctx, "pg_ctl", "reload", "-D", e.pgdata).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl reload: %w, output: %s", err, out)
	}
	return nil
}
