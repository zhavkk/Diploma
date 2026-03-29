package controller

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
	// Skip if running in an environment where we can't mock the command.
	if os.Getenv("GO_TEST_PG_REWIND_MOCK") == "1" {
		// This is the mock pg_rewind binary — just exit successfully.
		return
	}

	pgdata := t.TempDir()

	// Build a no-op helper binary that pretends to be pg_ctl and pg_rewind (exits 0).
	helperDir := t.TempDir()
	srcFile := filepath.Join(helperDir, "main.go")
	if err := os.WriteFile(srcFile, []byte(`package main; func main() {}`), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, bin := range []string{"pg_ctl", "pg_rewind"} {
		helperBin := filepath.Join(helperDir, bin)
		buildCmd := exec.Command("go", "build", "-o", helperBin, srcFile)
		buildCmd.Dir = helperDir
		if out, err := buildCmd.CombinedOutput(); err != nil {
			t.Fatalf("failed to build mock %s: %v\n%s", bin, err, out)
		}
	}

	// Put our mock binary first on PATH so exec.CommandContext finds it.
	t.Setenv("PATH", helperDir+":"+os.Getenv("PATH"))

	cmd := NewExecCommander(pgdata)
	if err := cmd.PgRewind(context.Background(), "host=new-primary port=5432"); err != nil {
		t.Fatalf("PgRewind: %v", err)
	}

	signalPath := filepath.Join(pgdata, "standby.signal")
	if _, err := os.Stat(signalPath); os.IsNotExist(err) {
		t.Fatalf("standby.signal was not created in PGDATA (%s)", pgdata)
	}
}
