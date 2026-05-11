package config_test

import (
	"testing"
	"time"

	"github.com/zhavkk/Diploma/pkg/config"
)

func TestRequireEnv_ReturnsValue(t *testing.T) {
	t.Setenv("TEST_REQUIRE_KEY", "hello")

	v, err := config.RequireEnv("TEST_REQUIRE_KEY")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "hello" {
		t.Errorf("RequireEnv = %q, want %q", v, "hello")
	}
}

func TestRequireEnv_ErrorWhenEmpty(t *testing.T) {
	t.Setenv("TEST_REQUIRE_KEY", "")

	_, err := config.RequireEnv("TEST_REQUIRE_KEY")
	if err == nil {
		t.Error("expected error when env var is empty")
	}
}

func TestEnvOr_ReturnsEnvValue(t *testing.T) {
	t.Setenv("TEST_ENVOR_KEY", "custom")

	v := config.EnvOr("TEST_ENVOR_KEY", "default")
	if v != "custom" {
		t.Errorf("EnvOr = %q, want %q", v, "custom")
	}
}

func TestEnvOr_ReturnsFallback(t *testing.T) {
	t.Setenv("TEST_ENVOR_KEY", "")

	v := config.EnvOr("TEST_ENVOR_KEY", "default")
	if v != "default" {
		t.Errorf("EnvOr = %q, want %q", v, "default")
	}
}

func TestEnvInt_ReturnsEnvValue(t *testing.T) {
	t.Setenv("TEST_ENVINT_KEY", "42")

	v := config.EnvInt("TEST_ENVINT_KEY", 10)
	if v != 42 {
		t.Errorf("EnvInt = %d, want 42", v)
	}
}

func TestEnvInt_ReturnsFallbackWhenEmpty(t *testing.T) {
	t.Setenv("TEST_ENVINT_KEY", "")

	v := config.EnvInt("TEST_ENVINT_KEY", 10)
	if v != 10 {
		t.Errorf("EnvInt = %d, want 10", v)
	}
}

func TestEnvInt_ReturnsFallbackWhenInvalid(t *testing.T) {
	t.Setenv("TEST_ENVINT_KEY", "notanumber")

	v := config.EnvInt("TEST_ENVINT_KEY", 10)
	if v != 10 {
		t.Errorf("EnvInt = %d, want 10", v)
	}
}

func TestEnvStringSlice_ReturnsEnvValue(t *testing.T) {
	t.Setenv("TEST_SLICE_KEY", "a, b ,c")

	v := config.EnvStringSlice("TEST_SLICE_KEY", []string{"x"})
	if len(v) != 3 || v[0] != "a" || v[1] != "b" || v[2] != "c" {
		t.Errorf("EnvStringSlice = %v, want [a b c]", v)
	}
}

func TestEnvStringSlice_ReturnsFallbackWhenEmpty(t *testing.T) {
	t.Setenv("TEST_SLICE_KEY", "")

	v := config.EnvStringSlice("TEST_SLICE_KEY", []string{"x"})
	if len(v) != 1 || v[0] != "x" {
		t.Errorf("EnvStringSlice = %v, want [x]", v)
	}
}

func TestEnvStringSlice_ReturnsFallbackWhenAllBlank(t *testing.T) {
	t.Setenv("TEST_SLICE_KEY", " , , ")

	v := config.EnvStringSlice("TEST_SLICE_KEY", []string{"x"})
	if len(v) != 1 || v[0] != "x" {
		t.Errorf("EnvStringSlice = %v, want [x]", v)
	}
}

func TestEnvDuration_ReturnsEnvValue(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		want  time.Duration
	}{
		{"seconds", "TEST_DURATION_KEY", "30s", 30 * time.Second},
		{"minutes", "TEST_DURATION_KEY", "5m", 5 * time.Minute},
		{"hours", "TEST_DURATION_KEY", "2h", 2 * time.Hour},
		{"milliseconds", "TEST_DURATION_KEY", "500ms", 500 * time.Millisecond},
		{"mixed", "TEST_DURATION_KEY", "1h30m", 90 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.key, tt.value)

			v := config.EnvDuration(tt.key, time.Minute)
			if v != tt.want {
				t.Errorf("EnvDuration = %v, want %v", v, tt.want)
			}
		})
	}
}

func TestEnvDuration_ReturnsFallbackWhenEmpty(t *testing.T) {
	t.Setenv("TEST_DURATION_KEY", "")

	fallback := 10 * time.Minute
	v := config.EnvDuration("TEST_DURATION_KEY", fallback)
	if v != fallback {
		t.Errorf("EnvDuration = %v, want %v", v, fallback)
	}
}

func TestEnvDuration_ReturnsFallbackWhenInvalid(t *testing.T) {
	t.Setenv("TEST_DURATION_KEY", "notaduration")

	fallback := 10 * time.Minute
	v := config.EnvDuration("TEST_DURATION_KEY", fallback)
	if v != fallback {
		t.Errorf("EnvDuration = %v, want %v", v, fallback)
	}
}
