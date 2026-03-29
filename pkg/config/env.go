package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// RequireEnv returns the value of the environment variable named by key.
// It returns an error if the variable is not set or is empty.
func RequireEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("config: required env var %q is not set", key)
	}
	return v, nil
}

// EnvOr returns the value of the environment variable named by key,
// or fallback if the variable is not set or is empty.
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvInt returns the value of the environment variable named by key
// parsed as an integer, or fallback if the variable is not set, empty,
// or cannot be parsed.
func EnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// EnvStringSlice returns the value of the environment variable named by key
// parsed as a comma-separated list of strings, or fallback if the variable
// is not set, empty, or contains no non-empty elements after splitting.
func EnvStringSlice(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			result = append(result, s)
		}
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}
