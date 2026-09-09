package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// P5-013: the bridge must resolve its auth token from either a secret FILE
// (Swarm/compose `secrets:`) or direct env — and fail loud on any malformed
// input rather than boot unauthenticated.
func TestAuthTokenFromEnv(t *testing.T) {
	writeSecret := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("file takes precedence over env", func(t *testing.T) {
		path := writeSecret(t, "file-token\n") // docker secrets carry a trailing newline
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", path)
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN", "env-token")
		got, err := authTokenFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "file-token" {
			t.Fatalf("file must win, got %q", got)
		}
	})

	t.Run("file content is trimmed", func(t *testing.T) {
		path := writeSecret(t, "  spaced-token \n")
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", path)
		got, err := authTokenFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "spaced-token" {
			t.Fatalf("token must be trimmed, got %q", got)
		}
	})

	t.Run("env used when file unset", func(t *testing.T) {
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN", "env-token")
		got, err := authTokenFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "env-token" {
			t.Fatalf("env must be used, got %q", got)
		}
	})

	t.Run("both unset is an error", func(t *testing.T) {
		if _, err := authTokenFromEnv(); err == nil {
			t.Fatal("expected error when neither file nor env is set")
		}
	})

	t.Run("unreadable file fails loud", func(t *testing.T) {
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", filepath.Join(t.TempDir(), "does-not-exist"))
		_, err := authTokenFromEnv()
		if err == nil || !strings.Contains(err.Error(), "unreadable") {
			t.Fatalf("expected loud unreadable-file error, got %v", err)
		}
	})

	t.Run("empty file fails loud", func(t *testing.T) {
		path := writeSecret(t, "   \n")
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", path)
		if _, err := authTokenFromEnv(); err == nil || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("expected loud empty-file error, got %v", err)
		}
	})
}
