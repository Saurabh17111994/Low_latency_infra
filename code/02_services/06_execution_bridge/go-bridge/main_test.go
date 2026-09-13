package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
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

// P3-247 — a mode=live boot posts to the venue, and "Arrow authentication
// failed" without the cause is unactionable: a DNS failure, bad credentials, a
// skewed TOTP and a contract change all read identically while the bridge is
// down and the operator is reproducing blind.
func TestStartupLoginPreservesTheCause(t *testing.T) {
	cause := &arrow.AuthError{Stage: "login", Err: errors.New("dial tcp: lookup api.arrow.trade: no such host")}
	err := startupLogin(func() error { return cause })
	if err == nil {
		t.Fatal("a failed login must not report success")
	}
	if !strings.Contains(err.Error(), "Arrow authentication failed") {
		t.Errorf("message %q lost the startup context operators grep for", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("message %q does not wrap the cause: network, credential, TOTP and parsing failures read identically", err)
	}
	var authErr *arrow.AuthError
	if !errors.As(err, &authErr) || authErr.Stage != "login" {
		t.Errorf("errors.As on %v did not recover the AuthError stage used for triage", err)
	}
}
