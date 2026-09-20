package main

import (
	"os"
	"path/filepath"
	"testing"
)

// CHG-249: Docker and Swarm secrets arrive as FILES. These tests pin the contract the
// ingestion stack relies on — the file form wins, the plain variable still works, an
// absent credential is empty rather than an error, and a malformed file is loud.

func TestSecretFromEnvPrefersTheFileForm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arrow_app_secret")
	if err := os.WriteFile(path, []byte("from-file\n"), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	t.Setenv("TEST_ARROW_SECRET", "plain-loses")
	t.Setenv("TEST_ARROW_SECRET_FILE", path)

	got, err := secretFromEnv("TEST_ARROW_SECRET")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "from-file" {
		t.Fatalf("want the file value, got %q", got)
	}
}

func TestSecretFromEnvFallsBackToThePlainVariable(t *testing.T) {
	t.Setenv("TEST_ARROW_SECRET", "plain-wins")
	t.Setenv("TEST_ARROW_SECRET_FILE", "")

	got, err := secretFromEnv("TEST_ARROW_SECRET")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "plain-wins" {
		t.Fatalf("want the plain value, got %q", got)
	}
}

func TestSecretFromEnvTreatsAnAbsentCredentialAsEmpty(t *testing.T) {
	t.Setenv("TEST_ARROW_SECRET", "")
	t.Setenv("TEST_ARROW_SECRET_FILE", "")

	got, err := secretFromEnv("TEST_ARROW_SECRET")
	if err != nil {
		t.Fatalf("absent credentials must not error: %v", err)
	}
	if got != "" {
		t.Fatalf("want empty, got %q", got)
	}
}

func TestSecretFromEnvIsLoudOnMalformedFiles(t *testing.T) {
	dir := t.TempDir()

	t.Setenv("TEST_ARROW_SECRET_FILE", filepath.Join(dir, "does-not-exist"))
	if _, err := secretFromEnv("TEST_ARROW_SECRET"); err == nil {
		t.Fatal("an unreadable secret file must be an error, not a silent fallback")
	}

	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("   \n"), 0o600); err != nil {
		t.Fatalf("write empty secret file: %v", err)
	}
	t.Setenv("TEST_ARROW_SECRET_FILE", empty)
	if _, err := secretFromEnv("TEST_ARROW_SECRET"); err == nil {
		t.Fatal("an empty secret file must be an error, not a silent fallback")
	}
}
