package main

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// K1 (2026-08-29): BRIDGE_BATCH_* env knobs. Unset = O-2 defaults; valid
// values override; invalid values are a fatal startup error (exit status 2,
// the file's exitFatalStart contract).
func TestBatchLimitsFromEnvDefaults(t *testing.T) {
	for _, k := range []string{"BRIDGE_BATCH_MAX_AGE_MS", "BRIDGE_BATCH_MAX_EVENTS", "BRIDGE_BATCH_MAX_BYTES"} {
		os.Unsetenv(k)
	}
	got := batchLimitsFromEnv(func(string, ...any) { t.Fatal("no fatal expected") })
	def := DefaultBatchLimits()
	if got != def {
		t.Fatalf("unset env must yield defaults: got %+v want %+v", got, def)
	}
}

func TestBatchLimitsFromEnvOverrides(t *testing.T) {
	t.Setenv("BRIDGE_BATCH_MAX_AGE_MS", "5")
	t.Setenv("BRIDGE_BATCH_MAX_EVENTS", "100")
	t.Setenv("BRIDGE_BATCH_MAX_BYTES", "4096")
	got := batchLimitsFromEnv(func(string, ...any) { t.Fatal("no fatal expected") })
	if got.MaxAge != 5*time.Millisecond {
		t.Errorf("MaxAge = %v, want 5ms", got.MaxAge)
	}
	if got.MaxEvents != 100 {
		t.Errorf("MaxEvents = %d, want 100", got.MaxEvents)
	}
	if got.MaxBytes != 4096 {
		t.Errorf("MaxBytes = %d, want 4096", got.MaxBytes)
	}
}

// TestBatchLimitsFromEnvFatalOnInvalid proves invalid values exit FATAL
// (status 2) via the exec-self subprocess pattern — os.Exit cannot be
// tested in-process (same approach as hft_policy_test.go).
func TestBatchLimitsFromEnvFatalOnInvalid(t *testing.T) {
	if os.Getenv("GO_BATCH_HELPER") == "1" {
		os.Setenv(os.Getenv("GO_BATCH_KEY"), os.Getenv("GO_BATCH_VALUE"))
		batchLimitsFromEnv(func(string, ...any) {})
		return
	}
	cases := []struct{ key, value string }{
		{"BRIDGE_BATCH_MAX_AGE_MS", "0"},   // below min
		{"BRIDGE_BATCH_MAX_AGE_MS", "abc"}, // non-integer
		{"BRIDGE_BATCH_MAX_EVENTS", "-1"},
		{"BRIDGE_BATCH_MAX_BYTES", "10"}, // below min 1024
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.value, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=TestBatchLimitsFromEnvFatalOnInvalid")
			cmd.Env = append(os.Environ(), "GO_BATCH_HELPER=1",
				"GO_BATCH_KEY="+c.key, "GO_BATCH_VALUE="+c.value)
			out, err := cmd.CombinedOutput()
			ee, ok := err.(*exec.ExitError)
			if !ok || ee.ExitCode() != 2 {
				t.Fatalf("%s=%s: exit=%v want status 2; output=%s",
					c.key, c.value, err, out)
			}
		})
	}
}
