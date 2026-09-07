package main

import (
	"errors"
	"testing"
)

// TestClassifyAuthRefresh — plan §hft_slot.go: bounded authentication refresh
// (3 attempts per slot failure episode), terminal after exhaustion.
func TestClassifyAuthRefresh(t *testing.T) {
	// No refresh function → terminal immediately.
	if got := classifyAuthRefresh(false, 0, errors.New("unauthorized")); got != authTerminalExhausted {
		t.Fatalf("no refresh fn: got %v, want authTerminalExhausted", got)
	}

	// R-023 regression: NO refresh function with a nil error (a token-only
	// deployment where refreshAuth == nil keeps refreshErr nil) must be
	// terminal — the old code returned authResumed and retried forever.
	if got := classifyAuthRefresh(false, 0, nil); got != authTerminalExhausted {
		t.Fatalf("no refresh fn, nil err (R-023): got %v, want authTerminalExhausted", got)
	}

	// Refresh succeeds → resume.
	if got := classifyAuthRefresh(true, 0, nil); got != authResumed {
		t.Fatalf("successful refresh: got %v, want authResumed", got)
	}

	// First failure (prior count 0) → retry.
	if got := classifyAuthRefresh(true, 0, errors.New("unauthorized")); got != authRetry {
		t.Fatalf("first failure: got %v, want authRetry", got)
	}
	// Second failure (prior count 1) → retry.
	if got := classifyAuthRefresh(true, 1, errors.New("unauthorized")); got != authRetry {
		t.Fatalf("second failure: got %v, want authRetry", got)
	}
	// Third failure (prior count 2) → terminal.
	if got := classifyAuthRefresh(true, 2, errors.New("unauthorized")); got != authTerminal {
		t.Fatalf("third failure: got %v, want authTerminal", got)
	}
	// Already exhausted (prior count 3) → terminal exhausted.
	if got := classifyAuthRefresh(true, 3, errors.New("unauthorized")); got != authTerminalExhausted {
		t.Fatalf("exhausted: got %v, want authTerminalExhausted", got)
	}

	// P1-028: an exhausted budget with no attempt this round reaches the
	// classifier as (hasRefresh=false, nil err) — the caller folds budget
	// state into hasRefresh — and must be terminal, never authResumed.
	// Without the call-site fold this input arrives as (true, 2, nil) and
	// wrongly resumes.
	if got := classifyAuthRefresh(false, 2, nil); got != authTerminalExhausted {
		t.Fatalf("exhausted budget, nil err (P1-028): got %v, want authTerminalExhausted", got)
	}
}

// TestClassifySubscriptionResponse — plan §Integrations: SUCCESS with
// success_count == requested and zero errors is accepted; all-invalid /
// parameter errors are terminal; any other partial outcome is rejected.
func TestClassifySubscriptionResponse(t *testing.T) {
	cases := []struct {
		name               string
		code               string
		success, errs, req int
		want               subscriptionResponseOutcome
	}{
		{"full success", "SUCCESS", 512, 0, 512, subAccepted},
		{"partial", "SUCCESS", 400, 112, 512, subPartial},
		{"nonzero error count", "SUCCESS", 500, 12, 512, subPartial},
		{"all invalid", "E_ALL_INVALID", 0, 512, 512, subTerminal},
		{"invalid json", "E_INVALID_JSON", 0, 512, 512, subTerminal},
		{"missing field", "E_MISSING_FIELD", 0, 512, 512, subTerminal},
		{"invalid param", "E_INVALID_PARAM", 0, 512, 512, subTerminal},
		{"unknown error code", "E_INTERNAL", 0, 512, 512, subPartial},
	}
	for _, c := range cases {
		if got := classifySubscriptionResponse(c.code, c.success, c.errs, c.req); got != c.want {
			t.Fatalf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
