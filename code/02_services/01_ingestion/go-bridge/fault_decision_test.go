package main

import (
	"context"
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

// CHG-538/AS-450: the read-loop call site must treat the FINAL allowed
// refresh as a refresh. A success on attempt maxAuthRefreshAttempts resumes;
// only the next (over-budget) error is terminal-exhausted. The old call site
// derived hasRefresh from the post-increment budget and stopped the slot with
// a false authentication_refresh_exhausted on a successful final refresh.
func TestReadLoopAuthRefreshFinalAttemptSuccess(t *testing.T) {
	tries, shared := 0, 0
	ok := func(context.Context) error { return nil }
	for attempt := 1; attempt <= maxAuthRefreshAttempts; attempt++ {
		outcome, err := readLoopAuthRefresh(&tries, &shared, ok, context.Background())
		if outcome != authResumed || err != nil {
			t.Fatalf("attempt %d: got (%v, %v), want (authResumed, nil)",
				attempt, outcome, err)
		}
	}
	if tries != maxAuthRefreshAttempts || shared != maxAuthRefreshAttempts {
		t.Fatalf("budget not synced: tries=%d shared=%d, want %d",
			tries, shared, maxAuthRefreshAttempts)
	}
	outcome, err := readLoopAuthRefresh(&tries, &shared, ok, context.Background())
	if outcome != authTerminalExhausted || err != nil {
		t.Fatalf("over budget: got (%v, %v), want (authTerminalExhausted, nil)",
			outcome, err)
	}
}

// The failing-refresh sequence: attempts 1..max-1 retry, the final failing
// attempt is terminal; the budget is shared across calls.
func TestReadLoopAuthRefreshFailureSequence(t *testing.T) {
	tries := 0
	fail := func(context.Context) error { return errors.New("unauthorized") }
	want := []authRefreshOutcome{authRetry, authRetry, authTerminal}
	for i, w := range want {
		outcome, err := readLoopAuthRefresh(&tries, nil, fail, context.Background())
		if outcome != w || err == nil {
			t.Fatalf("attempt %d: got (%v, %v), want (%v, err)", i+1, outcome, err, w)
		}
	}
}

// A nil refresh function (token-only deployment) is terminal-exhausted.
func TestReadLoopAuthRefreshNilFunction(t *testing.T) {
	tries := 0
	outcome, err := readLoopAuthRefresh(&tries, nil, nil, context.Background())
	if outcome != authTerminalExhausted || err != nil {
		t.Fatalf("nil refresh: got (%v, %v), want (authTerminalExhausted, nil)", outcome, err)
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
