package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestSandboxAutoReauth: fake clock expiry → exactly one re-auth → success;
// repeated failure → /healthz reports UP disabled, no order attempted, no infinite loop.
func TestSandboxAutoReauth(t *testing.T) {
	// Sequence: first Place returns 401 auth failure, re-auth succeeds, retry succeeds.
	t.Run("one_reauth_then_success", func(t *testing.T) {
		calls := 0
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			calls++
			if calls == 1 {
				return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
			}
			return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-REAUTH-1"}
		}}
		reauthCalls := 0
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			reauthCalls++
			return nil
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Outcome != OutcomeSuccess || result.BrokerOrderID != "BRK-REAUTH-1" {
			t.Fatalf("after re-auth: %+v", result)
		}
		if reauthCalls != 1 {
			t.Fatalf("reauthCalls=%d want 1", reauthCalls)
		}
		if calls != 2 {
			t.Fatalf("inner calls=%d want 2 (original + retry)", calls)
		}
		if rb.IsDisabled() {
			t.Fatal("broker should not be disabled after successful re-auth")
		}
		// healthz must still be UP
		server, _ := NewBridgeServer(rb, "test-token", "live")
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		server.Handler().ServeHTTP(rec, req)
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body["status"] != "UP" {
			t.Fatalf("healthz status=%v want UP", body["status"])
		}
	})

	t.Run("reauth_failure_then_disabled", func(t *testing.T) {
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
		}}
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			return errors.New("TOTP refresh failed")
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Outcome != OutcomeUnknown || result.Reason != "broker_disabled" {
			t.Fatalf("after failed re-auth: %+v", result)
		}
		if rb.ReauthCalls() != 1 {
			t.Fatalf("reauthCalls=%d want 1 (no loop)", rb.ReauthCalls())
		}
		if !rb.IsDisabled() {
			t.Fatal("broker should be disabled after re-auth failure")
		}
		// healthz must report UP disabled
		server, _ := NewBridgeServer(rb, "test-token", "live")
		// P3-054: this block used to require 503 from both paths, which is the
		// defect — liveness must keep answering while only readiness fails.
		for _, probe := range []struct {
			path     string
			wantCode int
		}{
			{"/healthz", http.StatusOK},
			{"/readyz", http.StatusServiceUnavailable},
		} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, probe.path, nil)
			server.Handler().ServeHTTP(rec, req)
			var body map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &body)
			if body["status"] != "UP disabled" {
				t.Fatalf("%s status=%v want UP disabled", probe.path, body["status"])
			}
			if rec.Code != probe.wantCode {
				t.Fatalf("%s code=%d want %d", probe.path, rec.Code, probe.wantCode)
			}
		}
	})

	t.Run("second_401_after_reauth_also_disabled", func(t *testing.T) {
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
		}}
		reauthCalls := 0
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			reauthCalls++
			return nil // re-auth succeeds but retry still 401
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Reason != "broker_disabled" {
			t.Fatalf("second 401 should become broker_disabled: %+v", result)
		}
		if reauthCalls != 1 {
			t.Fatalf("reauthCalls=%d want 1 (no second retry)", reauthCalls)
		}
		if !rb.IsDisabled() {
			t.Fatal("should be disabled after retry still returns 401")
		}
	})

	t.Run("non_auth_error_no_reauth", func(t *testing.T) {
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			return BrokerResult{Outcome: OutcomeRejected, Reason: "bad quantity"}
		}}
		reauthCalls := 0
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			reauthCalls++
			return nil
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Outcome != OutcomeRejected {
			t.Fatalf("should pass through rejected: %+v", result)
		}
		if reauthCalls != 0 {
			t.Fatalf("reauthCalls=%d want 0 for non-auth error", reauthCalls)
		}
	})
}

type countingBroker struct {
	fn func(context.Context, CommandEnvelope) BrokerResult
}

func (c *countingBroker) Place(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) Modify(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) Cancel(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) QueryOrder(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) ReconcileOrders(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) ReconcileTrades(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}
func (c *countingBroker) ReconcilePositions(ctx context.Context, ce CommandEnvelope) BrokerResult {
	return c.fn(ctx, ce)
}

// P3-241: a terminal REJECTED must never be retried. sanitizeReason scans the error
// text for retry-shaped words, and for a rejection that text is the broker's own
// free-form message, so a rejection reading "unauthorized symbol for this account"
// used to come back as broker_auth_failure — the exact category doWithReauth treats
// as a reason to re-authenticate and re-submit the command.
//
// Two guards, two tests: the category a rejection carries, and the retry decision
// itself, which must refuse a terminal outcome even when it arrives paired with an
// auth-failure category.
func TestRejectedOutcomeReasonIsNotRetryShaped(t *testing.T) {
	for _, brokerMessage := range []string{
		"unauthorized symbol for this account",
		"request timeout while matching",
		"forbidden by exchange rules",
	} {
		result := rejectedResult(errors.New(brokerMessage))
		if result.Outcome != OutcomeRejected {
			t.Fatalf("fixture: %q produced %s", brokerMessage, result.Outcome)
		}
		switch result.Reason {
		case "broker_auth_failure", "broker_timeout", "broker_forbidden":
			t.Fatalf("terminal rejection %q carries retry-shaped reason %q", brokerMessage, result.Reason)
		}
	}
}

func TestRejectedOutcomesAreNeverRetried(t *testing.T) {
	// The retry decision is independent of how the reason was derived: a rejection
	// paired with the auth-failure category is refused rather than re-submitted.
	paired := NewFakeBroker()
	paired.SetResult(CommandPlace, BrokerResult{Outcome: OutcomeRejected, Reason: "broker_auth_failure"})
	pairedReauthCalls := 0
	pairedBroker := NewReauthBroker(paired, func(context.Context) error {
		pairedReauthCalls++
		return nil
	})
	pairedResult := pairedBroker.Place(context.Background(), validPlaceCommand())
	if pairedReauthCalls != 0 {
		t.Fatalf("re-authenticated %d times for a rejection paired with an auth reason", pairedReauthCalls)
	}
	if pairedResult.Outcome != OutcomeRejected {
		t.Fatalf("paired outcome=%s reason=%s, want REJECTED", pairedResult.Outcome, pairedResult.Reason)
	}
	if calls := paired.Calls(CommandPlace); calls != 1 {
		t.Fatalf("broker called %d times for a terminal rejection, want 1", calls)
	}

	// End to end with the real derivation: a broker whose rejection message reads
	// like an auth failure is refused once and not re-submitted.
	inner := NewFakeBroker()
	inner.SetResult(CommandPlace, rejectedResult(errors.New("unauthorized symbol for this account")))
	reauthCalls := 0
	broker := NewReauthBroker(inner, func(context.Context) error {
		reauthCalls++
		return nil
	})
	result := broker.Place(context.Background(), validPlaceCommand())
	if calls := inner.Calls(CommandPlace); calls != 1 {
		t.Fatalf("broker called %d times for a terminal rejection, want 1", calls)
	}
	if reauthCalls != 0 {
		t.Fatalf("re-authenticated %d times for a terminal rejection", reauthCalls)
	}
	if result.Outcome != OutcomeRejected {
		t.Fatalf("outcome=%s reason=%s, want REJECTED", result.Outcome, result.Reason)
	}
}

// P3-048/P3-049 — a disabled broker is fail-closed for commands, not just for
// /healthz. Pre-fix the wrapper ran the inner broker first and consulted the
// disabled flag only once a re-auth was already due, so a bridge that had
// already given up kept sending orders to the venue and kept re-authenticating
// on every later broker_auth_failure.
func TestDisabledBrokerRefusesCommandsWithoutContactingTheVenue(t *testing.T) {
	calls := 0
	inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		calls++
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
	}}
	reauthCalls := 0
	rb := NewReauthBroker(inner, func(ctx context.Context) error {
		reauthCalls++
		return errors.New("TOTP refresh failed")
	})

	// The first command drives the broker into the disabled state.
	if first := rb.Place(t.Context(), validPlaceCommand()); first.Reason != "broker_disabled" {
		t.Fatalf("first command after failed re-auth: %+v want broker_disabled", first)
	}
	if !rb.IsDisabled() {
		t.Fatal("broker should be disabled after a failed re-auth")
	}
	venueCalls, reauths := calls, reauthCalls

	second := rb.Place(t.Context(), validPlaceCommand())
	if second.Outcome != OutcomeUnknown || second.Reason != "broker_disabled" {
		t.Fatalf("disabled bridge served a command: %+v want UNKNOWN/broker_disabled", second)
	}
	if calls != venueCalls {
		t.Fatalf("disabled bridge reached the venue: inner calls %d -> %d", venueCalls, calls)
	}
	if reauthCalls != reauths {
		t.Fatalf("disabled bridge re-authenticated again: reauth calls %d -> %d", reauths, reauthCalls)
	}
}

// P3-049 second half — the disabled flag can be set by another request while a
// command is in flight. The re-auth decision must re-read it, or a bridge that
// has already given up still spends a TOTP re-auth on the corpse of an old
// request.
func TestDisabledSetDuringTheCallStopsReauth(t *testing.T) {
	reauthCalls := 0
	var rb *ReauthBroker
	inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		// Another in-flight request gives up while this one waits on the venue.
		rb.markDisabled()
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
	}}
	rb = NewReauthBroker(inner, func(ctx context.Context) error {
		reauthCalls++
		return nil
	})

	result := rb.Place(t.Context(), validPlaceCommand())
	if result.Outcome != OutcomeUnknown || result.Reason != "broker_disabled" {
		t.Fatalf("command admitted while the broker was disabled mid-flight: %+v want UNKNOWN/broker_disabled", result)
	}
	if reauthCalls != 0 {
		t.Fatalf("re-authenticated %d times after the broker was disabled mid-flight, want 0", reauthCalls)
	}
}

// P3-261 — a caller that has already gone must not cost a TOTP re-auth and a
// second venue round trip. The production re-auth closure (main.go) ignores ctx
// entirely, so this is the only layer that can enforce it.
func TestExpiredContextSkipsReauthAndRetry(t *testing.T) {
	calls := 0
	inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		calls++
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
	}}
	reauthCalls := 0
	rb := NewReauthBroker(inner, func(ctx context.Context) error {
		reauthCalls++
		return nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // the caller disconnected before the first attempt even returned

	result := rb.Place(ctx, validPlaceCommand())
	if reauthCalls != 0 {
		t.Fatalf("re-authenticated %d times for a caller that had already gone, want 0", reauthCalls)
	}
	if calls != 1 {
		t.Fatalf("venue calls=%d want 1 (no retry with a dead context)", calls)
	}
	if result.Reason != "broker_auth_failure" {
		t.Fatalf("result=%+v want the original broker_auth_failure surfaced", result)
	}
}

// P3-261 second half — the context can also die while the re-auth itself runs
// (a TOTP round trip is slow). Retrying then spends a venue call nobody is
// waiting for, even though the re-auth genuinely happened.
func TestContextDeadAfterReauthSkipsTheRetry(t *testing.T) {
	calls := 0
	inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		calls++
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reauthCalls := 0
	rb := NewReauthBroker(inner, func(ctx context.Context) error {
		reauthCalls++
		cancel() // the caller gives up while the token is being refreshed
		return nil
	})

	result := rb.Place(ctx, validPlaceCommand())
	if reauthCalls != 1 {
		t.Fatalf("reauth calls=%d want 1 (the token was still refreshed)", reauthCalls)
	}
	if calls != 1 {
		t.Fatalf("venue calls=%d want 1 (no retry after the caller died)", calls)
	}
	if result.Reason != "broker_auth_failure" {
		t.Fatalf("result=%+v want the original broker_auth_failure surfaced", result)
	}
	if rb.IsDisabled() {
		t.Fatal("a caller that went away must not disable the broker")
	}
}

// P3-050/P3-262 — the SDK client is shared, so N concurrent broker_auth_failure
// results must not each run their own TOTP AutoLogin: that churns the token,
// risks rate-limiting, and contradicts the "calls reauth once" contract.
func TestConcurrentAuthFailuresShareOneReauth(t *testing.T) {
	const workers = 8
	ctx := t.Context()

	// The venue rejects every command until the token has been refreshed.
	var mu sync.Mutex
	refreshed := false
	inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		mu.Lock()
		ok := refreshed
		mu.Unlock()
		if !ok {
			return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
		}
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-SHARED-1"}
	}}

	// The re-auth is deliberately slow so that parallel AutoLogins would overlap.
	reauthCalls, inFlight, maxInFlight := 0, 0, 0
	rb := NewReauthBroker(inner, func(ctx context.Context) error {
		mu.Lock()
		reauthCalls++
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		refreshed = true
		inFlight--
		mu.Unlock()
		return nil
	})

	start := make(chan struct{})
	results := make([]BrokerResult, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = rb.Place(ctx, validPlaceCommand())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, res := range results {
		if res.Outcome != OutcomeSuccess {
			t.Fatalf("worker %d: %+v want SUCCESS once the shared token was refreshed", i, res)
		}
	}
	if reauthCalls != 1 {
		t.Fatalf("re-authenticated %d times for %d concurrent auth failures, want 1", reauthCalls, workers)
	}
	if maxInFlight != 1 {
		t.Fatalf("max concurrent re-auths=%d want 1 (AutoLogin must never run in parallel)", maxInFlight)
	}
}

// P3-263 — a retry that still sees 401 describes the token *it* was issued
// with. If a newer re-auth landed while that retry was in flight, the 401 says
// nothing about the fresh token, and latching disabled here would halt a
// healthy bridge until the process is restarted.
func TestRetryAuthFailureFromAStaleTokenDoesNotDisable(t *testing.T) {
	calls := 0
	var rb *ReauthBroker
	inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		calls++
		if calls == 2 {
			// Another request completes a newer re-auth while this retry is in
			// flight, so the 401 below is judged against a superseded token.
			rb.mu.Lock()
			rb.gen++
			rb.mu.Unlock()
		}
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
	}}
	rb = NewReauthBroker(inner, func(ctx context.Context) error { return nil })

	result := rb.Place(t.Context(), validPlaceCommand())
	if rb.IsDisabled() {
		t.Error("a 401 from a superseded token disabled a broker that holds a fresh one")
	}
	if result.Reason != "broker_auth_failure" {
		t.Fatalf("result=%+v want the retry's own auth failure returned", result)
	}
	if calls != 2 {
		t.Fatalf("inner calls=%d want 2 (original + one retry)", calls)
	}
}

// P3-054 — a disabled broker is intentionally alive but not ready. Failing
// liveness restarts a bridge whose only problem is a TOTP that the restart
// cannot restore, while /readyz already carries the "no commands" verdict.
func TestDisabledBrokerStaysLiveAndOnlyFailsReadiness(t *testing.T) {
	inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
	}}
	rb := NewReauthBroker(inner, func(ctx context.Context) error {
		return errors.New("totp rejected")
	})
	_ = rb.Place(t.Context(), validPlaceCommand())
	if !rb.IsDisabled() {
		t.Fatal("setup: the broker should be disabled after the re-auth failure")
	}
	server, err := NewBridgeServer(rb, "test-token", "live")
	if err != nil {
		t.Fatalf("NewBridgeServer: %v", err)
	}

	for _, tc := range []struct {
		path     string
		wantCode int
	}{
		{"/healthz", http.StatusOK},                // alive: the process can still serve and recover
		{"/readyz", http.StatusServiceUnavailable}, // not ready: no broker commands without a token
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		server.Handler().ServeHTTP(rec, req)
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body["status"] != "UP disabled" {
			t.Errorf("%s status=%v want UP disabled", tc.path, body["status"])
		}
		if rec.Code != tc.wantCode {
			t.Errorf("%s code=%d want %d (body=%s)", tc.path, rec.Code, tc.wantCode, rec.Body.String())
		}
		if _, present := body["ready"]; present != (tc.path == "/readyz") {
			t.Errorf("%s ready key present=%v, want present only on /readyz", tc.path, present)
		}
		if tc.path == "/readyz" && body["ready"] != false {
			t.Errorf("%s ready=%v want false", tc.path, body["ready"])
		}
	}
}
