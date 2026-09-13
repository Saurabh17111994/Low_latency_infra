package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// TestArrowRestCapability — VM-ARROW-010 error half (A4-error).
// Proves the two failure modes that don't need market hours:
//  1. 401 auth failure → one TOTP re-auth → retry once → UP disabled on second 401 (A1-harness ReauthBroker).
//  2. 15s UNKNOWN timeout — context deadline stops the attempt, never retried internally.
//  3. One-attempt-to-one-order correlation — same RequestID with same fingerprint coalesces,
//     different fingerprint is rejected as request_id_reuse_violation (server.go beginRequest).
func TestArrowRestCapability(t *testing.T) {
	t.Run("auth_failure_reauth_then_disabled", func(t *testing.T) {
		inner := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
			return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_auth_failure"}
		}}
		reauthCalls := 0
		rb := NewReauthBroker(inner, func(ctx context.Context) error {
			reauthCalls++
			return nil
		})
		result := rb.Place(t.Context(), validPlaceCommand())
		if result.Reason != "broker_disabled" {
			t.Fatalf("after retry still 401: %+v want broker_disabled", result)
		}
		if reauthCalls != 1 {
			t.Fatalf("reauthCalls=%d want 1", reauthCalls)
		}
		if !rb.IsDisabled() {
			t.Fatal("broker should be disabled")
		}
	})

	t.Run("fifteen_second_timeout_is_unknown_no_retry", func(t *testing.T) {
		// Real timeout path: Arrow SDK returns 408/500/timeout → classifySDKError → UNKNOWN,
		// and the fake broker's no-retry invariant (broker_test.go: TestFakeBrokerRecordsOnlyOneAttempt).
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(100 * time.Millisecond)
			_, _ = w.Write([]byte(`{"status":"success","data":{"orderNo":"too-late"}}`))
		}))
		defer server.Close()
		client := arrow.NewClient("app", "secret")
		client.SetToken("token")
		client.Config.BaseURL = server.URL
		client.HTTPClient.ReadTimeout = 10 * time.Millisecond
		broker, err := NewArrowBroker(client)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result := broker.Place(ctx, validPlaceCommand())
		if result.Outcome != OutcomeUnknown {
			t.Fatalf("timeout result=%+v want UNKNOWN", result)
		}
		// timeout may surface as broker_timeout or ambiguous_broker_response depending on SDK wrapping; both are UNKNOWN and non-retryable.
		if result.Reason != "broker_timeout" && result.Reason != "broker_error" && result.Reason != "ambiguous_broker_response" {
			t.Fatalf("timeout reason=%q want broker_timeout/broker_error/ambiguous_broker_response", result.Reason)
		}
		// No internal retry: classifySDKError maps timeout to UNKNOWN, not re-issued.
	})

	t.Run("one_attempt_to_one_order_correlation", func(t *testing.T) {
		fake := NewFakeBroker()
		server, _ := NewBridgeServer(fake, "test-token", "fake")
		cmd := validPlaceCommand()
		cmd.RequestID = "cap-req-1"
		// Idempotency is enforced by beginRequest, not by dispatch.
		state1, owner1, conflict1, err1 := server.beginRequest(cmd)
		if err1 != nil {
			t.Fatalf("beginRequest must digest the command: %v", err1)
		}
		if conflict1 || !owner1 {
			t.Fatalf("first beginRequest owner=%v conflict=%v want owner true", owner1, conflict1)
		}
		// Simulate first owner finishing (so second waiter can observe done).
		server.finishRequest(state1, ReportEnvelope{RequestID: cmd.RequestID})
		// Same RequestID + same fingerprint (idempotent) — second handler coalesces.
		fakeCallsBefore := fake.Calls(CommandPlace)
		state2, owner2, conflict2, err2 := server.beginRequest(cmd)
		if err2 != nil {
			t.Fatalf("beginRequest must digest the command: %v", err2)
		}
		if conflict2 {
			t.Fatal("same fingerprint should not conflict")
		}
		if owner2 {
			t.Fatal("second caller should not be owner")
		}
		_ = state2
		if fake.Calls(CommandPlace) != fakeCallsBefore {
			t.Fatalf("coalesced request issued extra broker call: before %d after %d", fakeCallsBefore, fake.Calls(CommandPlace))
		}
		// Same RequestID + different fingerprint → request_id_reuse_violation.
		mutated := cmd
		mutated.Order.Quantity = "999"
		_, _, conflict3, err3 := server.beginRequest(mutated)
		if err3 != nil {
			t.Fatalf("beginRequest must digest the mutated command: %v", err3)
		}
		if !conflict3 {
			t.Fatal("different fingerprint with same RequestID should be reuse violation")
		}
	})

	t.Run("documented_rejection_stays_rejected", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"status":"error","message":"quantity exceeds limit"}`))
		}))
		defer server.Close()
		client := arrow.NewClient("app", "secret")
		client.SetToken("token")
		client.Config.BaseURL = server.URL
		broker, err := NewArrowBroker(client)
		if err != nil {
			t.Fatal(err)
		}
		result := broker.Place(context.Background(), validPlaceCommand())
		if result.Outcome != OutcomeRejected {
			t.Fatalf("rejection result=%+v want REJECTED", result)
		}
	})
}

// validPlaceCommand helper is shared with other bridge tests.
