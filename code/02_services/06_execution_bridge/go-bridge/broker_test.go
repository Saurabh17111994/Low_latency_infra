package main

import (
	"context"
	"errors"
	"testing"
)

func TestSDKErrorClassificationNeverTurnsAmbiguityIntoSuccess(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		outcome string
	}{
		{name: "documented rejection", err: errors.New(`request failed with status 400: {"status":"error","message":"bad quantity"}`), outcome: OutcomeRejected},
		{name: "auth", err: errors.New(`request failed with status 401: {"status":"error"}`), outcome: OutcomeUnknown},
		{name: "timeout", err: errors.New(`request failed with status 408: timeout`), outcome: OutcomeUnknown},
		{name: "server", err: errors.New(`request failed with status 500: failed`), outcome: OutcomeUnknown},
		{name: "malformed", err: errors.New("order placement failed"), outcome: OutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifySDKError(test.err)
			if got.Outcome != test.outcome {
				t.Fatalf("outcome=%s, want %s (%+v)", got.Outcome, test.outcome, got)
			}
			if got.Outcome == OutcomeSuccess {
				t.Fatal("ambiguous SDK error became success")
			}
		})
	}
}

// P3-036/P3-032 (Place) and P3-038/P3-033 (Modify): Broker is exported and is
// wrapped by ReauthBroker, so a direct caller can hand the method a
// CommandEnvelope with no Order — the HTTP path's validateCommand does not
// protect that caller. The method must stay total: a terminal REJECTED result,
// never a nil dereference that takes the whole bridge process down.
func TestArrowBrokerNilOrderIsTerminalRejection(t *testing.T) {
	// No SDK client is needed: the guard must fire before any client use.
	broker := &ArrowBroker{}

	t.Run("place", func(t *testing.T) {
		got := broker.Place(context.Background(), CommandEnvelope{ClientOrderRef: "ref-1"})
		if got.Outcome != OutcomeRejected {
			t.Fatalf("Place(nil order) outcome=%s want %s (%+v)", got.Outcome, OutcomeRejected, got)
		}
	})

	t.Run("cancelled context still wins", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := broker.Place(ctx, CommandEnvelope{}); got.Outcome != OutcomeUnknown {
			t.Fatalf("Place(cancelled ctx, nil order) outcome=%s want %s", got.Outcome, OutcomeUnknown)
		}
	})
}

func TestFakeBrokerRecordsOnlyOneAttempt(t *testing.T) {
	fake := NewFakeBroker()
	fake.SetResult(CommandPlace, BrokerResult{Outcome: OutcomeUnknown, Reason: "ambiguous_broker_response"})
	command := validPlaceCommand()
	first := fake.Place(t.Context(), command)
	if first.Outcome != OutcomeUnknown || fake.Calls(CommandPlace) != 1 {
		t.Fatalf("first=%+v calls=%d", first, fake.Calls(CommandPlace))
	}
	// The bridge deliberately has no retry loop. A second call is only made by
	// an explicit caller after reconciliation, never internally.
	if fake.Calls(CommandPlace) != 1 {
		t.Fatal("fake broker call count changed without an explicit command")
	}
}
