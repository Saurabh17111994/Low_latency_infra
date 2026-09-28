package main

import (
	"context"
	"io"
	"testing"
	"time"
)

// H2-3: the bridge must tell the truth about why it ended — a terminal runtime failure
// exits 3, a requested stop (SIGINT/SIGTERM) stays 0. The exit-code decision and the
// policy-violation terminal outcome are the two halves of that contract.

func TestBridgeExitCodeMapping(t *testing.T) {
	cases := []struct {
		name     string
		terminal bool
		signal   bool
		want     int
	}{
		{"clean run", false, false, exitRequested},
		{"requested stop", false, true, exitRequested},
		{"terminal but signal requested", true, true, exitRequested},
		{"terminal runtime failure", true, false, exitTerminalRuntime},
	}
	for _, c := range cases {
		if got := bridgeExitCode(c.terminal, c.signal); got != c.want {
			t.Fatalf("%s: bridgeExitCode(%v,%v)=%d want %d",
				c.name, c.terminal, c.signal, got, c.want)
		}
	}
	if exitTerminalRuntime == exitFatalStart || exitTerminalRuntime == exitRequested {
		t.Fatalf("exitTerminalRuntime=%d must be distinct from the startup/requested codes",
			exitTerminalRuntime)
	}
}

func TestPolicyViolationIsATerminalRuntimeOutcome(t *testing.T) {
	// Extra sockets without the approval pair must end the runtime, not look like a clean stop.
	t.Setenv("ARROW_HFT_MULTI_CONNECTION_APPROVED", "")
	t.Setenv("DEPLOYMENT_ENV", "")
	old := bridgeEmitter
	bridgeEmitter = newTestProtoEmitter(io.Discard)
	defer func() { bridgeEmitter = old }()

	plan := SubscriptionPlan{Slots: []SlotAssignment{{SlotID: "hft-0"}, {SlotID: "hft-1"}}}
	cancelled := false
	terminal := runHFT(context.Background(), func() { cancelled = true }, nil, plan, 50,
		time.Second, nil, noopLogf)
	if !terminal {
		t.Fatal("a single-socket policy violation must be terminal for the runtime")
	}
	if !cancelled {
		t.Fatal("the policy violation must cancel the run")
	}
}
