package main

import (
	"context"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// TestSubscriptionPlanShards3000Tokens — T1: a 3000-instrument manifest shards
// deterministically into 3 slots of 1024+1024+952. The slot set is stable
// across builds (same fingerprint) and token→slot identity (SlotForToken)
// matches the contiguous ranges exactly.
func TestSubscriptionPlanShards3000Tokens(t *testing.T) {
	tokens := make([]int32, 3000)
	for i := range tokens {
		tokens[i] = int32(1000 + i)
	}
	plan, err := BuildSubscriptionPlan(tokens, 3, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatalf("BuildSubscriptionPlan(3000, 3) failed: %v", err)
	}
	if len(plan.Slots) != 3 {
		t.Fatalf("want 3 slots, got %d", len(plan.Slots))
	}
	want := []int{1024, 1024, 952}
	for i, slot := range plan.Slots {
		if len(slot.Tokens) != want[i] {
			t.Fatalf("slot %d: want %d tokens, got %d", i, want[i], len(slot.Tokens))
		}
		if slot.SlotID != "hft-"+string(rune('0'+i)) || slot.ConnectionID != "hft-"+string(rune('0'+i)) {
			t.Fatalf("slot %d: unexpected identity %q/%q", i, slot.SlotID, slot.ConnectionID)
		}
		// Deterministic contiguous range: strictly ascending tokens.
		for j := 1; j < len(slot.Tokens); j++ {
			if slot.Tokens[j] <= slot.Tokens[j-1] {
				t.Fatalf("slot %d tokens not strictly ascending at %d", i, j)
			}
		}
		for _, req := range slot.Requests {
			if len(req) == 0 || len(req) > MaxHFTTokensPerRequest {
				t.Fatalf("slot %d: request batch size %d out of range", i, len(req))
			}
		}
	}
	// Coverage: the slot union is exactly the input set, no overlap.
	seen := map[int32]bool{}
	for _, slot := range plan.Slots {
		for _, tok := range slot.Tokens {
			if seen[tok] {
				t.Fatalf("token %d assigned to multiple slots", tok)
			}
			seen[tok] = true
		}
	}
	if len(seen) != 3000 {
		t.Fatalf("plan covers %d tokens, want 3000", len(seen))
	}
	// Determinism: rebuilding even from shuffled input yields the same plan.
	shuffled := append([]int32(nil), tokens...)
	for i := len(shuffled) - 1; i > 0; i-- {
		j := (i * 7919) % (i + 1) // deterministic shuffle, no rand
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	plan2, err := BuildSubscriptionPlan(shuffled, 3, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}
	if plan2.Fingerprint != plan.Fingerprint {
		t.Fatalf("fingerprint not deterministic: %s vs %s", plan2.Fingerprint, plan.Fingerprint)
	}
	// SlotForToken agrees with the contiguous ranges, including boundaries.
	checks := []struct {
		token int32
		want  int
	}{
		{1000, 0}, {2023, 0}, {2024, 1}, {3047, 1}, {3048, 2}, {3999, 2},
		{999, -1}, {4000, -1},
	}
	for _, c := range checks {
		if got := SlotForToken(plan, c.token); got != c.want {
			t.Fatalf("SlotForToken(%d) = %d, want %d", c.token, got, c.want)
		}
	}
}

// TestSupervisorAuthTerminalIsolatedPerSlot — T1 kill-1-slot test: 3000-token
// plan (3 slots 1024+1024+952); slot 0's shared-token refresh fails all three
// attempts and must go TERMINAL while slots 1 and 2 stay healthy — shared
// context NOT cancelled, peer sockets NOT closed, supervisor reports exactly
// one terminal outcome.
func TestSupervisorAuthTerminalIsolatedPerSlot(t *testing.T) {
	tokens := make([]int32, 3000)
	for i := range tokens {
		tokens[i] = int32(1000 + i)
	}
	plan, err := BuildSubscriptionPlan(tokens, 3, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatalf("BuildSubscriptionPlan failed: %v", err)
	}

	// slot 0: subscribes, then the broker hits it with an auth-class read error
	// every epoch; the shared refreshAuth keeps failing → 3 attempts → terminal.
	authBurn := newFakeHFTStream()
	authBurn.ackCh = make(chan arrow.HFTResponsePacket, 8)
	authBurn.readErr = errFakeAuth

	const peer1Token, peer2Token = 2024, 3048
	healthyPeer1 := newFakeHFTStream()
	healthyPeer1.ackCh = make(chan arrow.HFTResponsePacket, 8)
	healthyPeer1.onFullTick = arrow.HFTFullTick{Token: peer1Token, LTP: 15200}
	healthyPeer2 := newFakeHFTStream()
	healthyPeer2.ackCh = make(chan arrow.HFTResponsePacket, 8)
	healthyPeer2.onFullTick = arrow.HFTFullTick{Token: peer2Token, LTP: 15300}

	makeFactory := func(_ *arrow.Client, slotIdx int) func() (hftStream, error) {
		switch slotIdx {
		case 0:
			return func() (hftStream, error) { return authBurn, nil }
		case 1:
			return func() (hftStream, error) { return healthyPeer1, nil }
		default:
			return func() (hftStream, error) { return healthyPeer2, nil }
		}
	}

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	ctx, cancel := context.WithCancel(context.Background())
	oldEmitter := bridgeEmitter
	capture := newSyncBuffer()
	bridgeEmitter = newTestProtoEmitter(capture)
	defer func() { bridgeEmitter = oldEmitter }()

	terminal := make(chan int, 1)
	go func() {
		terminal <- runHFTSupervisorWithFactory(ctx, makeFactory, client, plan, 50, 10*time.Second,
			func(context.Context) error { return errFakeAuth }, t.Logf)
	}()

	// Slot 0 burns 3 refresh attempts across reconnect epochs (backoffs 1s+2s
	// after attempts 1-2) and must reach TERMINAL on its own.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if containsAny(eventsAsStrings(t, capture.String()), "event=auth_failure") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	flushTestEmitter()
	out := capture.String()
	if !containsAny(eventsAsStrings(t, out), "event=auth_failure") {
		t.Fatalf("slot 0 must go terminal after 3 failed auth refreshes\n%s", out)
	}
	// Isolation assertions, while the other two slots are still running:
	// 1. The shared context was NOT cancelled by the bridge.
	if ctx.Err() != nil {
		t.Fatalf("shared context cancelled by the bridge: %v", ctx.Err())
	}
	// 2. Peer sockets are open and their epochs are still alive.
	if healthyPeer1.IsClosed() || healthyPeer2.IsClosed() {
		t.Fatal("peer streams must stay open while slot 0 is terminal")
	}
	// 3. The auth_failure belongs to slot hft-0 only.
	for _, e := range eventsFrom(t, out) {
		if e["event"] == "auth_failure" && e["slot_id"] != "hft-0" {
			t.Fatalf("auth_failure on non-slot-0 (%v)\n%s", e["slot_id"], out)
		}
	}
	// 4. Both peers reached ACTIVE with ticks flowing.
	if countState(t, out, "ACTIVE") < 2 {
		t.Fatalf("both peers must reach ACTIVE\n%s", out)
	}
	if !containsAny(ticksFrom(t, out), "token=2024") || !containsAny(ticksFrom(t, out), "token=3048") {
		t.Fatalf("peer ticks must flow while slot 0 is terminal\n%s", out)
	}
	// 5. The supervisor keeps waiting on the healthy peers (did not collapse).
	select {
	case n := <-terminal:
		t.Fatalf("supervisor returned %d terminal while peers still up", n)
	case <-time.After(300 * time.Millisecond):
	}

	// Shut down cleanly; exactly one slot ended terminal.
	cancel()
	if got := <-terminal; got != 1 {
		t.Fatalf("supervisor terminal count = %d, want 1\n%s", got, capture.String())
	}
}
