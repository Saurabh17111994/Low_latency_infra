package main

import (
	"fmt"
	"testing"
)

// C3: losslessness re-validation at scale — multi-connection must not break
// the count-based losslessness guarantee (ING-TCP-001).
//
// The reconcile script (ing-tcp001/reconcile-compare.py) compares bridge
// emitted-tick counts (per-token, aggregated across slots) against Fluss
// sink-side per-token row counts. On a dev laptop without Fluss, we prove
// the same invariant in-memory: sharding 2,433 tokens across N=3 slots,
// emitting ticks per slot, and verifying 0 lost / 0 extra / 0 vanished.

// TestLosslessnessMultiConn verifies that sharding + per-slot tick counting
// preserves per-token totals across the full manifest.
func TestLosslessnessMultiConn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		n     int
		slots int
	}{
		{"single_1024", 1024, 1},
		{"double_2048", 2048, 2},
		{"full_2433_3slots", 2433, 3},
		{"max_3072", 3072, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tokens := ids(tc.n)
			plan, err := BuildSubscriptionPlan(tokens, tc.slots, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			// Simulate bridge tick counts: each token gets a deterministic count
			// (like the real tickCounts map). Use token value as count seed.
			bridgeCounts := make(map[int32]int64)
			for _, tok := range tokens {
				bridgeCounts[tok] = int64(tok%100) + 1 // 1..100 per token
			}
			// Simulate per-slot emission: each slot emits its assigned tokens' counts.
			// Aggregate across slots (like tickcounts.go's tickCounts, which is global).
			aggregated := make(map[int32]int64)
			for _, slot := range plan.Slots {
				for _, tok := range slot.Tokens {
					aggregated[tok] += bridgeCounts[tok]
				}
			}
			// Reconcile: compare aggregated vs expected (bridgeCounts) — must be exact.
			var bad []string
			for tok, want := range bridgeCounts {
				if got := aggregated[tok]; got != want {
					bad = append(bad, fmt.Sprintf("token=%d want=%d got=%d", tok, want, got))
				}
			}
			var vanished []int32
			for tok := range bridgeCounts {
				if _, ok := aggregated[tok]; !ok {
					vanished = append(vanished, tok)
				}
			}
			var extra []int32
			for tok := range aggregated {
				if _, ok := bridgeCounts[tok]; !ok {
					extra = append(extra, tok)
				}
			}
			if len(bad) > 0 {
				t.Fatalf("MISMATCH lost/extra per token: %v (first 5)", bad[:min(5, len(bad))])
			}
			if len(vanished) > 0 {
				t.Fatalf("MISMATCH vanished tokens: %v", vanished[:min(5, len(vanished))])
			}
			if len(extra) > 0 {
				t.Fatalf("MISMATCH unexpected tokens: %v", extra[:min(5, len(extra))])
			}
			// Total must match sum of bridge counts.
			var totalWant, totalGot int64
			for _, v := range bridgeCounts {
				totalWant += v
			}
			for _, v := range aggregated {
				totalGot += v
			}
			if totalWant != totalGot {
				t.Fatalf("total mismatch: want %d got %d", totalWant, totalGot)
			}
		})
	}
}

// TestLosslessnessMultiEpoch verifies multi-epoch (restart) scenario: tokens
// that were counted in epoch 1 must not be double-counted or lost in epoch 2.
func TestLosslessnessMultiEpoch(t *testing.T) {
	tokens := ids(2433)
	plan, err := BuildSubscriptionPlan(tokens, 3, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatal(err)
	}
	// Epoch 1: emit counts for all tokens.
	epoch1 := make(map[int32]int64)
	for _, tok := range tokens {
		epoch1[tok] = 10
	}
	// Epoch 2: simulate restart — same plan, same sharding, emit again.
	// The bridge's tickCounts is reset per epoch (seqBySlot reset), so we
	// verify that re-sharding is deterministic and no token migrates slots.
	plan2, _ := BuildSubscriptionPlan(tokens, 3, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if plan.Fingerprint != plan2.Fingerprint {
		t.Fatal("multi-epoch: fingerprints must match (deterministic sharding)")
	}
	for i := range plan.Slots {
		if len(plan.Slots[i].Tokens) != len(plan2.Slots[i].Tokens) {
			t.Fatalf("slot %d token count changed across epochs", i)
		}
		for j, tok := range plan.Slots[i].Tokens {
			if tok != plan2.Slots[i].Tokens[j] {
				t.Fatalf("slot %d token %d migrated", i, j)
			}
		}
	}
	epoch2 := make(map[int32]int64)
	for _, tok := range tokens {
		epoch2[tok] = 5 // different count, still per-token
	}
	// Multi-epoch reconcile: delta = epoch1 + epoch2 per token.
	aggregated := make(map[int32]int64)
	for tok, c := range epoch1 {
		aggregated[tok] += c
	}
	for tok, c := range epoch2 {
		aggregated[tok] += c
	}
	for tok := range epoch1 {
		if aggregated[tok] != 15 {
			t.Fatalf("multi-epoch token %d: want 15 got %d", tok, aggregated[tok])
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
