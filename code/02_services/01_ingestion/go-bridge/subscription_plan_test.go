package main

import "testing"

func TestSubscriptionPlanBoundaries(t *testing.T) {
	for _, n := range []int{1, 512, 513, 1024, 1025, 2048, 3000, 3072} {
		tokens := make([]int32, n)
		for i := range tokens {
			tokens[i] = int32(n - i)
		}
		plan, err := BuildSubscriptionPlan(tokens, 3, 1024, 512)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		seen := map[int32]bool{}
		for _, slot := range plan.Slots {
			for _, token := range slot.Tokens {
				if seen[token] {
					t.Fatalf("duplicate %d", token)
				}
				seen[token] = true
			}
		}
		if len(seen) != n {
			t.Fatalf("n=%d got %d", n, len(seen))
		}
	}
	if _, err := BuildSubscriptionPlan(make([]int32, 3073), 3, 1024, 512); err == nil {
		t.Fatal("expected capacity rejection")
	}
	if _, err := BuildSubscriptionPlan([]int32{1, 1}, 1, 1024, 512); err == nil {
		t.Fatal("expected duplicate rejection")
	}
	// R-188: zero and negative tokens must be rejected at plan construction.
	if _, err := BuildSubscriptionPlan([]int32{0, 1}, 1, 1024, 512); err == nil {
		t.Fatal("expected zero-token rejection (R-188)")
	}
	if _, err := BuildSubscriptionPlan([]int32{-5, 1}, 1, 1024, 512); err == nil {
		t.Fatal("expected negative-token rejection (R-188)")
	}
}

// TestSlotForTokenSparseGaps — P1-029: sparse inputs keep gaps, and gap
// values must map to -1, not to the slot whose bounds span them.
func TestSlotForTokenSparseGaps(t *testing.T) {
	plan, err := BuildSubscriptionPlan([]int32{1, 3}, 1, 1024, 512)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if got := SlotForToken(plan, 1); got != 0 {
		t.Fatalf("SlotForToken(1) = %d, want 0", got)
	}
	if got := SlotForToken(plan, 3); got != 0 {
		t.Fatalf("SlotForToken(3) = %d, want 0", got)
	}
	if got := SlotForToken(plan, 2); got != -1 {
		t.Fatalf("SlotForToken(2) = %d, want -1 (gap value)", got)
	}
	if got := SlotForToken(plan, 0); got != -1 {
		t.Fatalf("SlotForToken(0) = %d, want -1", got)
	}
	if got := SlotForToken(plan, 4); got != -1 {
		t.Fatalf("SlotForToken(4) = %d, want -1", got)
	}
}

// ING-CAP-001 — 512 and 1024 manifests report exact capacity (request chunks
// and slots); an over-capacity manifest never starts.
func TestIngCap001CapacityAccounting(t *testing.T) {
	// 512 tokens on one connection → exactly one request of 512.
	plan, err := BuildSubscriptionPlan(ids(512), 1, 1024, 512)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Slots) != 1 || len(plan.Slots[0].Requests) != 1 || len(plan.Slots[0].Requests[0]) != 512 {
		t.Fatalf("512-token plan: slots=%d requests=%d first=%d",
			len(plan.Slots), len(plan.Slots[0].Requests), len(plan.Slots[0].Requests[0]))
	}

	// 1024 tokens → exactly two requests of 512 on one connection.
	plan, err = BuildSubscriptionPlan(ids(1024), 1, 1024, 512)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Slots) != 1 || len(plan.Slots[0].Requests) != 2 {
		t.Fatalf("1024-token plan: slots=%d requests=%d", len(plan.Slots), len(plan.Slots[0].Requests))
	}
	for i, r := range plan.Slots[0].Requests {
		if len(r) != 512 {
			t.Fatalf("request %d has %d tokens, want 512", i, len(r))
		}
	}

	// Over-capacity (connection limit exceeded on a single slot) never starts.
	if _, err := BuildSubscriptionPlan(ids(1025), 1, 1024, 512); err == nil {
		t.Fatal("ING-CAP-001: 1025 tokens on one connection must be rejected")
	}

	// 3000 tokens over 3 slots → chunks 1024, 1024, 952 (plan §Executive Summary).
	plan, err = BuildSubscriptionPlan(ids(3000), 3, 1024, 512)
	if err != nil {
		t.Fatal(err)
	}
	got := []int{}
	for _, s := range plan.Slots {
		got = append(got, len(s.Tokens))
	}
	want := []int{1024, 1024, 952}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("3000-token chunking: got %v, want %v", got, want)
		}
	}
}

func ids(n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = int32(i + 1)
	}
	return out
}

// ING-RES-002 — plan-boundary N: every token appears exactly once, each
// request ≤ MaxHFTTokensPerRequest, slot count ≤ configured connections, and
// validateRequestUnion rejects duplicates/missing tokens.
func TestIngRes002PlanBoundaries(t *testing.T) {
	// The 512-boundary neighborhoods plus the 1024 connection limit and one
	// over-capacity rejection (1025 on a single connection).
	for _, n := range []int{1, 511, 512, 513, 1023, 1024} {
		plan, err := BuildSubscriptionPlan(ids(n), 1, 1024, 512)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if len(plan.Slots) != 1 {
			t.Fatalf("n=%d: slots=%d, want 1", n, len(plan.Slots))
		}
		slot := plan.Slots[0]

		// Every token appears exactly once across the request union.
		seen := map[int32]bool{}
		for _, req := range slot.Requests {
			if len(req) == 0 || len(req) > MaxHFTTokensPerRequest {
				t.Fatalf("n=%d: request size %d outside (0, %d]", n, len(req), MaxHFTTokensPerRequest)
			}
			for _, tok := range req {
				if seen[tok] {
					t.Fatalf("n=%d: duplicate token %d", n, tok)
				}
				seen[tok] = true
			}
		}
		if len(seen) != n {
			t.Fatalf("n=%d: union has %d tokens, want %d", n, len(seen), n)
		}
		// The request union equals the slot's token set exactly.
		if err := validateRequestUnion(slot); err != nil {
			t.Fatalf("n=%d: validateRequestUnion rejected a valid plan: %v", n, err)
		}
		// Request chunk count = ceil(n / 512).
		wantRequests := (n + MaxHFTTokensPerRequest - 1) / MaxHFTTokensPerRequest
		if len(slot.Requests) != wantRequests {
			t.Fatalf("n=%d: requests=%d, want %d", n, len(slot.Requests), wantRequests)
		}
	}

	// 1025 tokens exceed one connection's capacity → never builds.
	if _, err := BuildSubscriptionPlan(ids(1025), 1, 1024, 512); err == nil {
		t.Fatal("ING-RES-002: 1025 tokens on one connection must be rejected")
	}
}

// ING-RES-002 — validateRequestUnion fails closed on duplicates and missing
// tokens (the plan's union invariant, exercised directly).
func TestIngRes002ValidateRequestUnionRejects(t *testing.T) {
	cases := []struct {
		name  string
		slot  SlotAssignment
	}{
		{
			name: "duplicate token across requests",
			slot: SlotAssignment{Tokens: []int32{1, 2, 3}, Requests: [][]int32{{1, 2}, {2, 3}}},
		},
		{
			// P1-027: duplicated ASSIGNMENT tokens used to pass when
			// requests mirrored them (seen[1]: 2->1->0, sweep sees 0).
			name: "duplicate token inside the assignment itself",
			slot: SlotAssignment{Tokens: []int32{1, 1}, Requests: [][]int32{{1}, {1}}},
		},
		{
			name: "duplicate token within one request",
			slot: SlotAssignment{Tokens: []int32{1, 2}, Requests: [][]int32{{1, 1, 2}}},
		},
		{
			name: "token outside the assignment",
			slot: SlotAssignment{Tokens: []int32{1, 2}, Requests: [][]int32{{1, 4}}},
		},
		{
			name: "missing assignment token",
			slot: SlotAssignment{Tokens: []int32{1, 2, 3}, Requests: [][]int32{{1, 2}}},
		},
		{
			name: "empty request",
			slot: SlotAssignment{Tokens: []int32{1}, Requests: [][]int32{{}}},
		},
		{
			name: "request over MaxHFTTokensPerRequest",
			slot: SlotAssignment{Tokens: ids(513), Requests: [][]int32{ids(513)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRequestUnion(tc.slot); err == nil {
				t.Fatal("expected the union invariant to be rejected")
			}
		})
	}
}
