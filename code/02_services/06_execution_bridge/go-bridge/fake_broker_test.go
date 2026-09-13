package main

import "testing"

// P3-243: a preset is an offline shortcut, not a licence to publish a shape the
// live broker cannot produce. ArrowBroker digests the payload it returns, so a
// preset that hands back Data with no digest passes here and fails in
// resultToReport against the venue.
func TestFakeBrokerPresetCarriesTheFingerprintOfItsData(t *testing.T) {
	fake := NewFakeBroker()
	data := []map[string]string{{"id": "preset-1", "orderStatus": "OPEN"}}
	fake.SetResult(CommandReconcileOrders, BrokerResult{
		Outcome: OutcomeSuccess, BrokerOrderID: "BRK-PRESET", Data: data,
	})

	got := fake.ReconcileOrders(t.Context(), validPlaceCommand())
	if got.Outcome != OutcomeSuccess || got.BrokerOrderID != "BRK-PRESET" {
		t.Fatalf("preset outcome=%s id=%s, want SUCCESS/BRK-PRESET: the preset branch must not rewrite what the test set",
			got.Outcome, got.BrokerOrderID)
	}
	want, err := fingerprint(data)
	if err != nil {
		t.Fatalf("fingerprint the preset payload: %v", err)
	}
	if got.Fingerprint != want {
		t.Errorf("preset fingerprint=%q, want %q: Data without a digest must be digested, not passed through bare",
			got.Fingerprint, want)
	}

	// Scoped to a *missing* digest: a test that presets a deliberately wrong one
	// is exercising the mismatch path and keeps seeing the value it chose.
	fake.SetResult(CommandReconcileOrders, BrokerResult{
		Outcome: OutcomeSuccess, Data: data, Fingerprint: "deliberately-wrong",
	})
	if got := fake.ReconcileOrders(t.Context(), validPlaceCommand()); got.Fingerprint != "deliberately-wrong" {
		t.Errorf("preset fingerprint=%q, want deliberately-wrong: a non-empty digest is the test's to choose",
			got.Fingerprint)
	}
}

// P3-244: the HTTP boundary validates the envelope before dispatch, but a test
// that drives the broker directly bypasses it. ArrowBroker dereferences c.Order
// on place/modify and forwards the id on cancel/query, so the double must refuse
// the same envelopes instead of answering SUCCESS for one the venue never sees.
func TestFakeBrokerRefusesEnvelopesTheRealBrokerWould(t *testing.T) {
	fake := NewFakeBroker()
	ctx := t.Context()

	noOrder := validPlaceCommand()
	noOrder.Order = nil
	if got := fake.Place(ctx, noOrder); got.Outcome != OutcomeRejected {
		t.Errorf("place with no order: outcome=%s, want REJECTED: the live broker would never send it", got.Outcome)
	}

	noID := validPlaceCommand()
	noID.Command = CommandCancel
	noID.Order = nil
	noID.BrokerOrderID = ""
	if got := fake.Cancel(ctx, noID); got.Outcome != OutcomeRejected {
		t.Errorf("cancel with no broker_order_id: outcome=%s, want REJECTED: the live broker would forward an empty id", got.Outcome)
	}

	// The guard must reject the invalid envelopes only, never every command.
	if got := fake.Place(ctx, validPlaceCommand()); got.Outcome != OutcomeSuccess {
		t.Errorf("valid place: outcome=%s reason=%s, want SUCCESS: the guard must not reject valid envelopes", got.Outcome, got.Reason)
	}
}

// P3-245: every placement gets its own id. One fixed id makes two orders
// indistinguishable, so a test cannot catch a conflated or swapped id.
func TestFakeBrokerGivesEveryPlacementItsOwnOrderID(t *testing.T) {
	fake := NewFakeBroker()
	ctx := t.Context()

	first := fake.Place(ctx, validPlaceCommand())
	if first.BrokerOrderID != "fake-broker-order-1" {
		t.Errorf("first place id=%q, want fake-broker-order-1: the counter keeps runs comparable", first.BrokerOrderID)
	}
	seen := map[string]bool{first.BrokerOrderID: true}
	for i := 2; i <= 5; i++ {
		got := fake.Place(ctx, validPlaceCommand())
		if seen[got.BrokerOrderID] {
			t.Errorf("place %d reused id %q: two placements must not share an id", i, got.BrokerOrderID)
		}
		seen[got.BrokerOrderID] = true
	}
}
