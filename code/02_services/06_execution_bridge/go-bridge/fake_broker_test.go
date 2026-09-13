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
