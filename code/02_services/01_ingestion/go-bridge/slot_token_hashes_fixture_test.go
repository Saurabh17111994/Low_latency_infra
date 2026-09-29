package main

import (
	"encoding/json"
	"os"
	"testing"
)

// H2-4: the shared fixture code/testdata/slot-token-hashes.json is GENERATED
// from this package's carve (BuildSubscriptionPlan + TokenSetHash) and is the
// cross-language anchor: the Java TokenSetHashTest and the Python validator
// both carve the same token lists and must land on the same slot ids, sizes and
// digests. This test pins the fixture back to the reference implementation —
// if the carve or the hash changes, the fixture must be regenerated
// deliberately and every consumer re-checked.
func TestSlotTokenHashFixtureMatchesTheGoCarve(t *testing.T) {
	raw, err := os.ReadFile("../../../testdata/slot-token-hashes.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture struct {
		ConnectionLimit int `json:"connectionLimit"`
		Cases           []struct {
			Name                string  `json:"name"`
			SlotCount           int     `json:"slotCount"`
			Tokens              []int32 `json:"tokens"`
			ManifestFingerprint string  `json:"manifestFingerprint"`
			Slots               []struct {
				SlotID string `json:"slotId"`
				Size   int    `json:"size"`
				Hash   string `json:"hash"`
			} `json:"slots"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if fixture.ConnectionLimit != MaxHFTTokensPerConnection {
		t.Fatalf("fixture connectionLimit=%d, want %d", fixture.ConnectionLimit, MaxHFTTokensPerConnection)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("fixture has no cases")
	}
	for _, c := range fixture.Cases {
		plan, err := BuildSubscriptionPlan(c.Tokens, c.SlotCount, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		if len(plan.Slots) != len(c.Slots) {
			t.Fatalf("%s: carve produced %d slots, fixture lists %d", c.Name, len(plan.Slots), len(c.Slots))
		}
		for i, want := range c.Slots {
			got := plan.Slots[i]
			if got.SlotID != want.SlotID || len(got.Tokens) != want.Size || TokenSetHash(got.Tokens) != want.Hash {
				t.Fatalf("%s slot %d: got %s/%d/%s, fixture says %s/%d/%s", c.Name, i,
					got.SlotID, len(got.Tokens), TokenSetHash(got.Tokens),
					want.SlotID, want.Size, want.Hash)
			}
		}
		if got := TokenSetHash(c.Tokens); got != c.ManifestFingerprint {
			t.Fatalf("%s: full-set TokenSetHash=%s, fixture manifestFingerprint=%s", c.Name, got, c.ManifestFingerprint)
		}
	}
}
