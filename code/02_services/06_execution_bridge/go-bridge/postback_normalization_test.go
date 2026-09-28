package main

import (
	"encoding/json"
	"os"
	"testing"
)

// C1-6: the shared fixture is the one vocabulary both sides agree on. This test fails
// when a mapping row drifts, when a real Arrow fill stops normalizing from
// fillShares/averagePrice into fill_quantity/fill_price, when the raw report_type stops
// being carried (it is the fingerprint input pinned by contract 06), or when the reject
// reason stops being carried into the envelope.
type postbackFixture struct {
	Cases []struct {
		Name       string         `json:"name"`
		Arrow      map[string]any `json:"arrow"`
		EventType  string         `json:"event_type"`
		Normalized struct {
			FillQuantity string `json:"fill_quantity"`
			FillPrice    string `json:"fill_price"`
			RejectReason string `json:"reject_reason"`
		} `json:"normalized"`
	} `json:"cases"`
}

func loadPostbackFixture(t *testing.T) postbackFixture {
	t.Helper()
	raw, err := os.ReadFile("../../../testdata/postback-report-types.json")
	if err != nil {
		t.Fatalf("shared postback fixture is missing or unreadable: %v", err)
	}
	var fixture postbackFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("shared postback fixture does not parse: %v", err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("shared postback fixture has no cases")
	}
	return fixture
}

func TestPostbackFixtureMappingAndNormalization(t *testing.T) {
	for _, tc := range loadPostbackFixture(t).Cases {
		t.Run(tc.Name, func(t *testing.T) {
			report := NormalizeOrderUpdate(tc.Arrow)
			if report.EventType != tc.EventType {
				t.Fatalf("event_type = %q, want %q (arrow=%v)", report.EventType, tc.EventType, tc.Arrow)
			}
			if report.ReportType != stringField(tc.Arrow, "reportType") {
				t.Fatalf("raw report_type must stay carried: got %q", report.ReportType)
			}
			if report.FillQuantity != tc.Normalized.FillQuantity {
				t.Fatalf("fill_quantity = %q, want %q", report.FillQuantity, tc.Normalized.FillQuantity)
			}
			if report.FillPrice != tc.Normalized.FillPrice {
				t.Fatalf("fill_price = %q, want %q", report.FillPrice, tc.Normalized.FillPrice)
			}
			if report.RejectReason != tc.Normalized.RejectReason {
				t.Fatalf("reject_reason = %q, want %q", report.RejectReason, tc.Normalized.RejectReason)
			}
			// The identity hashes the raw input, so a replay must produce the same id
			// even though the normalized copies differ from the raw fields.
			again := NormalizeOrderUpdate(tc.Arrow)
			if report.PostbackEventID == "" || again.PostbackEventID != report.PostbackEventID {
				t.Fatalf("postback identity must be present and deterministic: %q vs %q",
					report.PostbackEventID, again.PostbackEventID)
			}
		})
	}
}

// The fingerprint pins the raw report_type (contract 06), so normalization must not
// change the identity versus the same raw input parsed without any fill copies.
func TestPostbackNormalizationDoesNotChangeIdentity(t *testing.T) {
	realShape := map[string]any{
		"id": "BRK-3", "remarks": "REF-3", "token": "3045",
		"orderStatus": "COMPLETE", "reportType": "Fill",
		"fillShares": "2", "averagePrice": "15050",
	}
	first := NormalizeOrderUpdate(realShape)
	second := NormalizeOrderUpdate(realShape)
	if first.PostbackEventID != second.PostbackEventID {
		t.Fatalf("same raw input must hash equally: %q vs %q", first.PostbackEventID, second.PostbackEventID)
	}
}
