package main

import "testing"

// P1-145: the summary record must include the total WS frame count — the
// emit once serialized only the decoded-length histogram, losing HFTFrames.
func TestSummaryRecordIncludesHFTFrames(t *testing.T) {
	rec := summaryRecord(lenCounts{40: 2, 196: 3}, 7)
	if rec["kind"] != "summary" {
		t.Fatalf("kind = %v, want summary", rec["kind"])
	}
	got, ok := rec["hftFrames"]
	if !ok {
		t.Fatal("summary record drops hftFrames")
	}
	if got != 7 {
		t.Fatalf("hftFrames = %v, want 7", got)
	}
	if _, ok := rec["hft"]; !ok {
		t.Fatal("summary record drops hft histogram")
	}
}
