package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReportTickCountsEmptyMap — P1-048: an empty map must still emit the
// total=0 header (file and stderr marker), never a 0-byte report.
func TestReportTickCountsEmptyMap(t *testing.T) {
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{}
	oldPath := tickCountsFilePath
	tickCountsFilePath = filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	tickCountsMu.Unlock()
	defer func() {
		tickCountsMu.Lock()
		tickCountsFilePath = oldPath
		tickCounts = nil
		tickCountsMu.Unlock()
	}()

	reportTickCounts()

	raw, err := os.ReadFile(tickCountsFilePath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if !strings.Contains(string(raw), "arrow-tick-counts: total=0 chunk=0/1") {
		t.Fatalf("empty map must emit total=0 header, got %q", raw)
	}
}

// TestReportTickCountsNonEmpty — the normal path keeps per-token lines.
func TestReportTickCountsNonEmpty(t *testing.T) {
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{7: 3, 9: 5}
	oldPath := tickCountsFilePath
	tickCountsFilePath = filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	tickCountsMu.Unlock()
	defer func() {
		tickCountsMu.Lock()
		tickCountsFilePath = oldPath
		tickCounts = nil
		tickCountsMu.Unlock()
	}()

	reportTickCounts()

	raw, err := os.ReadFile(tickCountsFilePath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if !strings.Contains(string(raw), "total=8") || !strings.Contains(string(raw), "t=7:n=3") {
		t.Fatalf("report missing totals, got %q", raw)
	}
}
