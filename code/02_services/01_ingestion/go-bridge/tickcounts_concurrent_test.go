package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// P1-210 guard: concurrent recordTickCount during reportTickCounts must not
// deadlock and counts must reconcile.
func TestReportTickCountsConcurrent(t *testing.T) {
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{}
	tickCountsMu.Unlock()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(base int32) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				recordTickCount(base + int32(i%250))
			}
		}(int32(w * 250))
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			reportTickCounts()
		}
		close(done)
	}()
	wg.Wait()
	<-done
	tickCountsMu.Lock()
	total := int64(0)
	for _, n := range tickCounts {
		total += n
	}
	tickCountsMu.Unlock()
	if total != 8*5000 {
		t.Fatalf("counts lost under concurrent report: got %d want %d", total, 8*5000)
	}
	fmt.Printf("concurrent report OK total=%d\n", total)
}

// P1-211 guard: concurrent interval + final reports must serialize — every
// report writes the file completely, so the final file is exactly one full
// report (the map is fixed here, so all reports write identical bytes; a
// torn interleave would fail the exact match). Race-clean under -race.
func TestReportTickCountsConcurrentReports(t *testing.T) {
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
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				reportTickCounts()
			}
		}()
	}
	wg.Wait()
	raw, err := os.ReadFile(tickCountsFilePath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	want := "arrow-tick-counts: total=8 chunk=0/1 t=7:n=3 t=9:n=5\n"
	if string(raw) != want {
		t.Fatalf("concurrent reports must each leave a complete file, got %q want %q", raw, want)
	}
	if !strings.Contains(string(raw), "total=8") {
		t.Fatalf("report missing totals, got %q", raw)
	}
}
