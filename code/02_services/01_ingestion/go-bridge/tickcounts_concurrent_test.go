package main

import (
	"fmt"
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
