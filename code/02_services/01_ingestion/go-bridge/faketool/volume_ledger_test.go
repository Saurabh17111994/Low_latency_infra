package main

import (
	"sync"
	"testing"
)

// Q1=B: the fake broker's Volume field must be CUMULATIVE (the bridge's
// volumeDeltaTracker subtracts the previous tick's value; a per-frame quantity
// made the delta nil or random, so candles summed noise).
func TestVolumeLedgerIsMonotonicCumulative(t *testing.T) {
	v := newVolumeLedger()
	if got := v.add(101, 25); got != 25 {
		t.Fatalf("first add = %d, want 25", got)
	}
	if got := v.add(101, 10); got != 35 {
		t.Fatalf("second add = %d, want 35", got)
	}
	// A different token has its own counter.
	if got := v.add(202, 7); got != 7 {
		t.Fatalf("other token first add = %d, want 7", got)
	}
	// A zero-quantity frame repeats the counter (delta 0 = no trade).
	if got := v.add(101, 0); got != 35 {
		t.Fatalf("zero qty must not move the counter: %d", got)
	}
}

// The ledger is shared by the fixed ticker, the real-rate ticker and the
// snapshot burst; concurrent adds must not lose an increment.
func TestVolumeLedgerConcurrentAddsLoseNothing(t *testing.T) {
	v := newVolumeLedger()
	var wg sync.WaitGroup
	const goroutines, adds = 8, 1000
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < adds; i++ {
				v.add(303, 1)
			}
		}()
	}
	wg.Wait()
	if got := v.add(303, 0); got != goroutines*adds {
		t.Fatalf("cumulative = %d, want %d", got, goroutines*adds)
	}
}
