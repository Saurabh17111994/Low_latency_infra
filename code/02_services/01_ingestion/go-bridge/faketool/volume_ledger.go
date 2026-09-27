package main

import "sync"

// volumeLedger tracks per-token CUMULATIVE day volume for the fake broker.
//
// Both real feeds report cumulative volume — the bridge's volumeDeltaTracker
// derives raw_table_1.volume_delta as cumulative_now − cumulative_prev (nil for
// the first tick of an epoch or a counter that moved backwards). The fake
// broker used to write a fresh random per-frame quantity into the Volume field,
// so the tracker saw a counter that moved backwards half the time (delta nil)
// and random increments otherwise; candles then summed noise instead of trades.
// Every frame now carries a monotonic cumulative value while LTQ (12:16) stays
// the per-tick traded quantity.
//
// Safe for concurrent use: the fixed ticker, the real-rate ticker and the
// snapshot burst can touch the same token, and one ledger is shared across
// reconnects so the counter behaves like a day counter.
type volumeLedger struct {
	mu  sync.Mutex
	cum map[uint32]uint64
}

func newVolumeLedger() *volumeLedger {
	return &volumeLedger{cum: make(map[uint32]uint64)}
}

// add records qty for tok and returns the new cumulative volume.
func (v *volumeLedger) add(tok uint32, qty uint32) uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cum[tok] += uint64(qty)
	return v.cum[tok]
}
