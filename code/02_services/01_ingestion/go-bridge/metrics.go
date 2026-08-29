package main

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// reconnectState tracks consecutive reconnect attempts per slot. It backs the
// reconnect_consecutive field of the bridge_metrics control record.
var reconnectState = struct {
	mu      sync.Mutex
	perSlot map[string]int
}{perSlot: map[string]int{}}

// noteReconnect increments the slot's consecutive-reconnect counter. Called
// for every reconnect event: loop-level retries (supervisor onRetry) and
// in-epoch auth-resume reconnects (runHFTEpoch).
func noteReconnect(slotID string) {
	reconnectState.mu.Lock()
	defer reconnectState.mu.Unlock()
	reconnectState.perSlot[slotID]++
}

// noteRecovered resets the slot's counter when it reaches ACTIVE with a full
// subscription ack — a successful subscription breaks the streak.
func noteRecovered(slotID string) {
	reconnectState.mu.Lock()
	defer reconnectState.mu.Unlock()
	delete(reconnectState.perSlot, slotID)
}

// maxReconnectConsecutive returns the largest per-slot reconnect streak, so
// one slot's churn stays visible even while other slots are healthy.
func maxReconnectConsecutive() int {
	reconnectState.mu.Lock()
	defer reconnectState.mu.Unlock()
	max := 0
	for _, n := range reconnectState.perSlot {
		if n > max {
			max = n
		}
	}
	return max
}

// activeSockets is the number of slots with an open HFT stream. Java's
// active_sockets gauge prefers this value over lifecycle-derived counts
// because it exposes orphaned streams (a socket that never closed).
var activeSockets atomic.Int32

func addActiveSocket(delta int32) { activeSockets.Add(delta) }

// bridgeMetricsTicker emits one bridge_metrics control record (proto
// frames) every 10s until
// ctx is done. It exits promptly on cancel so the supervisor's goroutine
// count settles back to baseline (ING-RES-001 baseline+2).
func bridgeMetricsTicker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = bridgeEmitter.EmitMetrics(BridgeMetrics{
				TsMs:                 time.Now().UnixMilli(),
				ReconnectConsecutive: maxReconnectConsecutive(),
				ActiveSockets:        int(activeSockets.Load()),
				GoGoroutines:         runtime.NumGoroutine(),
			})
		case <-ctx.Done():
			return
		}
	}
}
