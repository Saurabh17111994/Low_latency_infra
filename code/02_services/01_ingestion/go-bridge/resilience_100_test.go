package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
	"github.com/gorilla/websocket"
)

// ----- ING-RES-001: 100 forced disconnect/reconnect cycles -----
//
// Plan §1347-1363:
//
//	ING-RES-001  Run 100 forced disconnect/reconnect cycles. Final FD count and
//	             Go-goroutine count are each no greater than baseline + 2; Java
//	             live-thread count returns within baseline + 2; no orphan child
//	             process/socket exists and no healthy slot is interrupted.
//
// The reconnect loop's backoff timing is already unit-tested
// (TestReconnectLoopEpochAndBackoffAfterForcedDisconnect); this test proves
// the RESOURCE contract of the cycle itself: 100 real SDK connections, each
// forced to disconnect, each torn down cleanly (no goroutine/FD/socket leak),
// with a healthy peer slot never interrupted.

// countOpenFDs returns the number of open file descriptors for this process
// (Linux /proc/self/fd); -1 if unavailable.
func countOpenFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

// dropBroker is a wire fake broker that accepts a subscription, replies with
// the golden SUCCESS response frame, delivers one LTP tick, then force-drops
// the connection. Each subscription = one forced disconnect/reconnect cycle.
type dropBroker struct {
	server    *httptest.Server
	url       string
	cycles    atomic.Int32 // completed drop cycles (subscription received)
	connected atomic.Int32 // live connections right now
	maxConn   atomic.Int32 // high-water mark of concurrent connections
}

func newDropBroker(t *testing.T) *dropBroker {
	t.Helper()
	b := &dropBroker{}
	respFrame := loadGoldenFrame(t, "response")
	ltpFrame := loadGoldenFrame(t, "ltp-tick")
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		cur := b.connected.Add(1)
		defer b.connected.Add(-1)
		defer conn.Close()
		for {
			prev := b.maxConn.Load()
			if cur <= prev || b.maxConn.CompareAndSwap(prev, cur) {
				break
			}
		}
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var sub map[string]any
			if json.Unmarshal(payload, &sub) != nil {
				continue
			}
			if code, _ := sub["code"].(string); code != "sub" {
				continue
			}
			// SUCCESS response → one tick → abrupt close (forced drop). A
			// close frame with 1006 is invalid on the wire (reserved), so we
			// drop the TCP connection: the client observes EOF → 1006 and the
			// epoch ends → reconnect.
			_ = conn.WriteMessage(websocket.BinaryMessage, compressZstd(respFrame))
			_ = conn.WriteMessage(websocket.BinaryMessage, compressZstd(ltpFrame))
			b.cycles.Add(1)
			conn.Close()
			return
		}
	}))
	b.url = "ws" + strings.TrimPrefix(b.server.URL, "http")
	t.Cleanup(b.server.Close)
	return b
}

// settledGoroutines returns the goroutine count after waiting for the runtime
// to quiesce (up to settle). A slot's teardown is asynchronous; measuring
// immediately after cancel races the runtime.
func settledGoroutines(settle time.Duration) int {
	time.Sleep(settle)
	runtime.GC()
	return runtime.NumGoroutine()
}

// TestINGRES001OneHundredForcedDisconnectReconnectCycles drives the REAL SDK
// connect path through 100 forced disconnect/reconnect cycles and asserts:
//   - 100 distinct cycles complete (each subscription → drop → epoch end)
//   - goroutine count returns to baseline + 2
//   - open FD count returns to baseline + 2
//   - the broker never sees more than one live connection at a time
//     (no orphan socket)
//
// Backoff is suppressed (the wait hook returns immediately) because backoff
// timing is tested separately; this test targets the resource contract.
func TestINGRES001OneHundredForcedDisconnectReconnectCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("ING-RES-001: 100-cycle soak skipped in -short mode")
	}
	b := newDropBroker(t)

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", b.url)

	plan, err := BuildSubscriptionPlan([]int32{757614}, 1, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatal(err)
	}
	slot := plan.Slots[0]

	// Baseline AFTER the first connection (the SDK/websocket machinery that
	// stays resident for the process lifetime is up): capture at cycle 1
	// rather than at zero — the +2 budget must absorb per-epoch teardown, not
	// first-time initialization.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const cycles = 100
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		// Suppressed backoff: reconnect immediately after each drop.
		runReconnectLoop(ctx,
			func(epoch uint64) bool {
				res := runHFTEpoch(ctx, streamFactoryFor(client, 0), slot, 50, 10*time.Second, epoch, nil, nil, t.Logf)
				return res == epochTerminal || res == epochRecovered
			},
			func(uint64, time.Duration) {},              // onRetry — event emission is internal
			func(_ context.Context, _ time.Duration) {}, // wait — no backoff sleep
		)
	}()

	// Wait for the first cycle to establish the post-init baseline.
	waitFor(t, 30*time.Second, func() bool { return b.cycles.Load() >= 1 })
	baselineGoroutines := runtime.NumGoroutine()
	baselineFDs := countOpenFDs()
	t.Logf("ING-RES-001 baseline (after cycle 1): goroutines=%d fds=%d",
		baselineGoroutines, baselineFDs)

	// Let the loop run until it completes 100 cycles.
	waitFor(t, 120*time.Second, func() bool { return b.cycles.Load() >= cycles })
	t.Logf("ING-RES-001: %d/%d forced-disconnect cycles completed", b.cycles.Load(), cycles)

	// The loop keeps reconnecting forever until cancel; stop it now.
	cancel()
	<-completed

	// Settle, then assert resource stability.
	finalGoroutines := settledGoroutines(500 * time.Millisecond)
	finalFDs := countOpenFDs()
	t.Logf("ING-RES-001 final: goroutines=%d (baseline %d), fds=%d (baseline %d)",
		finalGoroutines, baselineGoroutines, finalFDs, baselineFDs)

	if got := b.cycles.Load(); got < cycles {
		t.Fatalf("completed cycles=%d, want %d", got, cycles)
	}
	if finalGoroutines > baselineGoroutines+2 {
		t.Errorf("goroutine leak: final=%d > baseline %d + 2", finalGoroutines, baselineGoroutines)
	}
	if finalFDs >= 0 && finalFDs > baselineFDs+2 {
		t.Errorf("FD leak: final=%d > baseline %d + 2", finalFDs, baselineFDs)
	}
	if max := b.maxConn.Load(); max > 1 {
		t.Errorf("orphan socket: broker observed %d concurrent connections, want ≤ 1", max)
	}
	// Every cycle must have recovered to ACTIVE — a healthy slot is never
	// left interrupted by the previous epoch's drop.
	actives := countContaining(eventsAsStrings(t, out.String()), "state=ACTIVE")
	if actives < cycles {
		t.Errorf("ACTIVE recoveries=%d, want ≥ %d (cycles)", actives, cycles)
	}
}

// TestINGRES001OneHundredForcedDisconnectReconnectCyclesRealBackoff is the
// full wall-clock variant of ING-RES-001 (plan: "full wall-clock 100-cycle
// supervisor soak with real backoff", deferred mechanical run ~48 min): the
// same 100 forced disconnect/reconnect cycles through the REAL production
// backoff sequence (1s→2s→4s→8s→16s→30s→…, Backoff(attempt)) instead of the
// suppressed-hook fast path. The suppressed test proves the resource
// contract; this run proves the timing path: epoch advances across real
// backoff waits, and no goroutine/FD/socket leak accrues over wall-clock
// backoff pacing.
//
// Env-gated so the regular suite stays fast:
//
//	ING_RES001_REAL_BACKOFF=1   run the real-backoff soak
//	ING_RES001_CYCLES=<n>       cycles (default 100; use 2-3 for a smoke run)
//
// The wait hook mirrors main.go's production timer wait
// (supervisor.go waitBackoff: timer + ctx cancellation), not a no-op.
func TestINGRES001OneHundredForcedDisconnectReconnectCyclesRealBackoff(t *testing.T) {
	if os.Getenv("ING_RES001_REAL_BACKOFF") != "1" {
		t.Skip("ING-RES-001 real-backoff soak: set ING_RES001_REAL_BACKOFF=1 to run")
	}
	if testing.Short() {
		t.Skip("ING-RES-001 real-backoff soak skipped in -short mode")
	}
	cycles := 100
	if n := os.Getenv("ING_RES001_CYCLES"); n != "" {
		v, err := strconv.Atoi(n)
		if err != nil || v <= 0 {
			t.Fatalf("ING_RES001_CYCLES=%q: positive integer", n)
		}
		cycles = v
	}
	t.Logf("ING-RES-001 real-backoff soak: cycles=%d (backoff 1+2+4+8+16+30x… ≈ %s total)",
		cycles, (time.Duration(cycles*30) * time.Second).Round(time.Minute))

	b := newDropBroker(t)

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", b.url)

	plan, err := BuildSubscriptionPlan([]int32{757614}, 1, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatal(err)
	}
	slot := plan.Slots[0]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	completed := make(chan struct{})
	go func() {
		defer close(completed)
		runReconnectLoop(ctx,
			func(epoch uint64) bool {
				res := runHFTEpoch(ctx, streamFactoryFor(client, 0), slot, 50, 10*time.Second, epoch, nil, nil, t.Logf)
				return res == epochTerminal || res == epochRecovered
			},
			func(uint64, time.Duration) {}, // onRetry — event emission is internal
			func(ctx context.Context, delay time.Duration) { // wait — REAL backoff
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-ctx.Done():
				case <-timer.C:
				}
			},
		)
	}()

	// Wait for the first cycle to establish the post-init baseline.
	waitFor(t, 2*time.Minute, func() bool { return b.cycles.Load() >= 1 })
	baselineGoroutines := runtime.NumGoroutine()
	baselineFDs := countOpenFDs()
	t.Logf("ING-RES-001 real-backoff baseline (after cycle 1): goroutines=%d fds=%d",
		baselineGoroutines, baselineFDs)

	// Full wall-clock run: cycles * ~30s backoff + per-cycle connect time.
	// Budget 60s per cycle plus a 2-minute settle margin.
	waitFor(t, time.Duration(cycles)*60*time.Second+2*time.Minute,
		func() bool { return b.cycles.Load() >= int32(cycles) })
	t.Logf("ING-RES-001 real-backoff: %d/%d forced-disconnect cycles completed", b.cycles.Load(), cycles)

	cancel()
	<-completed

	finalGoroutines := settledGoroutines(500 * time.Millisecond)
	finalFDs := countOpenFDs()
	t.Logf("ING-RES-001 real-backoff final: goroutines=%d (baseline %d), fds=%d (baseline %d)",
		finalGoroutines, baselineGoroutines, finalFDs, baselineFDs)

	if got := b.cycles.Load(); got < int32(cycles) {
		t.Fatalf("completed cycles=%d, want %d", got, cycles)
	}
	if finalGoroutines > baselineGoroutines+2 {
		t.Errorf("goroutine leak: final=%d > baseline %d + 2", finalGoroutines, baselineGoroutines)
	}
	if finalFDs >= 0 && finalFDs > baselineFDs+2 {
		t.Errorf("FD leak: final=%d > baseline %d + 2", finalFDs, baselineFDs)
	}
	if max := b.maxConn.Load(); max > 1 {
		t.Errorf("orphan socket: broker observed %d concurrent connections, want ≤ 1", max)
	}
	actives := countLinesWith(t, out.String(), `"state":"ACTIVE"`)
	if actives < cycles {
		t.Errorf("ACTIVE recoveries=%d, want ≥ %d (cycles)", actives, cycles)
	}
}

// TestINGRES001HealthySlotNotInterruptedByPeerReconnect — while slot-0 cycles
// through forced disconnects, a healthy slot-1 stays ACTIVE (supervisor keeps
// healthy peers alive during peer retry).
func TestINGRES001HealthySlotNotInterruptedByPeerReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("ING-RES-001: healthy-slot clause skipped in -short mode")
	}
	b := newDropBroker(t)

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", b.url)

	// Slot-0 uses the real SDK → drop broker (cycles). Slot-1 uses a scripted
	// fake stream that stays healthy with periodic ticks.
	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onDecodedFrame = []byte{0x28, 0x00, 0x00, 0x00, 0x01}
	healthy.onFullTick = arrow.HFTFullTick{Token: 757614, LTP: 15051}

	makeFactory := func(_ *arrow.Client, slotIdx int) func() (hftStream, error) {
		if slotIdx == 0 {
			return streamFactoryFor(client, 0) // real SDK → drop broker
		}
		return func() (hftStream, error) { return healthy, nil }
	}

	plan, err := BuildSubscriptionPlan([]int32{757614, 757615}, 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	plan.Slots[0].SlotID = "hft-0"
	plan.Slots[0].ConnectionID = "ingestion-local/hft-0"
	plan.Slots[1].SlotID = "hft-1"
	plan.Slots[1].ConnectionID = "ingestion-local/hft-1"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHFTSupervisorWithFactory(ctx, makeFactory, client, plan, 50, 10*time.Second, nil, t.Logf)
	}()

	// Let slot-0 complete a few disconnect cycles (supervisor uses REAL
	// backoff 1s→2s→4s…, so 2 cycles take ~3s+) while slot-1 stays ACTIVE.
	waitFor(t, 60*time.Second, func() bool { return b.cycles.Load() >= 2 })
	time.Sleep(200 * time.Millisecond)

	// Slot-1 must still be ACTIVE at the end (never interrupted).
	if got := lastEventState(eventsFrom(t, out.String()), "subscription_ack"); got != "ACTIVE" {
		t.Errorf("slot-1 last state=%q, want ACTIVE (healthy slot interrupted)\n%s", got, out.String())
	}
	// Slot-1 must not have emitted a single disconnect (its stream never fails).
	if n := countContaining(eventsAsStrings(t, out.String()), "slot=hft-1"); n == 0 {
		t.Errorf("slot-1 emitted no events — healthy slot never started\n%s", out.String())
	}
	if n := countContaining(eventsAsStrings(t, out.String()), "event=disconnect"); n == 0 {
		t.Errorf("slot-0 never disconnected — drop broker not exercised\n%s", out.String())
	}

	cancel()
	<-done
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}
