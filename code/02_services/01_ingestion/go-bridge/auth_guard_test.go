package main

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// P1-031/P1-035 own-side shield: dials (RLock) and refresh (Lock) through
// bridgeAuthMu must never overlap. -race + overlap counters prove it.
func TestBridgeAuthMuOrdersRefreshVsDial(t *testing.T) {
	var activeDial, activeRefresh atomic.Int64
	var violations atomic.Int64
	refresh := wrapRefreshAuth(func(context.Context) error {
		if activeDial.Load() != 0 || activeRefresh.Load() != 0 {
			violations.Add(1)
		}
		activeRefresh.Add(1)
		time.Sleep(2 * time.Millisecond)
		activeRefresh.Add(-1)
		return nil
	})
	factory := func() (hftStream, error) {
		return guardedDial(func() (hftStream, error) {
			if activeRefresh.Load() != 0 {
				violations.Add(1)
			}
			activeDial.Add(1)
			time.Sleep(time.Millisecond)
			activeDial.Add(-1)
			return newFakeHFTStream(), nil
		})
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = refresh(context.Background()) }()
		go func() {
			defer wg.Done()
			s, err := factory()
			if err != nil || s == nil {
				t.Errorf("guarded dial failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if violations.Load() != 0 {
		t.Fatalf("dial overlapped refresh %d times", violations.Load())
	}
}

// P1-037 structural guard: second ReadHFTWithFrame on one stream is refused
// via onError; the inner stream runs exactly once.
func TestSingleReaderRefusesSecond(t *testing.T) {
	inner := newFakeHFTStream()
	s := guardSingleReader(inner)
	if s == nil {
		t.Fatal("guardSingleReader(nil-safe) returned nil for non-nil stream")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		s.ReadHFTWithFrame(ctx, nil, nil, nil, nil, nil, nil)
	}()
	time.Sleep(50 * time.Millisecond) // let the first reader claim the flag
	var refusal atomic.Int64
	s.ReadHFTWithFrame(ctx, nil, nil, nil, nil, nil, func(err error) {
		if err != nil && strings.Contains(err.Error(), "second ReadHFTWithFrame") {
			refusal.Add(1)
		}
	})
	if refusal.Load() != 1 {
		t.Fatalf("second reader: want exactly 1 refusal, got %d", refusal.Load())
	}
	cancel()
	<-firstDone
	if guardSingleReader(nil) != nil {
		t.Fatal("guardSingleReader(nil) must pass nil through")
	}
}

// P1-177 subscribe params pinned: own call site passes constant HFTExchNSECM
// (0, in 0..3) + mode "full" + hftRange-clamped latency; SlotConfig.Validate
// rejects out-of-range latency. If the call site ever parameterizes these,
// this test names the contract that must move with it.
func TestSubscribeParamsPinned(t *testing.T) {
	if arrow.HFTExchNSECM < 0 || arrow.HFTExchNSECM > 3 {
		t.Fatalf("HFTExchNSECM=%d out of vendored 0..3 range", arrow.HFTExchNSECM)
	}
	bad := baseSlotCfg()
	bad.LatencyMs = 49
	if err := bad.Validate(); err == nil {
		t.Fatal("latency 49 must be rejected (50..60000)")
	}
	bad.LatencyMs = 60001
	if err := bad.Validate(); err == nil {
		t.Fatal("latency 60001 must be rejected (50..60000)")
	}
	if err := baseSlotCfg().Validate(); err != nil {
		t.Fatalf("default slot config must validate: %v", err)
	}
}

// P1-177 own-side pre-wire gate: mode/exchSeg/ids/latency validated before
// touching vendored SubscribeHFTTokens (which checks mode + non-empty only).
// Range assertions track the vendored constants, so an upstream segment-table
// change forces this test to re-examine the pin.
func TestValidateSubscribeArgs(t *testing.T) {
	good := []int32{2885}
	if err := validateSubscribeArgs("full", arrow.HFTExchNSECM, good, 50); err != nil {
		t.Fatalf("canonical args must pass: %v", err)
	}
	if err := validateSubscribeArgs("ltpc", arrow.HFTExchBSEFO, good, 60000); err != nil {
		t.Fatalf("boundary args must pass: %v", err)
	}
	cases := []struct {
		name string
		mode string
		seg  int
		ids  []int32
		lat  int
	}{
		{"bad mode", "quote", 0, good, 50},
		{"empty mode", "", 0, good, 50},
		{"seg negative", "full", -1, good, 50},
		{"seg above range", "full", arrow.HFTExchBSEFO + 1, good, 50},
		{"empty ids", "full", 0, nil, 50},
		{"latency low", "full", 0, good, 49},
		{"latency high", "full", 0, good, 60001},
		{"latency zero", "full", 0, good, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateSubscribeArgs(tc.mode, tc.seg, tc.ids, tc.lat); err == nil {
				t.Fatalf("must reject mode=%q seg=%d ids=%v latency=%d", tc.mode, tc.seg, tc.ids, tc.lat)
			}
		})
	}
}
