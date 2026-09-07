package main

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
