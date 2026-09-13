package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNormalizeOrderUpdateMapsIdentityAndFill(t *testing.T) {
	update := map[string]any{
		"id": "BRK-1", "remarks": "INS1234567890123", "token": "3045",
		"orderStatus": "COMPLETE", "reportType": "Fill", "fillShares": "2",
		"averagePrice": "15050", "exchangeOrderID": "EX-1",
		"exchangeUpdateTime": "2026-08-19T10:00:00Z",
	}
	report := NormalizeOrderUpdate(update)
	if report.Outcome != OutcomeSuccess || report.BrokerOrderID != "BRK-1" ||
		report.ClientOrderRef != "INS1234567890123" || report.FillShares != "2" {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.PostbackEventID == "" || len(report.PostbackEventID) != 64 {
		t.Fatalf("postback identity missing: %q", report.PostbackEventID)
	}
	if again := NormalizeOrderUpdate(update); again.PostbackEventID != report.PostbackEventID {
		t.Fatal("identical postbacks must have the same deterministic event identity")
	}
}

func TestNormalizeOrderUpdateDoesNotAcceptUnknownStatus(t *testing.T) {
	report := NormalizeOrderUpdate(map[string]any{"id": "BRK-1", "orderStatus": "NEW_STATUS"})
	if report.Outcome != OutcomeUnknown {
		t.Fatalf("unknown status outcome=%s, want UNKNOWN", report.Outcome)
	}
}

type scriptedOrderSource struct {
	updates []map[string]any
	err     error
	closed  bool
}

func (s *scriptedOrderSource) Read(ctx context.Context, onUpdate func(map[string]any), onError func(error)) {
	for _, update := range s.updates {
		onUpdate(update)
	}
	if s.err != nil {
		onError(s.err)
	}
	<-ctx.Done()
}
func (s *scriptedOrderSource) Close() error {
	s.closed = true
	return nil
}

type returningOrderSource struct {
	updates []map[string]any
	closed  bool
}

func (s *returningOrderSource) Read(_ context.Context, onUpdate func(map[string]any), _ func(error)) {
	for _, update := range s.updates {
		onUpdate(update)
	}
}
func (s *returningOrderSource) Close() error {
	s.closed = true
	return nil
}

func TestRunPostbackLoopPublishesAndClosesOnCancellation(t *testing.T) {
	source := &scriptedOrderSource{updates: []map[string]any{{"id": "BRK-1", "orderStatus": "OPEN"}}, err: errors.New("disconnect")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports := make(chan ReportEnvelope, 1)
	done := make(chan struct{})
	go func() {
		RunPostbackLoop(ctx, func() (OrderUpdateSource, error) { return source, nil }, func(report ReportEnvelope) error {
			reports <- report
			cancel()
			return nil
		}, nil)
		close(done)
	}()
	select {
	case report := <-reports:
		if report.BrokerOrderID != "BRK-1" {
			t.Fatalf("report=%+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("postback report timeout")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("postback loop did not stop")
	}
	if !source.closed {
		t.Fatal("postback source must close on cancellation")
	}
}
func TestRunPostbackLoopReconnectsAfterReaderReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var connects int
	reports := make(chan ReportEnvelope, 1)
	source := &returningOrderSource{updates: []map[string]any{{"id": "BRK-1", "orderStatus": "OPEN"}}}
	done := make(chan struct{})
	go func() {
		runPostbackLoop(ctx, func() (OrderUpdateSource, error) {
			connects++
			if connects == 1 {
				return source, nil
			}
			cancel()
			return nil, errors.New("stop")
		}, func(report ReportEnvelope) error {
			reports <- report
			return nil
		}, nil, time.Nanosecond, time.Nanosecond)
		close(done)
	}()
	select {
	case report := <-reports:
		if report.BrokerOrderID != "BRK-1" {
			t.Fatalf("report=%+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("reader completion did not publish its final update")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("postback loop did not reconnect after reader completion")
	}
	if connects != 2 {
		t.Fatalf("connect attempts=%d, want 2", connects)
	}
}

// P3-047/P3-046: connect() can report success while yielding no source. The loop
// must treat that as a connect error (report + backoff retry) instead of calling
// Read/Close on a nil source, which panics inside the reader goroutine and takes
// the whole bridge process down — the loop runs as a bare `go RunPostbackLoop`,
// so nothing recovers it.
func TestRunPostbackLoopTreatsNilSourceAsConnectError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var seen []error
	done := make(chan struct{})
	go func() {
		defer close(done)
		runPostbackLoop(ctx,
			func() (OrderUpdateSource, error) { return nil, nil },
			func(ReportEnvelope) error { return nil },
			func(err error) {
				mu.Lock()
				seen = append(seen, err)
				mu.Unlock()
				cancel() // stop the loop after the first classified failure
			},
			time.Nanosecond, time.Nanosecond)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("postback loop did not exit after a nil-source connect error")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("a nil source must surface as a connect error, not as a usable stream")
	}
	if seen[0] == nil {
		t.Fatal("connect error must be non-nil")
	}
}

func TestNextBackoffClampsInvalidAndOverflowingValues(t *testing.T) {
	if got := nextBackoff(0, time.Second); got != 2*time.Nanosecond {
		t.Fatalf("nextBackoff(0, 1s)=%v, want 2ns", got)
	}
	if got := nextBackoff(700*time.Millisecond, time.Second); got != time.Second {
		t.Fatalf("nextBackoff(700ms, 1s)=%v, want 1s", got)
	}
	if got := nextBackoff(time.Second, time.Second); got != time.Second {
		t.Fatalf("nextBackoff(1s, 1s)=%v, want 1s", got)
	}
}

// P3-255/P3-260: the event id is the fill's identity downstream — the executor keys
// source_event_id on it and will not project a fill whose id it has already seen
// (04_executor/src/projection/mod.rs:651, compared at :262 and :561) — so two
// distinct partial fills must never carry the same one. The digest used to ignore
// fillPrice, fillQuantity, fillTime and exchangeOrderID.
func TestPostbackEventIDDistinguishesFillIdentity(t *testing.T) {
	base := map[string]any{
		"orderStatus": "EXECUTED", "reportType": "Fill", "id": "BRK-77",
		"remarks": "c-77", "fillShares": "10", "averagePrice": "150.5",
		"fillPrice": "15050", "fillQuantity": "10",
		"fillTime": "2026-09-13T09:15:00", "exchangeOrderID": "EX-1",
	}
	baseID := NormalizeOrderUpdate(base).PostbackEventID
	if baseID == "" {
		t.Fatal("base update produced no event id")
	}
	seen := map[string]string{baseID: "base"}
	var collisions []string
	for _, variant := range []struct{ field, value string }{
		{"fillPrice", "15051"},
		{"fillQuantity", "11"},
		{"fillTime", "2026-09-13T09:15:01"},
		{"exchangeOrderID", "EX-2"},
	} {
		clone := map[string]any{}
		for k, v := range base {
			clone[k] = v
		}
		clone[variant.field] = variant.value
		id := NormalizeOrderUpdate(clone).PostbackEventID
		if other, ok := seen[id]; ok {
			collisions = append(collisions, fmt.Sprintf("%s collides with %s", variant.field, other))
			continue
		}
		seen[id] = variant.field
	}
	if len(collisions) > 0 {
		t.Fatalf("distinct fills share one postback event id, so downstream dedup drops them: %v", collisions)
	}
	// A replayed update must still hash equally, or every reconnect re-projects a fill.
	if replay := NormalizeOrderUpdate(base).PostbackEventID; replay != baseID {
		t.Fatalf("replayed update changed identity: %q != %q", replay, baseID)
	}
}
