package main

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

// countEvents returns the number of bridge_event control records whose event
// field equals want (proto frames — NDJSON removed 2026-08-29).
func countEvents(t *testing.T, out string, event string) int {
	t.Helper()
	return countContaining(eventsAsStrings(t, out), "event="+event)
}

// lastLineEvent returns the event field of the final bridge_event control
// record (proto frames — NDJSON removed 2026-08-29).
func lastLineEvent(t *testing.T, out string) string {
	t.Helper()
	strs := eventsAsStrings(t, out)
	if len(strs) == 0 {
		return ""
	}
	rest := strings.TrimPrefix(strs[len(strs)-1], "event=")
	if i := strings.Index(rest, " state="); i >= 0 {
		return rest[:i]
	}
	return rest
}

// withShutdownOnce resets the package-level shutdown once and restores it after.
func withShutdownOnce(t *testing.T) {
	t.Helper()
	old := bridgeShutdownOnce
	bridgeShutdownOnce = &sync.Once{}
	t.Cleanup(func() { bridgeShutdownOnce = old })
}

// TestShutdownDrainEventEmittedOnce — the drain marker (bridge_shutdown) is
// emitted exactly once and is the last line after preceding tick writes.
func TestShutdownDrainEventEmittedOnce(t *testing.T) {
	old := bridgeEmitter
	var out bytes.Buffer
	bridgeEmitter = newTestProtoEmitter(&out)
	defer func() { bridgeEmitter = old }()
	withShutdownOnce(t)

	// Simulate a run that emitted ticks, then a single shutdown.
	_ = bridgeEmitter.EmitTick(Tick{Feed: "hft", Mode: "full", Token: 1000, LTP: 15050}, "hft-0", "hft-0", 1, time.Now(), nil)
	_ = bridgeEmitter.EmitTick(Tick{Feed: "hft", Mode: "full", Token: 1000, LTP: 15051}, "hft-0", "hft-0", 1, time.Now(), nil)
	emitShutdownEvent()

	if got := countEvents(t, out.String(), EventBridgeShutdown); got != 1 {
		t.Fatalf("bridge_shutdown emitted %d times, want 1\n%s", got, out.String())
	}
	// The shutdown event must be the terminal (last) line — the drain marker.
	if got := lastLineEvent(t, out.String()); got != EventBridgeShutdown {
		t.Fatalf("last event = %q, want %q (drain must be the final line)\n%s", got, EventBridgeShutdown, out.String())
	}
}

// TestShutdownDuplicateIsIdempotent — calling the shutdown path twice (e.g. a
// redundant signal handler + main fallthrough) must still emit exactly one
// bridge_shutdown event and not corrupt the stream.
func TestShutdownDuplicateIsIdempotent(t *testing.T) {
	old := bridgeEmitter
	var out bytes.Buffer
	bridgeEmitter = newTestProtoEmitter(&out)
	defer func() { bridgeEmitter = old }()
	withShutdownOnce(t)

	emitShutdownEvent()
	emitShutdownEvent() // duplicate shutdown path

	if got := countEvents(t, out.String(), EventBridgeShutdown); got != 1 {
		t.Fatalf("bridge_shutdown emitted %d times on duplicate shutdown, want 1\n%s", got, out.String())
	}
	// Stream must still be valid proto frames (no partial/corrupt frame).
	if frames := splitFrames(out.Bytes()); len(frames) == 0 {
		t.Fatalf("duplicate shutdown left no decodable frames: %q", out.String())
	}
}
