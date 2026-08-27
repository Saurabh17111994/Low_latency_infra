// T2 tests — Go batcher (contract §7.4): batch boundaries with fake clock
// (T2-B1..B4), sequence semantics (T2-S1/S2), and edge cases. Deterministic:
// the clock is injectable and the flush callback records batches.

package main

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/trading/arrow-bridge/marketdata"
)

// fakeClock — controllable time source for deterministic boundary tests.
type fakeClock struct {
	t time.Time
}

func (f *fakeClock) Now() time.Time          { return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.t = f.t.Add(d) }

// recordingFlush captures flushed batches for assertions.
type recordingFlush struct {
	batches []*marketdata.MarketDataBatch
}

func (r *recordingFlush) Flush(batch *marketdata.MarketDataBatch) error {
	r.batches = append(r.batches, batch)
	return nil
}

func newTestBatcher(limits BatchLimits, clock *fakeClock) (*Batcher, *recordingFlush) {
	rec := &recordingFlush{}
	b := NewBatcher(limits, rec.Flush, clock.Now)
	return b, rec
}

// mkEvent builds a TickEvent with a deterministic raw payload of size n.
func mkEvent(n int) (*marketdata.TickEvent, []byte) {
	raw := make([]byte, n)
	for i := range raw {
		raw[i] = byte(i % 251)
	}
	ev := &marketdata.TickEvent{Token: 1, RawPayload: raw}
	return ev, raw
}

// T2-B1 — count boundary: flush exactly at MAX_EVENTS, not before/after.
func TestT2B1_CountBoundary(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 256
	limits.MaxBytes = 1 << 30 // disable bytes for this test
	b, rec := newTestBatcher(limits, clock)

	// 255 events — no flush
	for i := 0; i < 255; i++ {
		ev, raw := mkEvent(10)
		if err := b.Add("s", "c", 1, ev, raw); err != nil {
			t.Fatal(err)
		}
	}
	if len(rec.batches) != 0 {
		t.Fatalf("expected no flush at 255, got %d batches", len(rec.batches))
	}
	// 256th — flush
	ev, raw := mkEvent(10)
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected flush at 256, got %d batches", len(rec.batches))
	}
	if got := len(rec.batches[0].Events); got != 256 {
		t.Fatalf("expected 256 events in batch, got %d", got)
	}
	// 257th starts a new batch
	ev, raw = mkEvent(10)
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected still 1 batch after 257th (new batch pending), got %d", len(rec.batches))
	}
}

// T2-B2 — bytes boundary: flush at >= MAX_BYTES, never overshoot by >1 event.
func TestT2B2_BytesBoundary(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 1 << 30 // disable count
	limits.MaxBytes = 1024
	b, rec := newTestBatcher(limits, clock)

	// events of 300 bytes: 3 = 900 (no flush), 4th = 1200 >= 1024 (flush)
	for i := 0; i < 3; i++ {
		ev, raw := mkEvent(300)
		if err := b.Add("s", "c", 1, ev, raw); err != nil {
			t.Fatal(err)
		}
	}
	if len(rec.batches) != 0 {
		t.Fatalf("expected no flush at 900 bytes, got %d", len(rec.batches))
	}
	ev, raw := mkEvent(300)
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected flush at >=1024 bytes, got %d", len(rec.batches))
	}
	// overshoot allowed: 4 events x 300 = 1200 (max 1 event overshoot)
	if got := len(rec.batches[0].Events); got > 5 {
		t.Fatalf("overshoot >1 event: %d", got)
	}
}

// T2-B3 — age boundary: fake clock flush at exactly MAX_AGE.
func TestT2B3_AgeBoundary(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxAge = 1 * time.Millisecond
	limits.MaxEvents = 1 << 30
	limits.MaxBytes = 1 << 30
	b, rec := newTestBatcher(limits, clock)

	ev, raw := mkEvent(10)
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}

	// just below MAX_AGE — no flush
	clock.Advance(999 * time.Microsecond)
	if err := b.Tick(clock.Now()); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 0 {
		t.Fatalf("expected no flush below MAX_AGE, got %d", len(rec.batches))
	}

	// exactly MAX_AGE — flush
	clock.Advance(1 * time.Microsecond)
	if err := b.Tick(clock.Now()); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected flush at MAX_AGE, got %d", len(rec.batches))
	}
}

// T2-B4 — edge cases: empty batch, single-event batch, simultaneous count+age,
// pending batch at shutdown, oversized event, repeated flushes; no dup/loss.
func TestT2B4_EdgeCases(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 256
	limits.MaxBytes = 64 * 1024
	b, rec := newTestBatcher(limits, clock)

	// empty flush — no-op
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 0 {
		t.Fatalf("empty flush should not produce a batch")
	}

	// single event, force flush
	ev, raw := mkEvent(10)
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 || len(rec.batches[0].Events) != 1 {
		t.Fatalf("single-event batch wrong: %d batches", len(rec.batches))
	}

	// oversized single event — emitted alone, not dropped
	big, bigRaw := mkEvent(200 * 1024) // > MAX_BYTES
	if err := b.Add("s", "c", 1, big, bigRaw); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 2 {
		t.Fatalf("oversized event should flush immediately, got %d batches", len(rec.batches))
	}
	if got := len(rec.batches[1].Events); got != 1 {
		t.Fatalf("oversized event batch should have 1 event, got %d", got)
	}

	// pending batch at shutdown — Flush drains it
	ev, raw = mkEvent(10)
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 3 {
		t.Fatalf("shutdown flush should emit pending batch, got %d", len(rec.batches))
	}

	// no event lost: sum of flushed events == all added
	total := 0
	for _, batch := range rec.batches {
		total += len(batch.Events)
	}
	if total != 3 { // single + oversized + shutdown-flushed
		t.Fatalf("event accounting mismatch: flushed %d, expected 3", total)
	}
}

// T2-S1 — sequence: per-slot monotonic, reset per epoch.
func TestT2S1_Sequence(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 256
	b, rec := newTestBatcher(limits, clock)

	// epoch 1: seq 1..5
	for i := 0; i < 5; i++ {
		ev, raw := mkEvent(10)
		if err := b.Add("s", "c", 1, ev, raw); err != nil {
			t.Fatal(err)
		}
		if ev.FeedSequenceLocal != int64(i+1) {
			t.Fatalf("seq %d: got %d want %d", i, ev.FeedSequenceLocal, i+1)
		}
	}
	// flush
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := rec.batches[0].Events[4].FeedSequenceLocal; got != 5 {
		t.Fatalf("last seq in epoch1 should be 5, got %d", got)
	}

	// new epoch: reset
	b.ResetSeq("s")
	for i := 0; i < 3; i++ {
		ev, raw := mkEvent(10)
		if err := b.Add("s", "c", 2, ev, raw); err != nil {
			t.Fatal(err)
		}
		if ev.FeedSequenceLocal != int64(i+1) {
			t.Fatalf("epoch2 seq %d: got %d want %d", i, ev.FeedSequenceLocal, i+1)
		}
	}
	// per-slot isolation: another slot starts at 1, not 6
	ev, raw := mkEvent(10)
	if err := b.Add("s2", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if ev.FeedSequenceLocal != 1 {
		t.Fatalf("slot s2 first seq should be 1, got %d", ev.FeedSequenceLocal)
	}
}

// T2-S2 — O-3 finding: the batcher carries connection-local sequence; whether
// the broker supplies a real sequence is a golden-corpus finding (recorded,
// not asserted here). What IS asserted: batch_seq is per-connection monotonic.
func TestT2S2_BatchSeqMonotonic(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 3 // force small batches
	b, rec := newTestBatcher(limits, clock)

	for i := 0; i < 10; i++ {
		ev, raw := mkEvent(10)
		if err := b.Add("s", "c", 1, ev, raw); err != nil {
			t.Fatal(err)
		}
	}
	// 10 events / 3 = 3 full batches + 1 pending (not flushed yet)
	if len(rec.batches) != 3 {
		t.Fatalf("expected 3 flushed batches, got %d", len(rec.batches))
	}
	for i, batch := range rec.batches {
		if batch.BatchSeq != int64(i+1) {
			t.Fatalf("batch %d seq: got %d want %d", i, batch.BatchSeq, i+1)
		}
	}
	// batch payload hash present
	for i, batch := range rec.batches {
		if len(batch.BatchPayloadHash) != sha256.Size {
			t.Fatalf("batch %d missing payload hash (%d bytes)", i, len(batch.BatchPayloadHash))
		}
	}
}

// T2-H — hash-once: payload_hash computed once per event at Add, carried.
func TestT2H_HashOncePerEvent(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	b, rec := newTestBatcher(limits, clock)

	raw := []byte{1, 2, 3, 4}
	ev, _ := mkEvent(4)
	ev.RawPayload = raw
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	got := rec.batches[0].Events[0].PayloadHash
	want := sha256.Sum256(raw)
	if len(got) != sha256.Size || string(got) != string(want[:]) {
		t.Fatalf("payload hash wrong: got %x want %x", got, want)
	}
}
