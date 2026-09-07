// T2 tests — Go batcher (contract §7.4): batch boundaries with fake clock
// (T2-B1..B4), sequence semantics (T2-S1/S2), and edge cases. Deterministic:
// the clock is injectable and the flush callback records batches.

package main

import (
	"crypto/sha256"
	"errors"
	"sync"
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

	// new epoch: the batcher restarts the sequence itself (P1-051 native:
	// no ResetSeq API exists anymore — the epoch change alone restarts).
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

// T2-B5 — P1-001 fail-fast guard: one open batch must never mix
// (ConnectionId, ConnectionEpoch). A conn or epoch change boundary-flushes
// with the old header intact; per-connection BatchSeq advances independently.
func TestT2B5_ConnEpochBoundary(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 1 << 30 // disable count flush
	limits.MaxBytes = 1 << 30  // disable bytes flush
	b, rec := newTestBatcher(limits, clock)

	for i := 0; i < 2; i++ {
		ev, raw := mkEvent(10)
		if err := b.Add("s1", "connA", 1, ev, raw); err != nil {
			t.Fatal(err)
		}
	}
	if len(rec.batches) != 0 {
		t.Fatalf("expected no flush before boundary, got %d", len(rec.batches))
	}
	// conn change — boundary flush of the connA batch
	ev, raw := mkEvent(10)
	if err := b.Add("s2", "connB", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected boundary flush on conn change, got %d", len(rec.batches))
	}
	if got := rec.batches[0].ConnectionId; got != "connA" {
		t.Fatalf("batch 0 conn: got %q want connA", got)
	}
	if got := len(rec.batches[0].Events); got != 2 {
		t.Fatalf("batch 0 events: got %d want 2", got)
	}
	// epoch bump on connB with an open batch — boundary flush again
	ev, raw = mkEvent(10)
	if err := b.Add("s2", "connB", 2, ev, raw); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 2 {
		t.Fatalf("expected boundary flush on epoch change, got %d", len(rec.batches))
	}
	if got := rec.batches[1].ConnectionId; got != "connB" {
		t.Fatalf("batch 1 conn: got %q want connB", got)
	}
	if got := rec.batches[1].ConnectionEpoch; got != 1 {
		t.Fatalf("batch 1 epoch: got %d want 1", got)
	}
	// drain: pending batch is connB epoch 2 with 1 event
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 3 {
		t.Fatalf("expected 3 batches after drain, got %d", len(rec.batches))
	}
	last := rec.batches[2]
	if last.ConnectionId != "connB" || last.ConnectionEpoch != 2 || len(last.Events) != 1 {
		t.Fatalf("batch 2 wrong: conn=%q epoch=%d events=%d",
			last.ConnectionId, last.ConnectionEpoch, len(last.Events))
	}
	// BatchSeq is per-connection: connA=1, connB=1,2
	if rec.batches[0].BatchSeq != 1 || rec.batches[1].BatchSeq != 1 || rec.batches[2].BatchSeq != 2 {
		t.Fatalf("batch seqs wrong: %d %d %d",
			rec.batches[0].BatchSeq, rec.batches[1].BatchSeq, rec.batches[2].BatchSeq)
	}
}

var errTestBoundary = errors.New("test boundary flush failure")

// flakyFlush fails while fail is true, then records. Fail-fast helper for
// P1-015/017 retry tests.
type flakyFlush struct {
	fail bool
	rec  *recordingFlush
}

func (f *flakyFlush) Flush(batch *marketdata.MarketDataBatch) error {
	if f.fail {
		return errTestBoundary
	}
	return f.rec.Flush(batch)
}

// T2-B5-ERR — P1-001 + P1-015/017 guard: a failed boundary flush retains the
// old batch and does NOT consume the current tick. The caller retries: the
// old batch goes out first (same number), then the retried tick.
func TestT2B5_BoundaryFlushErrorKeepsTick(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 1 << 30
	limits.MaxBytes = 1 << 30
	rec := &recordingFlush{}
	flaky := &flakyFlush{fail: true, rec: rec}
	b := NewBatcher(limits, flaky.Flush, clock.Now)

	ev, raw := mkEvent(10)
	if err := b.Add("s1", "connA", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	// boundary send fails — error out, old batch retained, tick not consumed
	ev2, raw2 := mkEvent(10)
	if err := b.Add("s2", "connB", 1, ev2, raw2); err == nil {
		t.Fatal("expected boundary flush error, got nil")
	}
	if len(rec.batches) != 0 {
		t.Fatalf("failed flush must send nothing, got %d batches", len(rec.batches))
	}
	if b.FlushedBatches.Load() != 0 || b.FlushedEvents.Load() != 0 {
		t.Fatalf("failed flush must count nothing, got batches=%d events=%d",
			b.FlushedBatches.Load(), b.FlushedEvents.Load())
	}
	// transport recovers: retry delivers the retained connA batch first
	flaky.fail = false
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected 1 batch after drain, got %d", len(rec.batches))
	}
	if got := rec.batches[0]; got.ConnectionId != "connA" || len(got.Events) != 1 {
		t.Fatalf("retried batch wrong: conn=%q events=%d", got.ConnectionId, len(got.Events))
	}
	// now retry the connB tick — boundary is clean (cur is nil), no error
	if err := b.Add("s2", "connB", 1, ev2, raw2); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(rec.batches))
	}
	got := rec.batches[1]
	if got.ConnectionId != "connB" || len(got.Events) != 1 {
		t.Fatalf("retried tick batch wrong: conn=%q events=%d", got.ConnectionId, len(got.Events))
	}
	if b.FlushedBatches.Load() != 2 || b.FlushedEvents.Load() != 2 {
		t.Fatalf("counters wrong: batches=%d events=%d", b.FlushedBatches.Load(), b.FlushedEvents.Load())
	}
}

// T2-F1 — P1-015/017 guard: a failed limit flush retains the batch with a
// stable number and counts nothing; the retry sends the same batch with the
// same BatchSeq, counted exactly once.
func TestT2F1_FailedFlushRetainsStableSeq(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 3 // 3rd Add triggers the limit flush
	limits.MaxBytes = 1 << 30
	rec := &recordingFlush{}
	flaky := &flakyFlush{fail: true, rec: rec}
	b := NewBatcher(limits, flaky.Flush, clock.Now)

	for i := 0; i < 3; i++ {
		ev, raw := mkEvent(10)
		if i < 2 {
			if err := b.Add("s", "c", 1, ev, raw); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := b.Add("s", "c", 1, ev, raw); err == nil {
			t.Fatal("expected limit flush error, got nil")
		}
	}
	if len(rec.batches) != 0 {
		t.Fatalf("failed flush must send nothing, got %d", len(rec.batches))
	}
	if b.FlushedBatches.Load() != 0 || b.FlushedEvents.Load() != 0 || b.FlushedBytes.Load() != 0 {
		t.Fatalf("failed flush must count nothing: %+v", b)
	}
	// recover and drain: same 3 events, first number, counted once
	flaky.fail = false
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected 1 batch after retry, got %d", len(rec.batches))
	}
	if got := len(rec.batches[0].Events); got != 3 {
		t.Fatalf("retried batch events: got %d want 3", got)
	}
	if got := rec.batches[0].BatchSeq; got != 1 {
		t.Fatalf("retried batch seq: got %d want 1 (stable across retry)", got)
	}
	if b.FlushedBatches.Load() != 1 || b.FlushedEvents.Load() != 3 {
		t.Fatalf("counters wrong after retry: batches=%d events=%d",
			b.FlushedBatches.Load(), b.FlushedEvents.Load())
	}
	// next batch continues the numbering with no hole
	for i := 0; i < 3; i++ {
		ev, raw := mkEvent(10)
		if err := b.Add("s", "c", 1, ev, raw); err != nil {
			t.Fatal(err)
		}
	}
	if len(rec.batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(rec.batches))
	}
	if got := rec.batches[1].BatchSeq; got != 2 {
		t.Fatalf("second batch seq: got %d want 2", got)
	}
}

// T2-S3 — P1-014/016 guard: concurrent Adds on one slot must land in number
// order. Number-take + field writes + append are one locked step; any future
// split-lock regresses here regardless of goroutine scheduling.
func TestT2S3_ConcurrentAddOrder(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 1 << 30 // disable count flush
	limits.MaxBytes = 1 << 30  // disable bytes flush
	b, rec := newTestBatcher(limits, clock)

	const workers = 8
	const perWorker = 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ev, raw := mkEvent(10)
				if err := b.Add("s", "c", 1, ev, raw); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(rec.batches))
	}
	events := rec.batches[0].Events
	if len(events) != workers*perWorker {
		t.Fatalf("events: got %d want %d", len(events), workers*perWorker)
	}
	for i, e := range events {
		if e.FeedSequenceLocal != int64(i+1) {
			t.Fatalf("position %d has seq %d: order/number mismatch", i, e.FeedSequenceLocal)
		}
	}
}

// T2-R1 — P1-002/003 guard: Add carries raw onto a BARE event (copied).
// The production caller pre-fills RawPayload, which masked this gap; a bare
// tick must still arrive with its bytes, and the batch hash must cover them.
func TestT2R1_AddCarriesRawPayload(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	limits := DefaultBatchLimits()
	limits.MaxEvents = 1 << 30
	limits.MaxBytes = 1 << 30
	b, rec := newTestBatcher(limits, clock)

	raw := []byte{1, 2, 3, 4, 5}
	ev := &marketdata.TickEvent{Token: 1} // bare: no RawPayload set
	if err := b.Add("s", "c", 1, ev, raw); err != nil {
		t.Fatal(err)
	}
	// caller mutates its buffer after Add — stored copy must not change
	for i := range raw {
		raw[i] = 9
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(rec.batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(rec.batches))
	}
	got := rec.batches[0].Events[0].RawPayload
	want := []byte{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("RawPayload len: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RawPayload[%d]: got %d want %d (not carried/copied)", i, got[i], want[i])
		}
	}
	wantHash := sha256.Sum256(want)
	if rec.batches[0].BatchPayloadHash == nil {
		t.Fatal("BatchPayloadHash missing")
	}
	for i := range wantHash {
		if rec.batches[0].BatchPayloadHash[i] != wantHash[i] {
			t.Fatal("BatchPayloadHash does not cover the carried payload")
		}
	}
}

// TestT2D1_BatchLimitsZeroDefaultsMaxAge — P1-142/143: BatchLimits{} must not
// flush on every Tick; MaxAge defaults to 1ms like the other limits.
func TestT2D1_BatchLimitsZeroDefaultsMaxAge(t *testing.T) {
	b := NewBatcher(BatchLimits{}, func(batch *marketdata.MarketDataBatch) error { return nil }, nil)
	if b.limits.MaxAge <= 0 {
		t.Fatalf("MaxAge not defaulted: %v", b.limits.MaxAge)
	}
}

// TestT2D2_FlushedCountersRaceFree — P1-142/143: concurrent flushes and
// counter reads must not race (run with -race).
func TestT2D2_FlushedCountersRaceFree(t *testing.T) {
	b := NewBatcher(DefaultBatchLimits(), func(batch *marketdata.MarketDataBatch) error { return nil }, nil)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				ev := Tick{Token: int32(w*1000 + i)}.toMarketTick().ToTickEvent(
					"s", "c", 1, int64(i), 0, []byte{byte(i)})
				_ = b.Add("s", "c", 1, ev, []byte{byte(i)})
				_ = b.FlushedBatches.Load()
				_ = b.FlushedEvents.Load()
				_ = b.FlushedBytes.Load()
			}
		}(w)
	}
	wg.Wait()
}
