// batch.go — Go batcher (T2): groups ticks into MarketDataBatch proto
// frames, flushing on count/bytes/age. Locked limits (O-2, contract §6 T2):
//
//	MAX_AGE    = 1ms   (measured p99 win, THR-PROBE-002)
//	MAX_EVENTS = 256
//	MAX_BYTES  = 64KiB
//
// Design (contract §7.4, Q3/Q5/Q13/Q15):
//   - Sequence is per-slot, connection-local, monotonic, reset per epoch.
//   - payload_hash computed ONCE per event at add time (Q5: no recompute).
//   - raw_payload carried as exact bytes (Q3/Q6: never base64/JSON).
//   - Fake clock (now func) makes boundary tests deterministic (T2-B*).
//   - Flush callback owns transport (proto frames / gRPC — the only
//     transport since the NDJSON pipe was removed 2026-08-29).

package main

import (
	"crypto/sha256"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trading/arrow-bridge/marketdata"
)

// BatchLimits are the flush thresholds (O-2). MAX_BYTES is a target, not a
// hard cap — an oversized single event is emitted alone rather than dropped.
type BatchLimits struct {
	MaxAge    time.Duration // default 1ms
	MaxEvents int           // default 256
	MaxBytes  int           // default 64KiB
}

// DefaultBatchLimits returns the locked O-2 defaults.
func DefaultBatchLimits() BatchLimits {
	return BatchLimits{
		MaxAge:    1 * time.Millisecond,
		MaxEvents: 256,
		MaxBytes:  64 * 1024,
	}
}

// batchLimitsFromEnv reads the BRIDGE_BATCH_* tuning knobs (K1, 2026-08-29).
// Unset keys fall back to the locked O-2 defaults; a non-integer or
// out-of-range value is a FATAL startup error (same contract as hftRange).
func batchLimitsFromEnv(logf func(string, ...any)) BatchLimits {
	limits := DefaultBatchLimits()

	if v := os.Getenv("BRIDGE_BATCH_MAX_AGE_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			logf("FATAL: BRIDGE_BATCH_MAX_AGE_MS=%s — must be an integer in 1..1000", v)
			os.Exit(exitFatalStart)
		}
		limits.MaxAge = time.Duration(n) * time.Millisecond
	}
	if v := os.Getenv("BRIDGE_BATCH_MAX_EVENTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1_000_000 {
			logf("FATAL: BRIDGE_BATCH_MAX_EVENTS=%s — must be an integer in 1..1000000", v)
			os.Exit(exitFatalStart)
		}
		limits.MaxEvents = n
	}
	if v := os.Getenv("BRIDGE_BATCH_MAX_BYTES"); v != "" {
		n, err := strconv.Atoi(v)
		// P1-212: cap well below the 64MiB wire frame cap (maxFrameLen) — a
		// batch at the old 64MiB max plus TransportFrame/MarketDataBatch
		// overhead deterministically failed 'frame too large', dropping ticks.
		if err != nil || n < 1024 || n > 16*1024*1024 {
			logf("FATAL: BRIDGE_BATCH_MAX_BYTES=%s — must be an integer in 1024..16777216", v)
			os.Exit(exitFatalStart)
		}
		limits.MaxBytes = n
	}
	return limits
}

// FlushFunc receives a complete batch for transport. Called synchronously;
// must not block the data path beyond its own write. Errors are returned to
// the producer (Add) so the caller decides (log/backpressure/halt).
type FlushFunc func(batch *marketdata.MarketDataBatch) error

// Batcher accumulates TickEvents into per-slot batches. Safe for concurrent
// use by multiple slots (each Add is atomic); the flush callback is invoked
// under the batcher's lock so batches are never interleaved.
type Batcher struct {
	mu       sync.Mutex
	limits   BatchLimits
	now      func() time.Time
	flush    FlushFunc
	cur      *marketdata.MarketDataBatch
	curBytes int
	curSince time.Time

	// per-slot sequence (connection-local, monotonic, reset per epoch)
	seqBySlot map[string]int64
	// last epoch seen per slot (R-185: any epoch change restarts the
	// sequence; epochs increase on every reconnect)
	epochBySlot map[string]int64
	// per-connection monotonic batch counter
	batchSeqByConn map[string]int64

	// metrics
	// P1-142/143: atomic so concurrent readers never race the flush path.
	FlushedBatches atomic.Int64
	FlushedEvents  atomic.Int64
	FlushedBytes   atomic.Int64
}

// NewBatcher creates a batcher with the given limits and flush callback.
// now is injectable for deterministic tests (T2-B3); pass nil for time.Now.
func NewBatcher(limits BatchLimits, flush FlushFunc, now func() time.Time) *Batcher {
	if now == nil {
		now = time.Now
	}
	// P1-142/143: default MaxAge like the other limits — BatchLimits{}
	// must not flush on every Tick.
	if limits.MaxAge <= 0 {
		limits.MaxAge = 1 * time.Millisecond
	}
	if limits.MaxEvents <= 0 {
		limits.MaxEvents = 256
	}
	if limits.MaxBytes <= 0 {
		limits.MaxBytes = 64 * 1024
	}
	return &Batcher{
		limits:         limits,
		now:            now,
		flush:          flush,
		seqBySlot:      map[string]int64{},
		epochBySlot:    map[string]int64{},
		batchSeqByConn: map[string]int64{},
	}
}

// Add appends one tick to the current batch, flushing first if a limit is
// reached. The payload hash is computed HERE, once (Q5). Returns the flush
// error if a flush happened and failed (caller decides backpressure).
// A connection/epoch boundary also flushes (P1-001): one open batch carries
// exactly one (ConnectionId, ConnectionEpoch) and never mixes.
func (b *Batcher) Add(slotID, connID string, epoch int64, ev *marketdata.TickEvent, raw []byte) error {
	// hash computed once at add time (Q5); pure on raw, safe outside the lock.
	h := sha256.Sum256(raw)

	b.mu.Lock()
	defer b.mu.Unlock()

	// P1-014/016 fail-fast: number-take + field writes + append are ONE
	// locked step. Taking the number in a separate lock let two same-slot
	// Adds invert order (numbers 1,2 landing as [2,1]).
	// P1-051 native fix: no external reset API exists. The counter restarts
	// when the epoch changes, here, atomically with the take — a reset can
	// neither race an append nor be forgotten by a caller.
	if b.epochBySlot[slotID] != epoch {
		b.epochBySlot[slotID] = epoch
		b.seqBySlot[slotID] = 0
	}
	b.seqBySlot[slotID]++
	ev.FeedSequenceLocal = b.seqBySlot[slotID]
	ev.PayloadHash = h[:]
	// P1-002/003 fail-fast: Add carries raw onto the event itself (copied —
	// the caller may reuse its buffer). Never rely on the caller to pre-fill
	// RawPayload; flushLocked hashes this field for the batch hash.
	ev.RawPayload = append([]byte(nil), raw...)

	now := b.now()
	// Invariant (P1-001 fail-fast): one open batch = one (conn, epoch).
	// This mismatch check is what enforces the epoch boundary on reconnect.
	if b.cur == nil {
		b.cur = &marketdata.MarketDataBatch{
			ConnectionId:    connID,
			ConnectionEpoch: epoch,
			CreatedMs:       now.UnixMilli(),
		}
		b.curSince = now
	} else if b.cur.ConnectionId != connID || b.cur.ConnectionEpoch != epoch {
		// Boundary: send the open batch before opening one with the new
		// header. On failure the old batch is retained (P1-015/017) and the
		// current tick is NOT consumed — the caller must retry this Add
		// later. Overwriting cur here would orphan the retained batch.
		if err := b.flushLocked(now); err != nil {
			return err
		}
		b.cur = &marketdata.MarketDataBatch{
			ConnectionId:    connID,
			ConnectionEpoch: epoch,
			CreatedMs:       now.UnixMilli(),
		}
		b.curSince = now
	}

	// byte accounting: proto-encode size estimate (raw + fixed overhead).
	// +16 covers field tags/lengths for the small fixed fields; the raw
	// payload dominates the encoded size.
	evSize := len(raw) + 16
	b.curBytes += evSize
	b.cur.Events = append(b.cur.Events, ev)

	// flush if count or bytes exceeded
	if len(b.cur.Events) >= b.limits.MaxEvents || b.curBytes >= b.limits.MaxBytes {
		return b.flushLocked(now)
	}
	return nil
}

// Tick advances the batcher clock (used by the age-flush ticker in production;
// tests call it directly). Returns flush error if an age flush happened.
func (b *Batcher) Tick(now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maybeAgeFlush(now)
}

// Flush force-flushes the current batch (shutdown drain, T2-B4 pending batch).
func (b *Batcher) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked(b.now())
}

// nextBatchSeq returns the next monotonic per-connection batch counter.
func (b *Batcher) nextBatchSeq(connID string) int64 {
	b.batchSeqByConn[connID]++
	return b.batchSeqByConn[connID]
}

// maybeAgeFlush flushes if the current batch has been open >= MaxAge.
func (b *Batcher) maybeAgeFlush(now time.Time) error {
	if b.cur == nil {
		return nil
	}
	if now.Sub(b.curSince) < b.limits.MaxAge {
		return nil
	}
	return b.flushLocked(now)
}

// flushLocked sends the current batch. State and success metrics change ONLY
// on success (P1-015/017 fail-fast): on error the batch, its byte count and
// its BatchSeq are retained so a later Add/Flush resends the same batch with
// the same number (no loss, no hole, no double count). Caller holds mu.
func (b *Batcher) flushLocked(now time.Time) error {
	if b.cur == nil {
		return nil
	}
	if len(b.cur.Events) == 0 {
		b.cur = nil
		return nil
	}
	// Stable number: assigned once, kept across retries. Counters start at
	// 1, so 0 means unassigned. A retried batch keeps its number so a failed
	// send that actually arrived is a detectable duplicate, not a hole.
	if b.cur.BatchSeq == 0 {
		b.cur.BatchSeq = b.nextBatchSeq(b.cur.ConnectionId)
	}
	// batch_payload_hash: sha256 over concatenated raw payloads (Q5, batch-level)
	h := sha256.New()
	for _, e := range b.cur.Events {
		h.Write(e.RawPayload)
	}
	b.cur.BatchPayloadHash = h.Sum(nil)

	toFlush := b.cur
	toBytes := b.curBytes
	if err := b.flush(toFlush); err != nil {
		return err // retained above: nothing cleared, nothing counted
	}
	b.FlushedBatches.Add(1)
	b.FlushedEvents.Add(int64(len(toFlush.Events)))
	b.FlushedBytes.Add(int64(toBytes))

	b.cur = nil
	b.curBytes = 0
	return nil
}
