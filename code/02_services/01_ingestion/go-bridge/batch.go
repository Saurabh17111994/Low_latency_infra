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
		if err != nil || n < 1024 || n > 64*1024*1024 {
			logf("FATAL: BRIDGE_BATCH_MAX_BYTES=%s — must be an integer in 1024..67108864", v)
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
	// per-connection monotonic batch counter
	batchSeqByConn map[string]int64

	// metrics
	FlushedBatches int64
	FlushedEvents  int64
	FlushedBytes   int64
}

// NewBatcher creates a batcher with the given limits and flush callback.
// now is injectable for deterministic tests (T2-B3); pass nil for time.Now.
func NewBatcher(limits BatchLimits, flush FlushFunc, now func() time.Time) *Batcher {
	if now == nil {
		now = time.Now
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
		batchSeqByConn: map[string]int64{},
	}
}

// Add appends one tick to the current batch, flushing first if a limit is
// reached. The payload hash is computed HERE, once (Q5). Returns the flush
// error if a flush happened and failed (caller decides backpressure).
func (b *Batcher) Add(slotID, connID string, epoch int64, ev *marketdata.TickEvent, raw []byte) error {
	// per-slot sequence, monotonic
	seq := b.nextSeq(slotID)
	ev.FeedSequenceLocal = seq

	// hash computed once at add time (Q5)
	h := sha256.Sum256(raw)
	ev.PayloadHash = h[:]

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	if b.cur == nil {
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

// nextSeq returns the next monotonic per-slot sequence (starts at 1).
func (b *Batcher) nextSeq(slotID string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seqBySlot[slotID]++
	return b.seqBySlot[slotID]
}

// nextBatchSeq returns the next monotonic per-connection batch counter.
func (b *Batcher) nextBatchSeq(connID string) int64 {
	b.batchSeqByConn[connID]++
	return b.batchSeqByConn[connID]
}

// ResetSeq zeroes the per-slot sequence (new epoch, R-185).
func (b *Batcher) ResetSeq(slotID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seqBySlot[slotID] = 0
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

// flushLocked flushes the current batch and resets state. Caller holds mu.
func (b *Batcher) flushLocked(now time.Time) error {
	if b.cur == nil {
		return nil
	}
	if len(b.cur.Events) == 0 {
		b.cur = nil
		return nil
	}
	b.cur.BatchSeq = b.nextBatchSeq(b.cur.ConnectionId)
	// batch_payload_hash: sha256 over concatenated raw payloads (Q5, batch-level)
	h := sha256.New()
	for _, e := range b.cur.Events {
		h.Write(e.RawPayload)
	}
	b.cur.BatchPayloadHash = h.Sum(nil)

	b.FlushedBatches++
	b.FlushedEvents += int64(len(b.cur.Events))
	b.FlushedBytes += int64(b.curBytes)

	toFlush := b.cur
	err := b.flush(toFlush)

	b.cur = nil
	b.curBytes = 0
	return err
}
