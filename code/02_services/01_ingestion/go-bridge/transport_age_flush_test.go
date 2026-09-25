// transport_age_flush_test.go — wired T2 age flush (CHG-316): a partial batch
// must be flushed by the ticker within ~MaxAge without waiting for the next
// tick to arrive. Measured gap this closes: with the ticker missing, the
// remainder of each burst waited for the next tick — S1 p90 481 ms at a 2 Hz
// feed (one 500 ms period; the same ~20% fraction paid only 49 ms at 20 Hz).

package main

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/trading/arrow-bridge/marketdata"
	"google.golang.org/protobuf/proto"
)

// completeFrames parses as many whole length-prefixed frames as b holds and
// ignores a trailing partial frame (the age-flush ticker writes concurrently
// with the reader).
func completeFrames(b []byte) []*marketdata.TransportFrame {
	var frames []*marketdata.TransportFrame
	for len(b) >= 4 {
		n := int(binary.LittleEndian.Uint32(b[:4]))
		if n <= 0 || len(b) < 4+n {
			break
		}
		var f marketdata.TransportFrame
		if err := proto.Unmarshal(b[4:4+n], &f); err != nil {
			break
		}
		frames = append(frames, &f)
		b = b[4+n:]
	}
	return frames
}

// waitCompleteFrames polls the sync buffer until at least want complete
// frames are present (the ticker writes concurrently with the reader).
func waitCompleteFrames(t *testing.T, out *syncBuffer, want int, timeout time.Duration) []*marketdata.TransportFrame {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		frames := completeFrames([]byte(out.String()))
		if len(frames) >= want {
			return frames
		}
		if time.Now().After(deadline) {
			t.Fatalf("wanted >= %d frames within %s, got %d (bytes=%d)",
				want, timeout, len(frames), len(out.String()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestProtoEmitterAgeFlushTickerFlushesPartialBatch drives the wired ticker
// end to end: 3 ticks (< MaxEvents) must reach the wire with no Flush() and
// no further EmitTick calls, StopAgeFlush must stop the flushing, and an
// explicit drain still writes the pending batch (shutdown contract).
func TestProtoEmitterAgeFlushTickerFlushesPartialBatch(t *testing.T) {
	out := newSyncBuffer()
	limits := BatchLimits{MaxAge: 20 * time.Millisecond, MaxEvents: 256, MaxBytes: 1 << 20}
	e := newProtoEmitterWithLimits(out, limits)
	defer func() { _ = e.Close() }()

	for i := 0; i < 3; i++ {
		tk := sampleTick()
		tk.Token = int32(100000 + i)
		if err := e.EmitTick(tk, "hft-0", "hft-0", 1, time.Now(), []byte{byte(i)}); err != nil {
			t.Fatalf("EmitTick %d: %v", i, err)
		}
	}

	// No Flush and no further Add: the ticker alone must drain the batch.
	frames := waitCompleteFrames(t, out, 1, 2*time.Second)
	batch, ok := frames[0].Payload.(*marketdata.TransportFrame_MarketBatch)
	if !ok {
		t.Fatalf("payload not MarketDataBatch: %T", frames[0].Payload)
	}
	if got := len(batch.MarketBatch.Events); got != 3 {
		t.Fatalf("auto-flushed batch events = %d, want 3", got)
	}

	// StopAgeFlush (synchronous) must stop the flushing.
	e.StopAgeFlush()
	tk := sampleTick()
	tk.Token = int32(200000)
	if err := e.EmitTick(tk, "hft-0", "hft-0", 1, time.Now(), []byte{0xEE}); err != nil {
		t.Fatalf("EmitTick after stop: %v", err)
	}
	time.Sleep(3 * limits.MaxAge)
	if got := len(completeFrames([]byte(out.String()))); got != 1 {
		t.Fatalf("frames after StopAgeFlush = %d, want 1 (ticker still flushing?)", got)
	}

	// The explicit drain still writes the pending batch.
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(completeFrames([]byte(out.String()))); got != 2 {
		t.Fatalf("frames after drain = %d, want 2", got)
	}
}
