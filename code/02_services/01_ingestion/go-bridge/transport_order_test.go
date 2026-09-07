package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/trading/arrow-bridge/marketdata"
)

// TestProtoEmitterControlAfterTicks — P1-050: a control frame must never
// overtake ticks buffered earlier. One tick stays buffered (limits far above
// one event); the following EmitEvent must flush it first, so the batch
// frame precedes the control frame on the wire.
func TestProtoEmitterControlAfterTicks(t *testing.T) {
	var buf bytes.Buffer
	batcher := NewBatcher(DefaultBatchLimits(),
		func(b *marketdata.MarketDataBatch) error { return protoWriteFrame(&buf, b) }, nil)
	e := NewProtoEmitter(&buf, batcher)

	raw := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	if err := e.EmitTick(sampleTick(), "hft-0", "hft-0", 1, time.UnixMilli(1_720_000_000_000), raw); err != nil {
		t.Fatalf("EmitTick: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("tick flushed early: %d bytes on the wire before any control", buf.Len())
	}
	if err := e.EmitEvent(BridgeEvent{Event: "reconnect", SlotID: "hft-0", ConnectionID: "hft-0",
		ConnectionEpoch: 2, State: "backoff", Reason: "authentication_refreshed",
		ReceivedTsMs: 1_720_000_001_000}); err != nil {
		t.Fatalf("EmitEvent: %v", err)
	}

	frames := readFrames(t, buf.Bytes())
	if len(frames) != 2 {
		t.Fatalf("want 2 frames (batch + control), got %d", len(frames))
	}
	if _, ok := frames[0].Payload.(*marketdata.TransportFrame_MarketBatch); !ok {
		t.Fatalf("frame 0 must be the drained tick batch, got %T", frames[0].Payload)
	}
	ev, ok := frames[1].Payload.(*marketdata.TransportFrame_Control)
	if !ok {
		t.Fatalf("frame 1 must be the control event, got %T", frames[1].Payload)
	}
	if ev.Control.Event != "reconnect" {
		t.Fatalf("control event = %s, want reconnect", ev.Control.Event)
	}

	// Metrics drain too.
	var buf2 bytes.Buffer
	batcher2 := NewBatcher(DefaultBatchLimits(),
		func(b *marketdata.MarketDataBatch) error { return protoWriteFrame(&buf2, b) }, nil)
	e2 := NewProtoEmitter(&buf2, batcher2)
	if err := e2.EmitTick(sampleTick(), "hft-0", "hft-0", 1, time.UnixMilli(1_720_000_000_000), raw); err != nil {
		t.Fatalf("EmitTick: %v", err)
	}
	if err := e2.EmitMetrics(BridgeMetrics{TsMs: 1_720_000_000_000, ActiveSockets: 1}); err != nil {
		t.Fatalf("EmitMetrics: %v", err)
	}
	frames2 := readFrames(t, buf2.Bytes())
	if len(frames2) != 2 {
		t.Fatalf("want 2 frames (batch + metrics), got %d", len(frames2))
	}
	if _, ok := frames2[0].Payload.(*marketdata.TransportFrame_MarketBatch); !ok {
		t.Fatalf("frame 0 must be the drained tick batch, got %T", frames2[0].Payload)
	}
}
