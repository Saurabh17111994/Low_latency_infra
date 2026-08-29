package main

// Test helper: builds the proto emitter the same way initBridgeEmitter does
// (transport.go) but without the TRANSPORT env dependency — the tests that
// capture bridge output now capture length-prefixed TransportFrame protobuf.

import (
	"io"
	"time"

	"github.com/trading/arrow-bridge/marketdata"
)

func newTestProtoEmitter(w io.Writer) *ProtoEmitter {
	return NewProtoEmitter(w, NewBatcher(DefaultBatchLimits(), func(batch *marketdata.MarketDataBatch) error {
		return protoWriteFrame(w, batch)
	}, nil))
}

// flushTestEmitter drains the emitter's batcher so buffered ticks reach the
// captured writer. The NDJSON emitter wrote every tick synchronously; the
// proto emitter batches (count/bytes/age), so tests that read the capture
// right after a short epoch must flush first.
func flushTestEmitter() {
	if bridgeEmitter == nil {
		return
	}
	_ = bridgeEmitter.Flush()
	// age-based flush is time-driven; give the batcher one tick to drain.
	time.Sleep(5 * time.Millisecond)
}
