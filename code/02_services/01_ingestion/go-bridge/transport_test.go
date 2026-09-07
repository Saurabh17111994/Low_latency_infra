// transport_test.go — T6 proto emitter tests:
//   - frame shape (length-prefixed, parseable back)
//   - batching (count flush, age flush, batch_seq/batch_payload_hash)
//   - control records ride same transport, distinguished, immediate
//   - payload_hash computed once, raw payload bit-exact
//   - TRANSPORT selection (proto vs NDJSON fallback)

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"errors"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/trading/arrow-bridge/marketdata"
	"google.golang.org/protobuf/proto"
)

// readFrames parses all length-prefixed TransportFrames from b.
func readFrames(t *testing.T, b []byte) []*marketdata.TransportFrame {
	t.Helper()
	var frames []*marketdata.TransportFrame
	for len(b) > 0 {
		if len(b) < 4 {
			t.Fatalf("truncated frame header: %d bytes left", len(b))
		}
		n := int(binary.LittleEndian.Uint32(b[:4]))
		b = b[4:]
		if n > len(b) {
			t.Fatalf("truncated frame body: want %d have %d", n, len(b))
		}
		f := &marketdata.TransportFrame{}
		if err := proto.Unmarshal(b[:n], f); err != nil {
			t.Fatalf("unmarshal frame: %v", err)
		}
		frames = append(frames, f)
		b = b[n:]
	}
	return frames
}

func sampleTick() Tick {
	return Tick{
		Feed: "hft", Mode: "full", Token: 100001,
		LTP: 12345, Close: 12000, Open: 11900, High: 12400, Low: 11800,
		VWAP: 12100, LTQ: 5, Volume: 1000, TBQ: 2000, TSQ: 1500,
		ATV: 3, BTV: 4, OI: 99, TS: 1_720_000_000_000,
		BidPx: [5]int32{100, 99, 98, 97, 96}, AskPx: [5]int32{101, 102, 103, 104, 105},
		BidSize: [5]int32{10, 20, 30, 40, 50}, AskSize: [5]int32{11, 21, 31, 41, 51},
		BidOrd: [5]uint16{1, 2, 3, 4, 5}, AskOrd: [5]uint16{6, 7, 8, 9, 10},
	}
}

// TestProtoEmitterFrames: one tick → flushed → one parseable MarketDataBatch frame.
func TestProtoEmitterFrames(t *testing.T) {
	var buf bytes.Buffer
	// tiny limits: 1 event flushes immediately
	batcher := NewBatcher(BatchLimits{MaxAge: time.Hour, MaxEvents: 1, MaxBytes: 1 << 20},
		func(b *marketdata.MarketDataBatch) error { return protoWriteFrame(&buf, b) }, nil)
	e := NewProtoEmitter(&buf, batcher)

	raw := []byte{0x00, 0x01, 0xFF, 0x80, 0x00} // binary-safe
	if err := e.EmitTick(sampleTick(), "hft-0", "hft-0", 1, time.UnixMilli(1_720_000_000_500), raw); err != nil {
		t.Fatalf("EmitTick: %v", err)
	}
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	frames := readFrames(t, buf.Bytes())
	if len(frames) != 1 {
		t.Fatalf("want 1 frame, got %d", len(frames))
	}
	f := frames[0]
	if f.ProtocolVersion != TransportVersion {
		t.Errorf("protocol_version = %d, want %d", f.ProtocolVersion, TransportVersion)
	}
	batch, ok := f.Payload.(*marketdata.TransportFrame_MarketBatch)
	if !ok {
		t.Fatalf("payload not MarketDataBatch: %T", f.Payload)
	}
	mb := batch.MarketBatch
	if len(mb.Events) != 1 {
		t.Fatalf("want 1 event, got %d", len(mb.Events))
	}
	ev := mb.Events[0]
	if ev.Token != 100001 || ev.Mode != "full" || ev.Feed != "hft" {
		t.Errorf("event fields wrong: %+v", ev)
	}
	if !bytes.Equal(ev.RawPayload, raw) {
		t.Errorf("raw_payload not bit-exact: %x != %x", ev.RawPayload, raw)
	}
	if len(ev.PayloadHash) != 32 {
		t.Errorf("payload_hash not sha256: %d bytes", len(ev.PayloadHash))
	}
	if mb.ConnectionId != "hft-0" || mb.ConnectionEpoch != 1 || mb.BatchSeq != 1 {
		t.Errorf("batch identity wrong: %+v", mb)
	}
	if len(mb.BatchPayloadHash) != 32 {
		t.Errorf("batch_payload_hash not sha256: %d bytes", len(mb.BatchPayloadHash))
	}
}

// TestProtoEmitterBatching: MaxEvents=2 → 3 ticks → 2 frames (2+1).
func TestProtoEmitterBatching(t *testing.T) {
	var buf bytes.Buffer
	batcher := NewBatcher(BatchLimits{MaxAge: time.Hour, MaxEvents: 2, MaxBytes: 1 << 20},
		func(b *marketdata.MarketDataBatch) error { return protoWriteFrame(&buf, b) }, nil)
	e := NewProtoEmitter(&buf, batcher)

	for i := 0; i < 3; i++ {
		tk := sampleTick()
		tk.Token = int32(100000 + i)
		if err := e.EmitTick(tk, "hft-0", "hft-0", 1, time.Now(), []byte{byte(i)}); err != nil {
			t.Fatalf("EmitTick %d: %v", i, err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	frames := readFrames(t, buf.Bytes())
	if len(frames) != 2 {
		t.Fatalf("want 2 frames (2+1), got %d", len(frames))
	}
	f0 := frames[0].GetMarketBatch()
	f1 := frames[1].GetMarketBatch()
	if len(f0.Events) != 2 || len(f1.Events) != 1 {
		t.Errorf("event split wrong: %d + %d", len(f0.Events), len(f1.Events))
	}
	// batch_seq monotonic per connection
	if f0.BatchSeq != 1 || f1.BatchSeq != 2 {
		t.Errorf("batch_seq not monotonic: %d, %d", f0.BatchSeq, f1.BatchSeq)
	}
	// per-slot feed_sequence_local monotonic across batches
	if f0.Events[0].FeedSequenceLocal != 1 || f0.Events[1].FeedSequenceLocal != 2 || f1.Events[0].FeedSequenceLocal != 3 {
		t.Errorf("feed_sequence_local wrong: %d %d %d",
			f0.Events[0].FeedSequenceLocal, f0.Events[1].FeedSequenceLocal, f1.Events[0].FeedSequenceLocal)
	}
}

// TestProtoEmitterControl: events/metrics ride same transport as ControlRecord
// frames, distinguishable, written immediately (not batched).
func TestProtoEmitterControl(t *testing.T) {
	var buf bytes.Buffer
	batcher := NewBatcher(DefaultBatchLimits(),
		func(b *marketdata.MarketDataBatch) error { return protoWriteFrame(&buf, b) }, nil)
	e := NewProtoEmitter(&buf, batcher)
	e.SetManifestFingerprint("fp-abc")
	e.SetSlotTokenHash("hft-0", "hash-0")

	if err := e.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: "hft-0", ConnectionID: "hft-0",
		ConnectionEpoch: 1, State: "connecting", ReceivedTsMs: 1_720_000_000_000}); err != nil {
		t.Fatalf("EmitEvent: %v", err)
	}
	if err := e.EmitMetrics(BridgeMetrics{TsMs: 1_720_000_000_000, ReconnectConsecutive: 0, ActiveSockets: 1, GoGoroutines: 5}); err != nil {
		t.Fatalf("EmitMetrics: %v", err)
	}

	frames := readFrames(t, buf.Bytes())
	if len(frames) != 2 {
		t.Fatalf("want 2 control frames, got %d", len(frames))
	}
	ev, ok := frames[0].Payload.(*marketdata.TransportFrame_Control)
	if !ok {
		t.Fatalf("frame 0 not control: %T", frames[0].Payload)
	}
	if ev.Control.RecordType != "bridge_event" {
		t.Errorf("record_type = %s, want bridge_event", ev.Control.RecordType)
	}
	if ev.Control.Event != "slot_state" || ev.Control.State != "connecting" {
		t.Errorf("event fields wrong: %+v", ev.Control)
	}
	if ev.Control.ManifestFingerprint != "fp-abc" || ev.Control.AssignedTokenSetHash != "hash-0" {
		t.Errorf("slot identity not filled: %+v", ev.Control)
	}
	m, ok := frames[1].Payload.(*marketdata.TransportFrame_Control)
	if !ok {
		t.Fatalf("frame 1 not control: %T", frames[1].Payload)
	}
	if m.Control.RecordType != "bridge_metrics" || m.Control.TsMs != 1_720_000_000_000 || m.Control.GoGoroutines != 5 {
		t.Errorf("metrics fields wrong: %+v", m.Control)
	}
}

// TestProtoEmitterHashOnce: raw payload with a 0x00 byte — hash must be the
// sha256 of the exact bytes (never a base64/JSON re-encoding).
func TestProtoEmitterHashOnce(t *testing.T) {
	var buf bytes.Buffer
	batcher := NewBatcher(BatchLimits{MaxAge: time.Hour, MaxEvents: 1, MaxBytes: 1 << 20},
		func(b *marketdata.MarketDataBatch) error { return protoWriteFrame(&buf, b) }, nil)
	e := NewProtoEmitter(&buf, batcher)

	raw := []byte{0x00, 0x00, 0xFF, 0x00, 0x01}
	if err := e.EmitTick(sampleTick(), "hft-0", "hft-0", 1, time.Now(), raw); err != nil {
		t.Fatalf("EmitTick: %v", err)
	}
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	frames := readFrames(t, buf.Bytes())
	ev := frames[0].GetMarketBatch().Events[0]

	sum := sha256.Sum256(raw)
	if !bytes.Equal(ev.PayloadHash, sum[:]) {
		t.Errorf("payload_hash != sha256(raw): %x != %x", ev.PayloadHash, sum)
	}
}

// TestInitBridgeEmitter: TRANSPORT env selects the proto emitter. Proto is
// the ONLY transport (NDJSON pipe removed 2026-08-29): "pipe", unset, and
// unknown values are FATAL (the bridge exits 2 — verified via subprocess in
// TestInitBridgeEmitterRejectsNonProto, since os.Exit cannot run in-process).
func TestInitBridgeEmitter(t *testing.T) {
	t.Setenv("TRANSPORT", "proto")
	e := initBridgeEmitter(io.Discard)
	if _, ok := e.(*ProtoEmitter); !ok {
		t.Fatalf("TRANSPORT=proto → %T, want *ProtoEmitter", e)
	}
	t.Setenv("TRANSPORT", "grpc")
	e = initBridgeEmitter(io.Discard)
	if _, ok := e.(*ProtoEmitter); !ok {
		t.Fatalf("TRANSPORT=grpc → %T, want *ProtoEmitter", e)
	}
}

// TestInitBridgeEmitterRejectsNonProto: TRANSPORT=pipe / unset / unknown must
// be FATAL (exit 2) with a loud message — the bridge never silently falls
// back to NDJSON. Runs the real initBridgeEmitter in a subprocess (see
// transport_emit_probe_test.go) because os.Exit would kill the test binary
// in-process.
func TestInitBridgeEmitterRejectsNonProto(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		env  string // value or "" for unset
	}{
		{"explicit-pipe", "pipe"},
		{"unset", ""},
		{"unknown", "banana"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(exe, "-test.run=TestBridgeEmitterFatalProbe")
			cmd.Env = append(os.Environ(),
				"BRIDGE_EMITTER_PROBE=1",
				"TRANSPORT="+tc.env,
			)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("TRANSPORT=%q: expected exit(2), got success", tc.env)
			}
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
				t.Fatalf("TRANSPORT=%q: expected exit code 2, got %v (out=%q)", tc.env, err, out)
			}
			if !bytes.Contains(out, []byte("FATAL")) || !bytes.Contains(out, []byte("TRANSPORT")) {
				t.Fatalf("TRANSPORT=%q: stderr must name FATAL + TRANSPORT, got %q", tc.env, out)
			}
		})
	}
}

// lockedWriter models a pipe: each Write call is atomic, but a frame spans
// two Writes (header + body), so frames interleave without emitter-level
// locking. P1-005: that interleaving corrupted the Java length prefix.
type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// TestProtoEmitterConcurrentFraming — P1-005 guard: tick flushes racing
// control frames must never interleave mid-frame. Storms one shared writer
// through the REAL init path, then every byte must parse as length-prefixed
// frames (readFrames fails the test on any corruption) with exact count.
func TestProtoEmitterConcurrentFraming(t *testing.T) {
	t.Setenv("TRANSPORT", "proto")
	t.Setenv("BRIDGE_BATCH_MAX_EVENTS", "1") // every tick flushes
	t.Setenv("BRIDGE_BATCH_MAX_AGE_MS", "1000")

	var mu sync.Mutex
	var buf bytes.Buffer
	em := initBridgeEmitter(&lockedWriter{mu: &mu, buf: &buf})

	const tickWorkers = 4
	const ticksEach = 200
	const ctlWorkers = 2
	const ctlsEach = 100
	var wg sync.WaitGroup
	for w := 0; w < tickWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ticksEach; i++ {
				tk := sampleTick()
				tk.Token = int32(100001 + w)
				if err := em.EmitTick(tk, "hft-0", "s", 1, time.UnixMilli(1_720_000_000_500), []byte{0x01, 0x02}); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for w := 0; w < ctlWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ctlsEach; i++ {
				ev := BridgeEvent{Event: EventReconnect, SlotID: "s", ConnectionID: "hft-0", ConnectionEpoch: 1, State: string(SlotBackoff), ReceivedTsMs: 1}
				if err := em.EmitEvent(ev); err != nil {
					t.Error(err)
					return
				}
				if err := em.EmitMetrics(BridgeMetrics{TsMs: 1, ActiveSockets: 1}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := em.Flush(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	data := append([]byte(nil), buf.Bytes()...)
	mu.Unlock()
	frames := readFrames(t, data)
	want := tickWorkers*ticksEach + ctlWorkers*ctlsEach*2
	if len(frames) != want {
		t.Fatalf("frames: got %d want %d (lost or merged)", len(frames), want)
	}
}
