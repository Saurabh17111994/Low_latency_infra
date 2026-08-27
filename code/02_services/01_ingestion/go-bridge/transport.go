// transport.go — proto transport emitter (T6, contract §6 T6).
//
// TRANSPORT=pipe  (default) — NDJSON lines on stdout (existing path, fallback).
// TRANSPORT=proto — length-prefixed protobuf TransportFrame on stdout:
//
//	[4-byte little-endian length][protobuf TransportFrame bytes]
//
// Market ticks are accumulated into MarketDataBatch by the T2 Batcher
// (1ms/256/64KiB) and flushed as frames. Control records (bridge_event,
// bridge_metrics, broker_quarantine) ride the same transport as
// ControlRecord frames, distinguished by record_type — never interleaved
// into market batches (Q20). Control frames flush immediately (ordering).
//
// The NDJSON and proto emitters share the BridgeEmitter interface so the
// production emit call sites are unchanged.

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/trading/arrow-bridge/marketdata"
	"google.golang.org/protobuf/proto"
)

// TransportVersion is the framing protocol version. Java rejects unknown
// versions → quarantine (contract §T6-I2).
const TransportVersion = 1

// Frame max length: 64MiB (batcher caps batches at 64KiB + overhead; this
// bounds memory and rejects corrupt length prefixes).
const maxFrameLen = 64 << 20

// Transport is the emit interface shared by NDJSON and proto emitters.
type Transport interface {
	EmitTick(t Tick, connectionID, slotID string, epoch uint64, received time.Time, rawPayload []byte) error
	EmitEvent(event BridgeEvent) error
	EmitMetrics(m BridgeMetrics) error
	SetManifestFingerprint(fp string)
	SetSlotTokenHash(slotID, hash string)
	ResetSeq(slotID string)
	Flush() error // drain pending batched ticks (shutdown)
	Close() error
}

// ProtoEmitter emits length-prefixed TransportFrame protobuf records to w.
// Market ticks are batched via the T2 Batcher; control records are written
// immediately as standalone ControlRecord frames.
type ProtoEmitter struct {
	mu      sync.Mutex
	w       io.Writer
	batcher *Batcher
	// slot-identity map for control records (same contract as NDJSON emitter)
	fingerprint     string
	tokenHashBySlot map[string]string
}

// NewProtoEmitter creates a proto emitter with the locked T2 batch limits.
func NewProtoEmitter(w io.Writer, batcher *Batcher) *ProtoEmitter {
	e := &ProtoEmitter{w: w, tokenHashBySlot: map[string]string{}}
	e.batcher = batcher
	return e
}

// writeFrame writes one length-prefixed TransportFrame. Caller holds mu.
func (e *ProtoEmitter) writeFrame(frame *marketdata.TransportFrame) error {
	b, err := proto.Marshal(frame)
	if err != nil {
		return fmt.Errorf("proto marshal: %w", err)
	}
	if len(b) > maxFrameLen {
		return fmt.Errorf("frame too large: %d bytes", len(b))
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := e.w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = e.w.Write(b)
	return err
}

// toMarketTick converts the main-package Tick (NDJSON shape) to the proto
// package Tick so its ToTickEvent mapping applies (single mapping contract).
func (t Tick) toMarketTick() marketdata.Tick {
	return marketdata.Tick{
		Feed: t.Feed, Mode: t.Mode, Token: t.Token,
		LTP: t.LTP, Close: t.Close, Open: t.Open, High: t.High, Low: t.Low,
		VWAP: t.VWAP, LTQ: t.LTQ, Volume: t.Volume, TBQ: t.TBQ, TSQ: t.TSQ,
		ATV: t.ATV, BTV: t.BTV, OI: t.OI, TS: t.TS,
		BidPx: t.BidPx, AskPx: t.AskPx, BidSz: t.BidSize, AskSz: t.AskSize,
		BidOrd: t.BidOrd, AskOrd: t.AskOrd,
	}
}

// EmitTick batches one tick into the batcher. The flush callback writes a
// MarketDataBatch frame synchronously (batcher lock → writeFrame).
func (e *ProtoEmitter) EmitTick(t Tick, connectionID, slotID string, epoch uint64, received time.Time, rawPayload []byte) error {
	ev := t.toMarketTick().ToTickEvent(slotID, connectionID, epoch, received.UnixMilli(), 0, rawPayload)
	// batcher.Add computes payload_hash once (Q5), assigns per-slot seq, and
	// flushes on count/bytes/age via the callback below.
	return e.batcher.Add(slotID, connectionID, int64(epoch), ev, rawPayload)
}

// controlFrame builds a TransportFrame wrapping a ControlRecord.
func (e *ProtoEmitter) controlFrame(recordType string, ev *BridgeEvent, m *BridgeMetrics) *marketdata.TransportFrame {
	cr := &marketdata.ControlRecord{
		RecordType:      recordType,
		ContractVersion: NDJSONContractVersion,
	}
	if ev != nil {
		cr.Event = ev.Event
		cr.SlotId = ev.SlotID
		cr.ConnectionId = ev.ConnectionID
		cr.ConnectionEpoch = int64(ev.ConnectionEpoch)
		cr.State = ev.State
		cr.AssignedTokens = int32(ev.AssignedTokens)
		cr.AcknowledgedTokens = int32(ev.AcknowledgedTokens)
		cr.RejectedTokens = int32(ev.RejectedTokens)
		cr.Reason = ev.Reason
		cr.ReceivedTsMs = ev.ReceivedTsMs
		cr.ManifestFingerprint = ev.ManifestFingerprint
		cr.AssignedTokenSetHash = ev.AssignedTokenSetHash
	}
	if m != nil {
		cr.TsMs = m.TsMs
		cr.ReconnectConsecutive = int32(m.ReconnectConsecutive)
		cr.ActiveSockets = int32(m.ActiveSockets)
		cr.GoGoroutines = int32(m.GoGoroutines)
	}
	return &marketdata.TransportFrame{
		Payload:         &marketdata.TransportFrame_Control{Control: cr},
		ProtocolVersion: TransportVersion,
	}
}

// EmitEvent writes a bridge_event control frame immediately (no batching).
func (e *ProtoEmitter) EmitEvent(event BridgeEvent) error {
	event.RecordType = "bridge_event"
	event.ContractVersion = NDJSONContractVersion
	event.Reason = sanitizeDiagnostic(event.Reason)
	// R-097: validate BEFORE write (same as NDJSON emitter) — an invalid
	// event must never reach the wire.
	if err := validateBridgeEvent(event); err != nil {
		return fmt.Errorf("bridge event rejected: %w", err)
	}
	e.mu.Lock()
	if event.ManifestFingerprint == "" {
		event.ManifestFingerprint = e.fingerprint
	}
	if event.AssignedTokenSetHash == "" {
		event.AssignedTokenSetHash = e.tokenHashBySlot[event.SlotID]
	}
	frame := e.controlFrame("bridge_event", &event, nil)
	err := e.writeFrame(frame)
	e.mu.Unlock()
	return err
}

// EmitMetrics writes a bridge_metrics control frame immediately.
func (e *ProtoEmitter) EmitMetrics(m BridgeMetrics) error {
	m.RecordType = "bridge_metrics"
	m.ContractVersion = NDJSONContractVersion
	if m.TsMs <= 0 {
		return fmt.Errorf("bridge metrics rejected: ts_ms must be positive")
	}
	e.mu.Lock()
	frame := e.controlFrame("bridge_metrics", nil, &m)
	err := e.writeFrame(frame)
	e.mu.Unlock()
	return err
}

// SetManifestFingerprint stores the plan-wide fingerprint for control frames.
func (e *ProtoEmitter) SetManifestFingerprint(fp string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fingerprint = fp
}

// SetSlotTokenHash stores a slot's assigned-token-set hash for control frames.
func (e *ProtoEmitter) SetSlotTokenHash(slotID, hash string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tokenHashBySlot[slotID] = hash
}

// ResetSeq forwards to the batcher (per-slot seq reset on epoch, R-185).
func (e *ProtoEmitter) ResetSeq(slotID string) {
	e.batcher.ResetSeq(slotID)
}

// Flush drains the batcher (shutdown). Control frames are already written.
func (e *ProtoEmitter) Flush() error {
	return e.batcher.Flush()
}

// Close drains the batcher (idempotent with Flush).
func (e *ProtoEmitter) Close() error {
	return e.Flush()
}

// BridgeEmitter keeps its type name as the transport-agnostic interface —
// main.go's `bridgeEmitter` global is reassigned based on TRANSPORT.
var _ Transport = (*BridgeEmitter)(nil)
var _ Transport = (*ProtoEmitter)(nil)

// initBridgeEmitter selects the emitter based on TRANSPORT env (T6, Q21).
//   - "proto" (or "grpc" alias for the future gRPC mode): proto frames
//   - anything else ("pipe", unset): NDJSON (fallback / rollback path)
func initBridgeEmitter(w io.Writer) Transport {
	raw := os.Getenv("TRANSPORT")
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "proto", "grpc":
		fmt.Fprintf(os.Stderr, "arrow-bridge: transport=proto frames (TRANSPORT=%s)\n", v)
		batcher := NewBatcher(DefaultBatchLimits(), func(batch *marketdata.MarketDataBatch) error {
			// synchronous flush under batcher lock — write the frame
			return protoWriteFrame(w, batch)
		}, nil)
		return NewProtoEmitter(w, batcher)
	case "pipe":
		fmt.Fprintf(os.Stderr, "arrow-bridge: transport=NDJSON pipe (TRANSPORT=pipe — deliberate rollback path)\n")
		return NewBridgeEmitter(w)
	default:
		// TRANSPORT unset or unknown: fall back to NDJSON for compatibility
		// (rollback / old deployments), but say so LOUDLY — a silently unset
		// TRANSPORT previously let benches run the NDJSON path by accident.
		fmt.Fprintf(os.Stderr, "arrow-bridge: WARNING transport=NDJSON pipe (TRANSPORT=%q — UNSET/unknown; proto is the low-latency path, set TRANSPORT=proto explicitly)\n", raw)
		return NewBridgeEmitter(w)
	}
}

// protoWriteFrame is the batcher flush callback: wraps a MarketDataBatch in
// a TransportFrame and writes it length-prefixed.
func protoWriteFrame(w io.Writer, batch *marketdata.MarketDataBatch) error {
	frame := &marketdata.TransportFrame{
		Payload:         &marketdata.TransportFrame_MarketBatch{MarketBatch: batch},
		ProtocolVersion: TransportVersion,
	}
	b, err := proto.Marshal(frame)
	if err != nil {
		return fmt.Errorf("proto marshal: %w", err)
	}
	if len(b) > maxFrameLen {
		return fmt.Errorf("frame too large: %d bytes", len(b))
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
