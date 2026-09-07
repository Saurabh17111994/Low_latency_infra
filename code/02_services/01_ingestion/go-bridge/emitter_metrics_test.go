package main

import (
	"bytes"
	"testing"
)

// P1-286 guard: validateBridgeMetrics mirrors validateBridgeEvent —
// negative gauges are rejected, zero gauges (fresh slot / idle bridge)
// stay valid.
func TestValidateBridgeMetrics(t *testing.T) {
	valid := BridgeMetrics{RecordType: "bridge_metrics", ContractVersion: ContractVersion,
		TsMs: 1_720_000_000_000, ReconnectConsecutive: 0, ActiveSockets: 1, GoGoroutines: 5}
	if err := validateBridgeMetrics(valid); err != nil {
		t.Fatalf("zero gauges must validate: %v", err)
	}
	rejected := []BridgeMetrics{
		{RecordType: "bridge_metrics", ContractVersion: ContractVersion, TsMs: 1_720_000_000_000, ReconnectConsecutive: -1},
		{RecordType: "bridge_metrics", ContractVersion: ContractVersion, TsMs: 1_720_000_000_000, ActiveSockets: -1},
		{RecordType: "bridge_metrics", ContractVersion: ContractVersion, TsMs: 1_720_000_000_000, GoGoroutines: -1},
		{RecordType: "bridge_metrics", ContractVersion: ContractVersion, TsMs: 0},
		{RecordType: "bridge_metrics", ContractVersion: ContractVersion, TsMs: -5},
		{RecordType: "bridge_event", ContractVersion: ContractVersion, TsMs: 1_720_000_000_000},
		{RecordType: "", ContractVersion: ContractVersion, TsMs: 1_720_000_000_000},
		{RecordType: "bridge_metrics", ContractVersion: ContractVersion + 1, TsMs: 1_720_000_000_000},
	}
	for _, m := range rejected {
		if err := validateBridgeMetrics(m); err == nil {
			t.Fatalf("must reject: %+v", m)
		}
	}
}

// P1-286 guard: the EmitMetrics reject path never reaches the wire.
func TestEmitMetricsRejectsNegativeGauge(t *testing.T) {
	var buf bytes.Buffer
	e := newTestProtoEmitter(&buf)
	if err := e.EmitMetrics(BridgeMetrics{TsMs: 1_720_000_000_000, ActiveSockets: -1}); err == nil {
		t.Fatal("EmitMetrics must reject a negative gauge")
	}
	if buf.Len() != 0 {
		t.Fatalf("rejected metrics must not reach the wire, got %d bytes", buf.Len())
	}
}
