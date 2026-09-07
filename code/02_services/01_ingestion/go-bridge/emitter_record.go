// emitter_record.go — the bridge's record-level contract, independent of the
// transport that carries it.
//
// The only wire is length-prefixed protobuf TransportFrame (transport.go, T6).
// The records below are the payload of a frame's `control` field:
// BridgeEvent → ControlRecord{record_type:"bridge_event"}, BridgeMetrics →
// ControlRecord{record_type:"bridge_metrics"}. They keep their json tags
// because cmd/gen-corpus and the testdata/golden/*.golden fixtures describe a
// record's shape in JSON — that is a DATA format, not a transport.
//
// Ticks are NOT modelled here. A Tick (main.go) is mapped straight onto a proto
// TickEvent by the T2 batcher, which owns payload_hash and feed_sequence_local.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// ContractVersion is the record-level contract version carried by every
// control record. A Java reader that does not understand it quarantines.
const ContractVersion = 2

const (
	EventSlotState       = "slot_state"
	EventSubscriptionAck = "subscription_ack"
	EventHeartbeatFailed = "heartbeat_failed"
	EventFeedStalled     = "feed_stalled"
	EventDisconnect      = "disconnect"
	EventReconnect       = "reconnect"
	EventAuthFailure     = "auth_failure"
	EventBridgeShutdown  = "bridge_shutdown"
)

var bridgeEvents = map[string]bool{
	EventSlotState: true, EventSubscriptionAck: true, EventHeartbeatFailed: true,
	EventFeedStalled: true, EventDisconnect: true, EventReconnect: true,
	EventAuthFailure: true, EventBridgeShutdown: true,
}

// BridgeEvent is one lifecycle control record (slot state machine, subscription
// ack, fault signal, drain marker).
type BridgeEvent struct {
	RecordType         string `json:"record_type"`
	ContractVersion    int    `json:"contract_version"`
	Event              string `json:"event"`
	SlotID             string `json:"slot_id"`
	ConnectionID       string `json:"connection_id"`
	ConnectionEpoch    uint64 `json:"connection_epoch"`
	State              string `json:"state"`
	AssignedTokens     int    `json:"assigned_tokens,omitempty"`
	AcknowledgedTokens int    `json:"acknowledged_tokens,omitempty"`
	RejectedTokens     int    `json:"rejected_tokens,omitempty"`
	Reason             string `json:"reason,omitempty"`
	ReceivedTsMs       int64  `json:"received_ts_ms"`
	// ManifestFingerprint and AssignedTokenSetHash are the slot-identity
	// fields of the safety contract (plan §Slot-scoped safety propagation).
	// Both are lowercase SHA-256 hex over the sorted token set (8-byte
	// big-endian per token) — byte-identical to the Java computation. They
	// are optional on the wire so the supervisor's emit sites stay simple;
	// EmitEvent fills them from the emitter's identity map, and the Java
	// side validates them when present.
	ManifestFingerprint  string `json:"manifest_fingerprint,omitempty"`
	AssignedTokenSetHash string `json:"assigned_token_set_hash,omitempty"`
}

// BridgeMetrics is the supervisor's periodic health snapshot (control record
// with record_type "bridge_metrics"). Contract version stays ContractVersion
// — the record is an additive extension of the v2 contract, consumed only by
// ingestion-side gauges; a Java version that predates it ignores it.
type BridgeMetrics struct {
	RecordType           string `json:"record_type"`
	ContractVersion      int    `json:"contract_version"`
	TsMs                 int64  `json:"ts_ms"`
	ReconnectConsecutive int    `json:"reconnect_consecutive"`
	ActiveSockets        int    `json:"active_sockets"`
	GoGoroutines         int    `json:"go_goroutines"`
}

// sha256Hex returns the lowercase SHA-256 hex digest of b.
// R-186: the empty-input special case was removed — an empty raw payload must
// still carry its real digest (sha256 of ""), not be dropped via omitempty.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// secretPattern scrubs values after secret-bearing names.
//
// Two-pass design (ING-SEC-RED-001): the Bearer pattern runs FIRST so that
// `Bearer <token>` (space-separated) is consumed before the name=value pattern
// can eat only the literal `Bearer` and leave the real token exposed (e.g.
// `Authorization=Bearer secretToken`).
// P1-024: the separator allows optional quotes/whitespace so quoted-JSON
// diagnostics ("ARROW_TOKEN":"secret") match, and generic password/secret
// names are covered. The value excludes quotes/brackets so JSON framing
// survives redaction. Verified: plain prose ("token expired", "tokens=5",
// "secretary=x") is untouched — only name+separator shapes redact.
var secretPattern = regexp.MustCompile(`(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|ARROW_REQUEST_TOKEN|access_token|authorization|password|passwd|secret|appID|token)["']?\s*[=:]\s*["']?[^&\s,}"'\]]+`)
var bearerPattern = regexp.MustCompile(`(?i)\bBearer[=:\s]+[^\s,}]+`)

// sanitizeDiagnostic redacts secret-bearing values and bounds the result.
// Every free-text field that reaches the wire (control records, stderr logs)
// goes through it.
func sanitizeDiagnostic(s string) string {
	s = bearerPattern.ReplaceAllString(s, "Bearer=[REDACTED]")
	s = secretPattern.ReplaceAllString(s, "$1=[REDACTED]")
	// R-187: byte-boundary truncation could split a multi-byte UTF-8 rune,
	// which json.Marshal would corrupt with U+FFFD. Truncate on a rune
	// boundary instead.
	if len(s) > 512 {
		r := []rune(s)
		if len(r) > 512 {
			s = string(r[:512])
		}
	}
	return strings.TrimSpace(s)
}

// validateBridgeEvent is the R-097 source gate: EmitEvent validates before
// writing, so an invalid control record never reaches the wire.
func validateBridgeEvent(event BridgeEvent) error {
	if event.RecordType != "bridge_event" {
		return fmt.Errorf("invalid record_type")
	}
	if event.ContractVersion != ContractVersion {
		return fmt.Errorf("unsupported contract_version %d", event.ContractVersion)
	}
	if event.SlotID == "" || event.ConnectionID == "" {
		return fmt.Errorf("slot_id and connection_id are required")
	}
	if event.State == "" {
		return fmt.Errorf("state is required")
	}
	if !bridgeEvents[event.Event] {
		return fmt.Errorf("unknown event %q", event.Event)
	}
	if event.ConnectionEpoch == 0 {
		return fmt.Errorf("connection_epoch must be positive")
	}
	if event.ReceivedTsMs <= 0 {
		return fmt.Errorf("received_ts_ms must be positive")
	}
	if event.AssignedTokens < 0 || event.AcknowledgedTokens < 0 || event.RejectedTokens < 0 {
		return fmt.Errorf("token counts cannot be negative")
	}
	return nil
}
