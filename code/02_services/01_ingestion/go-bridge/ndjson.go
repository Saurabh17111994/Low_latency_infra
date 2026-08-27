package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const NDJSONContractVersion = 2

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

// BridgeMetrics is the supervisor's periodic health snapshot (NDJSON
// record_type "bridge_metrics"). Contract version stays NDJSONContractVersion
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

type BridgeEmitter struct {
	mu        sync.Mutex
	w         io.Writer
	seqBySlot map[string]uint64
	seqMu     sync.Mutex
	// fingerprint is the plan-wide manifest fingerprint; tokenHashBySlot maps
	// each slot id to its assigned-token-set hash. EmitEvent fills empty
	// BridgeEvent identity fields from these so emit sites stay unchanged.
	fingerprint     string
	tokenHashBySlot map[string]string
}

func NewBridgeEmitter(w io.Writer) *BridgeEmitter {
	return &BridgeEmitter{w: w, seqBySlot: map[string]uint64{}, tokenHashBySlot: map[string]string{}}
}

// SetManifestFingerprint records the plan-wide manifest fingerprint (see
// tokenSetHash). Called once at startup before any slot goroutine emits.
func (e *BridgeEmitter) SetManifestFingerprint(fp string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fingerprint = fp
}

// SetSlotTokenHash records the assigned-token-set hash for one slot.
func (e *BridgeEmitter) SetSlotTokenHash(slotID, hash string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tokenHashBySlot[slotID] = hash
}

// nextSeq returns the next monotonic per-slot tick sequence (starts at 1).
// The counter is keyed by slot id and is independent per slot; it resets
// when the process restarts (a new connection epoch begins).
func (e *BridgeEmitter) nextSeq(slotID string) uint64 {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()
	e.seqBySlot[slotID]++
	return e.seqBySlot[slotID]
}

// resetSeq zeroes the per-slot tick sequence (R-185). The doc contract says
// the counter "resets when a new connection epoch begins" — callers invoke it
// at the start of each epoch so feed_sequence_local restarts after a reconnect
// instead of growing for the process lifetime.
// ResetSeq zeroes the per-slot tick sequence (new epoch, R-185).
func (e *BridgeEmitter) ResetSeq(slotID string) {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()
	e.seqBySlot[slotID] = 0
}

// ── Per-token tick counters (count-based losslessness evidence) ─────────────
//
// ARROW_TICK_COUNTS=<intervalSeconds> enables a per-token count of every
// emitted tick. The wire protocol has no sequence numbers, so losslessness is
// verified by reconciling these counts (source of truth: the bytes read off
// the broker TCP socket) against the rows actually stored in Fluss per token.
// The counts are reported to stderr on the interval AND once at shutdown as:
//
//	arrow-tick-counts: total=N t=TOKEN:n t=TOKEN:n ...
var (
	tickCountsOn bool
	tickCountsMu sync.Mutex
	tickCounts   map[int32]int64
)

// finalTickCountReport guards the shutdown report so it is emitted exactly
// once even though both the interval goroutine (on ctx.Done) and main (after
// runHFT returns) may reach for it. main's synchronous call is authoritative:
// a goroutine-only report can be killed mid-write when Go main returns on
// SIGTERM, which would lose the ING-TCP-001 epoch count.
var finalTickCountReport sync.Once

// tickCountsFilePath is where the final report is ALSO written (in addition
// to stderr). The Java parent closes the child's pipe streams as soon as its
// own JVM shutdown begins, so a stderr-only report can be lost to SIGPIPE
// (exit 141) before the reconcile can read it. A file survives the teardown.
// Env: ARROW_TICK_COUNTS_FILE (default /tmp/arrow-tick-counts.txt).
var tickCountsFilePath = "/tmp/arrow-tick-counts.txt"

func recordTickCount(token int32) {
	tickCountsMu.Lock()
	if tickCounts == nil {
		tickCounts = map[int32]int64{}
	}
	tickCounts[token]++
	tickCountsMu.Unlock()
}

// Java's log handler truncates bridge stderr lines (measured 603 B), so the
// per-token report is emitted as multiple bounded lines:
//
//	arrow-tick-counts: total=N chunk=0/52 t=TOKEN:n ...(20 per line)

var tickCountChunkSize = 20

func reportTickCounts() {
	tickCountsMu.Lock()
	keys := make([]int32, 0, len(tickCounts))
	for t := range tickCounts {
		keys = append(keys, t)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	total := int64(0)
	for _, t := range keys {
		total += tickCounts[t]
	}
	lines := (len(keys) + tickCountChunkSize - 1) / tickCountChunkSize
	// Build the full report in memory first.
	var buf strings.Builder
	for c := 0; c < lines; c++ {
		fmt.Fprintf(&buf, "arrow-tick-counts: total=%d chunk=%d/%d", total, c, lines)
		lo, hi := c*tickCountChunkSize, (c+1)*tickCountChunkSize
		if hi > len(keys) {
			hi = len(keys)
		}
		for _, t := range keys[lo:hi] {
			fmt.Fprintf(&buf, " t=%d:n=%d", t, tickCounts[t])
		}
		buf.WriteByte('\n')
	}
	tickCountsMu.Unlock()
	// Persist to a file FIRST (while stderr is still open). The parent JVM
	// closes the child's pipe streams as soon as its own shutdown begins, so
	// a stderr write after that point dies with SIGPIPE (exit 141) and the
	// ING-TCP-001 reconcile would lose the epoch count. The file survives.
	if err := os.WriteFile(tickCountsFilePath, []byte(buf.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "arrow-tick-counts: WARN file write failed %s: %v\n", tickCountsFilePath, err)
	}
	// Then mirror to stderr (best-effort; may die on SIGPIPE if the parent
	// already closed the pipe, but the file above is already written).
	fmt.Fprint(os.Stderr, buf.String())
}

func (e *BridgeEmitter) EmitTick(t Tick, connectionID, slotID string, epoch uint64, received time.Time, rawPayload []byte) error {
	value := struct {
		Tick
		RecordType        string `json:"record_type"`
		ContractVersion   int    `json:"contract_version"`
		ConnectionID      string `json:"connection_id"`
		ConnectionEpoch   uint64 `json:"connection_epoch"`
		SlotID            string `json:"slot_id"`
		FeedSequenceLocal uint64 `json:"feed_sequence_local"`
		ReceivedTsMs      int64  `json:"received_ts_ms"`
		RawPayload        string `json:"raw_payload,omitempty"`
		PayloadHash       string `json:"payload_hash"`
	}{
		Tick: t, RecordType: "tick", ContractVersion: NDJSONContractVersion,
		ConnectionID: connectionID, ConnectionEpoch: epoch, SlotID: slotID,
		FeedSequenceLocal: e.nextSeq(slotID),
		ReceivedTsMs:      received.UnixMilli(),
		RawPayload:        base64.StdEncoding.EncodeToString(rawPayload),
		PayloadHash:       sha256Hex(rawPayload),
	}
	if tickCountsOn {
		recordTickCount(t.Token)
	}
	return e.write(value)
}

// sha256Hex returns the lowercase SHA-256 hex digest of b.
// R-186: the empty-input special case was removed — an empty raw payload must
// still carry its real digest (sha256 of ""), not be dropped via omitempty.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (e *BridgeEmitter) EmitEvent(event BridgeEvent) error {
	event.RecordType = "bridge_event"
	event.ContractVersion = NDJSONContractVersion
	event.Reason = sanitizeDiagnostic(event.Reason)
	// Fill the slot-identity fields from the emitter's identity map when the
	// call site did not set them (R-206: the Java side validates these when
	// present, so an empty field is an explicit "identity not configured").
	e.mu.Lock()
	if event.ManifestFingerprint == "" {
		event.ManifestFingerprint = e.fingerprint
	}
	if event.AssignedTokenSetHash == "" {
		event.AssignedTokenSetHash = e.tokenHashBySlot[event.SlotID]
	}
	e.mu.Unlock()
	// R-097: validateBridgeEvent was never invoked by production — EmitEvent
	// wrote events without validation, so invalid events were emitted silently.
	if err := validateBridgeEvent(event); err != nil {
		return fmt.Errorf("bridge event rejected: %w", err)
	}
	return e.write(event)
}

// EmitMetrics writes one bridge_metrics NDJSON line through the same
// mutex-protected emitter as every other record — one record per line, no
// interleaving (R-184). The supervisor calls it on its 10s ticker.
func (e *BridgeEmitter) EmitMetrics(m BridgeMetrics) error {
	m.RecordType = "bridge_metrics"
	m.ContractVersion = NDJSONContractVersion
	if m.TsMs <= 0 {
		return fmt.Errorf("bridge metrics rejected: ts_ms must be positive")
	}
	return e.write(m)
}

// Flush is a no-op for the NDJSON emitter — every record is written
// synchronously; there is no pending batch to drain.
func (e *BridgeEmitter) Flush() error { return nil }

// Close is a no-op for the NDJSON emitter (see Flush).
func (e *BridgeEmitter) Close() error { return nil }

func (e *BridgeEmitter) write(value any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err = e.w.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// secretPattern scrubs values after secret-bearing names.
//
// Two-pass design (ING-SEC-RED-001): the Bearer pattern runs FIRST so that
// `Bearer <token>` (space-separated) is consumed before the name=value pattern
// can eat only the literal `Bearer` and leave the real token exposed (e.g.
// `Authorization=Bearer secretToken`).
var secretPattern = regexp.MustCompile(`(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|access_token|authorization|appID|token)([=:][^&\s,}]+)`)
var bearerPattern = regexp.MustCompile(`(?i)\bBearer[=:\s]+[^\s,}]+`)

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

func validateBridgeEvent(event BridgeEvent) error {
	if event.RecordType != "bridge_event" {
		return fmt.Errorf("invalid record_type")
	}
	if event.ContractVersion != NDJSONContractVersion {
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
