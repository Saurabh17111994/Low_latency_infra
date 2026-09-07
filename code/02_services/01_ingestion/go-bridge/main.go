package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// Tick is the unified tick record consumed by the Java pipeline. The emitter
// maps it onto a proto TickEvent (the NDJSON pipe was removed 2026-08-29);
// the json tags stay because cmd/gen-corpus and the
// testdata/golden/*.golden fixtures describe the record in JSON — a data
// format, not a transport.
// All prices in integer paise (₹1 = 100 paise). Timestamps in epoch ms.
type Tick struct {
	Feed   string `json:"feed"` // "hft" (the only feed since the Standard feed was removed 2026-08-14)
	Mode   string `json:"mode"` // "ltp", "ltpc", "quote", "full"
	Token  int32  `json:"token"`
	LTP    int32  `json:"ltp_paise"`
	Close  int32  `json:"close_paise,omitempty"`
	Open   int32  `json:"open_paise,omitempty"`
	High   int32  `json:"high_paise,omitempty"`
	Low    int32  `json:"low_paise,omitempty"`
	VWAP   int32  `json:"vwap_paise,omitempty"`
	LTQ    int32  `json:"ltq,omitempty"`
	Volume int64  `json:"volume,omitempty"`
	TBQ    int64  `json:"total_buy_qty,omitempty"`
	TSQ    int64  `json:"total_sell_qty,omitempty"`
	ATV    uint32 `json:"atv,omitempty"`
	BTV    uint32 `json:"btv,omitempty"`
	OI     int64  `json:"open_interest,omitempty"`
	// unix epoch milliseconds
	// P1-021: depth ladders are fixed [5] arrays — encoding/json omitempty
	// NEVER omits arrays (Len>0 always), so the tags were a lie. Removed:
	// LTP ticks always carry explicit zero ladders (wire-identical before
	// and after — proven by byte-identical regenerated goldens).
	TS      int64     `json:"ts_ms"`
	BidPx   [5]int32  `json:"bid_px"`
	AskPx   [5]int32  `json:"ask_px"`
	BidSize [5]int32  `json:"bid_qty"`
	AskSize [5]int32  `json:"ask_qty"`
	BidOrd  [5]uint16 `json:"bid_orders"`
	AskOrd  [5]uint16 `json:"ask_orders"`
}

// bridgeEmitter is the transport emitter. It is deliberately nil until main()
// calls initBridgeEmitter (T6) — before ANY emit — so the process can never
// write through an unconfigured transport.
var bridgeEmitter Transport

// maxDecodeErrorsPer10s is the decode-error burst threshold per slot (plan
// §Error Handling): 100 errors in 10 seconds closes the slot and reconnects.
const maxDecodeErrorsPer10s = 100

// Process exit statuses per plan §main.go:
//
//	0 — requested shutdown
//	1 — unexpected supervisor failure (HFT connect error, stdout broken pipe)
//	2 — fatal auth/plan/config failure at startup
const (
	exitRequested  = 0
	exitSupervisor = 1
	exitFatalStart = 2
)

// HFT runtime tuning, set once from env in main() (see the policy block).
// Package vars keep the values readable from runHFTEpoch without threading
// them through every supervisor/slot signature.
var (
	heartbeatInterval = 3 * time.Second
	stallTimeout      = 15 * time.Second
)

func main() {
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "arrow-bridge: "+format+"\n", args...)
	}

	appID := envOrFatal("ARROW_APP_ID")
	appSecret := envOrFatal("ARROW_APP_SECRET")
	userID := envOrDefault("ARROW_USER_ID", "")
	password := envOrDefault("ARROW_PASSWORD", "")
	totpKey := envOrDefault("ARROW_TOTP_KEY", "")

	client := arrow.NewClient(appID, appSecret)
	var refreshAuth func(context.Context) error

	// T6: select the transport emitter (TRANSPORT=proto|grpc — the only
	// values; anything else is FATAL exit 2) BEFORE
	// any emit — the bridge must not write a line transport to a proto reader.
	// Runs in BOTH fake-broker and live modes.
	bridgeEmitter = initBridgeEmitter(os.Stdout)

	// Test-only fake-broker auth mode (ARROW_FAKE_BROKER=1, paired with the
	// ARROW_HFT_URL development override): skip AutoLogin so bridge lifecycle
	// tests (FullStackE2E) can run against faketool without hitting the real
	// login API. Production is unchanged — without the flag the TOTP trio is
	// mandatory and AutoLogin is the only auth path (ARROW_TOKEN removed
	// 2026-08-24).
	if isFakeBrokerMode() {
		logf("fake-broker auth mode (test-only): skipping AutoLogin")
		client.SetToken("fake-token")
		refreshAuth = func(context.Context) error { return nil }
	} else {
		if userID == "" || password == "" || totpKey == "" {
			logf("live mode requires ARROW_USER_ID+PASSWORD+TOTP_KEY (ARROW_TOKEN removed 2026-08-24)")
			// Fatal auth failure → status 2 (plan §main.go).
			os.Exit(exitFatalStart)
		}
		refreshAuth = func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return client.AutoLogin(userID, password, totpKey)
		}
		logf("auto-login user=%s", userID)
		if err := client.AutoLogin(userID, password, totpKey); err != nil {
			fmt.Fprintf(os.Stderr, "arrow-bridge: AutoLogin failed: %v\n", err)
			// Fatal auth failure → status 2 (plan §main.go).
			os.Exit(exitFatalStart)
		}
		logf("authenticated (token_len=%d)", len(client.GetToken()))
	}

	// instrument tokens — auto-extracted from Arrow broker CSV
	// Priority: 1) ARROW_INSTRUMENT_TOKENS env var, 2) CSV at
	// ARROW_INSTRUMENT_MANIFEST (fallback to approved 1,024 manifest),
	// 3) fail fast — no synthetic fallback in production (plan: startup MUST
	// reject rather than silently ingest fake tokens).
	csvPath := os.Getenv("ARROW_INSTRUMENT_MANIFEST")
	if csvPath == "" {
		csvPath = "/instruments/NSE_CM_EQUITY (1024).csv"
	}
	tokens := loadTokensFromCSV("ARROW_INSTRUMENT_TOKENS", csvPath)
	if len(tokens) == 0 {
		logf("FATAL: no instrument tokens (set ARROW_INSTRUMENT_TOKENS or ARROW_INSTRUMENT_MANIFEST CSV); refusing to start")
		os.Exit(exitFatalStart)
	}
	logf("%d instruments", len(tokens))
	// ARROW_HFT_CONNECTIONS is tunable 1..MaxHFTConnections (C1 plug-and-play):
	// default 1 preserves today's single-socket behavior; 2-3 enable the
	// multi-connection fan-out for the full 2,433-instrument manifest when
	// the Premium subscription allows concurrent sessions. Out-of-range or
	// non-integer values exit FATAL (hftRange).
	slotCount := hftRange(logf, "ARROW_HFT_CONNECTIONS", 1, 1, MaxHFTConnections)
	plan, err := BuildSubscriptionPlan(tokens, slotCount, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		logf("FATAL: invalid subscription plan: %v", err)
		os.Exit(exitFatalStart)
	}
	logf("subscription plan=%s slots=%d", plan.Fingerprint, len(plan.Slots))
	// Slot identity for the safety contract: the bridge events carry
	// manifest_fingerprint (plan-wide token-set hash) and per-slot
	// assigned_token_set_hash, byte-identical to Java's computation so the
	// ingestion side can cross-check them (warn-only).
	var allTokens []int32
	for _, s := range plan.Slots {
		allTokens = append(allTokens, s.Tokens...)
		bridgeEmitter.SetSlotTokenHash(s.SlotID, tokenSetHash(s.Tokens))
	}
	bridgeEmitter.SetManifestFingerprint(tokenSetHash(allTokens))

	// HFT policy keys — every key below is read from env and enforced here,
	// mirroring the Java IngestionConfig exactInt/intRange checks so a wrong
	// value fails closed in the bridge too (it also runs standalone). Pinned
	// keys must equal the pin exactly; tunable keys must be within range.
	// (ING-UNIT-018 parity: the Go side previously read ARROW_HFT_LATENCY_MS
	// inline with fail-open semantics — a non-integer was silently ignored and
	// there was no upper bound — while Java's intRange(50,50,60000) rejects
	// both. It now flows through hftRange like every other tunable.)
	latencyMs := hftRange(logf, "ARROW_HFT_LATENCY_MS", 50, 50, 60_000)
	_ = hftPin(logf, "ARROW_HFT_MAX_TOKENS_PER_CONNECTION", 1024)
	_ = hftPin(logf, "ARROW_HFT_MAX_TOKENS_PER_REQUEST", 512)
	heartbeatInterval = time.Duration(hftPin(logf, "ARROW_HFT_HEARTBEAT_SECONDS", 3)) * time.Second
	stallTimeout = time.Duration(hftRange(logf, "ARROW_HFT_STALL_TIMEOUT_SECONDS", 15, 5, 60)) * time.Second
	responseTimeout := time.Duration(hftRange(logf, "ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", 10, 1, 60)) * time.Second
	_ = hftPin(logf, "ARROW_HFT_RECONNECT_BASE_SECONDS", 1)
	_ = hftPin(logf, "ARROW_HFT_RECONNECT_MAX_SECONDS", 30)
	_ = hftPin(logf, "ARROW_HFT_AUTH_REFRESH_ATTEMPTS", 3)
	_ = hftPin(logf, "ARROW_HFT_MIN_ACTIVE_SLOTS", 1)
	logf("HFT policy: response_timeout=%s stall_timeout=%s heartbeat=%s", responseTimeout, stallTimeout, heartbeatInterval)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Count-based losslessness evidence (ING-TCP-001): ARROW_TICK_COUNTS=<sec>
	// enables per-token emitted-tick counters, reported on the interval and at
	// shutdown. Reconciliation is done against per-token row counts in Fluss.
	if v := os.Getenv("ARROW_TICK_COUNTS"); v != "" {
		// The parent JVM closes our pipe streams the moment its own shutdown
		// begins. With counters on, the final report is written to a FILE
		// first (source of truth for the reconcile); the stderr mirror would
		// otherwise raise SIGPIPE (exit 141) and mask the clean shutdown. Ignore
		// SIGPIPE so a closed pipe yields an EPIPE error instead of a kill —
		// the file report is already persisted by then.
		signal.Ignore(syscall.SIGPIPE)
		if f := os.Getenv("ARROW_TICK_COUNTS_FILE"); f != "" {
			tickCountsFilePath = f
		}
		interval := 60
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			interval = n
		}
		tickCountsOn = true
		go func() {
			ticker := time.NewTicker(time.Duration(interval) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					reportTickCounts()
				case <-ctx.Done():
					// Final counts at shutdown — the goroutine may be killed
					// before it writes when Go main returns, so main also calls
					// finalTickCountReport.Do(reportTickCounts) after runHFT
					// returns (see below). The Once makes exactly one report.
					finalTickCountReport.Do(reportTickCounts)
					return
				}
			}
		}()
		logf("tick counters enabled (interval=%ds)", interval)
	}

	runHFT(ctx, cancel, client, plan, latencyMs, responseTimeout, refreshAuth, logf)
	// Drain point: all EmitTick/EmitEvent calls are synchronous and ordered
	// under the emitter mutex, so the bridge_shutdown event below is
	// guaranteed to be the last frame. Duplicate shutdown paths are
	// collapsed by bridgeShutdownOnce — the event is emitted exactly once.
	// Emit the final per-token tick count synchronously HERE (before any
	// further work or return) so the ING-TCP-001 shutdown report cannot be
	// lost to a goroutine/main exit race.
	maybeReportFinalTickCounts()
	// T6: drain any pending batched ticks BEFORE the shutdown marker so the
	// Java reader sees the complete stream (proto mode: Flush writes the
	// final MarketDataBatch frame).
	if err := bridgeEmitter.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: final flush failed: %v\n", err)
	}
	emitShutdownEvent()
	_ = bridgeEmitter.Close()
}

// maybeReportFinalTickCounts emits the ING-TCP-001 shutdown report exactly
// once, and only when tick counters are enabled (P1-209: the unconditional
// finalTickCountReport.Do here ran reportTickCounts even with
// ARROW_TICK_COUNTS unset, creating/truncating the counts file and writing
// a stray stderr line on every disabled shutdown).
func maybeReportFinalTickCounts() {
	if !tickCountsOn {
		return
	}
	finalTickCountReport.Do(reportTickCounts)
}

// isFakeBrokerMode reports the test-only fake-broker auth mode
// (ARROW_FAKE_BROKER=1/true, see main()).
func isFakeBrokerMode() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ARROW_FAKE_BROKER")))
	return v == "1" || v == "true"
}

// bridgeShutdownOnce collapses duplicate shutdown paths so the
// bridge_shutdown drain event is emitted exactly once per process.
// A pointer so tests can reset it between cases.
var bridgeShutdownOnce = &sync.Once{}

// emitShutdownEvent writes the terminal bridge_shutdown event (the drain
// marker). Safe to call multiple times from any goroutine.
func emitShutdownEvent() {
	bridgeShutdownOnce.Do(func() {
		_ = bridgeEmitter.EmitEvent(BridgeEvent{
			Event:           EventBridgeShutdown,
			SlotID:          "hft-0",
			ConnectionID:    "hft-0",
			ConnectionEpoch: 1,
			State:           string(SlotTerminal),
			Reason:          "drain_complete",
			ReceivedTsMs:    time.Now().UnixMilli(),
		})
	})
}

func runHFT(ctx context.Context, cancel context.CancelFunc, client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) {
	if len(plan.Slots) == 0 {
		cancel()
		return
	}
	// Production policy: exactly one Arrow HFT socket is approved for this
	// phase. Multi-slot is exercised by tests (runHFTSupervisor) and the
	// deferred multi-connection phase; here we refuse to start.
	if len(plan.Slots) != 1 {
		slot := plan.Slots[0]
		logf("FATAL: this deployment is approved for exactly one Arrow HFT socket; planned=%d", len(plan.Slots))
		// R-177: a deployment/policy violation is NOT a credential failure —
		// emit disconnect (non-auth vocabulary) so alerting keyed on
		// auth_failure does not fire for a non-credential condition.
		_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "disconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: 1, State: string(SlotTerminal), Reason: "single_socket_policy_violation", ReceivedTsMs: time.Now().UnixMilli()})
		cancel()
		return
	}
	runHFTSupervisor(ctx, client, plan, latencyMs, responseTimeout, refreshAuth, logf)
}

func runReconnectLoop(ctx context.Context, run func(uint64) bool, onRetry func(uint64, time.Duration), wait func(context.Context, time.Duration)) {
	for attempt, epoch := 0, uint64(1); ; attempt, epoch = attempt+1, epoch+1 {
		if run(epoch) {
			return
		}
		if ctx.Err() != nil {
			return
		}
		delay := Backoff(attempt)
		onRetry(epoch, delay)
		wait(ctx, delay)
		if ctx.Err() != nil {
			return
		}
	}
}

func runHFTEpoch(ctx context.Context, streamFactory hftStreamFactory, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, epoch uint64, refreshAuth func(context.Context) error, authRefreshes *int, logf func(string, ...any)) slotEpochResult {
	tokens := slot.Tokens
	stream, err := streamFactory()
	if err != nil {
		_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "disconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
		logf("HFT connect failed: %v", err)
		// R-301: a dial failure carrying an auth signal (e.g. the broker
		// rejecting the WebSocket upgrade because the AutoLogin session token
		// expired — "websocket: bad handshake") must trigger a fresh TOTP
		// re-login BEFORE the next dial. The reconnect loop otherwise retries
		// with the same stale Config.Token forever: the read-loop auth-refresh
		// branch below never runs because the socket never opens.
		if refreshAuth != nil && isHFTAuthError(err.Error()) {
			if authRefreshes != nil && *authRefreshes >= maxAuthRefreshAttempts {
				logf("HFT auth refresh exhausted on dial failure")
				return epochTerminal
			}
			if authRefreshes != nil {
				*authRefreshes++
			}
			if rerr := refreshAuth(ctx); rerr != nil {
				logf("HFT auth refresh failed on dial failure: %v", sanitizeDiagnostic(rerr.Error()))
			} else {
				_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "reconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: "authentication_refreshed", ReceivedTsMs: time.Now().UnixMilli()})
				logf("HFT auth refreshed after dial failure")
			}
		}
		return epochRetryable
	}
	addActiveSocket(1)
	defer func() { addActiveSocket(-1) }()
	defer stream.Close()

	// R-218: epochStop must be CLOSE-based, not send-based. The
	// heartbeat and watchdog goroutines select on the same channel; a single
	// buffered send can be consumed by one of them, starving the epoch's
	// main select and leaving the epoch stuck until ctx.Done (no retry).
	// close() wakes every receiver.
	epochStop := make(chan struct{})
	var epochStopOnce sync.Once
	signalEpochStop := func() {
		epochStopOnce.Do(func() { close(epochStop) })
	}
	_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotConnecting), ReceivedTsMs: time.Now().UnixMilli()})
	responses := make(chan arrow.HFTResponsePacket, 1)
	lastFrameNanos := atomic.Int64{}
	decodeErrors := 0
	decodeWindow := time.Now()
	// T1: the auth-refresh budget is owned by the slot's reconnect loop
	// (runHFTSlotWithFactory) so it survives reconnect epochs and exhaustion
	// terminates that slot only. nil (direct single-epoch callers) falls back
	// to an epoch-local budget, preserving the pre-T1 per-epoch semantics.
	authTries := 0
	if authRefreshes != nil {
		authTries = *authRefreshes
	}
	syncAuthBudget := func() {
		if authRefreshes != nil {
			*authRefreshes = authTries
		}
	}
	terminalAuthFailure := false
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	// R-185: a new connection epoch begins. The batcher restarts the
	// per-slot tick sequence itself when it sees the new epoch — no reset
	// call needed (P1-051: no external reset API, nothing to race).

	// onDecoded fires immediately before each LTP/full tick dispatch in the
	// same read goroutine, so lastDecoded holds the exact decompressed packet
	// bytes for the next tick callback. This preserves the plan's raw_payload
	// invariant: decoded JSON must not replace the original broker packet bytes.
	var lastDecoded []byte
	go stream.ReadHFTWithFrame(readCtx,
		func(t arrow.HFTLTPTick) {
			lastFrameNanos.Store(time.Now().UnixNano())
			// P1-212: surface emit failures — an EmitTick error (batch
			// flush / frame-too-large / transport write) must never
			// silently drop ticks.
			if err := bridgeEmitter.EmitTick(Tick{
				Feed: "hft", Mode: "ltpc", Token: t.Token,
				LTP: t.LTP, VWAP: t.VWAP, Volume: t.Volume,
				ATV: t.ATV, BTV: t.BTV,
				// P1-023: LTT is microseconds — /1e3 yields ms like the
				// full path (TS ns /1e6). The old /1e6 emitted SECONDS,
				// 1000x below every full tick's ts_ms.
				TS: int64(t.LTT / 1_000),
			}, slot.ConnectionID, slot.SlotID, epoch, time.Now(), lastDecoded); err != nil {
				logf("HFT emit tick failed: %v", sanitizeDiagnostic(err.Error()))
			}
		},
		func(t arrow.HFTFullTick) {
			lastFrameNanos.Store(time.Now().UnixNano())
			// P1-212: surface emit failures — see the LTP path above.
			if err := bridgeEmitter.EmitTick(Tick{
				Feed: "hft", Mode: "full", Token: t.Token,
				LTP: t.LTP, LTQ: t.LTQ, VWAP: t.VWAP,
				Open: t.Open, High: t.High, Close: t.Close, Low: t.Low,
				TBQ: t.TBQ, TSQ: t.TSQ, Volume: t.Volume,
				OI: int64(t.OI), ATV: t.ATV, BTV: t.BTV,
				BidPx: t.BidPx, AskPx: t.AskPx,
				BidSize: t.BidSize, AskSize: t.AskSize,
				BidOrd: t.BidOrd, AskOrd: t.AskOrd,
				TS: int64(t.TS / 1_000_000),
			}, slot.ConnectionID, slot.SlotID, epoch, time.Now(), lastDecoded); err != nil {
				logf("HFT emit tick failed: %v", sanitizeDiagnostic(err.Error()))
			}
		},
		func(r arrow.HFTResponsePacket) {
			select {
			case responses <- r:
			default:
			}
		},
		func(mt int, payload []byte) {
			lastFrameNanos.Store(time.Now().UnixNano())
		},
		func(frame []byte) {
			lastDecoded = frame
		},
		func(err error) {
			message := err.Error()
			if isHFTAuthError(message) {
				refreshErr := error(nil)
				if refreshAuth != nil && authTries < maxAuthRefreshAttempts {
					authTries++
					syncAuthBudget()
					refreshErr = refreshAuth(ctx)
				}
				// classifyAuthRefresh expects the prior count; authTries was
				// already incremented above, so pass authTries-1.
				// P1-028: fold an already-exhausted budget into hasRefresh=false.
				// Otherwise no refresh runs, refreshErr stays nil, and the
				// classifier reads (true, n, nil) as authResumed — a bogus
				// authentication_refreshed event plus a retry that never
				// refreshes, looping forever (the shared budget persists across
				// epochs). Exhausted now routes to authTerminalExhausted.
				switch classifyAuthRefresh(refreshAuth != nil && authTries < maxAuthRefreshAttempts, authTries-1, refreshErr) {
				case authResumed:
					noteReconnect(slot.SlotID)
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "reconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: "authentication_refreshed", ReceivedTsMs: time.Now().UnixMilli()})
					signalEpochStop()
					return
				case authRetry:
					logf("HFT authentication refresh failed (attempt %d): %v", authTries, sanitizeDiagnostic(err.Error()))
					signalEpochStop()
					return
				default: // authTerminal or authTerminalExhausted
					reason := "authentication_refresh_failed"
					if refreshErr == nil {
						reason = "authentication_refresh_exhausted"
					}
					// Set the flag BEFORE signalEpochStop: close() synchronizes
					// with the epoch's `<-epochStop` receive, so the receiver is
					// guaranteed to observe the terminal decision (previously
					// ordered after the close, an auth-terminal arriving while a
					// subscription response was pending was misread as retryable
					// and the slot reconnected forever).
					terminalAuthFailure = true
					// R-297: signal the epoch stop BEFORE the emit — EmitEvent
					// blocks on a full stdout pipe, and the stop must never wait
					// behind a write that cannot complete.
					signalEpochStop()
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "auth_failure", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotTerminal), Reason: reason, ReceivedTsMs: time.Now().UnixMilli()})
					logf("HFT authentication failed; refresh exhausted")
					// T1: auth exhaustion is terminal for THIS slot only. The
					// shared process context is deliberately NOT cancelled, so
					// peer slots keep running with their own refresh episodes.
					// Returning epochTerminal stops this slot; the supervisor
					// records the terminal outcome and leaves peers alone.
					return
				}
			}
			if isHFTDecodeError(message) {
				now := time.Now()
				burst := isDecodeErrorBurst(decodeErrors, decodeWindow, now)
				decodeErrors = burst.count
				decodeWindow = burst.windowStart
				logf("HFT decode error count=%d: %s", decodeErrors, sanitizeDiagnostic(message))
				// Plan §Error Handling: burst threshold is 100 errors in 10
				// seconds per slot; exceeding it closes that slot and reconnects.
				if burst.exceeded {
					// R-297 wedge fix: signal the epoch stop BEFORE the emit —
					// EmitEvent blocks on a full stdout pipe, and the stop must
					// never wait behind a write that cannot complete.
					signalEpochStop()
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "feed_stalled", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotStalled), Reason: "decode_error_burst", ReceivedTsMs: time.Now().UnixMilli()})
				}
				return
			}
			// R-297 wedge fix: signal the epoch stop BEFORE the emit — a
			// disconnect event must not block the reconnect behind a full pipe.
			signalEpochStop()
			_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "disconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
			logf("HFT stream ended: %v", err)
		},
	)

	_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "slot_state", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotSubscribing), ReceivedTsMs: time.Now().UnixMilli()})
	acknowledged := 0
	for _, request := range slot.Requests {
		if err := stream.SubscribeHFTTokens("full", arrow.HFTExchNSECM, request, latencyMs); err != nil {
			_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotPartial), AssignedTokens: len(tokens), AcknowledgedTokens: acknowledged, RejectedTokens: len(request), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
			logf("HFT subscribe write failed: %v", err)
			signalEpochStop()
			return epochRetryable
		}
		select {
		case r := <-responses:
			switch classifySubscriptionResponse(r.ErrorCode, int(r.SuccessCount), int(r.ErrorCount), len(request)) {
			case subAccepted:
				acknowledged += int(r.SuccessCount)
			case subTerminal:
				_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotTerminal), AssignedTokens: len(tokens), AcknowledgedTokens: acknowledged + int(r.SuccessCount), RejectedTokens: int(r.ErrorCount), Reason: sanitizeDiagnostic(r.ErrorMsg), ReceivedTsMs: time.Now().UnixMilli()})
				logf("HFT subscription invalid: code=%s success=%d errors=%d", r.ErrorCode, r.SuccessCount, r.ErrorCount)
				signalEpochStop()
				return epochTerminal
			default: // subPartial
				_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotPartial), AssignedTokens: len(tokens), AcknowledgedTokens: acknowledged + int(r.SuccessCount), RejectedTokens: int(r.ErrorCount), Reason: sanitizeDiagnostic(r.ErrorMsg), ReceivedTsMs: time.Now().UnixMilli()})
				logf("HFT subscription rejected: code=%s success=%d errors=%d", r.ErrorCode, r.SuccessCount, r.ErrorCount)
				signalEpochStop()
				return epochRetryable
			}
		case <-time.After(responseTimeout):
			_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotTerminal), AssignedTokens: len(tokens), RejectedTokens: len(tokens), Reason: "subscription_response_timeout", ReceivedTsMs: time.Now().UnixMilli()})
			signalEpochStop()
			return epochTerminal
		case <-epochStop:
			// R-217: a disconnect/auth-refresh/decode-burst that fires
			// epochStop while a subscription response is pending must not
			// wait out the full responseTimeout and degrade into a TERMINAL.
			// The stream is already gone — report and retry with backoff so
			// reconnect cycles survive mid-subscription drops.
			// T1: an auth-refresh exhaustion, however, is a terminal decision
			// even mid-subscription — it must stop THIS slot, not retry.
			if terminalAuthFailure {
				return epochTerminal
			}
			return epochRetryable
		case <-ctx.Done():
			return epochRecovered
		}
	}
	logf("HFT subscribed %d tokens (latency=%dms)", len(tokens), latencyMs)
	lastFrameNanos.Store(time.Now().UnixNano())
	noteRecovered(slot.SlotID)
	_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "subscription_ack", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotActive), AssignedTokens: len(tokens), AcknowledgedTokens: len(tokens), ReceivedTsMs: time.Now().UnixMilli()})

	// R-059: heartbeat and watchdog goroutines must stop when the epoch ends
	// (epochStop), not just on the process context — otherwise a dead epoch's
	// goroutines keep writing to a closed stream and emit spurious events.
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	go func() {
		for {
			select {
			case <-heartbeat.C:
				if err := stream.WriteText("PONG"); err != nil {
					// R-297 wedge fix: signal the epoch stop BEFORE the emit —
					// EmitEvent blocks on a full stdout pipe, and the stop must
					// never wait behind a write that cannot complete.
					signalEpochStop()
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "heartbeat_failed", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: sanitizeDiagnostic(err.Error()), ReceivedTsMs: time.Now().UnixMilli()})
					logf("HFT heartbeat failed: %v", err)
					return
				}
			case <-ctx.Done():
				return
			case <-epochStop:
				return
			}
		}
	}()
	watchdog := time.NewTicker(time.Second)
	defer watchdog.Stop()
	go func() {
		for {
			select {
			case <-watchdog.C:
				last := lastFrameNanos.Load()
				if last > 0 && time.Since(time.Unix(0, last)) > stallTimeout {
					// R-297 wedge fix: signal the epoch stop BEFORE the emit —
					// the feed_stalled emit was blocking on a full stdout pipe,
					// so the epoch never stopped and the slot never reconnected.
					signalEpochStop()
					_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "feed_stalled", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotStalled), Reason: fmt.Sprintf("no_tick_for_%ds", int(stallTimeout.Seconds())), ReceivedTsMs: time.Now().UnixMilli()})
					return
				}
			case <-ctx.Done():
				return
			case <-epochStop:
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-epochStop:
	}
	if terminalAuthFailure {
		return epochTerminal
	}
	if ctx.Err() != nil {
		return epochRecovered
	}
	return epochRetryable
}

func isHFTDecodeError(message string) bool {
	return strings.Contains(message, "hft unknown packet") ||
		strings.Contains(message, "hft incomplete frame") ||
		(strings.Contains(message, "hft ") && strings.Contains(message, "parse"))
}

// decodeBurstResult is the outcome of one decode-error observation against the
// 10-second window.
type decodeBurstResult struct {
	count       int // running error count within the current window
	windowStart time.Time
	exceeded    bool // count reached maxDecodeErrorsPer10s within the window
}

// isDecodeErrorBurst applies the plan's decode-error burst policy: errors are
// counted in a rolling 10-second window; reaching maxDecodeErrorsPer10s
// (100) within one window reports exceeded=true. A window older than 10s
// resets the count. Callers pass the prior count and window start.
func isDecodeErrorBurst(priorCount int, windowStart time.Time, now time.Time) decodeBurstResult {
	if !windowStart.IsZero() && now.Sub(windowStart) > 10*time.Second {
		priorCount = 0
		windowStart = now
	}
	if windowStart.IsZero() {
		windowStart = now
	}
	count := priorCount + 1
	return decodeBurstResult{
		count:       count,
		windowStart: windowStart,
		exceeded:    count >= maxDecodeErrorsPer10s,
	}
}

func isHFTAuthError(message string) bool {
	lower := strings.ToLower(message)
	// P1-290: the bare "handshake" match also caught unrelated transport
	// errors (e.g. "remote error: tls: handshake failure"), burning the
	// bounded 3-attempt TOTP refresh budget on transient network issues.
	// Require the WebSocket handshake signal; the R-301 broker rejection
	// ("websocket: bad handshake") still matches both disjuncts below.
	return strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "authentication") ||
		strings.Contains(lower, "invalid token") ||
		strings.Contains(lower, "token expired") ||
		strings.Contains(lower, "401") ||
		strings.Contains(lower, "bad handshake") ||
		(strings.Contains(lower, "websocket") && strings.Contains(lower, "handshake"))
}

// authRefreshOutcome classifies one authentication-refresh step per the plan:
// the slot may retry (still within the 3-attempt budget), become terminal
// because the budget is exhausted, or resume because the refresh succeeded.
type authRefreshOutcome int

const (
	authRetry             authRefreshOutcome = iota // refresh failed, attempts remain
	authTerminal                                    // refresh failed and attempts exhausted
	authTerminalExhausted                           // no refresh function or budget already exhausted
	authResumed                                     // refresh succeeded
)

// classifyAuthRefresh is a pure decision helper for the plan's bounded auth
// refresh (3 attempts per slot failure episode). hasRefresh=false means no
// refresh is possible → terminal. refreshErr != nil means this refresh failed.
//
// R-023: a nil refreshErr must NOT be treated as success when no refresh is
// possible — a token-only deployment (refreshAuth == nil, so refreshErr stays
// nil) previously looped "reconnect / authentication_refreshed" forever.
func classifyAuthRefresh(hasRefresh bool, authRefreshes int, refreshErr error) authRefreshOutcome {
	if !hasRefresh || authRefreshes >= maxAuthRefreshAttempts {
		return authTerminalExhausted
	}
	if refreshErr == nil {
		return authResumed
	}
	// This attempt counted: the next check sees authRefreshes+1.
	if authRefreshes+1 >= maxAuthRefreshAttempts {
		return authTerminal
	}
	return authRetry
}

// subscriptionResponseOutcome classifies a subscription response against the
// requested batch size (plan §Integrations): SUCCESS with success_count equal
// to the batch and zero errors is accepted; an all-invalid/parameter error is
// terminal; any other partial/nonzero-error outcome is a partial rejection.
type subscriptionResponseOutcome int

const (
	subAccepted subscriptionResponseOutcome = iota
	subPartial
	subTerminal
)

func classifySubscriptionResponse(errorCode string, successCount, errorCount, requested int) subscriptionResponseOutcome {
	if errorCode == "SUCCESS" && successCount == requested && errorCount == 0 {
		return subAccepted
	}
	switch errorCode {
	case "E_ALL_INVALID", "E_INVALID_JSON", "E_MISSING_FIELD", "E_INVALID_PARAM":
		return subTerminal
	default:
		return subPartial
	}
}

func envOrFatal(key string) string {
	v := os.Getenv(key)
	if v == "" {
		// R-175: a missing required env is a fatal config/startup failure —
		// exit status 2 per the file's own contract, not 1 (unexpected
		// supervisor failure).
		fmt.Fprintf(os.Stderr, "arrow-bridge: missing required env: %s\n", key)
		os.Exit(exitFatalStart)
	}
	return v
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// hftPin enforces a pinned HFT policy env key (mirrors Java exactInt): unset
// returns the pin; a value other than the pin is a FATAL startup error.
func hftPin(logf func(string, ...any), key string, pin int) int {
	v := os.Getenv(key)
	if v == "" {
		return pin
	}
	n, err := strconv.Atoi(v)
	if err != nil || n != pin {
		logf("FATAL: %s=%s — pinned to %d", key, v, pin)
		os.Exit(exitFatalStart)
	}
	return n
}

// hftRange reads a tunable HFT policy env key (mirrors Java intRange): unset
// returns defVal; a non-integer or out-of-range value is a FATAL startup
// error.
func hftRange(logf func(string, ...any), key string, defVal, min, max int) int {
	v := os.Getenv(key)
	if v == "" {
		return defVal
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		logf("FATAL: %s=%s — must be in range %d..%d", key, v, min, max)
		os.Exit(exitFatalStart)
	}
	return n
}

func parseTokensEnv(key string) []int32 {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	var out []int32
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "arrow-bridge: invalid token %q: %v\n", s, err)
			continue
		}
		// R-176: a token outside the int32 range silently wrapped and negative
		// values were accepted verbatim — both corrupt the subscription set.
		if n <= 0 || int64(n) > int64(^uint32(0)>>1) {
			fmt.Fprintf(os.Stderr, "arrow-bridge: token %d out of range (must be 1..%d); skipping\n", n, int64(^uint32(0)>>1))
			continue
		}
		out = append(out, int32(n))
	}
	return out
}

// loadTokensFromCSV reads Arrow broker instrument CSV and extracts all tokens.
// Priority: 1) ARROW_INSTRUMENT_TOKENS env var, 2) CSV file at csvPath, 3) synthetic fallback.
func loadTokensFromCSV(envKey, csvPath string) []int32 {
	// 1. ENV var override (explicit is always respected)
	if tokens := parseTokensEnv(envKey); len(tokens) > 0 {
		fmt.Fprintf(os.Stderr, "arrow-bridge: %d tokens from %s env var\n", len(tokens), envKey)
		return tokens
	}

	// 2. Parse CSV file — Arrow format: Exchange,Segment,ExchSeg,Token,...
	f, err := os.Open(csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: cannot open %s: %v (falling back)\n", csvPath, err)
		return nil
	}
	defer f.Close()

	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "arrow-bridge: cannot read CSV header from %s: %v\n", csvPath, err)
		return nil
	}

	// Find Token column index
	tokenIdx := -1
	for i, col := range header {
		if strings.TrimSpace(col) == "Token" {
			tokenIdx = i
			break
		}
	}
	if tokenIdx < 0 {
		fmt.Fprintf(os.Stderr, "arrow-bridge: CSV %s has no Token column\n", csvPath)
		return nil
	}

	var tokens []int32
	lineNum := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// P1-156: fail closed — a malformed row (notably
			// csv.ErrFieldCount on ragged rows) must not be swallowed as
			// clean end-of-input, silently truncating the manifest into a
			// partial token plan. Startup MUST reject rather than ingest a
			// subset.
			fmt.Fprintf(os.Stderr, "arrow-bridge: FATAL: cannot parse CSV %s at line %d: %v; refusing to start with a partial token set\n", csvPath, lineNum+1, err)
			os.Exit(exitFatalStart)
		}
		lineNum++
		if len(record) <= tokenIdx {
			continue
		}
		tokenStr := strings.TrimSpace(record[tokenIdx])
		if tokenStr == "" {
			continue
		}
		n, err := strconv.Atoi(tokenStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "arrow-bridge: bad token at CSV line %d: %q\n", lineNum, tokenStr)
			continue
		}
		// R-176: reject out-of-range/negative tokens instead of silently
		// narrowing to int32.
		if n <= 0 || int64(n) > int64(^uint32(0)>>1) {
			fmt.Fprintf(os.Stderr, "arrow-bridge: token %d out of range at CSV line %d; skipping\n", n, lineNum)
			continue
		}
		tokens = append(tokens, int32(n))
	}

	fmt.Fprintf(os.Stderr, "arrow-bridge: %d tokens from %s (%d lines)\n", len(tokens), csvPath, lineNum-1)
	return tokens
}
