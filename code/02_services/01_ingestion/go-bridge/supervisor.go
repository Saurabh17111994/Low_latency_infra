package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// slotEpochResult is the outcome of one slot epoch (one connect attempt).
// It drives the reconnect loop's decision to retry or stop the slot.
type slotEpochResult int

// maxAuthRefreshAttempts is the per-slot auth-refresh budget (T1, plan
// decision #20): a shared Arrow token may be refreshed 3 times per slot
// across reconnect epochs; exhaustion terminates that slot only. Mirrors
// ARROW_HFT_AUTH_REFRESH_ATTEMPTS (pinned to 3) and SlotConfig.MaxAuthRefreshes.
const maxAuthRefreshAttempts = 3

const (
	// epochRetryable — the connection dropped, stalled, or heartbeats failed;
	// the slot should back off and retry with a fresh epoch.
	epochRetryable slotEpochResult = iota
	// epochTerminal — the subscription was rejected as invalid or auth refresh
	// was exhausted; the slot must stop and be reported as terminal.
	epochTerminal
	// epochRecovered — the slot reached ACTIVE and was later cancelled or
	// ended cleanly; it is done without error.
	epochRecovered
)

// slotOutcome is the terminal result of one slot's supervised run.
type slotOutcome struct {
	slotID   string
	terminal bool // true if the slot stopped due to a terminal error
	reason   string
}

// streamFactoryFor builds the production stream factory for one slot.
// ARROW_HFT_URL is a development/test-only override to point the bridge at a
// local fake broker; production uses the SDK constant via NewStreamsWithHFT.
func streamFactoryFor(client *arrow.Client, _ int) func() (hftStream, error) {
	return func() (hftStream, error) {
		if override := os.Getenv("ARROW_HFT_URL"); override != "" {
			return client.ConnectHFTDataStreamURL(override)
		}
		streams, err := client.NewStreamsWithHFT()
		if err != nil {
			return nil, err
		}
		return streams.HFTDataStream, nil
	}
}

// runHFTSlot runs one slot's reconnect loop until the slot terminates, the
// context is cancelled, or a terminal error stops the slot. It returns the
// slot's terminal outcome so the supervisor can aggregate.
//
// T1 isolation: the slot path deliberately holds NO cancel handle for the
// shared process context — a slot can only stop itself (per-slot terminal),
// never its peers.
func runHFTSlot(ctx context.Context, client *arrow.Client, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) slotOutcome {
	return runHFTSlotWithFactory(ctx, streamFactoryFor(client, 0), slot, latencyMs, responseTimeout, refreshAuth, logf)
}

// runHFTSlotWithFactory is runHFTSlot with an injectable stream factory.
func runHFTSlotWithFactory(ctx context.Context, factory func() (hftStream, error), slot SlotAssignment, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) slotOutcome {
	outcome := slotOutcome{slotID: slot.SlotID}
	// T1: the auth-refresh budget is PER SLOT and spans reconnect epochs. It is
	// threaded by pointer into runHFTEpoch so attempts survive reconnects — a
	// per-epoch counter would reset on every retry and a persistently failing
	// token refresh would never exhaust. Exhaustion returns epochTerminal,
	// which stops THIS slot only.
	authRefreshes := 0
	runReconnectLoop(ctx, func(epoch uint64) bool {
		result := runHFTEpoch(ctx, factory, slot, latencyMs, responseTimeout, epoch, refreshAuth, &authRefreshes, logf)
		switch result {
		case epochTerminal:
			// Terminal: report and stop this slot (the epoch already emitted
			// the TERMINAL event). No retry.
			outcome.terminal = true
			outcome.reason = "terminal"
			return true
		case epochRecovered:
			return true
		default: // epochRetryable — reconnect with backoff.
			return false
		}
	}, func(epoch uint64, delay time.Duration) {
		noteReconnect(slot.SlotID)
		_ = bridgeEmitter.EmitEvent(BridgeEvent{Event: "reconnect", SlotID: slot.SlotID, ConnectionID: slot.ConnectionID, ConnectionEpoch: epoch, State: string(SlotBackoff), Reason: fmt.Sprintf("retry_in_%s", delay), ReceivedTsMs: time.Now().UnixMilli()})
	}, func(ctx context.Context, delay time.Duration) {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	})
	return outcome
}

// runHFTSupervisor starts one slot goroutine per slot, keeps healthy slots
// alive during peer retry, and aggregates terminal outcomes. It returns the
// number of slots that ended terminal.
//
// T1 policy: a terminal slot (subscription rejected, auth refresh exhausted,
// panic) stops ITSELF; healthy peers are never disturbed because the slot
// path holds no handle on the shared context. Only the process context being
// cancelled (shutdown via signal) stops every slot.
func runHFTSupervisor(ctx context.Context, client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) int {
	return runHFTSupervisorWithFactory(ctx, streamFactoryFor, client, plan, latencyMs, responseTimeout, refreshAuth, logf)
}

// runHFTSupervisorWithFactory is runHFTSupervisor with an injectable stream
// factory builder, so tests can drive the real supervisor with fake streams.
// The builder is invoked once per slot with the slot index, allowing tests to
// give each slot a distinct scripted stream deterministically.
func runHFTSupervisorWithFactory(ctx context.Context, makeFactory func(*arrow.Client, int) func() (hftStream, error), client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf func(string, ...any)) int {
	if len(plan.Slots) == 0 {
		return 0
	}
	var wg sync.WaitGroup
	outcomes := make([]slotOutcome, len(plan.Slots))
	for i, slot := range plan.Slots {
		wg.Add(1)
		go func(i int, slot SlotAssignment) {
			defer wg.Done()
			// R-200: a panic in one slot's main flow must not take down the
			// whole bridge — recover, record the slot as terminal, and let the
			// supervisor aggregate.
			defer func() {
				if r := recover(); r != nil {
					logf("HFT slot %s panicked: %v", slot.SlotID, r)
					outcomes[i] = slotOutcome{slotID: slot.SlotID, terminal: true, reason: fmt.Sprintf("panic: %v", r)}
				}
			}()
			outcomes[i] = runHFTSlotWithFactory(ctx, makeFactory(client, i), slot, latencyMs, responseTimeout, refreshAuth, logf)
		}(i, slot)
	}
	// Supervisor health snapshot: one bridge_metrics control record per 10s
	// (reconnect_consecutive, active_sockets, go_goroutines) via the active
	// transport (proto frames). Exits on cancel
	// so a cancelled supervisor's goroutine count settles for ING-RES-001.
	go bridgeMetricsTicker(ctx)
	wg.Wait()
	terminal := 0
	for _, o := range outcomes {
		if o.terminal {
			terminal++
		}
	}
	return terminal
}
