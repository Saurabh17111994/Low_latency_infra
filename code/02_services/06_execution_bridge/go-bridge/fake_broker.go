package main

import (
	"context"
	"sync"
)

// FakeBroker is an offline-only broker double. It records command counts and
// lets tests force SUCCESS, REJECTED, or UNKNOWN outcomes deterministically.
type FakeBroker struct {
	mu            sync.Mutex
	results       map[string]BrokerResult
	defaultResult *BrokerResult
	calls         map[string]int
}

func NewFakeBroker() *FakeBroker {
	return &FakeBroker{results: map[string]BrokerResult{}, calls: map[string]int{}}
}

func (f *FakeBroker) SetResult(command string, result BrokerResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results[command] = result
}

// SetDefaultResult answers every command without a preset. It exists so the
// disabled-mode fake can be fail-closed (P3-249): its deny list only names the
// commands known today, and the fake otherwise reports SUCCESS, so a command
// added later would report success while health says "UP disabled"/not-ready.
func (f *FakeBroker) SetDefaultResult(result BrokerResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.defaultResult = &result
}

func (f *FakeBroker) Calls(command string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[command]
}

func (f *FakeBroker) result(ctx context.Context, command string, c CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	f.mu.Lock()
	f.calls[command]++
	result, ok := f.results[command]
	if !ok && f.defaultResult != nil {
		result, ok = *f.defaultResult, true
	}
	f.mu.Unlock()
	if ok {
		// P3-243: a preset is an offline shortcut, not a licence to publish a shape
		// the live broker cannot produce. ArrowBroker digests the payload it returns,
		// so Data with no digest would pass here and fail in resultToReport live.
		if result.Data != nil && result.Fingerprint == "" {
			fp, err := fingerprint(result.Data)
			if err != nil {
				return unknownResult(err)
			}
			result.Fingerprint = fp
		}
		return result
	}
	// P3-244: the HTTP boundary validates the envelope before dispatch, but a test
	// that drives the broker directly bypasses it. ArrowBroker dereferences c.Order
	// on place/modify and forwards the id on cancel/query, so refuse the same
	// envelopes instead of answering SUCCESS for one the venue would never see.
	if err := validateCommand(c); err != nil {
		return rejectedResult(err)
	}
	result = BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: c.BrokerOrderID}
	if command == CommandPlace {
		result.BrokerOrderID = "fake-broker-order-1"
	}
	if command == CommandReconcileOrders {
		result.Data = []map[string]string{{"id": "fake-broker-order-1", "orderStatus": "OPEN"}}
	}
	if command == CommandReconcileTrades {
		result.Data = []map[string]string{}
	}
	if command == CommandReconcilePosition {
		result.Data = []map[string]string{}
	}
	fp, err := fingerprint(result.Data)
	if err != nil {
		// P3-474: the fake must not publish an empty idempotency digest either.
		return unknownResult(err)
	}
	result.Fingerprint = fp
	return result
}

func (f *FakeBroker) Place(ctx context.Context, c CommandEnvelope) BrokerResult {
	return f.result(ctx, CommandPlace, c)
}
func (f *FakeBroker) Modify(ctx context.Context, c CommandEnvelope) BrokerResult {
	return f.result(ctx, CommandModify, c)
}
func (f *FakeBroker) Cancel(ctx context.Context, c CommandEnvelope) BrokerResult {
	return f.result(ctx, CommandCancel, c)
}
func (f *FakeBroker) QueryOrder(ctx context.Context, c CommandEnvelope) BrokerResult {
	return f.result(ctx, CommandQueryOrder, c)
}
func (f *FakeBroker) ReconcileOrders(ctx context.Context, c CommandEnvelope) BrokerResult {
	return f.result(ctx, CommandReconcileOrders, c)
}
func (f *FakeBroker) ReconcileTrades(ctx context.Context, c CommandEnvelope) BrokerResult {
	return f.result(ctx, CommandReconcileTrades, c)
}
func (f *FakeBroker) ReconcilePositions(ctx context.Context, c CommandEnvelope) BrokerResult {
	return f.result(ctx, CommandReconcilePosition, c)
}
