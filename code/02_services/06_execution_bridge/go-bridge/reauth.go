package main

import (
	"context"
	"sync"
)

// ReauthBroker wraps a Broker with automatic re-auth on 401/token_expired.
// On the first broker_auth_failure it calls reauth once, retries the command
// once, and surfaces the retry result. If reauth itself fails, the broker is
// marked disabled, subsequent health reflects UP disabled, and subsequent
// commands are refused locally without touching the venue. Never loops.
type ReauthBroker struct {
	mu          sync.Mutex
	inner       Broker
	reauth      func(context.Context) error
	disabled    bool
	reauthCalls int
}

func NewReauthBroker(inner Broker, reauth func(context.Context) error) *ReauthBroker {
	return &ReauthBroker{inner: inner, reauth: reauth}
}

func (r *ReauthBroker) IsDisabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.disabled
}

func (r *ReauthBroker) ReauthCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reauthCalls
}

func (r *ReauthBroker) markDisabled() {
	r.mu.Lock()
	r.disabled = true
	r.mu.Unlock()
}

func (r *ReauthBroker) doWithReauth(ctx context.Context, fn func() BrokerResult) BrokerResult {
	// P3-048/P3-049: disabled is fail-closed for commands, not just for /healthz.
	// A bridge that has given up must not reach the venue and must not spend
	// another TOTP re-auth. The flag used to be read only by the health handler,
	// so every later command still called the broker and re-authenticated again
	// on its next broker_auth_failure.
	if r.IsDisabled() {
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	result := fn()
	// P3-241: the retry is guarded by the outcome as well as the reason. Reauth
	// re-submits the command, which is only ever acceptable for an outcome that is
	// unknown — never for a terminal REJECTED, whatever reason it arrived with. The
	// reason alone was not enough: any producer of a BrokerResult can pair a terminal
	// outcome with an auth-failure reason, and re-submitting an order the venue has
	// already refused is worse than any auth failure it would fix.
	if result.Outcome == OutcomeRejected || result.Reason != "broker_auth_failure" {
		return result
	}
	// P3-049: re-read the flag — another request may have disabled the broker
	// while this command was in flight, and its re-auth failure is the verdict
	// this one must obey.
	if r.IsDisabled() {
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	// First auth failure: attempt exactly one re-auth.
	if r.reauth == nil {
		r.markDisabled()
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	r.mu.Lock()
	r.reauthCalls++
	r.mu.Unlock()
	if err := r.reauth(ctx); err != nil {
		r.markDisabled()
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	// Re-auth succeeded: retry the command exactly once, no further re-auth.
	retry := fn()
	if retry.Outcome != OutcomeRejected && retry.Reason == "broker_auth_failure" {
		r.markDisabled()
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	return retry
}

func (r *ReauthBroker) Place(ctx context.Context, c CommandEnvelope) BrokerResult {
	return r.doWithReauth(ctx, func() BrokerResult { return r.inner.Place(ctx, c) })
}
func (r *ReauthBroker) Modify(ctx context.Context, c CommandEnvelope) BrokerResult {
	return r.doWithReauth(ctx, func() BrokerResult { return r.inner.Modify(ctx, c) })
}
func (r *ReauthBroker) Cancel(ctx context.Context, c CommandEnvelope) BrokerResult {
	return r.doWithReauth(ctx, func() BrokerResult { return r.inner.Cancel(ctx, c) })
}
func (r *ReauthBroker) QueryOrder(ctx context.Context, c CommandEnvelope) BrokerResult {
	return r.doWithReauth(ctx, func() BrokerResult { return r.inner.QueryOrder(ctx, c) })
}
func (r *ReauthBroker) ReconcileOrders(ctx context.Context, c CommandEnvelope) BrokerResult {
	return r.doWithReauth(ctx, func() BrokerResult { return r.inner.ReconcileOrders(ctx, c) })
}
func (r *ReauthBroker) ReconcileTrades(ctx context.Context, c CommandEnvelope) BrokerResult {
	return r.doWithReauth(ctx, func() BrokerResult { return r.inner.ReconcileTrades(ctx, c) })
}
func (r *ReauthBroker) ReconcilePositions(ctx context.Context, c CommandEnvelope) BrokerResult {
	return r.doWithReauth(ctx, func() BrokerResult { return r.inner.ReconcilePositions(ctx, c) })
}
