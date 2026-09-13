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
//
// P3-050/P3-262: reauth is called at most once per auth-failure generation and
// never concurrently. A burst of 401s shares one (typically slow, TOTP-backed)
// refresh instead of churning the token of the shared SDK client, so the
// closure must be safe to call from the command path but never has to be
// re-entrant.
type ReauthBroker struct {
	mu          sync.Mutex
	inner       Broker
	reauth      func(context.Context) error
	disabled    bool
	reauthCalls int
	gen         int        // bumped by every successful re-auth; the election token
	reauthMu    sync.Mutex // serializes the election: at most one re-auth in flight
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
	r.mu.Lock()
	disabled, gen := r.disabled, r.gen
	r.mu.Unlock()
	if disabled {
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
	// P3-261: a caller that has already gone must not cost a TOTP re-auth. The
	// production re-auth closure (main.go) ignores ctx entirely, so this layer is
	// the only place the deadline can be honoured; the caller's own auth failure
	// is surfaced unchanged rather than dressed up as a fresh verdict.
	if ctx.Err() != nil {
		return result
	}
	// First auth failure: attempt exactly one re-auth — but only one across all
	// the commands that are failing at the same time.
	if r.reauth == nil {
		r.markDisabled()
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	// P3-050/P3-262: elect one re-auth per generation. A command that loses the
	// election waits here, then finds the generation already bumped and retries
	// on the token someone else refreshed, instead of running its own AutoLogin.
	r.reauthMu.Lock()
	r.mu.Lock()
	disabled, current := r.disabled, r.gen
	r.mu.Unlock()
	if disabled {
		r.reauthMu.Unlock()
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	if current != gen {
		r.reauthMu.Unlock()
		return r.retryOnce(ctx, fn, result, current)
	}
	r.mu.Lock()
	r.reauthCalls++
	r.mu.Unlock()
	err := r.reauth(ctx)
	ourGen := gen
	if err == nil {
		r.mu.Lock()
		r.gen++
		ourGen = r.gen
		r.mu.Unlock()
	}
	r.reauthMu.Unlock()
	if err != nil {
		r.markDisabled()
		return BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"}
	}
	return r.retryOnce(ctx, fn, result, ourGen)
}

// retryOnce re-issues the command on the token generation the election produced
// and applies the post-retry verdict. It never re-enters re-auth ("never
// loops"), and it honours P3-261: a caller that has gone by now keeps its
// original auth failure rather than provoking a venue call nobody waits for.
func (r *ReauthBroker) retryOnce(ctx context.Context, fn func() BrokerResult, original BrokerResult, gen int) BrokerResult {
	// P3-261: the re-auth may have taken the caller past its deadline. Retrying
	// then spends a venue round trip nobody is waiting for.
	if ctx.Err() != nil {
		return original
	}
	// Re-auth succeeded: retry the command exactly once, no further re-auth.
	retry := fn()
	if retry.Outcome != OutcomeRejected && retry.Reason == "broker_auth_failure" {
		// P3-263: this 401 describes the token the retry was issued with. If a
		// newer re-auth landed while the retry was in flight, the fresh token has
		// not been disproved, and latching disabled here would halt a bridge that
		// is holding a good token until the process is restarted. Only the
		// generation that owns the token may fail the broker closed.
		r.mu.Lock()
		superseded := r.gen != gen
		r.mu.Unlock()
		if superseded {
			return retry
		}
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
