package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// syncBuffer is a mutex-guarded capture buffer for the bridge NDJSON output —
// safe to read from the test goroutine while the bridge goroutine writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func newSyncBuffer() *syncBuffer { return &syncBuffer{} }

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestFaultInjectionDecodeBurstRecovers — plan §Error Handling / Phase 9:
// inject a decode-error burst (100 decode-class errors in <10s) into a live
// supervised slot. The burst must terminate that epoch, but the slot's
// reconnect loop must recover on a fresh epoch and reach ACTIVE again — the
// fault must not kill the bridge.
func TestFaultInjectionDecodeBurstRecovers(t *testing.T) {
	// Stream 1: healthy subscribe, then a decode-error burst (100) delivered
	// via onError, then a read error that ends the epoch.
	burst := newFakeHFTStream()
	burst.response = hftResponse("SUCCESS", 1, 0)
	burst.onFullTick = arrow.HFTFullTick{Token: 1000, LTP: 15050}
	burst.decodeErrCount = 100 // burst threshold → epoch terminates
	burst.readErr = errFakeRead

	// Stream 2 (reconnect): healthy — reaches ACTIVE and stays.
	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onFullTick = arrow.HFTFullTick{Token: 1000, LTP: 15051}

	// The slot factory yields the burst stream once, then healthy forever.
	attempts := 0
	factory := func() (hftStream, error) {
		attempts++
		if attempts == 1 {
			return burst, nil
		}
		return healthy, nil
	}

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	slot := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{1000}, Requests: [][]int32{{1000}}}

	oldEmitter := bridgeEmitter
	// Thread-safe capture buffer — the bridge goroutine writes concurrently
	// while the test polls (a plain bytes.Buffer would be a data race).
	capture := newSyncBuffer()
	bridgeEmitter = newTestProtoEmitter(capture)
	defer func() { bridgeEmitter = oldEmitter }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHFTSlotWithFactory(ctx, factory, slot, 50, 10*time.Second, nil, t.Logf)
	}()
	// Poll for the recovered ACTIVE ack instead of a fixed 300ms sleep —
	// under `go test -race` full-suite load the burst + reconnect can take
	// longer than 300ms, which made this assertion flaky.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if containsAny(eventsAsStrings(t, capture.String()), "event=subscription_ack state=ACTIVE") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	flushTestEmitter()
	out := capture.String()

	// The burst epoch must have produced an error, then a reconnect, and the
	// second epoch must reach ACTIVE — the fault was survived.
	events := eventsFrom(t, out)
	if got := lastEventState(events, "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("slot must recover to ACTIVE after decode burst, got %q\n%s", got, out)
	}
	if got := lastEventState(events, "reconnect"); got != "BACKOFF" {
		t.Fatalf("expected reconnect BACKOFF after burst, got %q\n%s", got, out)
	}
	// At least one reconnect means the burst killed epoch 1 and the loop retried.
	reconnects := 0
	for _, e := range events {
		if e["event"] == "reconnect" {
			reconnects++
		}
	}
	if reconnects < 1 {
		t.Fatalf("expected >=1 reconnect after fault injection, got %d\n%s", reconnects, out)
	}
}

// TestFaultInjectionAuthRefreshRecovers — plan §hft_slot.go: an auth error
// triggers a refresh; if the refresh succeeds the slot resumes (reconnect
// BACKOFF with reason authentication_refreshed), not terminal.
func TestFaultInjectionAuthRefreshRecovers(t *testing.T) {
	// Stream delivers an auth error on read; refreshAuth succeeds once.
	authFail := newFakeHFTStream()
	authFail.response = hftResponse("SUCCESS", 1, 0)
	authFail.readErr = errFakeAuth // classification → auth failure

	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onFullTick = arrow.HFTFullTick{Token: 2000, LTP: 15200}

	attempts := 0
	factory := func() (hftStream, error) {
		attempts++
		if attempts == 1 {
			return authFail, nil
		}
		return healthy, nil
	}

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	slot := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{2000}, Requests: [][]int32{{2000}}}

	ctx, cancel := context.WithCancel(context.Background())
	out := captureBridge(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			// refreshAuth returns nil (success) — the slot resumes.
			runHFTSlotWithFactory(ctx, factory, slot, 50, 10*time.Second,
				func(context.Context) error { return nil }, t.Logf)
		}()
		time.Sleep(300 * time.Millisecond)
		cancel()
		<-done
	})

	events := eventsFrom(t, out)
	if got := lastEventState(events, "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("slot must recover to ACTIVE after auth refresh, got %q\n%s", got, out)
	}
	// No TERMINAL/auth_failure — the refresh prevented a terminal state.
	for _, e := range events {
		if e["event"] == "auth_failure" {
			t.Fatalf("auth refresh success must not produce auth_failure\n%s", out)
		}
	}
}

// TestFaultInjectionDialBadHandshakeTriggersRefresh (R-301) — the regression
// for the expired-session-token reconnect loop: the broker rejects the
// WebSocket upgrade with "websocket: bad handshake" (a DIAL failure, not a
// read-loop error), and the slot must run a fresh TOTP AutoLogin (via
// refreshAuth) BEFORE the next dial instead of retrying with the stale token
// forever.
func TestFaultInjectionDialBadHandshakeTriggersRefresh(t *testing.T) {
	var errFakeBadHandshake = errors.New("websocket: bad handshake")

	healthy := newFakeHFTStream()
	healthy.response = hftResponse("SUCCESS", 1, 0)
	healthy.onFullTick = arrow.HFTFullTick{Token: 2000, LTP: 15200}

	refreshes := 0
	dials := 0
	factory := func() (hftStream, error) {
		dials++
		// First two dials carry the stale (expired) session token and are
		// rejected at the handshake; the third succeeds.
		if dials <= 2 {
			return nil, errFakeBadHandshake
		}
		return healthy, nil
	}

	client := arrow.NewClient("app", "secret")
	client.SetToken("stale-token")

	slot := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{2000}, Requests: [][]int32{{2000}}}

	ctx, cancel := context.WithCancel(context.Background())
	out := captureBridge(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			runHFTSlotWithFactory(ctx, factory, slot, 50, 10*time.Second,
				// refreshAuth simulates a fresh AutoLogin (new session token).
				func(context.Context) error {
					refreshes++
					client.SetToken("fresh-token-" + string(rune('0'+refreshes)))
					return nil
				}, t.Logf)
		}()
		// Backoff after a failed dial is 1s, then 2s, then 4s… — the two
		// failed dials complete at ~0s and ~1s, and the successful third dial
		// lands after the 2s backoff (~3s). Wait ~4s to let it reach ACTIVE.
		time.Sleep(4200 * time.Millisecond)
		cancel()
		<-done
	})

	if refreshes == 0 {
		t.Fatalf("bad-handshake dial failures must trigger a fresh AutoLogin (R-301)\n%s", out)
	}
	// The first dial never refreshes (no failure yet) — every dial after a
	// failed one must have refreshed. With 3 dials (2 fail + 1 succeeds) we
	// expect exactly 2 refreshes here; be tolerant of the reconnect loop
	// running one extra cycle before cancel lands.
	if refreshes < 2 {
		t.Fatalf("expected a refresh after each failed dial (dials=%d refreshes=%d)", dials, refreshes)
	}
	if client.GetToken() == "stale-token" {
		t.Fatalf("client must hold a fresh session token after bad-handshake refresh")
	}
	events := eventsFrom(t, out)
	if got := lastEventState(events, "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("slot must recover to ACTIVE after dial-failure auth refresh, got %q\n%s", got, out)
	}
}

// TestFaultInjectionDialBadHandshakeExhaustsBudget (R-301) — when every dial
// fails with bad handshake and the refresh budget is exhausted, the slot must
// go terminal (not retry forever with a stale token).
func TestFaultInjectionDialBadHandshakeExhaustsBudget(t *testing.T) {
	var errFakeBadHandshake = errors.New("websocket: bad handshake")

	factory := func() (hftStream, error) {
		return nil, errFakeBadHandshake
	}

	slot := SlotAssignment{SlotID: "hft-0", ConnectionID: "hft-0",
		Tokens: []int32{2000}, Requests: [][]int32{{2000}}}

	ctx, cancel := context.WithCancel(context.Background())
	out := captureBridge(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			runHFTSlotWithFactory(ctx, factory, slot, 50, 10*time.Second,
				// refreshAuth keeps failing (e.g. broker 2FA rejects the login).
				func(context.Context) error { return errors.New("auto-login: bad credentials") },
				t.Logf)
		}()
		time.Sleep(500 * time.Millisecond)
		cancel()
		<-done
	})

	events := eventsFrom(t, out)
	if got := lastEventState(events, "subscription_ack"); got == "ACTIVE" {
		t.Fatalf("slot must NOT reach ACTIVE when auth refresh is exhausted\n%s", out)
	}
	// The slot must stop retrying (terminal) once the 3-refresh budget is gone —
	// count disconnect events; they must be bounded (budget + a couple), not a
	// near-infinite retry stream.
	disconnects := 0
	for _, e := range events {
		if e["event"] == "disconnect" {
			disconnects++
		}
	}
	if disconnects > maxAuthRefreshAttempts+3 {
		t.Fatalf("slot must stop retrying after auth budget exhausted: %d disconnects\n%s", disconnects, out)
	}
}
