package main

import (
	"testing"
	"time"
)

func TestHFTDecodeErrorClassification(t *testing.T) {
	for _, message := range []string{"hft unknown packet: 5 trailing bytes", "hft incomplete frame: want 40 bytes", "hft full parse failed"} {
		if !isHFTDecodeError(message) {
			t.Fatalf("expected decode error: %q", message)
		}
	}
	if isHFTDecodeError("websocket: close 1006") {
		t.Fatal("network close must not be classified as decode error")
	}
}

func TestHFTAuthErrorClassification(t *testing.T) {
	for _, message := range []string{
		"websocket unauthorized",
		"authentication failed",
		"invalid token",
		"token expired",
		"HTTP 401 from broker",
	} {
		if !isHFTAuthError(message) {
			t.Fatalf("message %q was not classified as authentication failure", message)
		}
	}
	if isHFTAuthError("websocket: close 1006") {
		t.Fatal("transport close was classified as authentication failure")
	}
}

// P1-290 guard: only the WebSocket handshake is an auth signal — a bare TLS
// "handshake failure" must not burn the bounded TOTP refresh budget, while
// the R-301 broker rejection stays auth.
func TestHFTAuthErrorWebSocketHandshake(t *testing.T) {
	if !isHFTAuthError("websocket: bad handshake") {
		t.Fatal("R-301 'websocket: bad handshake' must stay an auth failure")
	}
	if isHFTAuthError("remote error: tls: handshake failure") {
		t.Fatal("TLS transport handshake failure must not be an auth failure")
	}
}

// TestDecodeErrorBurstThreshold — plan §Error Handling: 100 errors in 10s
// closes the slot (exceeded=true); 99 within the window does not; a window
// older than 10s resets the count.
func TestDecodeErrorBurstThreshold(t *testing.T) {
	start := time.Now()
	// 99 errors within the window → not exceeded.
	win, count := start, 0
	for i := 0; i < 99; i++ {
		res := isDecodeErrorBurst(count, win, start.Add(time.Duration(i)*time.Millisecond))
		count, win = res.count, res.windowStart
		if res.exceeded {
			t.Fatalf("exceeded at %d errors, want only at 100", i+1)
		}
	}
	// 100th error in-window → exceeded.
	res := isDecodeErrorBurst(count, win, start.Add(99*time.Millisecond))
	if !res.exceeded {
		t.Fatalf("expected burst exceeded at 100 errors, count=%d", res.count)
	}
	if res.count != 100 {
		t.Fatalf("count=%d, want 100", res.count)
	}

	// Window older than 10s → resets and never exceeds on first error.
	old := start.Add(-11 * time.Second)
	res = isDecodeErrorBurst(99, old, start)
	if res.count != 1 {
		t.Fatalf("window reset expected count=1, got %d", res.count)
	}
	if res.exceeded {
		t.Fatal("first error after window reset must not exceed")
	}
}

// TestExitStatusPolicy — plan §main.go exit statuses: fatal startup (auth/plan)
// is 2; supervisor/unexpected failures are 1; requested shutdown is 0.
func TestExitStatusPolicy(t *testing.T) {
	if exitRequested != 0 || exitSupervisor != 1 || exitFatalStart != 2 {
		t.Fatalf("exit statuses out of contract: %d/%d/%d",
			exitRequested, exitSupervisor, exitFatalStart)
	}
}
