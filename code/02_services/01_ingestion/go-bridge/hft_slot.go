package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

type SlotState string

// hftStream is the minimal HFT connection surface the slot supervisor needs
// (plan §hft_slot.go: HFTStream). It hides the concrete SDK type so tests can
// inject a fake stream; production uses the real SDK implementation.
type hftStream interface {
	SubscribeHFTTokens(mode string, exchSeg int, ids []int32, latencyMS int) error
	WriteText(payload string) error
	ReadHFTWithFrame(ctx context.Context,
		onLTP func(arrow.HFTLTPTick),
		onFull func(arrow.HFTFullTick),
		onResponse func(arrow.HFTResponsePacket),
		onFrame func(mt int, payload []byte),
		onDecoded func(frame []byte),
		onError func(err error))
	Close() error
}

// hftStreamFactory builds an hftStream for one connection attempt (plan
// §hft_slot.go: HFTStreamFactory).
type hftStreamFactory func() (hftStream, error)

const (
	SlotAuthenticating SlotState = "AUTHENTICATING"
	SlotConnecting     SlotState = "CONNECTING"
	SlotSubscribing    SlotState = "SUBSCRIBING"
	SlotActive         SlotState = "ACTIVE"
	SlotStalled        SlotState = "STALLED"
	SlotBackoff        SlotState = "BACKOFF"
	SlotPartial        SlotState = "PARTIAL"
	SlotAuthFailed     SlotState = "AUTH_FAILED"
	SlotTerminal       SlotState = "TERMINAL"
)

type SlotConfig struct {
	Mode             string
	LatencyMs        int
	Heartbeat        time.Duration
	StallTimeout     time.Duration
	ResponseTimeout  time.Duration
	MaxAuthRefreshes int
}

func (c SlotConfig) Validate() error {
	if c.Mode != "full" {
		return fmt.Errorf("HFT mode must be full")
	}
	if c.LatencyMs < 50 || c.LatencyMs > 60000 {
		return fmt.Errorf("latency must be 50..60000ms")
	}
	if c.Heartbeat != 3*time.Second {
		return fmt.Errorf("heartbeat must be exactly 3s")
	}
	if c.StallTimeout != 15*time.Second {
		return fmt.Errorf("stall timeout must be exactly 15s")
	}
	if c.ResponseTimeout != 10*time.Second {
		return fmt.Errorf("response timeout must be exactly 10s")
	}
	if c.MaxAuthRefreshes <= 0 {
		return fmt.Errorf("max auth refreshes must be positive")
	}
	return nil
}

type HFTSlot struct {
	mu         sync.Mutex
	state      SlotState
	epoch      uint64
	assignment SlotAssignment
	config     SlotConfig
	lastFrame  time.Time
	connectAt  time.Time
	closed     bool
}

func NewHFTSlot(assignment SlotAssignment, config SlotConfig) (*HFTSlot, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if len(assignment.Tokens) == 0 || len(assignment.Tokens) > MaxHFTTokensPerConnection {
		return nil, fmt.Errorf("invalid assignment size")
	}
	if err := validateRequestUnion(assignment); err != nil {
		return nil, err
	}
	return &HFTSlot{state: SlotTerminal, assignment: assignment, config: config}, nil
}

// validateRequestUnion enforces the plan's invariant that the union of all
// request batches equals the assignment's token set exactly — no token
// missing, no token repeated, no token outside the assignment.
func validateRequestUnion(assignment SlotAssignment) error {
	seen := make(map[int32]int, len(assignment.Tokens))
	for _, t := range assignment.Tokens {
		// P1-027: reject a duplicated assignment token outright. Counting
		// up here lets Tokens=[1,1] pass when requests mirror it (2->1->0),
		// violating the documented "no token repeated" invariant.
		if _, dup := seen[t]; dup {
			return fmt.Errorf("assignment token %d duplicated", t)
		}
		seen[t] = 1
	}
	for _, request := range assignment.Requests {
		if len(request) == 0 || len(request) > MaxHFTTokensPerRequest {
			return fmt.Errorf("invalid request size")
		}
		for _, t := range request {
			n, ok := seen[t]
			if !ok {
				return fmt.Errorf("request token %d not in assignment", t)
			}
			if n == 0 {
				return fmt.Errorf("request token %d duplicated across requests", t)
			}
			seen[t] = n - 1
		}
	}
	for t, remaining := range seen {
		if remaining != 0 {
			return fmt.Errorf("assignment token %d missing from requests", t)
		}
	}
	return nil
}

func (s *HFTSlot) BeginConnect() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	// R-096: a late/racing BeginConnect after Close() must not resurrect the
	// slot or bump the epoch. Closed slots are terminal, forever.
	if s.closed {
		return 0
	}
	s.epoch++
	s.state = SlotAuthenticating
	s.lastFrame = time.Time{}
	s.connectAt = time.Now()
	return s.epoch
}
func (s *HFTSlot) SetState(state SlotState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.state = state
	}
}
func (s *HFTSlot) State() SlotState           { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *HFTSlot) Epoch() uint64              { s.mu.Lock(); defer s.mu.Unlock(); return s.epoch }
func (s *HFTSlot) ObserveFrame(now time.Time) { s.mu.Lock(); defer s.mu.Unlock(); s.lastFrame = now }
func (s *HFTSlot) Stalled(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != SlotActive {
		return false
	}
	// R-095: a slot that reaches ACTIVE but never receives a single frame must
	// still be detected as stalled. BeginConnect() resets lastFrame to zero,
	// and the old guard required !IsZero() — a blind window that defeated
	// stall detection for a completely silent connection. Compare against
	// connectAt when no frame has ever arrived.
	if s.lastFrame.IsZero() {
		return now.Sub(s.connectAt) > s.config.StallTimeout
	}
	return now.Sub(s.lastFrame) > s.config.StallTimeout
}
func (s *HFTSlot) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.state = SlotTerminal
}

// Run drives the slot's lifecycle: it transitions the slot to AUTHENTICATING
// (R-153 — the old implementation never connected, so any caller treating it
// as the per-slot main loop would silently never subscribe), waits for ctx
// cancellation, then closes the slot. The real per-epoch driver is
// runHFTEpoch/runHFTSlot (supervisor.go); this is the passive lifecycle
// watcher used where only state tracking is needed. Returns an error if the
// slot is already closed.
func (s *HFTSlot) Run(ctx context.Context) error {
	if s.BeginConnect() == 0 {
		return fmt.Errorf("slot is closed; cannot run")
	}
	<-ctx.Done()
	s.Close()
	return nil
}

func Backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= 5 {
		return 30 * time.Second
	}
	return time.Second << attempt
}
