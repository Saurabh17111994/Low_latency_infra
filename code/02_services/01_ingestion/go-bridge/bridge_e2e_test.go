package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
)

// fakeBrokerWire is an end-to-end fake HFT broker speaking the real wire
// format: JSON `sub` inbound, zstd-compressed little-endian binary outbound
// (response + full-tick frames). It can force a disconnect and accept a
// reconnect (a fresh WebSocket connection).
//
// Multi-slot: each connection emits a full tick for the FIRST token in the
// requested ids (so slot-0 gets its own token's tick on its own connection).
type fakeBrokerWire struct {
	server       *httptest.Server
	url          string
	connections  atomic.Int32   // R-094 class: the WS handler runs per-connection in its own goroutine
	disconnect   map[int]bool   // connection indices to force-close after responding (1-based)
	allInvalid   bool           // respond E_ALL_INVALID (terminal) instead of SUCCESS
	tokenInvalid map[int32]bool // respond E_ALL_INVALID for subscriptions whose first token matches
	requestCount chan int
}

func newFakeBrokerWire(t *testing.T, disconnectAt ...int) *fakeBrokerWire {
	f := &fakeBrokerWire{
		disconnect:   map[int]bool{},
		tokenInvalid: map[int32]bool{},
		requestCount: make(chan int, 16),
	}
	for _, idx := range disconnectAt {
		f.disconnect[idx] = true
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connIdx := int(f.connections.Add(1))
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var sub map[string]any
			if json.Unmarshal(payload, &sub) == nil {
				if code, _ := sub["code"].(string); code != "sub" {
					continue
				}
				f.requestCount <- 1
				// Respond with a SUCCESS frame covering the requested ids.
				// The bridge sends symIds:[{exch_seg, ids}] (plan §Integrations).
				ids := []any{}
				if symIds, ok := sub["symIds"].([]any); ok && len(symIds) > 0 {
					if seg, ok := symIds[0].(map[string]any); ok {
						if segIDs, ok := seg["ids"].([]any); ok {
							ids = segIDs
						}
					}
				}
				var firstToken int32 = 1000
				if len(ids) > 0 {
					if f, ok := ids[0].(float64); ok {
						firstToken = int32(f)
					}
				}
				var resp []byte
				if f.allInvalid || f.tokenInvalid[firstToken] {
					resp = buildInvalidResponseFrame(len(ids))
				} else {
					resp = buildResponseFrame(len(ids))
				}
				if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(resp)); err != nil {
					return
				}
				// Emit a full tick for the first requested token so each slot
				// observes a frame + tick on its own connection.
				full := buildFullTickFrame(firstToken)
				if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(full)); err != nil {
					return
				}
				// Force-close this connection after it has delivered its
				// response, so the reconnect loop is exercised.
				if f.disconnect[connIdx] {
					_ = conn.WriteControl(websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseNormalClosure, "forced"), time.Now())
					return
				}
			}
		}
	}))
	f.url = "ws" + strings.TrimPrefix(f.server.URL, "http")
	return f
}

func (f *fakeBrokerWire) close() { f.server.Close() }

// countLinesWith returns how many NDJSON lines contain the given substring.
func countLinesWith(t *testing.T, out, substr string) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// Wire frame sizes/types (mirror the SDK's unexported constants).
const (
	hftSizeResponse = 540
	hftSizeFull     = 196
	hftPktResponse  = 99
	hftPktFull      = 2
	hftExchNSECM    = 0
)

// buildResponseFrame builds a 540-byte response frame: SUCCESS with
// success_count = n, request_type=subscribe(0), mode=full(1).
func buildResponseFrame(n int) []byte {
	frame := make([]byte, hftSizeResponse)
	binary.LittleEndian.PutUint32(frame[0:4], hftSizeResponse)
	frame[4] = hftPktResponse
	copy(frame[6:22], "SUCCESS")
	frame[534] = 0 // subscribe
	frame[535] = 1 // full
	binary.LittleEndian.PutUint16(frame[536:538], uint16(n))
	binary.LittleEndian.PutUint16(frame[538:540], 0)
	return frame
}

// buildInvalidResponseFrame builds a 540-byte response frame: E_ALL_INVALID
// with error_count = n, request_type=subscribe(0), mode=full(1).
func buildInvalidResponseFrame(n int) []byte {
	frame := make([]byte, hftSizeResponse)
	binary.LittleEndian.PutUint32(frame[0:4], hftSizeResponse)
	frame[4] = hftPktResponse
	copy(frame[6:22], "E_ALL_INVALID")
	frame[534] = 0 // subscribe
	frame[535] = 1 // full
	binary.LittleEndian.PutUint16(frame[536:538], 0)
	binary.LittleEndian.PutUint16(frame[538:540], uint16(n))
	return frame
}

// buildFullTickFrame builds a 196-byte full tick for the given token.
func buildFullTickFrame(token int32) []byte {
	frame := make([]byte, hftSizeFull)
	binary.LittleEndian.PutUint16(frame[0:2], hftSizeFull)
	frame[2] = hftPktFull
	frame[3] = hftExchNSECM
	binary.LittleEndian.PutUint32(frame[4:8], uint32(token))
	binary.LittleEndian.PutUint32(frame[8:12], 15050) // LTP in paise
	return frame
}

func compressZstd(payload []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	defer enc.Close()
	return enc.EncodeAll(payload, nil)
}

// TestBridgeE2EFakeBrokerSubscribeTickAndReconnect — the real bridge path
// (runHFT + SDK ConnectHFTDataStreamURL) against a wire-format fake broker:
// subscribe → ACTIVE → tick emitted → forced disconnect → reconnect → epoch
// bump → resubscribe → ACTIVE again.
func TestBridgeE2EFakeBrokerSubscribeTickAndReconnect(t *testing.T) {
	// Force-close the FIRST connection after it delivers its response: the
	// bridge reaches ACTIVE + emits a tick, then the reconnect loop must
	// reconnect, resubscribe, and reach ACTIVE again on a fresh connection.
	fake := newFakeBrokerWire(t, 1)
	defer fake.close()

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	// Point the bridge at the fake broker (dev-only override).
	t.Setenv("ARROW_HFT_URL", fake.url)

	plan, err := BuildSubscriptionPlan([]int32{1000}, 1, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		runHFT(ctx, cancel, client, plan, 50, 10*time.Second, nil, t.Logf)
		close(done)
	}()

	// Let the bridge connect, subscribe, receive the tick, then hit the forced
	// disconnect and complete the 1s-backoff reconnect to a second connection.
	// Poll (not a fixed sleep): under -race the machine runs ~2x slower and a
	// fixed 1.6s window races the 1s reconnect backoff + second epoch.
	deadline := time.Now().Add(6 * time.Second)
	for {
		snap := out.String()
		tickCount := len(tickMapsFrom(t, snap))
		hasReconnect := lastEventState(eventsFrom(t, snap), "reconnect") == "BACKOFF"
		if tickCount >= 2 && hasReconnect && fake.connections.Load() >= 2 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done
	flushTestEmitter()

	events := eventsFrom(t, out.String())
	if got := lastEventState(events, "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("expected ACTIVE, got %q\n%s", got, out.String())
	}
	// Recovery integration: ticks must flow BOTH before (connection 1) and
	// after (connection 2) the forced disconnect — the feed resumes.
	tickCount := len(tickMapsFrom(t, out.String()))
	if tickCount < 2 {
		t.Fatalf("expected >=2 tick emissions across the disconnect (feed must resume), got %d\n%s", tickCount, out.String())
	}
	// The forced disconnect at subscription 1 must trigger a reconnect event.
	if got := lastEventState(events, "reconnect"); got != "BACKOFF" {
		t.Fatalf("expected reconnect BACKOFF, got %q\n%s", got, out.String())
	}
	// The reconnect loop must have started a second epoch (2+ connection attempts).
	if got := fake.connections.Load(); got < 2 {
		t.Fatalf("expected >=2 broker connections after reconnect, got %d", got)
	}
}

// TestFakeBrokerMultiSlotSupervisor — the supervisor starts two slots
// concurrently against the wire-format fake broker; both reach ACTIVE and each
// emits its own token's tick on its own connection. Production single-socket
// policy is bypassed by driving runHFTSupervisor directly.
func TestFakeBrokerMultiSlotSupervisor(t *testing.T) {
	fake := newFakeBrokerWire(t)
	defer fake.close()

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", fake.url)

	// Two explicit slots (bypassing the greedy planner, which packs 2 tokens
	// into one slot). Each slot carries its own token on its own connection.
	plan := SubscriptionPlan{Slots: []SlotAssignment{
		{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1000}, Requests: [][]int32{{1000}}},
		{SlotID: "hft-1", ConnectionID: "hft-1", Tokens: []int32{1001}, Requests: [][]int32{{1001}}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHFTSupervisor(ctx, client, plan, 50, 10*time.Second, nil, t.Logf)
	}()
	// Poll: both slots ACTIVE + each emits its own token's tick (race-safe).
	deadline := time.Now().Add(6 * time.Second)
	for {
		snap := out.String()
		events := eventsFrom(t, snap)
		active := 0
		for _, e := range events {
			if e["event"] == "subscription_ack" && e["state"] == "ACTIVE" {
				active++
			}
		}
		if active >= 2 && containsAny(ticksFrom(t, snap), "token=1000") && containsAny(ticksFrom(t, snap), "token=1001") {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done
	flushTestEmitter()

	events := eventsFrom(t, out.String())
	active := 0
	for _, e := range events {
		if e["event"] == "subscription_ack" && e["state"] == "ACTIVE" {
			active++
		}
	}
	if active < 2 {
		t.Fatalf("expected 2 ACTIVE subscriptions, got %d\n%s", active, out.String())
	}
	// Each slot's token tick must appear (tokens 1000 and 1001).
	if !containsAny(ticksFrom(t, out.String()), "token=1000") || !containsAny(ticksFrom(t, out.String()), "token=1001") {
		t.Fatalf("expected ticks for both slot tokens\n%s", out.String())
	}
	// Two distinct slot IDs must appear in events.
	slots := map[string]bool{}
	for _, e := range events {
		if s, _ := e["slot_id"].(string); s != "" {
			slots[s] = true
		}
	}
	if len(slots) < 2 {
		t.Fatalf("expected 2 distinct slot IDs, got %v\n%s", slots, out.String())
	}
}

// TestFakeBrokerForcedOneSlotDisconnect — connection 1 (slot-0) is force-closed
// after responding; slot-0 reconnects (epoch bump, second connection) while
// slot-1 stays ACTIVE on its own connection throughout. The supervisor keeps
// the healthy slot alive during the peer's retry.
func TestFakeBrokerForcedOneSlotDisconnect(t *testing.T) {
	fake := newFakeBrokerWire(t, 1) // force-close connection 1 (slot-0's first)
	defer fake.close()

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", fake.url)

	// Two explicit slots (bypassing the greedy planner).
	plan := SubscriptionPlan{Slots: []SlotAssignment{
		{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1000}, Requests: [][]int32{{1000}}},
		{SlotID: "hft-1", ConnectionID: "hft-1", Tokens: []int32{1001}, Requests: [][]int32{{1001}}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHFTSupervisor(ctx, client, plan, 50, 10*time.Second, nil, t.Logf)
	}()
	// Poll: slot-0 reconnects (1s backoff) while slot-1 stays ACTIVE.
	deadline := time.Now().Add(8 * time.Second)
	for {
		snap := out.String()
		events := eventsFrom(t, snap)
		active := 0
		for _, e := range events {
			if e["event"] == "subscription_ack" && e["state"] == "ACTIVE" {
				active++
			}
		}
		if active >= 2 && lastEventState(events, "reconnect") == "BACKOFF" && fake.connections.Load() >= 3 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done
	flushTestEmitter()

	events := eventsFrom(t, out.String())
	// Both slots must reach ACTIVE eventually (slot-0 after reconnect).
	active := 0
	for _, e := range events {
		if e["event"] == "subscription_ack" && e["state"] == "ACTIVE" {
			active++
		}
	}
	if active < 2 {
		t.Fatalf("expected 2 ACTIVE subscriptions (one post-reconnect), got %d\n%s", active, out.String())
	}
	// Slot-0 must have reconnected (BACKOFF) while slot-1 was untouched.
	if got := lastEventState(events, "reconnect"); got != "BACKOFF" {
		t.Fatalf("expected reconnect BACKOFF, got %q\n%s", got, out.String())
	}
	// The reconnect must have started a second connection for the forced slot.
	if fake.connections.Load() < 3 { // conn1 (slot0) + conn2 (slot1) + conn3 (slot0 retry)
		t.Fatalf("expected >=3 broker connections, got %d", fake.connections.Load())
	}
}

// TestFakeBrokerAllSlotTerminal — the fake broker rejects every subscription
// with E_ALL_INVALID; both slots stop terminal and the supervisor aggregates
// two terminal outcomes (no infinite retry, no healthy peer).
func TestFakeBrokerAllSlotTerminal(t *testing.T) {
	fake := newFakeBrokerWire(t)
	fake.allInvalid = true
	defer fake.close()

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", fake.url)

	// Two explicit slots (bypassing the greedy planner).
	plan := SubscriptionPlan{Slots: []SlotAssignment{
		{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1000}, Requests: [][]int32{{1000}}},
		{SlotID: "hft-1", ConnectionID: "hft-1", Tokens: []int32{1001}, Requests: [][]int32{{1001}}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	terminal := make(chan int, 1)
	go func() {
		terminal <- runHFTSupervisor(ctx, client, plan, 50, 10*time.Second, nil, t.Logf)
	}()
	// The supervisor should finish on its own (both slots terminal) without
	// needing a cancel — bounded wait, then cancel as a safety net.
	select {
	case n := <-terminal:
		if n != 2 {
			t.Fatalf("expected 2 terminal slot outcomes, got %d\n%s", n, out.String())
		}
	case <-time.After(8 * time.Second): // -race runs ~2x slower
		cancel()
		t.Fatalf("supervisor did not terminate after all slots went terminal\n%s", out.String())
	}
	// R-219: the supervisor's bridgeMetricsTicker goroutine runs until ctx is
	// done — on the success path the supervisor returns without cancelling,
	// so the ticker would leak into the next test and race its
	// bridgeEmitter reassignment. Always cancel after the wait.
	cancel()

	events := eventsFrom(t, out.String())
	terminalCount := 0
	for _, e := range events {
		if e["event"] == "subscription_ack" && e["state"] == "TERMINAL" {
			terminalCount++
		}
	}
	if terminalCount < 2 {
		t.Fatalf("expected 2 TERMINAL subscription_acks, got %d\n%s", terminalCount, out.String())
	}
	// No ACTIVE — nothing healthy survived.
	if containsAny(eventsAsStrings(t, out.String()), "state=ACTIVE") {
		t.Fatalf("no slot should reach ACTIVE when all subscriptions are invalid\n%s", out.String())
	}
}

// TestSlotIsolationSuppressesOnlyAssignedInstruments — plan §Production
// hardening: a slot failure suppresses only the instruments assigned to that
// slot; healthy peers' instruments keep flowing. Slot-0's token (1000) is
// rejected as E_ALL_INVALID → slot-0 stops terminal; slot-1's token (1001)
// stays subscribed and keeps emitting ticks. The supervisor must NOT cancel
// slot-1 when slot-0 goes terminal.
func TestSlotIsolationSuppressesOnlyAssignedInstruments(t *testing.T) {
	fake := newFakeBrokerWire(t)
	fake.tokenInvalid[1000] = true // slot-0's token rejected (terminal)
	defer fake.close()

	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", fake.url)

	plan := SubscriptionPlan{Slots: []SlotAssignment{
		{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1000}, Requests: [][]int32{{1000}}},
		{SlotID: "hft-1", ConnectionID: "hft-1", Tokens: []int32{1001}, Requests: [][]int32{{1001}}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHFTSupervisor(ctx, client, plan, 50, 10*time.Second, nil, t.Logf)
	}()
	// Poll: slot-0 TERMINAL while slot-1 ACTIVE + emitting (race-safe).
	deadline := time.Now().Add(6 * time.Second)
	for {
		snap := out.String()
		events := eventsFrom(t, snap)
		terminal := 0
		for _, e := range events {
			if e["event"] == "subscription_ack" && e["state"] == "TERMINAL" {
				terminal++
			}
		}
		if terminal >= 1 && slotEventState(events, "hft-1", "subscription_ack") == "ACTIVE" && containsAny(ticksFrom(t, snap), "token=1001") {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done
	flushTestEmitter()

	// Slot-0 (token 1000) must be TERMINAL — its assigned instrument is
	// suppressed (no ticks for token 1000 emitted as trade-eligible).
	events := eventsFrom(t, out.String())
	terminal := 0
	for _, e := range events {
		if e["event"] == "subscription_ack" && e["state"] == "TERMINAL" {
			terminal++
		}
	}
	if terminal < 1 {
		t.Fatalf("expected slot-0 to be TERMINAL, got %d\n%s", terminal, out.String())
	}
	// Slot-1 (token 1001) must reach ACTIVE and keep emitting ticks. Check
	// slot-1's own ack (the last ack overall may be slot-0's TERMINAL).
	if got := slotEventState(events, "hft-1", "subscription_ack"); got != "ACTIVE" {
		t.Fatalf("healthy slot-1 must reach ACTIVE, got %q\n%s", got, out.String())
	}
	if !containsAny(ticksFrom(t, out.String()), "token=1001") {
		t.Fatalf("slot-1's instrument must keep emitting ticks\n%s", out.String())
	}
	// Slot-0's token must NOT produce any ticks (suppressed by terminal state).
	if containsAny(ticksFrom(t, out.String()), "token=1000") {
		t.Fatalf("slot-0's assigned instrument must be suppressed after terminal\n%s", out.String())
	}
}

// slotEventState returns the last event state for the given event + slot ID.
func slotEventState(events []map[string]any, slotID, event string) string {
	for i := len(events) - 1; i >= 0; i-- {
		if s, _ := events[i]["slot_id"].(string); s != slotID {
			continue
		}
		if e, _ := events[i]["event"].(string); e == event {
			if st, ok := events[i]["state"].(string); ok {
				return st
			}
		}
	}
	return ""
}

// lockedBuffer is a mutex-guarded bytes.Buffer: the poll-based E2E tests read
// the emitter output while the bridge goroutine writes it, so a plain
// bytes.Buffer would be a data race under `go test -race`.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func newLockedBuffer() *lockedBuffer { return &lockedBuffer{} }

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
