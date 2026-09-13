package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func startTestServer(t *testing.T, broker Broker) (*BridgeServer, *httptest.Server) {
	t.Helper()
	bridge, err := NewBridgeServer(broker, "internal-secret", "fake")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(bridge.Handler())
	t.Cleanup(server.Close)
	return bridge, server
}

func postCommand(t *testing.T, serverURL string, command CommandEnvelope, token string) (int, ReportEnvelope) {
	t.Helper()
	body, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, serverURL+commandPath, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var report ReportEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, report
}

func TestPrivateCommandRequiresBearerAuth(t *testing.T) {
	_, server := startTestServer(t, NewFakeBroker())
	status, report := postCommand(t, server.URL, validPlaceCommand(), "")
	if status != http.StatusUnauthorized || report.Outcome != OutcomeUnknown {
		t.Fatalf("status=%d report=%+v", status, report)
	}
}

func TestPlaceCommandUsesFakeBrokerAndReturnsExplicitOutcome(t *testing.T) {
	fake := NewFakeBroker()
	_, server := startTestServer(t, fake)
	status, report := postCommand(t, server.URL, validPlaceCommand(), "internal-secret")
	if status != http.StatusOK || report.Outcome != OutcomeSuccess || report.BrokerOrderID != "fake-broker-order-1" {
		t.Fatalf("status=%d report=%+v", status, report)
	}
	if fake.Calls(CommandPlace) != 1 {
		t.Fatalf("place calls=%d, want 1", fake.Calls(CommandPlace))
	}
}

func TestUnknownPlaceOutcomeIsReturnedWithoutRetry(t *testing.T) {
	fake := NewFakeBroker()
	fake.SetResult(CommandPlace, BrokerResult{Outcome: OutcomeUnknown, Reason: "ambiguous_broker_response"})
	_, server := startTestServer(t, fake)
	status, report := postCommand(t, server.URL, validPlaceCommand(), "internal-secret")
	if status != http.StatusOK || report.Outcome != OutcomeUnknown || report.Reason != "ambiguous_broker_response" {
		t.Fatalf("status=%d report=%+v", status, report)
	}
	if fake.Calls(CommandPlace) != 1 {
		t.Fatalf("unknown place was retried: calls=%d", fake.Calls(CommandPlace))
	}
}

func TestDuplicateRequestIDReturnsCachedOutcomeWithoutSecondBrokerCall(t *testing.T) {
	fake := NewFakeBroker()
	_, server := startTestServer(t, fake)
	command := validPlaceCommand()
	status, first := postCommand(t, server.URL, command, "internal-secret")
	if status != http.StatusOK || first.Outcome != OutcomeSuccess {
		t.Fatalf("first status=%d report=%+v", status, first)
	}
	status, second := postCommand(t, server.URL, command, "internal-secret")
	if status != http.StatusOK || second.BrokerOrderID != first.BrokerOrderID {
		t.Fatalf("second status=%d report=%+v", status, second)
	}
	if fake.Calls(CommandPlace) != 1 {
		t.Fatalf("duplicate request reached broker: calls=%d", fake.Calls(CommandPlace))
	}
}

func TestRequestIDReuseWithDifferentContentIsRejected(t *testing.T) {
	fake := NewFakeBroker()
	_, server := startTestServer(t, fake)
	command := validPlaceCommand()
	if status, _ := postCommand(t, server.URL, command, "internal-secret"); status != http.StatusOK {
		t.Fatal("initial request failed")
	}
	command.Order.Quantity = "3"
	status, report := postCommand(t, server.URL, command, "internal-secret")
	if status != http.StatusConflict || report.Reason != "request_id_reuse_violation" {
		t.Fatalf("status=%d report=%+v", status, report)
	}
	if fake.Calls(CommandPlace) != 1 {
		t.Fatal("request-id violation reached broker")
	}
}

func TestPrivateEventsMapAndDeliverPostback(t *testing.T) {
	bridge, server := startTestServer(t, NewFakeBroker())
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + eventsPath
	header := http.Header{}
	header.Set("Authorization", "Bearer internal-secret")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := bridge.hub.Publish(NormalizeOrderUpdate(map[string]any{"id": "BRK-2", "remarks": "INS2", "orderStatus": "OPEN"})); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var report ReportEnvelope
	if err := json.Unmarshal(payload, &report); err != nil {
		t.Fatal(err)
	}
	if report.Command != "postback" || report.BrokerOrderID != "BRK-2" {
		t.Fatalf("report=%+v", report)
	}
}

func TestMalformedCommandDoesNotReachBroker(t *testing.T) {
	fake := NewFakeBroker()
	_, server := startTestServer(t, fake)
	req, err := http.NewRequest(http.MethodPost, server.URL+commandPath, strings.NewReader(`{"record_type":"execution_command","contract_version":1,"request_id":"x","command":"place",}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer internal-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, data)
	}
	if fake.Calls(CommandPlace) != 0 {
		t.Fatal("malformed command reached broker")
	}
}

// K4 (2026-08-29): EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS — default 10s, valid
// override honored, invalid value rejected (error, not hang).
func TestCommandTimeoutFromEnv(t *testing.T) {
	t.Setenv("EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS", "")
	d, err := commandTimeoutFromEnv()
	if err != nil || d != 10*time.Second {
		t.Fatalf("unset env: got %v, err %v; want 10s, nil", d, err)
	}

	t.Setenv("EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS", "5000")
	d, err = commandTimeoutFromEnv()
	if err != nil || d != 5*time.Second {
		t.Fatalf("5000: got %v, err %v; want 5s, nil", d, err)
	}

	for _, bad := range []string{"0", "-1", "50", "abc", "700000"} {
		t.Setenv("EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS", bad)
		if _, err := commandTimeoutFromEnv(); err == nil {
			t.Fatalf("%s: expected error, got nil", bad)
		}
	}
}

// P3-264 — a payload the bridge cannot serialize must not be reported as a
// SUCCESS with Data silently dropped: downstream reads the missing payload as
// "nothing to reconcile" and never asks again.
func TestUnserializableReportDataFailsClosed(t *testing.T) {
	broker := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		return BrokerResult{
			Outcome: OutcomeSuccess, BrokerOrderID: "BRK-UNSERIALIZABLE",
			Data: make(chan int), // json.Marshal cannot represent a channel
		}
	}}
	_, server := startTestServer(t, broker)
	code, report := postCommand(t, server.URL, validPlaceCommand(), "internal-secret")
	if code != http.StatusOK {
		t.Fatalf("code=%d want 200", code)
	}
	if report.Outcome != OutcomeUnknown {
		t.Errorf("outcome=%s want UNKNOWN: a payload that cannot be serialized is not a success", report.Outcome)
	}
	if report.Data != nil {
		t.Errorf("data=%s want omitted", report.Data)
	}
	if report.Reason == "" {
		t.Error("the failing report must carry a reason token")
	}
	// The correlation fields are what make the failure reconcilable.
	if report.RequestID != "req-1" || report.InstructionID != "instruction-1" || report.ExecutionAttemptID != "attempt-1" {
		t.Errorf("correlation lost: request=%s instruction=%s attempt=%s",
			report.RequestID, report.InstructionID, report.ExecutionAttemptID)
	}
}

// postCommandWithContext is postCommand with a caller-controlled request
// context, so a test can make one client disconnect while another is in flight.
// It reports through t.Error rather than t.Fatal: callers use it from
// goroutines, where Fatal only stops that goroutine (go vet flags the pattern)
// and can hang the test.
func postCommandWithContext(t *testing.T, handler http.Handler, ctx context.Context, command CommandEnvelope, token string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(command)
	if err != nil {
		t.Error(err)
		return httptest.NewRecorder()
	}
	req := httptest.NewRequest(http.MethodPost, commandPath, strings.NewReader(string(body))).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// P3-051 first half — a duplicate command whose venue call is still in flight
// makes the follower wait on the owner. The follower used to wait with no
// select, pinning its goroutine and its connection even after its own client had
// gone.
func TestFollowerStopsWaitingWhenItsClientDisconnects(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	broker := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release // hold the venue call open for the whole test
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-1"}
	}}
	bridge, err := NewBridgeServer(broker, "internal-secret", "fake")
	if err != nil {
		t.Fatal(err)
	}
	handler := bridge.Handler()
	command := validPlaceCommand()

	owner := make(chan struct{})
	go func() {
		defer close(owner)
		postCommandWithContext(t, handler, context.Background(), command, "internal-secret")
	}()
	<-entered // the owner is inside the broker call
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		postCommandWithContext(t, handler, ctx, command, "internal-secret")
	}()
	time.Sleep(50 * time.Millisecond) // let the follower reach the wait
	cancel()                          // its client disconnects

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the follower is still pinned after its client disconnected: it waits on state.done with no ctx select")
	}
}

// P3-051 second half — the owner must complete the dedup state on every path. A
// panic in dispatch (no recover in handleCommand) used to leave the state
// unfinished forever, so that RequestID made every later caller a follower of a
// request that would never finish.
func TestFollowerUnblocksWhenTheOwnerPanics(t *testing.T) {
	broker := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		panic("venue adapter exploded")
	}}
	bridge, err := NewBridgeServer(broker, "internal-secret", "fake")
	if err != nil {
		t.Fatal(err)
	}
	handler := bridge.Handler()
	command := validPlaceCommand()

	// The owner panics. net/http recovers this in production; the harness does it
	// here so the panic does not take the test binary down.
	ownerDone := make(chan struct{})
	go func() {
		defer close(ownerDone)
		defer func() { _ = recover() }()
		postCommandWithContext(t, handler, context.Background(), command, "internal-secret")
	}()
	<-ownerDone

	// A later reuse of that RequestID must answer, not wait forever. Its context
	// never expires, so only a completed state can release it.
	done := make(chan struct{})
	var report ReportEnvelope
	go func() {
		defer close(done)
		rec := postCommandWithContext(t, handler, context.Background(), command, "internal-secret")
		_ = json.Unmarshal(rec.Body.Bytes(), &report)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the dedup state was never completed: this RequestID is blocked for the process lifetime")
	}
	if report.Outcome != OutcomeUnknown || report.Reason == "" {
		t.Fatalf("report=%+v want a fail-closed UNKNOWN with a reason", report)
	}
}

// P3-052 — the venue calls take no context (only LoginContext and
// GetPositionsContext do), so a command deadline can only be enforced here. A
// stalled Arrow request used to hold the handler — and its caller — far past
// commandTimeout.
func TestCommandDeadlineIsEnforcedWhileTheVenueCallIsStalled(t *testing.T) {
	t.Setenv("EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS", "150")
	release := make(chan struct{})
	broker := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		<-release // a stalled venue that never honours the deadline
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-LATE"}
	}}
	bridge, err := NewBridgeServer(broker, "internal-secret", "fake")
	if err != nil {
		t.Fatal(err)
	}
	defer close(release)

	done := make(chan ReportEnvelope, 1)
	go func() {
		rec := postCommandWithContext(t, bridge.Handler(), context.Background(), validPlaceCommand(), "internal-secret")
		var report ReportEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &report)
		done <- report
	}()
	select {
	case report := <-done:
		if report.Outcome != OutcomeUnknown || report.Reason == "" {
			t.Fatalf("report=%+v want a fail-closed UNKNOWN at the deadline", report)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler is still blocked: commandTimeout was not enforced while the venue call stalled")
	}
}

// P3-052 second half — the deadlined reply must not lose the venue's answer. The
// stalled call still owns the dedup state, so a caller that reuses the
// RequestID after the timeout gets the real outcome rather than UNKNOWN forever.
// (No separate red state exists for this half: before the deadline is enforced
// at all, the first reply never arrives and this test stops at that failure.)
func TestLateVenueResultReachesTheDedupState(t *testing.T) {
	t.Setenv("EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS", "150")
	release := make(chan struct{})
	broker := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		<-release
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-LATE"}
	}}
	bridge, err := NewBridgeServer(broker, "internal-secret", "fake")
	if err != nil {
		t.Fatal(err)
	}
	handler := bridge.Handler()
	command := validPlaceCommand()

	first := make(chan ReportEnvelope, 1)
	go func() {
		rec := postCommandWithContext(t, handler, context.Background(), command, "internal-secret")
		var report ReportEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &report)
		first <- report
	}()
	select {
	case report := <-first:
		if report.Outcome != OutcomeUnknown {
			t.Fatalf("first reply=%+v want UNKNOWN at the deadline", report)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no reply at the deadline, so the late-result half cannot be checked")
	}

	close(release) // the venue finally answers
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec := postCommandWithContext(t, handler, context.Background(), command, "internal-secret")
		var report ReportEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &report)
		if report.Outcome == OutcomeSuccess && report.BrokerOrderID == "BRK-LATE" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the venue's late SUCCESS never reached the dedup state")
}

// P3-053 first half — the dedup map is keyed by client-supplied RequestID and
// used to be retained for the process lifetime, so authenticated traffic could
// grow it without bound. Completed entries are evicted oldest-first at the cap;
// an in-flight entry must survive, because forgetting one would let its
// RequestID dispatch the same order a second time.
func TestDedupMapIsBoundedAndKeepsInFlightRequests(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	broker := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		if c.RequestID == "req-stall" {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-" + c.RequestID}
	}}
	bridge, err := NewBridgeServer(broker, "internal-secret", "fake")
	if err != nil {
		t.Fatal(err)
	}
	handler := bridge.Handler()
	ctx := context.Background()

	stalled := validPlaceCommand()
	stalled.RequestID = "req-stall"
	go postCommandWithContext(t, handler, ctx, stalled, "internal-secret")
	<-entered

	for i := 0; i < maxTrackedRequests+50; i++ {
		command := validPlaceCommand()
		command.RequestID = fmt.Sprintf("req-%d", i)
		if rec := postCommandWithContext(t, handler, ctx, command, "internal-secret"); rec.Code != http.StatusOK {
			t.Fatalf("request %d code=%d want 200", i, rec.Code)
		}
	}

	bridge.requestMu.Lock()
	size := len(bridge.requests)
	_, stalledStillTracked := bridge.requests["req-stall"]
	bridge.requestMu.Unlock()
	close(release)

	if size > maxTrackedRequests {
		t.Errorf("dedup map holds %d identities with a cap of %d: it grows without bound", size, maxTrackedRequests)
	}
	if !stalledStillTracked {
		t.Error("an in-flight identity was evicted: its RequestID could dispatch a second order")
	}
}

// P3-053 second half — the RequestID is a client-supplied map key, so its length
// is bounded with the key set it feeds.
func TestOverlongRequestIDIsRefusedBeforeTheBroker(t *testing.T) {
	calls := 0
	broker := &countingBroker{fn: func(ctx context.Context, c CommandEnvelope) BrokerResult {
		calls++
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-1"}
	}}
	_, server := startTestServer(t, broker)
	command := validPlaceCommand()
	command.RequestID = strings.Repeat("r", maxRequestIDLength+1)

	code, report := postCommand(t, server.URL, command, "internal-secret")
	if code != http.StatusBadRequest {
		t.Errorf("code=%d want 400 for a %d-character request_id", code, len(command.RequestID))
	}
	if !strings.Contains(report.Reason, "request_id") {
		t.Errorf("reason=%q want it to name request_id", report.Reason)
	}
	if calls != 0 {
		t.Errorf("broker calls=%d want 0: an over-long id must not reach the venue", calls)
	}
}

// P3-055 first half — the events endpoint is control-only and its read loop
// discards whatever arrives, but each frame was buffered whole before being
// discarded, so one authenticated client could OOM the bridge with one frame.
func TestEventsEndpointRejectsOversizedFrames(t *testing.T) {
	_, server := startTestServer(t, &countingBroker{fn: func(context.Context, CommandEnvelope) BrokerResult {
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-1"}
	}})
	url := "ws" + strings.TrimPrefix(server.URL, "http") + eventsPath
	conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Authorization": []string{"Bearer internal-secret"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, make([]byte, maxEventFrameBytes*4)); err != nil {
		t.Fatalf("write oversized frame: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
		t.Fatalf("read after an oversized frame returned %v, want a %d close: the frame is buffered whole before being discarded",
			err, websocket.CloseMessageTooBig)
	}
}

// P3-055 second half — every events connection costs a subscription, a read
// goroutine and a ping ticker, and nothing capped how many one authenticated
// client could open.
func TestEventsSubscribersAreCapped(t *testing.T) {
	bridge, server := startTestServer(t, &countingBroker{fn: func(context.Context, CommandEnvelope) BrokerResult {
		return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: "BRK-1"}
	}})
	url := "ws" + strings.TrimPrefix(server.URL, "http") + eventsPath
	header := http.Header{"Authorization": []string{"Bearer internal-secret"}}

	conns := make([]*websocket.Conn, 0, maxEventSubscribers+1)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for i := 0; i < maxEventSubscribers; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(url, header)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, conn)
		waitForSubscribers(t, bridge, i+1)
	}

	over, _, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		return // a refused upgrade is a refusal too
	}
	conns = append(conns, over)
	_ = over.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := over.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
		t.Fatalf("subscriber %d read %v, want a %d close: subscribers are unbounded",
			maxEventSubscribers+1, err, websocket.CloseTryAgainLater)
	}
}

// waitForSubscribers waits until the hub holds at least want subscribers: the
// handler subscribes just after the handshake, so a dial can return first.
func waitForSubscribers(t *testing.T, bridge *BridgeServer, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bridge.hub.mu.Lock()
		have := len(bridge.hub.subscribers)
		bridge.hub.mu.Unlock()
		if have >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the hub never reached %d subscribers", want)
}
