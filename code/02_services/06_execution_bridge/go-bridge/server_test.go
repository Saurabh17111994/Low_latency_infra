package main

import (
	"context"
	"encoding/json"
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
