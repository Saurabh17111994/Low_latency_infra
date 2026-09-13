package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

func TestArrowBrokerUsesPinnedSDKForPlaceAndPreservesReference(t *testing.T) {
	var got arrow.OrderRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/order/regular" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("appId") != "app" || r.Header.Get("token") != "token" {
			t.Fatalf("missing Arrow auth headers")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"orderNo":"BRK-1","requestTime":"now"}}`))
	}))
	defer server.Close()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(t.Context(), validPlaceCommand())
	if result.Outcome != OutcomeSuccess || result.BrokerOrderID != "BRK-1" {
		t.Fatalf("result=%+v", result)
	}
	if got.Remarks != validPlaceCommand().ClientOrderRef || got.TransactionType != "B" || got.Price != "15050" {
		t.Fatalf("Arrow request mapping=%+v", got)
	}
}

func TestArrowBrokerTreatsMalformedSuccessAsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{}}`))
	}))
	defer server.Close()
	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(t.Context(), validPlaceCommand())
	if result.Outcome != OutcomeUnknown || result.Reason != "malformed_success_response" {
		t.Fatalf("result=%+v", result)
	}
}

func TestArrowBrokerRejectsDocumentedHTTPRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":"error","message":"bad quantity"}`))
	}))
	defer server.Close()
	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	result := broker.Place(t.Context(), validPlaceCommand())
	if result.Outcome != OutcomeRejected || result.Reason != "broker_error" {
		t.Fatalf("result=%+v", result)
	}
}

func TestNoCredentialsAppearInSanitizedReasons(t *testing.T) {
	reason := sanitizeReason(errors.New("token=secret-app-token"))
	if strings.Contains(reason, "secret-app-token") {
		t.Fatalf("secret leaked in reason %q", reason)
	}
}

// P3-034/P3-037: the caller's context must bound the time the bridge spends
// waiting on the broker. The pinned SDK takes no context for order calls and its
// client sets no per-request deadline, so a stalled Arrow endpoint used to hold
// the command past its own commandTimeout and the HTTP handler blocked for as
// long as the venue took. The request may still reach the broker after the bridge
// stops waiting, so the outcome must be UNKNOWN — never SUCCESS, and never a
// terminal rejection or the auth failure the reauth wrapper retries.
func TestArrowBrokerStopsWaitingWhenContextExpires(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // accept the request and never answer it
	}))
	// LIFO: release the stalled handler before Close waits for it, so the
	// abandoned SDK call cannot outlive the test.
	defer server.Close()
	defer close(release)

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	client.Config.BaseURL = server.URL
	broker, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan BrokerResult, 1)
	go func() { done <- broker.Place(ctx, validPlaceCommand()) }()

	select {
	case result := <-done:
		t.Logf("abandoned call: outcome=%s reason=%s", result.Outcome, result.Reason)
		if result.Outcome != OutcomeUnknown {
			t.Fatalf("outcome=%s reason=%s, want UNKNOWN", result.Outcome, result.Reason)
		}
		if result.Reason == "" {
			t.Fatalf("abandoned call reported no reason")
		}
		if result.Reason == "broker_auth_failure" {
			t.Fatalf("abandoned call reported as an auth failure, which triggers a re-auth and a re-submit")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Place did not return within 2s of its 50ms context deadline")
	}
}

// P3-035/P3-039: the HTTP status envelope is parsed out of the SDK's error string,
// so it has to survive the shapes those errors actually take — a pretty-printed JSON
// body, a trailing newline, and the SDK error wrapped by its caller. The old
// single-line, end-anchored pattern matched none of the multi-line shapes and fell
// through to the generic branch, turning a terminal REJECTED into an ambiguous
// UNKNOWN, which halts a decision the venue had already made.
func TestClassifySDKErrorReadsMultiLineStatusEnvelopes(t *testing.T) {
	pretty := "{\n  \"status\": \"error\",\n  \"message\": \"price outside the allowed band\"\n}"
	cases := []struct {
		name string
		err  error
	}{
		{"single line", fmt.Errorf("request failed with status 400: %s",
			`{"status":"error","message":"price outside the allowed band"}`)},
		{"pretty printed body", fmt.Errorf("request failed with status 422: %s", pretty)},
		{"trailing newline", fmt.Errorf("request failed with status 409: %s\n",
			`{"status":"error","message":"duplicate client order id"}`)},
		{"wrapped by the caller", fmt.Errorf("place order: %w",
			fmt.Errorf("request failed with status 400: %s", `{"status":"error","message":"unknown symbol"}`))},
	}
	var missed []string
	for _, tc := range cases {
		result := classifySDKError(tc.err)
		if result.Outcome != OutcomeRejected {
			missed = append(missed, fmt.Sprintf("%s -> %s/%s", tc.name, result.Outcome, result.Reason))
		}
	}
	if len(missed) > 0 {
		t.Fatalf("%d of %d status envelopes were not classified as REJECTED: %v", len(missed), len(cases), missed)
	}
}
