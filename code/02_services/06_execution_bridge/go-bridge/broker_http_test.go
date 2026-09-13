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

// P3-240: the two readers of a rejection body have to agree. ClassifyBrokerResponse
// recognises a rejection through status:"error" or success:false and through any of
// message, errorMessage and error_message; documentedRejectionMessage only knew the
// first status and the first two keys. Every shape the classifier calls REJECTED at a
// terminal status must therefore come back REJECTED from classifySDKError, not
// demoted to an ambiguous UNKNOWN — UNKNOWN is HALT without retry, so a demotion
// turns a decision the venue already made into manual work.
func TestClassifySDKErrorAgreesWithClassifierOnRejectionShapes(t *testing.T) {
	const message = "price outside the allowed band"
	bodies := []string{
		`{"status":"error","message":"` + message + `"}`,
		`{"status":"error","errorMessage":"` + message + `"}`,
		`{"status":"error","error_message":"` + message + `"}`,
		`{"success":false,"message":"` + message + `"}`,
		`{"success":false,"errorMessage":"` + message + `"}`,
		`{"success":false,"error_message":"` + message + `"}`,
	}
	statuses := []int{http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity}
	// Pin the table size: the assertion below counts mismatches, so a table that
	// silently shrank would pass without testing anything.
	if len(bodies)*len(statuses) != 18 {
		t.Fatalf("fixture: %d body/status pairs, want 18", len(bodies)*len(statuses))
	}

	var mismatched []string
	for _, body := range bodies {
		for _, status := range statuses {
			// Fixture guard: this test is only meaningful for shapes the classifier
			// itself refuses to demote.
			if got := ClassifyBrokerResponse(status, body); got != OutcomeRejected {
				t.Fatalf("fixture: classifier calls %s at %d %s, not REJECTED", body, status, got)
			}
			result := classifySDKError(fmt.Errorf("request failed with status %d: %s", status, body))
			if result.Outcome != OutcomeRejected {
				mismatched = append(mismatched, fmt.Sprintf("%d %s -> %s/%s", status, body, result.Outcome, result.Reason))
			}
		}
	}
	if len(mismatched) > 0 {
		t.Fatalf("%d of %d recognised rejections were demoted by classifySDKError: %v",
			len(mismatched), len(bodies)*len(statuses), mismatched)
	}
}

// P3-237: a status the classifier cannot resolve is UNKNOWN, which is HALT without
// retry, and the reason is what an operator reads to tell those cases apart. They all
// used to arrive as the generic broker_error, so a rate limit was indistinguishable
// from an outage. The category is now derived from the code the classifier saw, not
// from matching text in the sentence around it.
func TestUnknownStatusesCarryDistinctReasons(t *testing.T) {
	cases := []struct {
		status int
		reason string
	}{
		{http.StatusUnauthorized, "broker_auth_failure"},
		{http.StatusForbidden, "broker_forbidden"},
		{http.StatusRequestTimeout, "broker_timeout"},
		{http.StatusTooManyRequests, "broker_rate_limited"},
		{http.StatusInternalServerError, "broker_unavailable"},
		{http.StatusServiceUnavailable, "broker_unavailable"},
	}
	var wrong []string
	for _, tc := range cases {
		result := classifySDKError(fmt.Errorf("request failed with status %d: ", tc.status))
		if result.Outcome != OutcomeUnknown {
			wrong = append(wrong, fmt.Sprintf("%d -> %s, want UNKNOWN", tc.status, result.Outcome))
			continue
		}
		if result.Reason != tc.reason {
			wrong = append(wrong, fmt.Sprintf("%d -> %q, want %q", tc.status, result.Reason, tc.reason))
		}
	}
	if len(wrong) > 0 {
		t.Fatalf("%d of %d statuses carried the wrong reason: %v", len(wrong), len(cases), wrong)
	}
}
