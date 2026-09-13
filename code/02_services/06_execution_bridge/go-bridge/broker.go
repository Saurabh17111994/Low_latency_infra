package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/arrow-trade/go-arrow/arrow"
)

// Broker is the narrow broker capability surface used by the server. Keeping
// it behind an interface makes all order behavior testable without Arrow.
type Broker interface {
	Place(context.Context, CommandEnvelope) BrokerResult
	Modify(context.Context, CommandEnvelope) BrokerResult
	Cancel(context.Context, CommandEnvelope) BrokerResult
	QueryOrder(context.Context, CommandEnvelope) BrokerResult
	ReconcileOrders(context.Context, CommandEnvelope) BrokerResult
	ReconcileTrades(context.Context, CommandEnvelope) BrokerResult
	ReconcilePositions(context.Context, CommandEnvelope) BrokerResult
}

type BrokerResult struct {
	Outcome         string
	Reason          string
	BrokerOrderID   string
	ExchangeOrderID string
	OrderStatus     string
	ReportType      string
	Data            any
	Fingerprint     string
}

// ArrowBroker adapts the pinned go-arrow SDK. Credentials remain inside this
// object/process; no SDK client is passed across the private protocol.
type ArrowBroker struct{ client *arrow.Client }

func NewArrowBroker(client *arrow.Client) (*ArrowBroker, error) {
	if client == nil {
		return nil, errors.New("arrow client is required")
	}
	return &ArrowBroker{client: client}, nil
}

func (b *ArrowBroker) Place(ctx context.Context, c CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	// P3-036/P3-032: Broker is an exported interface wrapped by ReauthBroker, so a
	// direct caller can pass an envelope with no Order — validateCommand only
	// guards the HTTP path. Dereferencing it would panic the whole bridge process
	// instead of returning a terminal client error.
	if c.Order == nil {
		return rejectedResult(errors.New("order is required"))
	}
	req, err := toArrowOrder(*c.Order, c.ClientOrderRef)
	if err != nil {
		return rejectedResult(err)
	}
	resp, err := b.client.PlaceOrder("regular", req)
	if err != nil {
		return classifySDKError(err)
	}
	if resp == nil || strings.TrimSpace(resp.Data.OrderNo) == "" {
		return unknownResult(errors.New("place response missing orderNo"))
	}
	return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: resp.Data.OrderNo,
		Data: resp, Fingerprint: fingerprint(resp)}
}

func (b *ArrowBroker) Modify(ctx context.Context, c CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	// P3-038/P3-033: same totality requirement as Place — a nil Order from a
	// direct caller must be a terminal REJECTED, never a process-killing panic.
	if c.Order == nil {
		return rejectedResult(errors.New("order is required"))
	}
	req, err := toArrowOrder(*c.Order, c.ClientOrderRef)
	if err != nil {
		return rejectedResult(err)
	}
	resp, err := b.client.ModifyOrder("regular", c.BrokerOrderID, req)
	if err != nil {
		return classifySDKError(err)
	}
	if resp == nil || strings.TrimSpace(resp.Data.OrderNo) == "" {
		return unknownResult(errors.New("modify response missing orderNo"))
	}
	return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: resp.Data.OrderNo,
		Data: resp, Fingerprint: fingerprint(resp)}
}

func (b *ArrowBroker) Cancel(ctx context.Context, c CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	if err := b.client.CancelOrder("regular", c.BrokerOrderID); err != nil {
		return classifySDKError(err)
	}
	return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: c.BrokerOrderID}
}

func (b *ArrowBroker) QueryOrder(ctx context.Context, c CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	resp, err := b.client.GetOrder(c.BrokerOrderID)
	if err != nil {
		return classifySDKError(err)
	}
	return BrokerResult{Outcome: OutcomeSuccess, BrokerOrderID: c.BrokerOrderID,
		Data: resp, Fingerprint: fingerprint(resp)}
}

func (b *ArrowBroker) ReconcileOrders(ctx context.Context, _ CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	data, err := b.client.GetOrderBook()
	if err != nil {
		return classifySDKError(err)
	}
	return BrokerResult{Outcome: OutcomeSuccess, Data: data, Fingerprint: fingerprint(data)}
}

func (b *ArrowBroker) ReconcileTrades(ctx context.Context, _ CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	data, err := b.client.GetTradeBook()
	if err != nil {
		return classifySDKError(err)
	}
	return BrokerResult{Outcome: OutcomeSuccess, Data: data, Fingerprint: fingerprint(data)}
}

func (b *ArrowBroker) ReconcilePositions(ctx context.Context, _ CommandEnvelope) BrokerResult {
	if err := ctx.Err(); err != nil {
		return unknownResult(err)
	}
	data, err := b.client.GetPositions()
	if err != nil {
		return classifySDKError(err)
	}
	return BrokerResult{Outcome: OutcomeSuccess, Data: data, Fingerprint: fingerprint(data)}
}

func toArrowOrder(o OrderCommand, ref string) (arrow.OrderRequest, error) {
	if err := validateOrderCommand(o); err != nil {
		return arrow.OrderRequest{}, err
	}
	transaction := strings.ToUpper(strings.TrimSpace(o.TransactionType))
	if transaction == "BUY" {
		transaction = "B"
	} else if transaction == "SELL" {
		transaction = "S"
	}
	price := strings.TrimSpace(o.Price)
	// P3-045: arrow_broker.md documents "0" for market orders. Every accepted zero
	// form is canonicalised so the request bytes do not depend on the caller's
	// spelling of zero; a non-zero price is passed through untouched rather than
	// silently rewritten.
	if strings.EqualFold(strings.TrimSpace(o.OrderType), "MKT") && priceIsZero(price) {
		price = "0"
	}
	return arrow.OrderRequest{
		Exchange: o.Exchange, Quantity: o.Quantity, Product: strings.ToUpper(o.Product),
		Symbol: o.Symbol, TransactionType: transaction, OrderType: strings.ToUpper(o.OrderType),
		Price: price, Validity: strings.ToUpper(o.Validity), Remarks: ref,
		MarketProtection: o.MarketProtection,
	}, nil
}

var statusErrorPattern = regexp.MustCompile(`request failed with status ([0-9]{3}):\s*(.*)$`)

func classifySDKError(err error) BrokerResult {
	if err == nil {
		return unknownResult(errors.New("missing broker error"))
	}
	message := strings.TrimSpace(err.Error())
	if matches := statusErrorPattern.FindStringSubmatch(message); len(matches) == 3 {
		status, _ := strconv.Atoi(matches[1])
		body := strings.TrimSpace(matches[2])
		// Pure classification per dossier (broker_classification.go). This keeps the
		// HTTP-code table offline, without Arrow/Fluss, and ensures UNKNOWN is HALT.
		outcome := ClassifyBrokerResponse(status, body)
		switch outcome {
		case OutcomeSuccess:
			// Error path must never be treated as acceptance.
			return unknownResult(errors.New("ambiguous Arrow response"))
		case OutcomeRejected:
			msg := documentedRejectionMessage(body)
			if msg == "" {
				return unknownResult(errors.New("ambiguous Arrow response"))
			}
			return rejectedResult(errors.New(msg))
		default:
			if status == http.StatusUnauthorized || status == http.StatusForbidden ||
				status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 {
				return unknownResult(fmt.Errorf("arrow status %d", status))
			}
			return unknownResult(errors.New("ambiguous Arrow response"))
		}
	}
	// No HTTP status envelope: transport failure, malformed error wrapper, or timeout.
	// WAVE9-F: preserve the SDK's semantic order errors (validation + empty-OrderNo
	// P1-043) — collapsing them to "ambiguous" broke
	// TestArrowBrokerTreatsMalformedSuccessAsUnknown, which pins
	// malformed_success_response for success-without-OrderNo.
	lower := strings.ToLower(message)
	if strings.Contains(lower, "empty orderno") || strings.Contains(lower, "missing orderno") {
		return unknownResult(err)
	}
	if strings.Contains(lower, "timeout") {
		return unknownResult(fmt.Errorf("timeout: %s", message))
	}
	return unknownResult(errors.New("ambiguous Arrow response"))
}

func documentedRejectionMessage(body string) string {
	var value struct {
		Message      string `json:"message"`
		ErrorMessage string `json:"errorMessage"`
		Status       string `json:"status"`
	}
	if json.Unmarshal([]byte(body), &value) != nil {
		return ""
	}
	if strings.EqualFold(value.Status, "error") {
		if strings.TrimSpace(value.Message) != "" {
			return strings.TrimSpace(value.Message)
		}
		return strings.TrimSpace(value.ErrorMessage)
	}
	return ""
}

func rejectedResult(err error) BrokerResult {
	return BrokerResult{Outcome: OutcomeRejected, Reason: sanitizeReason(err)}
}

func unknownResult(err error) BrokerResult {
	return BrokerResult{Outcome: OutcomeUnknown, Reason: sanitizeReason(err)}
}

func sanitizeReason(err error) string {
	if err == nil {
		return "unknown broker outcome"
	}
	// Never return SDK error bodies verbatim: a body may contain request
	// metadata or credentials. The protocol carries a bounded category only.
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "timeout"):
		return "broker_timeout"
	case strings.Contains(message, "unauthorized") || strings.Contains(message, "status 401"):
		return "broker_auth_failure"
	case strings.Contains(message, "forbidden") || strings.Contains(message, "status 403"):
		return "broker_forbidden"
	case strings.Contains(message, "missing orderno") || strings.Contains(message, "empty orderno"):
		return "malformed_success_response"
	case strings.Contains(message, "ambiguous"):
		return "ambiguous_broker_response"
	default:
		return "broker_error"
	}
}
