package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// OrderUpdateSource hides the SDK's concrete WebSocket type and makes
// reconnect/postback behavior testable with deterministic fakes.
type OrderUpdateSource interface {
	Read(context.Context, func(map[string]any), func(error))
	Close() error
}

type arrowOrderUpdateSource struct{ stream *arrow.OrderStream }

func (s *arrowOrderUpdateSource) Read(ctx context.Context, onUpdate func(map[string]any), onError func(error)) {
	s.stream.ReadUpdates(ctx, onUpdate, onError)
}
func (s *arrowOrderUpdateSource) Close() error {
	if s == nil || s.stream == nil {
		return nil
	}
	return s.stream.Close()
}

func NewArrowOrderUpdateSource(client *arrow.Client) (OrderUpdateSource, error) {
	if client == nil {
		return nil, fmt.Errorf("arrow client is required")
	}
	stream, err := client.ConnectOrderStream()
	if err != nil {
		return nil, err
	}
	return &arrowOrderUpdateSource{stream: stream}, nil
}

// RunPostbackLoop reconnects after a dropped order-update socket. It does not
// retry a place command; it only restores observation of broker reports.
func RunPostbackLoop(ctx context.Context, connect func() (OrderUpdateSource, error), publish func(ReportEnvelope) error, onError func(error)) {
	runPostbackLoop(ctx, connect, publish, onError, time.Second, 30*time.Second)
}

// runPostbackLoop is the deterministic implementation used by the public
// runtime wrapper and by the offline reconnect tests.  The production wrapper
// keeps the conservative one-second-to-thirty-second backoff; tests can use a
// millisecond backoff without sleeping through a real recovery interval.
func runPostbackLoop(ctx context.Context, connect func() (OrderUpdateSource, error), publish func(ReportEnvelope) error, onError func(error), initialBackoff, maxBackoff time.Duration) {
	backoff := initialBackoff
	for ctx.Err() == nil {
		source, err := connect()
		// P3-047/P3-046: a connect that reports success while yielding no source is
		// a connect failure, not a readable stream. source.Read/source.Close would
		// panic on the nil interface inside the reader goroutine and nothing
		// recovers it — the loop runs as a bare `go RunPostbackLoop` in main.go, so
		// that panic takes the whole bridge process down.
		if err == nil && source == nil {
			err = fmt.Errorf("postback connect returned no order-update source")
		}
		if err != nil {
			if onError != nil {
				onError(err)
			}
			if !sleepContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		// Reset after a healthy connect. Reset to the caller-supplied initial
		// backoff (production passes time.Second; deterministic tests pass a
		// millisecond) rather than a hard-coded one second, so the reconnect
		// interval honours the configured value instead of racing callers that
		// intentionally shrink it for reproducible lifecycle tests.
		backoff = initialBackoff
		updates := make(chan map[string]any, 16)
		errors := make(chan error, 1)
		readDone := make(chan struct{})
		readCtx, cancel := context.WithCancel(ctx)
		go func() {
			defer close(readDone)
			source.Read(readCtx, func(update map[string]any) {
				select {
				case updates <- update:
				case <-readCtx.Done():
				}
			}, func(readErr error) {
				select {
				case errors <- readErr:
				default:
				}
			})
		}()

		closed := false
		for !closed && ctx.Err() == nil {
			select {
			case update := <-updates:
				report := NormalizeOrderUpdate(update)
				if err := publish(report); err != nil && onError != nil {
					onError(err)
				}
			case err := <-errors:
				if err != nil && onError != nil {
					onError(err)
				}
				// A broker can deliver a burst of reports and then disconnect
				// (e.g. fills followed by an immediate socket drop). The reader
				// sequenced those reports into `updates` before it signalled the
				// termination, so drain them here rather than drop a valid
				// postback merely because the close arrived in the same select.
				for ctx.Err() == nil {
					select {
					case drained := <-updates:
						dreport := NormalizeOrderUpdate(drained)
						if err := publish(dreport); err != nil && onError != nil {
							onError(err)
						}
					default:
						closed = true
					}
					if closed {
						break
					}
				}
			case <-readDone:
				// Reader returned without an explicit error (clean close).
				// Drain any queued postbacks that were sequenced before the
				// return, then trigger a reconnect.
				for {
					select {
					case drained := <-updates:
						dreport := NormalizeOrderUpdate(drained)
						if err := publish(dreport); err != nil && onError != nil {
							onError(err)
						}
					default:
						closed = true
						break
					}
					if closed {
						break
					}
				}
			case <-ctx.Done():
				closed = true
			}
		}
		cancel()
		_ = source.Close()
		if ctx.Err() == nil {
			if !sleepContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current <= 0 {
		return 2 * time.Nanosecond
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

// NormalizeOrderUpdate maps Arrow's JSON postback shape without making the
// bridge a position engine. Unknown lifecycle values become UNKNOWN.
func NormalizeOrderUpdate(update map[string]any) ReportEnvelope {
	status := stringField(update, "orderStatus")
	reportType := stringField(update, "reportType")
	outcome := OutcomeUnknown
	reason := "unknown_order_update"
	if knownOrderStatus(status) || knownReportType(reportType) {
		outcome = OutcomeSuccess
		reason = ""
	}
	if strings.EqualFold(status, "REJECTED") || strings.EqualFold(reportType, "Rejected") {
		outcome = OutcomeRejected
		reason = "broker_rejected"
	}

	report := ReportEnvelope{
		RecordType: RecordReport, ContractVersion: ProtocolVersion,
		Command: "postback", Outcome: outcome, Reason: reason,
		ClientOrderRef:  stringField(update, "remarks"),
		BrokerOrderID:   stringField(update, "id"),
		ExchangeOrderID: stringField(update, "exchangeOrderID"),
		OrderStatus:     status, ReportType: reportType,
		FillShares:      stringField(update, "fillShares"),
		AveragePrice:    stringField(update, "averagePrice"),
		FillPrice:       stringField(update, "fillPrice"),
		FillQuantity:    stringField(update, "fillQuantity"),
		FillTime:        stringField(update, "fillTime"),
		InstrumentToken: stringField(update, "token"), ReceivedTsMs: nowMs(),
	}
	// P3-474: the digest input is unchanged, so ids already issued for earlier
	// updates keep deduplicating. json.Marshal cannot fail on map[string]string,
	// but the error is handled explicitly rather than collapsed: this id is the
	// upstream dedup key for the event, so a report that cannot carry one is never
	// published as a success.
	eventID, err := fingerprint(map[string]string{
		"id": report.BrokerOrderID, "remarks": report.ClientOrderRef,
		"status": report.OrderStatus, "report_type": report.ReportType,
		"fill_shares": report.FillShares, "average_price": report.AveragePrice,
		"exchange_update_time": stringField(update, "exchangeUpdateTime"),
	})
	if err != nil {
		report.Outcome = OutcomeUnknown
		report.Reason = "postback_event_id_unavailable"
		return report
	}
	report.PostbackEventID = eventID
	return report
}

func knownOrderStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "PENDING", "OPEN", "COMPLETE", "CANCELLED", "CANCELED", "REJECTED", "TRIGGER_PENDING":
		return true
	default:
		return false
	}
}

func knownReportType(reportType string) bool {
	switch strings.ToLower(strings.TrimSpace(reportType)) {
	case "newack", "pendingnew", "fill", "canceled", "cancelled", "rejected":
		return true
	default:
		return false
	}
}

func stringField(update map[string]any, key string) string {
	value, ok := update[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return v.String()
	case float64:
		return fmt.Sprintf("%g", v)
	default:
		return fmt.Sprint(v)
	}
}
