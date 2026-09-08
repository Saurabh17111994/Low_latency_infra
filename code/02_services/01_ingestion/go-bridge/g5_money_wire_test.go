package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// G5 guard: margin money accepts numeric, integer-string, and decimal-rupee
// shapes without failing the fetch (P1-040).
func TestG5MarginMoneyShapes(t *testing.T) {
	for _, body := range []string{
		`{"status":"success","data":{"requiredMargin":14675,"minimumCashRequired":100,"marginUsedAfterTrade":200,"charge":{"brokerage":10,"exchangeTxnFee":5,"gst":{"cgst":1,"igst":1,"sgst":1,"total":3},"ipft":0,"sebiCharges":0,"stampDuty":0,"total":18,"transactionTax":0}}}`,
		`{"status":"success","data":{"requiredMargin":"14675","minimumCashRequired":"100","marginUsedAfterTrade":"200","charge":{"brokerage":"10","exchangeTxnFee":"5","gst":{"cgst":"1","igst":"1","sgst":"1","total":"3"},"ipft":"0","sebiCharges":"0","stampDuty":"0","total":"18","transactionTax":"0"}}}`,
		`{"status":"success","data":{"requiredMargin":"146.75","minimumCashRequired":"1.00","marginUsedAfterTrade":"2.00","charge":{"brokerage":"0.10","exchangeTxnFee":"0.05","gst":{"cgst":"0.01","igst":"0.01","sgst":"0.01","total":"0.03"},"ipft":"0","sebiCharges":"0","stampDuty":"0","total":"0.18","transactionTax":"0"}}}`,
	} {
		var r arrow.MarginResponse
		if err := json.Unmarshal([]byte(body), &r); err != nil {
			t.Fatalf("G5/P1-040: margin shape must decode: %v\n%s", err, body[:80])
		}
		if int64(r.Data.RequiredMargin) != 14675 {
			t.Fatalf("G5/P1-040: requiredMargin must be 14675 paise, got %d", int64(r.Data.RequiredMargin))
		}
	}
}

// G5 guard: positions accept string AND numeric wire values (P1-044).
func TestG5PositionsMixedShapes(t *testing.T) {
	body := `{"status":"success","data":[{"symbol":"X","qty":"10","avgPrice":14675,"ltp":15000,"close":"14900","requestTime":"t"}]}`
	var r arrow.PositionsResponse
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		t.Fatalf("G5/P1-044: mixed string/number positions must decode: %v", err)
	}
	if string(r.Data[0].Qty) != "10" || string(r.Data[0].AvgPrice) != "14675" {
		t.Fatalf("G5/P1-044: qty/avgPrice mismatch: %+v", r.Data[0])
	}
}

// G5 guard: quote LTP accepts int and decimal-string paise (P1-196).
func TestG5QuoteMoneyShapes(t *testing.T) {
	for _, body := range []string{
		`{"token":1,"ltp":14675,"close":14900}`,
		`{"token":"1","ltp":"146.75","close":"149.00"}`,
	} {
		var q arrow.QuoteLTP
		if err := json.Unmarshal([]byte(body), &q); err != nil {
			t.Fatalf("G5/P1-196: quote shape must decode: %v\n%s", err, body)
		}
		if int64(q.Ltp) != 14675 {
			t.Fatalf("G5/P1-196: ltp must be 14675 paise, got %d", int64(q.Ltp))
		}
	}
}

// G5 guard: empty quote batch short-circuits without network (P1-197).
func TestG5EmptyQuoteBatchShortCircuits(t *testing.T) {
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = "http://127.0.0.1:1"
	got, err := c.GetQuotes(nil, arrow.InfoQuoteLTP)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("G5/P1-197: nil batch must return empty non-nil without network, got %v %v", got, err)
	}
}

// G5 guard: bad quote mode rejected before URL build (R-243).
func TestG5BadQuoteModeRejected(t *testing.T) {
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = "http://127.0.0.1:1"
	if _, err := c.GetQuotes([]arrow.QuoteInstrument{{Exchange: "NSE", Symbol: "X"}}, arrow.InfoQuoteMode("bogus")); err == nil ||
		!strings.Contains(err.Error(), "invalid quote mode") {
		t.Fatalf("G5/R-243: bogus mode must fail fast, got %v", err)
	}
}
