package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// G7 guard: nil slices normalize to [] on the wire, never `null`
// (P1-185 basket orders, P1-186 greeks tokens).
func TestG7NilSlicesMarshalAsEmptyArray(t *testing.T) {
	var basketBody, greeksBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		r.Body.Read(b)
		switch {
		case strings.HasSuffix(r.URL.Path, "/margin/basket"):
			basketBody = string(b)
			w.Write([]byte(`{"status":"success","data":{}}`))
		case strings.HasSuffix(r.URL.Path, "/info/greeks"):
			greeksBody = string(b)
			w.Write([]byte(`{"status":"success","data":[]}`))
		}
	}))
	defer srv.Close()
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = srv.URL
	if _, err := c.GetBasketMargin(arrow.BasketMarginRequest{}); err != nil {
		t.Fatalf("G7/P1-185: basket call failed: %v", err)
	}
	if !strings.Contains(basketBody, `"orders":[]`) {
		t.Fatalf("G7/P1-185: nil Orders must marshal as [], got %s", basketBody)
	}
	if _, err := c.GetGreeks(nil); err != nil {
		t.Fatalf("G7/P1-186: greeks call failed: %v", err)
	}
	if strings.TrimSpace(greeksBody) != "[]" {
		t.Fatalf("G7/P1-186: nil tokens must marshal as [], got %s", greeksBody)
	}
}

// G7 guard: data:null success returns non-nil empty, never (nil,nil)
// (P1-298 index list; P1-195 positions already guarded in G5/G6).
func TestG7NullDataReturnsEmptyNonNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":null}`))
	}))
	defer srv.Close()
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = srv.URL
	idx, err := c.GetIndexList()
	if err != nil || idx == nil || len(idx) != 0 {
		t.Fatalf("G7/P1-298: data:null must be empty non-nil, got %v %v", idx, err)
	}
	pos, err := c.GetPositions()
	if err != nil || pos == nil || len(pos) != 0 {
		t.Fatalf("G7/P1-195: data:null must be empty non-nil, got %v %v", pos, err)
	}
	g, err := c.GetGreeks([]int{1})
	if err != nil || string(g) != "[]" {
		t.Fatalf("G7: greeks null data must be [], got %s %v", string(g), err)
	}
	var raw json.RawMessage = g
	if raw == nil {
		t.Fatal("G7: greeks RawMessage must be non-nil")
	}
}

// G7 guard: error pages never parse as CSV rows; header row dropped
// (P1-041 CSV sniff, P1-187 shape guard + header drop).
func TestG7CSVRejectsErrorPages(t *testing.T) {
	for _, body := range []string{
		`{"status":"error","message":"bad segment"}`,
		`<html><body>login</body></html>`,
		``,
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(body))
		}))
		c := arrow.NewClient("id", "secret")
		c.Config.BaseURL = srv.URL
		if _, err := c.GetInstruments(arrow.InstrumentSegmentAll); err == nil {
			t.Fatalf("G7/P1-041/187: error body must not parse as CSV: %q", body)
		}
		srv.Close()
	}
}

// G7 guard: candle rejects non-array bodies; missing base fails fast
// (P1-042 shape gate, P1-188 base guard).
func TestG7CandleShapeAndBase(t *testing.T) {
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = "http://127.0.0.1:1"
	c.Config.HistoricalBaseURL = ""
	if _, err := c.GetCandleData(arrow.ExchangeNSE, "1", "1m", "a", "b", false); err == nil ||
		!strings.Contains(err.Error(), "HistoricalBaseURL") {
		t.Fatalf("G7/P1-188: empty base must fail fast, got %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"error","message":"bad token"}`))
	}))
	defer srv.Close()
	c2 := arrow.NewClient("id", "secret")
	c2.Config.BaseURL = srv.URL
	c2.Config.HistoricalBaseURL = srv.URL
	if _, err := c2.GetCandleData(arrow.ExchangeNSE, "1", "1m", "a", "b", false); err == nil {
		t.Fatal("G7/P1-042: error envelope must not return as candles")
	}
}
