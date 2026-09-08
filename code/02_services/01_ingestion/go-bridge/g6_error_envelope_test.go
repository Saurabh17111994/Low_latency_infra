package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// G6 guard: every envelope-bearing getter must surface code+message from a
// {"status":"error","message",...} body via the shared apiError helper —
// never a bare status string (P1-179/181/183/184/194/203).
func TestG6EnvelopesSurfaceCodeAndMessage(t *testing.T) {
	body := `{"status":"error","message":"bad quantity supplied","errorCode":"E_BAD_QTY","data":null}`
	cases := []struct {
		name string
		call func(c *arrow.Client) error
	}{
		{"holdings", func(c *arrow.Client) error { _, err := c.GetHoldings(); return err }},
		{"limits", func(c *arrow.Client) error { _, err := c.GetLimits(); return err }},
		{"positions", func(c *arrow.Client) error { _, err := c.GetPositions(); return err }},
		{"user", func(c *arrow.Client) error { _, err := c.GetUserDetails(); return err }},
		{"greeks", func(c *arrow.Client) error { _, err := c.GetGreeks([]int{1}); return err }},
		{"quotes", func(c *arrow.Client) error {
			_, err := c.GetQuotes([]arrow.QuoteInstrument{{Exchange: "NSE", Symbol: "X"}}, arrow.InfoQuoteLTP)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(body))
			}))
			defer srv.Close()
			c := arrow.NewClient("id", "secret")
			c.Config.BaseURL = srv.URL
			err := tc.call(c)
			if err == nil {
				t.Fatalf("G6/%s: error envelope must fail", tc.name)
			}
			if !strings.Contains(err.Error(), "bad quantity supplied") {
				t.Fatalf("G6/%s: error must carry server message, got %v", tc.name, err)
			}
			if !strings.Contains(err.Error(), "E_BAD_QTY") {
				t.Fatalf("G6/%s: error must carry server code, got %v", tc.name, err)
			}
		})
	}
}

// G6 guard: bare-status body (no message fields) must still fail with the
// status visible, never silent success (apiError status-only fallback).
func TestG6BareStatusStillFailsWithStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"error","data":null}`))
	}))
	defer srv.Close()
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = srv.URL
	_, err := c.GetPositions()
	if err == nil || !strings.Contains(err.Error(), "error") {
		t.Fatalf("G6: bare-status envelope must fail with status visible, got %v", err)
	}
}

// G6 guard: corrupt JSON must fail with endpoint context + body excerpt,
// never a bare syntax error (P1-183 pattern, applied package-wide).
func TestG6CorruptJSONCarriesExcerpt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{corrupt-json`))
	}))
	defer srv.Close()
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = srv.URL
	_, err := c.GetPositions()
	if err == nil {
		t.Fatal("G6: corrupt JSON must fail")
	}
	var syn *json.SyntaxError
	if !errors.As(err, &syn) {
		t.Fatalf("G6: corrupt-JSON error must wrap syntax error (%%w), got %v", err)
	}
	if !strings.Contains(err.Error(), "corrupt-json") {
		t.Fatalf("G6: corrupt-JSON error must carry body excerpt, got %v", err)
	}
}
