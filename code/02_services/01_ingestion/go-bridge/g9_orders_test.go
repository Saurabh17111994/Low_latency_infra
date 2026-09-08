package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

func g9Client(url string) *arrow.Client {
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = url
	return c
}

func g9ValidOrder() arrow.OrderRequest {
	return arrow.OrderRequest{
		Exchange: "NSE", Quantity: "10", Product: "CNC", Symbol: "RELIANCE",
		TransactionType: "B", OrderType: "LMT", Price: "150.50", Validity: "DAY",
	}
}

// G9 guard: MARKET orders omit price on the wire (P1-189); LIMIT keeps it.
func TestG9MarketOrderOmitsPrice(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		r.Body.Read(b)
		body = string(b)
		w.Write([]byte(`{"status":"success","data":{"orderNo":"123"}}`))
	}))
	defer srv.Close()
	o := g9ValidOrder()
	o.OrderType = "MKT"
	o.Price = ""
	if _, err := g9Client(srv.URL).PlaceOrder("regular", o); err != nil {
		t.Fatalf("G9/P1-189: market place failed: %v", err)
	}
	if strings.Contains(body, `"price"`) {
		t.Fatalf("G9/P1-189: MARKET wire body must omit price, got %s", body)
	}
	o.OrderType = "LMT"
	o.Price = "150.50"
	if _, err := g9Client(srv.URL).PlaceOrder("regular", o); err != nil {
		t.Fatalf("G9/P1-189: limit place failed: %v", err)
	}
	if !strings.Contains(body, `"price":"150.50"`) {
		t.Fatalf("G9/P1-189: LIMIT wire body must carry price, got %s", body)
	}
}

// G9 guard: path-hostile variety/orderID rejected before URL build (P1-191/193).
func TestG9PathHostileIDsRejected(t *testing.T) {
	c := g9Client("http://127.0.0.1:1")
	o := g9ValidOrder()
	if _, err := c.ModifyOrder("reg/ular", "1", o); err == nil {
		t.Fatal("G9/P1-191: slash variety must fail fast")
	}
	if err := c.CancelOrder("regular", "a?b"); err == nil {
		t.Fatal("G9/P1-191/193: hostile orderID must fail fast")
	}
	if _, err := c.GetOrder(""); err == nil {
		t.Fatal("G9/P1-191: empty orderID must fail fast")
	}
}

// G9 guard: broker rejection preserves code+message on every order path (P1-192).
func TestG9RejectionsPreserveCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/user/orders") && r.Method == "DELETE":
			w.Write([]byte(`{"status":"error","errorCode":"E_X","message":"nope","data":null}`))
		case strings.HasSuffix(r.URL.Path, "/user/orders"):
			w.Write([]byte(`{"status":"error","errorCode":"E_X","message":"nope","data":null}`))
		case strings.HasSuffix(r.URL.Path, "/user/trades"):
			w.Write([]byte(`{"status":"error","errorCode":"E_X","message":"nope","data":null}`))
		case r.Method == "DELETE":
			w.Write([]byte(`{"status":"error","errorCode":"E_X","message":"nope"}`))
		default:
			w.Write([]byte(`{"status":"error","errorCode":"E_X","message":"nope","data":{}}`))
		}
	}))
	defer srv.Close()
	c := g9Client(srv.URL)
	o := g9ValidOrder()
	checks := []struct {
		name string
		call func() error
	}{
		{"place", func() error { _, err := c.PlaceOrder("regular", o); return err }},
		{"modify", func() error { _, err := c.ModifyOrder("regular", "1", o); return err }},
		{"cancel", func() error { return c.CancelOrder("regular", "1") }},
		{"get", func() error { _, err := c.GetOrder("1"); return err }},
		{"book", func() error { _, err := c.GetOrderBook(); return err }},
		{"trade", func() error { _, err := c.GetTradeBook(); return err }},
		{"cancelall", func() error { return c.CancelAllOrders() }},
	}
	for _, tc := range checks {
		err := tc.call()
		if err == nil || !strings.Contains(err.Error(), "E_X") || !strings.Contains(err.Error(), "nope") {
			t.Fatalf("G9/P1-192/%s: must preserve code+message, got %v", tc.name, err)
		}
	}
}

// G9 guard: single-order fetch uses canonical OrderDetails (P1-190) —
// Validity-family fields survive GetOrder.
func TestG9GetOrderKeepsCanonicalFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":[{"id":"7","validity":"DAY","orderSource":"API","leavesQuantity":"3"}]}`))
	}))
	defer srv.Close()
	got, err := g9Client(srv.URL).GetOrder("7")
	if err != nil {
		t.Fatalf("G9/P1-190: get failed: %v", err)
	}
	if got.Data[0].Validity != "DAY" || got.Data[0].OrderSource != "API" || got.Data[0].LeavesQuantity != "3" {
		t.Fatalf("G9/P1-190: canonical fields lost: %+v", got.Data[0])
	}
}

// G9 guard: data:null book/trade success returns empty non-nil (P1-190-adjacent).
func TestG9NullBooksReturnEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":null}`))
	}))
	defer srv.Close()
	c := g9Client(srv.URL)
	book, err := c.GetOrderBook()
	if err != nil || book == nil {
		t.Fatalf("G9: null book must be empty non-nil, got %v %v", book, err)
	}
	trades, err := c.GetTradeBook()
	if err != nil || trades == nil {
		t.Fatalf("G9: null trades must be empty non-nil, got %v %v", trades, err)
	}
	var _ = json.Marshal
}
