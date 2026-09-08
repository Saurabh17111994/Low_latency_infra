package main

import (
	"errors"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// G2 guard: zero-value Client must fail closed, never panic (P1-169).
func TestG2NilHTTPClientFailsClosed(t *testing.T) {
	var zero arrow.Client
	if _, err := zero.GetPositions(); err == nil {
		t.Fatal("G2/P1-169: zero-value Client.GetPositions must error, not panic-or-succeed")
	}
}

// G2 guard: non-2xx errors are typed *StatusError so callers can
// errors.As-switch on code (401 re-auth vs 429 vs 5xx) (P1-168).
func TestG2StatusErrorIsSwitchable(t *testing.T) {
	se := &arrow.StatusError{StatusCode: 429, Body: "too many requests"}
	var as *arrow.StatusError
	if !errors.As(se, &as) || as.StatusCode != 429 {
		t.Fatal("G2/P1-168: StatusError must be errors.As-switchable with StatusCode")
	}
}
