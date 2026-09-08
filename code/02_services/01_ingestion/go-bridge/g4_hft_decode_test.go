package main

import (
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// G4 guard: CAS-257 (legacy full 241 + 16B trailer) must parse via the
// legacy-full path with trailer fields populated (P1-200).
func TestG4CAS257ParsesLegacyFull(t *testing.T) {
	base := make([]byte, 241)
	for i := range base {
		base[i] = byte(i)
	}
	frame := append(append([]byte{}, base...), make([]byte, 16)...)
	tick, err := arrow.ParseMarketTick(frame)
	if err != nil {
		t.Fatalf("G4/P1-200: CAS-257 must parse, got %v", err)
	}
	if tick.Mode != arrow.StreamModeFull {
		t.Fatalf("G4/P1-200: CAS-257 mode must be full, got %q", tick.Mode)
	}
}

// G4 guard: unknown frame size must fail closed, never a silent zero tick (P1-200).
func TestG4UnknownSizeFailsClosed(t *testing.T) {
	tick, err := arrow.ParseMarketTick(make([]byte, 100))
	if err == nil {
		t.Fatalf("G4/P1-200: size-100 must error, got zero tick %+v", tick)
	}
	if !strings.Contains(err.Error(), "unsupported market tick") {
		t.Fatalf("G4/P1-200: error must name unsupported size, got %v", err)
	}
}

// G4 guard: CAS-265 (current full 249 + trailer) still parses (regression pin).
func TestG4CAS265StillParses(t *testing.T) {
	base := make([]byte, 249)
	for i := range base {
		base[i] = byte(i)
	}
	frame := append(append([]byte{}, base...), make([]byte, 16)...)
	tick, err := arrow.ParseMarketTick(frame)
	if err != nil {
		t.Fatalf("G4/P1-200: CAS-265 must still parse, got %v", err)
	}
	if tick.Mode != arrow.StreamModeFull {
		t.Fatalf("G4/P1-200: CAS-265 mode must be full, got %q", tick.Mode)
	}
}
