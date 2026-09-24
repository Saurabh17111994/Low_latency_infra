package main

import (
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// The two v4 write-path guards that decide "absent, not zero":
//
//   - HasCAS: whether the closing-auction trailer was actually on the wire, so
//     the CAS columns stay NULL for an ordinary full tick instead of becoming 0.
//   - LTTmsFromSeconds: a last-traded-time that cannot be an epoch degrades to
//     unknown instead of a fabricated 1970 timestamp in the row.
func TestCASPresenceAndDecode(t *testing.T) {
	cases := []struct {
		name    string
		base    int
		trailer int
		wantCAS bool
	}{
		{"current wire (249), no trailer", 249, 0, false},
		{"current wire (249) + CAS", 249, 16, true},
		{"legacy wire (241), no trailer", 241, 0, false},
		{"legacy wire (241) + CAS", 241, 16, true},
	}
	for _, tc := range cases {
		frame := make([]byte, tc.base+tc.trailer)
		for i := range frame {
			frame[i] = byte(i % 251)
		}
		if tc.trailer == 16 {
			// Non-zero trailer: the decoded values then prove the CAS block is
			// read from the tail in big-endian order, not zeroed by default.
			for i := 0; i < 16; i++ {
				frame[tc.base+i] = byte(i + 1)
			}
		}
		tick, err := arrow.ParseMarketTick(frame)
		if err != nil {
			t.Fatalf("%s: parse failed: %v", tc.name, err)
		}
		if tick.HasCAS != tc.wantCAS {
			t.Fatalf("%s: HasCAS = %v, want %v", tc.name, tick.HasCAS, tc.wantCAS)
		}
		if tc.trailer != 16 {
			continue
		}
		const wantImbalance = int64(0x0102030405060708)
		if tick.ImbalanceQty != wantImbalance {
			t.Errorf("%s: ImbalanceQty = %#x, want %#x", tc.name, tick.ImbalanceQty, wantImbalance)
		}
		if tick.IndicativeClose != 0x090A0B0C {
			t.Errorf("%s: IndicativeClose = %#x, want 0x090A0B0C", tc.name, tick.IndicativeClose)
		}
		if tick.RefPrice != 0x0D0E0F10 {
			t.Errorf("%s: RefPrice = %#x, want 0x0D0E0F10", tc.name, tick.RefPrice)
		}
	}
}

func TestLTTmsFromSecondsGate(t *testing.T) {
	cases := []struct {
		sec  int64
		want int64
		ok   bool
	}{
		{0, 0, false},           // absent
		{-1, 0, false},          // nonsense
		{999_999_999, 0, false}, // one second below the epoch floor
		{1_000_000_000, 1_000_000_000_000, true},
		{1_700_000_000, 1_700_000_000_000, true}, // a real 2023 timestamp
	}
	for _, tc := range cases {
		got, ok := arrow.LTTmsFromSeconds(tc.sec)
		if ok != tc.ok || got != tc.want {
			t.Errorf("LTTmsFromSeconds(%d) = (%d, %v), want (%d, %v)",
				tc.sec, got, ok, tc.want, tc.ok)
		}
	}
}
