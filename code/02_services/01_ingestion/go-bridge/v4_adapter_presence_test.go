package main

import (
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// v4 presence guard, adapter level: "not reported" must reach the row as absent
// (nil), never as a zero value. imbalance_qty 0 during the closing auction means a
// balanced book, so a tick with no CAS frame must leave the pointers nil or a
// downstream reader would consume a fabricated balanced-book observation.
func TestStandardFeedOmitsCASTrioWithoutTheTrailer(t *testing.T) {
	tick := arrow.MarketTick{Token: 1, Mode: arrow.StreamModeFull, HasCAS: false,
		ImbalanceQty: 0, IndicativeClose: 0, RefPrice: 0}
	out := toHFTFullTick(tick)
	if out.ImbalanceQty != nil || out.IndicativeClose != nil || out.RefPrice != nil {
		t.Fatalf("no CAS frame: want a nil trio, got %v/%v/%v",
			out.ImbalanceQty, out.IndicativeClose, out.RefPrice)
	}
	tick.HasCAS = true
	tick.ImbalanceQty, tick.IndicativeClose, tick.RefPrice = 1234, 132900, 131700
	out = toHFTFullTick(tick)
	if out.ImbalanceQty == nil || *out.ImbalanceQty != 1234 {
		t.Fatalf("with a CAS frame: imbalance = %v, want 1234", out.ImbalanceQty)
	}
	if out.IndicativeClose == nil || *out.IndicativeClose != 132900 ||
		out.RefPrice == nil || *out.RefPrice != 131700 {
		t.Fatalf("with a CAS frame: indicative/ref = %v/%v, want 132900/131700",
			out.IndicativeClose, out.RefPrice)
	}
}
