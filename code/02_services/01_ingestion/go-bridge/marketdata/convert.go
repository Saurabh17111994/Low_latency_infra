// Package marketdata — generated proto + Tick→proto mapping contract (T1).
//
// convert.go holds the ONLY sanctioned translation from the bridge's Tick
// struct to the transport proto. It is a pure function: no I/O, no hashing
// (hashing is the caller's job per Q5 "computed once"), no business logic.
// The mapping is locked by tests T1-P1/T1-P2 (see market_data_test.go).

package marketdata

import "time"

// Tick mirrors the bridge's canonical Tick struct (go-bridge/main.go:21).
// Duplicated here to keep the mapping test self-contained and free of import
// cycles; the bridge converts its real Tick to this shape before marshaling.
type Tick struct {
	Feed   string
	Mode   string
	Token  int32
	LTP    int32
	Close  int32
	Open   int32
	High   int32
	Low    int32
	VWAP   int32
	LTQ    int32
	Volume int64
	TBQ    int64
	TSQ    int64
	ATV    uint32
	BTV    uint32
	OI     int64
	TS     int64 // unix epoch ms
	BidPx  [5]int32
	AskPx  [5]int32
	BidSz  [5]int32
	AskSz  [5]int32
	BidOrd [5]uint16
	AskOrd [5]uint16

	// v4 full-mode fields. LowerLimit/UpperLimit/LTTms are reported by both feeds
	// (0 LTTms = unknown). The rest are feed-specific or per-tick conditional and
	// carry presence so the row can store NULL: see ToTickEvent.
	LowerLimit  int32
	UpperLimit  int32
	LTTms       int64
	ChangeFlag  int32
	OIDayHigh   int64
	OIDayLow    int64
	Imbalance   *int64
	Indicative  *int64
	RefPrice    *int64
	VolumeDelta *int64
}

// Feed names carried on the wire (bridge supervisor.go). Duplicated as literals
// rather than imported to keep this package free of a bridge import cycle; the
// T1 mapping tests lock the behaviour these two strings select.
const (
	feedStandard = "token"
	feedHFT      = "hft"
)

func optI64(v int64) *int64 { return &v }
func optI32(v int32) *int32 { return &v }

// ToTickEvent maps a Tick to a proto TickEvent. Field-by-field, locked by
// T1-P1. Prices stay integer paise — never float.
//
// P1-291: connID and epoch are intentionally ignored — TickEvent carries no
// per-event connection fields; connection identity and epoch are stamped on
// the MarketDataBatch header in Batcher.Add (batch.go). The parameters stay
// so every EmitTick call site keeps passing provenance explicitly (dropping
// them churns callers+tests for zero behavior gain).
func (t Tick) ToTickEvent(slotID string, connID string, epoch uint64, receivedMs, seq int64, raw []byte) *TickEvent {
	ev := &TickEvent{
		SlotId:            slotID,
		Mode:              t.Mode,
		Token:             t.Token,
		Feed:              t.Feed,
		TsMs:              t.TS,
		ReceivedMs:        receivedMs,
		GoReceivedMs:      receivedMs,             // T8: staged-latency provenance (T1 receipt)
		GoEmitMs:          time.Now().UnixMilli(), // T8: batcher Add time (before marshal)
		FeedSequenceLocal: seq,
		LtpPaise:          int64(t.LTP),
		ClosePaise:        int64(t.Close),
		OpenPaise:         int64(t.Open),
		HighPaise:         int64(t.High),
		LowPaise:          int64(t.Low),
		VwapPaise:         int64(t.VWAP),
		Ltq:               int64(t.LTQ),
		Volume:            t.Volume,
		TotalBuyQty:       t.TBQ,
		TotalSellQty:      t.TSQ,
		OpenInterest:      t.OI,
		RawPayload:        raw,
		// Both feeds report these, so they need no presence: a limit of 0 paise is
		// not a value the exchange publishes, and 0 ltt_ms cannot be an epoch.
		LowerLimitPaise: int64(t.LowerLimit),
		UpperLimitPaise: int64(t.UpperLimit),
		LttMs:           t.LTTms,
	}
	// v4 feed-specific presence. Which of these exist is a property of the feed,
	// and this function is the only place that sees both the value and the feed
	// name, so the decision lives here and Java only honours presence. A tick with
	// neither feed name (synthetic corpora) deliberately sets nothing, which keeps
	// existing golden frames byte-identical.
	switch t.Feed {
	case feedStandard:
		ev.ChangeFlag = optI32(t.ChangeFlag)
		ev.OiDayHigh = optI64(t.OIDayHigh)
		ev.OiDayLow = optI64(t.OIDayLow)
	case feedHFT:
		ev.Atv = optI64(int64(t.ATV))
		ev.Btv = optI64(int64(t.BTV))
	}
	// Conditional in both feeds: the closing-auction trio arrives only when the
	// tick carried the trailer, and volume_delta is unknown for a token's first
	// tick in an epoch.
	ev.ImbalanceQty = t.Imbalance
	ev.IndicativeClosePaise = t.Indicative
	ev.RefPricePaise = t.RefPrice
	ev.VolumeDelta = t.VolumeDelta
	// depth arrays — fixed 5 (Q-O2). Zero values preserved exactly.
	ev.BidPx = append([]int32(nil), t.BidPx[:]...)
	ev.AskPx = append([]int32(nil), t.AskPx[:]...)
	ev.BidQty = append([]int32(nil), t.BidSz[:]...)
	ev.AskQty = append([]int32(nil), t.AskSz[:]...)
	for _, v := range t.BidOrd {
		ev.BidOrders = append(ev.BidOrders, uint32(v))
	}
	for _, v := range t.AskOrd {
		ev.AskOrders = append(ev.AskOrders, uint32(v))
	}
	return ev
}
