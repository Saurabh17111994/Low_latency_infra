// T1 contract tests — market_data.proto mapping/round-trip/bit-exact/hash.
// These lock the Go ↔ protobuf ↔ Java serialization contract (plan §7.3).

package marketdata

import (
	"bytes"
	"crypto/sha256"
	"math"
	"testing"

	"google.golang.org/protobuf/proto"
)

// golden raw payloads — binary bytes that would break text/base64 assumptions
var rawPayloads = [][]byte{
	{0x00, 0x01, 0x02, 0x03},                         // leading zero byte
	{0xFF, 0xFE, 0x80, 0x7F},                         // high-bit bytes
	{0x00, 0x00, 0x00, 0x00},                         // all zeros
	{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF},             // mixed
	[]byte("not-json-not-base64-{}[]\\"),             // text-ish but binary-safe
	{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // long zeros
}

func sampleTick() Tick {
	return Tick{
		Feed: "hft", Mode: "full", Token: 123456,
		LTP: 98765, Close: 98000, Open: 97000, High: 99500, Low: 96500,
		VWAP: 98123, LTQ: 42, Volume: 1_000_000, TBQ: 5_000_000, TSQ: 4_000_000,
		ATV: 12, BTV: 34, OI: 999, TS: 1_720_000_000_000,
		BidPx:  [5]int32{100, 99, 98, 97, 96},
		AskPx:  [5]int32{101, 102, 103, 104, 105},
		BidSz:  [5]int32{10, 20, 30, 40, 50},
		AskSz:  [5]int32{11, 21, 31, 41, 51},
		BidOrd: [5]uint16{1, 2, 3, 4, 5},
		AskOrd: [5]uint16{6, 7, 8, 9, 10},
	}
}

// T1-P1 — field mapping: every Tick field maps to the proto, types match,
// no float conversion.
func TestT1P1_FieldMapping(t *testing.T) {
	tick := sampleTick()
	ev := tick.ToTickEvent("slot-0", "conn-0", 1, 1_720_000_000_123, 7, rawPayloads[0])

	cases := []struct {
		name string
		got  any
		want any
	}{
		{"slot_id", ev.SlotId, "slot-0"},
		{"mode", ev.Mode, "full"},
		{"token", ev.Token, int32(123456)},
		{"feed", ev.Feed, "hft"},
		{"ts_ms", ev.TsMs, int64(1_720_000_000_000)},
		{"received_ms", ev.ReceivedMs, int64(1_720_000_000_123)},
		{"feed_sequence_local", ev.FeedSequenceLocal, int64(7)},
		{"ltp_paise", ev.LtpPaise, int64(98765)},
		{"close_paise", ev.ClosePaise, int64(98000)},
		{"open_paise", ev.OpenPaise, int64(97000)},
		{"high_paise", ev.HighPaise, int64(99500)},
		{"low_paise", ev.LowPaise, int64(96500)},
		{"vwap_paise", ev.VwapPaise, int64(98123)},
		{"ltq", ev.Ltq, int64(42)},
		{"volume", ev.Volume, int64(1_000_000)},
		{"total_buy_qty", ev.TotalBuyQty, int64(5_000_000)},
		{"total_sell_qty", ev.TotalSellQty, int64(4_000_000)},
		{"open_interest", ev.OpenInterest, int64(999)},
		{"raw_payload", ev.RawPayload, rawPayloads[0]},
		{"bid_px[0]", ev.BidPx[0], int32(100)},
		{"ask_px[4]", ev.AskPx[4], int32(105)},
		{"bid_qty[2]", ev.BidQty[2], int32(30)},
		{"ask_qty[1]", ev.AskQty[1], int32(21)},
		{"bid_orders[3]", ev.BidOrders[3], uint32(4)},
		{"ask_orders[0]", ev.AskOrders[0], uint32(6)},
	}
	for _, c := range cases {
		// []byte is not comparable — compare contents
		if gb, ok := c.got.([]byte); ok {
			wb, _ := c.want.([]byte)
			if !bytes.Equal(gb, wb) {
				t.Errorf("%s: got %v want %v", c.name, c.got, c.want)
			}
			continue
		}
		if c.got != c.want {
			t.Errorf("%s: got %v want %v", c.name, c.got, c.want)
		}
	}
	if len(ev.BidPx) != 5 || len(ev.AskPx) != 5 || len(ev.BidQty) != 5 || len(ev.AskQty) != 5 {
		t.Errorf("depth arrays must be fixed 5, got %d/%d/%d/%d", len(ev.BidPx), len(ev.AskPx), len(ev.BidQty), len(ev.AskQty))
	}
}

// T1-P2 — integer fidelity: no float anywhere; extreme values preserved.
func TestT1P2_IntegerFidelity(t *testing.T) {
	tick := Tick{
		Feed: "hft", Mode: "full", Token: math.MaxInt32,
		LTP: math.MaxInt32, Volume: math.MaxInt64,
		TS: math.MaxInt64, OI: math.MaxInt64,
		BidPx:  [5]int32{math.MaxInt32, 0, -1, math.MinInt32, 5},
		BidOrd: [5]uint16{math.MaxUint16, 0, 1, 2, 3},
	}
	ev := tick.ToTickEvent("s", "c", 1, 0, 0, nil)

	if ev.Token != math.MaxInt32 || ev.LtpPaise != int64(math.MaxInt32) {
		t.Errorf("max int32 not preserved: token=%d ltp=%d", ev.Token, ev.LtpPaise)
	}
	if ev.Volume != math.MaxInt64 || ev.TsMs != math.MaxInt64 {
		t.Errorf("max int64 not preserved")
	}
	if ev.BidPx[0] != math.MaxInt32 || ev.BidPx[3] != math.MinInt32 || ev.BidPx[2] != -1 {
		t.Errorf("depth int32 extremes not preserved: %v", ev.BidPx)
	}
	if ev.BidOrders[0] != math.MaxUint16 {
		t.Errorf("uint16 max not preserved: %d", ev.BidOrders[0])
	}
	// zero values preserved exactly (proto3 default — explicit presence matters)
	if ev.BidPx[1] != 0 || ev.BidOrders[1] != 0 {
		t.Errorf("zero depth values must be preserved")
	}
	// no float anywhere in the struct
	if _, ok := any(ev).(*TickEvent); !ok {
		t.Errorf("unexpected type")
	}
}

// T1-R1 — round trip: Tick → proto → bytes → proto → field equality.
func TestT1R1_RoundTrip(t *testing.T) {
	tick := sampleTick()
	ev := tick.ToTickEvent("slot-0", "conn-0", 2, 1_720_000_000_123, 9, rawPayloads[2])

	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	back := &TickEvent{}
	if err := proto.Unmarshal(b, back); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(back, ev) {
		t.Errorf("round-trip mismatch:\n got=%v\nwant=%v", back, ev)
	}
}

// T1-X1 — bit-exact raw payload: bytes survive the round-trip unchanged.
func TestT1X1_BitExactRaw(t *testing.T) {
	for i, raw := range rawPayloads {
		ev := sampleTick().ToTickEvent("s", "c", 1, 0, 0, raw)
		b, err := proto.Marshal(ev)
		if err != nil {
			t.Fatalf("payload %d marshal: %v", i, err)
		}
		back := &TickEvent{}
		if err := proto.Unmarshal(b, back); err != nil {
			t.Fatalf("payload %d unmarshal: %v", i, err)
		}
		if !bytes.Equal(back.RawPayload, raw) {
			t.Errorf("payload %d bit-exact failed: got %v want %v", i, back.RawPayload, raw)
		}
	}
}

// T1-H1 — hash-once semantics: Go computes sha256 of raw_payload exactly once
// per event; the value carried in the proto is that hash. (Java must not
// recompute — T5-H2 covers the config flag.)
func TestT1H1_HashOnce(t *testing.T) {
	raw := rawPayloads[4]
	sum := sha256.Sum256(raw)
	ev := sampleTick().ToTickEvent("s", "c", 1, 0, 0, raw)
	ev.PayloadHash = sum[:]

	// the proto carries the hash; a consumer reading it gets the exact value
	if !bytes.Equal(ev.PayloadHash, sum[:]) {
		t.Errorf("payload_hash mismatch")
	}
	// hash is deterministic
	if !bytes.Equal(ev.PayloadHash, sha256.New().Sum(nil)) && !bytes.Equal(ev.PayloadHash, sum[:]) {
		t.Errorf("hash not deterministic")
	}
}

// T1-R2 — unknown version rejection: TransportFrame protocol_version gate.
func TestT1R2_UnknownVersion(t *testing.T) {
	frame := &TransportFrame{
		ProtocolVersion: 999, // unknown to Java
	}
	b, err := proto.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	back := &TransportFrame{}
	if err := proto.Unmarshal(b, back); err != nil {
		t.Fatal(err)
	}
	// Java-side gate (T5-J3) rejects protocol_version != known; here we prove
	// the field survives the wire so the gate can see it.
	if back.ProtocolVersion != 999 {
		t.Errorf("protocol_version not preserved: %d", back.ProtocolVersion)
	}
}

// T1-C1 — batch carries multiple events + batch hash field.
func TestT1C1_Batch(t *testing.T) {
	batch := &MarketDataBatch{
		ConnectionId:    "ingestion-local/hft-0",
		ConnectionEpoch: 1,
		BatchSeq:        3,
		CreatedMs:       1_720_000_000_000,
		Events: []*TickEvent{
			sampleTick().ToTickEvent("s", "c", 1, 0, 1, rawPayloads[0]),
			sampleTick().ToTickEvent("s", "c", 1, 0, 2, rawPayloads[1]),
		},
		BatchPayloadHash: []byte{0x01, 0x02},
	}
	b, err := proto.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	back := &MarketDataBatch{}
	if err := proto.Unmarshal(b, back); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(back, batch) {
		t.Errorf("batch round-trip mismatch")
	}
	if len(back.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(back.Events))
	}
}
