package main

// Test helper: decode captured proto output into its logical records so the
// NDJSON-era assertions (countEvents/countState/lastLineEvent) can be
// expressed against proto frames. Mirrors what the Java side does with
// ProtoFrameReader.

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"google.golang.org/protobuf/proto"
	"testing"

	"github.com/trading/arrow-bridge/marketdata"
)

// protoEvents returns every TickEvent (as JSON-ish key=value) and ControlRecord
// decoded from the captured proto bytes, in stream order. Each control record
// is rendered as `bridge_event|slot_state|slot=hft-0|state=ACTIVE|event=...`
// style tokens so existing substring assertions keep working with minimal
// churn; each tick is rendered with its token and price so
// `"token":1000`-style checks become `token=1000`.
func protoRecords(t *testing.T, out string) (ticks []string, controls []string) {
	t.Helper()
	b := []byte(out)
	for len(b) > 0 {
		if len(b) < 4 {
			break
		}
		n := int(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24)
		b = b[4:]
		if n > len(b) {
			break
		}
		f := &marketdata.TransportFrame{}
		if err := proto.Unmarshal(b[:n], f); err != nil {
			break
		}
		b = b[n:]
		switch p := f.Payload.(type) {
		case *marketdata.TransportFrame_MarketBatch:
			for _, ev := range p.MarketBatch.Events {
				ticks = append(ticks, "token="+itoa(int64(ev.Token))+" ltp="+itoa(ev.LtpPaise)+" mode="+ev.Mode+" feed="+ev.Feed+" slot="+ev.SlotId)
			}
		case *marketdata.TransportFrame_Control:
			c := p.Control
			controls = append(controls, "bridge_event|"+c.Event+"|slot="+c.SlotId+"|state="+c.State+"|epoch="+itoa(int64(c.ConnectionEpoch)))
		}
	}
	return ticks, controls
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [12]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func containsAny(haystack []string, needles ...string) bool {
	for _, h := range haystack {
		for _, n := range needles {
			if strings.Contains(h, n) {
				return true
			}
		}
	}
	return false
}

func countContaining(haystack []string, needle string) int {
	n := 0
	for _, h := range haystack {
		if strings.Contains(h, needle) {
			n++
		}
	}
	return n
}

// eventsAsStrings renders each bridge_event control record as a string like
// "event=subscription_ack state=ACTIVE slot=hft-0" for substring asserts.
func eventsAsStrings(t *testing.T, out string) []string {
	t.Helper()
	var strs []string
	for _, e := range eventsFrom(t, out) {
		strs = append(strs,
			"event="+asStr(e["event"])+" state="+asStr(e["state"])+" slot="+asStr(e["slot_id"]))
	}
	return strs
}

func asStr(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// tickMapsFrom decodes the captured proto stream into tick field maps with
// the same keys the old NDJSON tick lines had (feed/mode/token/ltp_paise/
// .../raw_payload/payload_hash/bid_px/ask_px), so golden-corpus comparisons
// keep working against the .golden data files.
func tickMapsFrom(t *testing.T, out string) []map[string]any {
	t.Helper()
	var ticks []map[string]any
	for _, b := range splitFrames([]byte(out)) {
		f := &marketdata.TransportFrame{}
		if err := proto.Unmarshal(b, f); err != nil {
			continue
		}
		mb, ok := f.Payload.(*marketdata.TransportFrame_MarketBatch)
		if !ok {
			continue
		}
		for _, ev := range mb.MarketBatch.GetEvents() {
			bidPx := ev.GetBidPx()
			askPx := ev.GetAskPx()
			m := map[string]any{
				"feed":             ev.GetFeed(),
				"mode":             ev.GetMode(),
				"token":            ev.GetToken(),
				"ltp_paise":        ev.GetLtpPaise(),
				"close_paise":      ev.GetClosePaise(),
				"open_paise":       ev.GetOpenPaise(),
				"high_paise":       ev.GetHighPaise(),
				"low_paise":        ev.GetLowPaise(),
				"vwap_paise":       ev.GetVwapPaise(),
				"ltq":              ev.GetLtq(),
				"volume":           ev.GetVolume(),
				"total_buy_qty":    ev.GetTotalBuyQty(),
				"total_sell_qty":   ev.GetTotalSellQty(),
				"ts_ms":            ev.GetTsMs(),
				"bid_px":           bidPx,
				"ask_px":           askPx,
				"raw_payload":      base64.StdEncoding.EncodeToString(ev.GetRawPayload()),
				"payload_hash":     hex.EncodeToString(ev.GetPayloadHash()),
			}
			ticks = append(ticks, m)
		}
	}
	return ticks
}
