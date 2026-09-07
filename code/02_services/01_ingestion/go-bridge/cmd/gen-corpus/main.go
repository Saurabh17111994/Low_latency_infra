// gen-corpus generates the golden HFT packet corpus for the ingestion
// pipeline. It emits the same wire frames the fake broker serves and the
// real Arrow HFT socket is expected to send, in the canonical byte layouts
// the Go bridge decodes (see third_party/go-arrow/arrow/hft_stream.go):
//
//	response: 540-byte subscribe/unsubscribe ack (pkt_type 99, uint32 size
//	          at 0..4, pkt_type at 4, error code 6..22, request_type 534,
//	          mode 535, success/error counts 536..540)
//	full:     196-byte full-depth tick (pkt_type 2, int16 size at 0..2,
//	          pkt_type at 2, fields per parseHFTFull)
//	ltp:      40-byte LTP tick (pkt_type 1, int16 size at 0..2, pkt_type
//	          at 2, fields per parseHFTLTP)
//	unknown:  64-byte frame with a pkt_type the decoder cannot route
//	          (must be rejected by hftPacketMeta / dispatched as decode
//	          error)
//
// Output: one raw frame file and one .golden NDJSON-format file per frame
// under go-bridge/testdata/golden/. The .golden files are the authoritative
// decode contract for the deterministic tick fields — the Go golden tests
// decode the raw frames and compare the tick-field subset (feed/mode/token,
// prices/qtys/ts_ms, ladders, Base64 raw_payload + SHA-256 payload_hash),
// and the Java PayloadHashValidator tests consume the same Base64
// raw_payload + SHA-256 payload_hash pair. The envelope fields
// (connection_id/epoch/slot_id/feed_sequence_local/received_ts_ms) are
// illustrative-only and never asserted (P1-147).
//
// Frames are raw uncompressed bytes: raw_payload is base64(raw) (P1-148).
// The generator is deterministic: all tick-field timestamps are fixed, so
// regeneration is reproducible.
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	// Mirrors faketool/main.go + third_party/go-arrow/arrow/hft_stream.go
	// (hftSizeResponse/hftSizeFull/hftSizeLTP, hftPktResponse/hftPktFull/
	// hftPktLTP). The vendored parser is unexported and frozen (Wave 9), so
	// the constants cannot be imported — P1-146: checkFrame below
	// re-decodes every built frame at the parser's documented offsets
	// before anything is written, so builder/parser drift fails fast.
	hftSizeResponse = 540
	hftSizeFull     = 196
	hftSizeLTP      = 40
	hftPktResponse  = 99
	hftPktFull      = 2
	hftPktLTP       = 1
	// The fake broker subscribes a single token present in the approved CSV.
	token = int32(757614)
)

// golden is the NDJSON-format record the Java pipeline consumes. It must
// match the Go bridge's EmitTick output shape (transport.go EmitTick ->
// Batcher.Add proto TickEvent; the NDJSON pipe was removed 2026-08-29) for
// the deterministic tick fields, so the .golden files are the decode
// contract for that subset — P1-147: the envelope fields below are
// illustrative-only (the bridge stamps received_ts_ms with time.Now() and
// assigns epoch/seq per run, and the golden tests assert only
// feed/mode/token/prices/qty/ts_ms/ladders/raw_payload/payload_hash).
type golden struct {
	Feed   string `json:"feed"`
	Mode   string `json:"mode"`
	Token  int32  `json:"token"`
	LTP    int32  `json:"ltp_paise"`
	Close  int32  `json:"close_paise,omitempty"`
	Open   int32  `json:"open_paise,omitempty"`
	High   int32  `json:"high_paise,omitempty"`
	Low    int32  `json:"low_paise,omitempty"`
	VWAP   int32  `json:"vwap_paise,omitempty"`
	LTQ    int32  `json:"ltq,omitempty"`
	Volume int64  `json:"volume,omitempty"`
	TBQ    int64  `json:"total_buy_qty,omitempty"`
	TSQ    int64  `json:"total_sell_qty,omitempty"`
	ATV    uint32 `json:"atv,omitempty"`
	BTV    uint32 `json:"btv,omitempty"`
	// P1-022: the wire carries OI and EmitTick emits open_interest — the
	// golden must certify it. Scalar omitempty is honest (0 omits).
	OI              int64     `json:"open_interest,omitempty"`
	TS              int64     `json:"ts_ms"`
	BidPx           [5]int32  `json:"bid_px"`
	AskPx           [5]int32  `json:"ask_px"`
	BidSize         [5]int32  `json:"bid_qty"`
	AskSize         [5]int32  `json:"ask_qty"`
	BidOrd          [5]uint16 `json:"bid_orders"`
	AskOrd          [5]uint16 `json:"ask_orders"`
	RecordType      string    `json:"record_type"`
	ContractVersion int       `json:"contract_version"`
	// P1-147: illustrative-only envelope — stamped per run (received_ts_ms =
	// time.Now(), epoch/seq assigned by Batcher.Add), never asserted; the
	// golden tests compare only the tick-field subset + raw_payload/hash.
	ConnectionID      string `json:"connection_id"`
	ConnectionEpoch   uint64 `json:"connection_epoch"`
	SlotID            string `json:"slot_id"`
	FeedSequenceLocal uint64 `json:"feed_sequence_local"`
	ReceivedTsMs      int64  `json:"received_ts_ms"`
	RawPayload        string `json:"raw_payload"`
	PayloadHash       string `json:"payload_hash"`
}

func buildFullFrame() []byte {
	f := make([]byte, hftSizeFull)
	binary.LittleEndian.PutUint16(f[0:2], hftSizeFull)
	f[2] = hftPktFull
	f[3] = 0 // exch_seg NSE cash
	binary.LittleEndian.PutUint32(f[4:8], uint32(token))
	binary.LittleEndian.PutUint32(f[8:12], 15050)          // ltp
	binary.LittleEndian.PutUint32(f[12:16], 10)            // ltq
	binary.LittleEndian.PutUint32(f[16:20], 15040)         // vwap
	binary.LittleEndian.PutUint32(f[20:24], 14900)         // open
	binary.LittleEndian.PutUint32(f[24:28], 15100)         // high
	binary.LittleEndian.PutUint32(f[28:32], 15000)         // close
	binary.LittleEndian.PutUint32(f[32:36], 14850)         // low
	binary.LittleEndian.PutUint32(f[36:40], 1_752_540_000) // ltt (s epoch, int32 — unused by bridge)
	binary.LittleEndian.PutUint32(f[40:44], 14850)         // dpr_l
	binary.LittleEndian.PutUint32(f[44:48], 15100)         // dpr_h
	binary.LittleEndian.PutUint64(f[48:56], 1_000)         // tbuys
	binary.LittleEndian.PutUint64(f[56:64], 2_000)         // tsells
	binary.LittleEndian.PutUint64(f[64:72], 100_000)       // volume
	// bid/ask px, size, orders: 5 x int32, 5 x int32, 5 x int32, 5 x int32,
	// 5 x uint16, 5 x uint16
	for i := 0; i < 5; i++ {
		off := 72 + i*4
		binary.LittleEndian.PutUint32(f[off:off+4], uint32(15000-int32(i*10)))
		off = 92 + i*4
		binary.LittleEndian.PutUint32(f[off:off+4], uint32(15010+int32(i*10)))
		off = 112 + i*4
		binary.LittleEndian.PutUint32(f[off:off+4], uint32(100*(i+1)))
		off = 132 + i*4
		binary.LittleEndian.PutUint32(f[off:off+4], uint32(50*(i+1)))
		off = 152 + i*2
		binary.LittleEndian.PutUint16(f[off:off+2], uint16(3+i))
		off = 162 + i*2
		binary.LittleEndian.PutUint16(f[off:off+2], uint16(2+i))
	}
	binary.LittleEndian.PutUint64(f[172:180], 500_000)                   // oi
	binary.LittleEndian.PutUint64(f[180:188], 1_752_540_000_000_000_000) // ts (ns; bridge TS = ts/1e6 ms)
	binary.LittleEndian.PutUint32(f[188:192], 1_000)                     // atv
	binary.LittleEndian.PutUint32(f[192:196], 2_000)                     // btv
	return f
}

func buildLTPFrame() []byte {
	f := make([]byte, hftSizeLTP)
	binary.LittleEndian.PutUint16(f[0:2], hftSizeLTP)
	f[2] = hftPktLTP
	f[3] = 0
	binary.LittleEndian.PutUint32(f[4:8], uint32(token))
	binary.LittleEndian.PutUint32(f[8:12], 15050)
	binary.LittleEndian.PutUint32(f[12:16], 15040)
	binary.LittleEndian.PutUint64(f[16:24], 100_000)
	binary.LittleEndian.PutUint64(f[24:32], 1_752_540_000_000_000) // ltt (µs; bridge TS = ltt/1e6 ms)
	binary.LittleEndian.PutUint32(f[32:36], 1_000)
	binary.LittleEndian.PutUint32(f[36:40], 2_000)
	return f
}

func buildResponseFrame() []byte {
	f := make([]byte, hftSizeResponse)
	binary.LittleEndian.PutUint32(f[0:4], hftSizeResponse)
	f[4] = hftPktResponse
	f[5] = 0
	copy(f[6:22], "SUCCESS")
	// error_msg is the 512 bytes 22..534, zero-filled here
	f[534] = 0                                   // request_type: sub
	f[535] = 1                                   // mode: full
	binary.LittleEndian.PutUint16(f[536:538], 1) // success count
	binary.LittleEndian.PutUint16(f[538:540], 0) // error count
	return f
}

func buildUnknownFrame() []byte {
	// A frame the decoder cannot route: valid length prefix but pkt_type 7
	// (not LTP/full/response). dispatchHFTPayload must reject it as an
	// unknown packet (decode error) — never a tick.
	f := make([]byte, 64)
	binary.LittleEndian.PutUint16(f[0:2], 64)
	f[2] = 7
	binary.LittleEndian.PutUint32(f[4:8], uint32(token))
	return f
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// P1-146 self-check: re-decode a built frame at the offsets the vendored
// parser documents (third_party/go-arrow/arrow/hft_stream.go: hftPacketMeta,
// parseHFTLTP/parseHFTFull/parseHFTResponse) and report the mismatch with
// context. The parser itself is unexported and vendored code is frozen, so
// this mirror-decode is the drift tripwire for the hand-duplicated sizes,
// pkt types, offsets and timestamp units.
func checkFrame(name string, raw []byte) error {
	switch name {
	case "full-tick":
		if len(raw) != hftSizeFull {
			return fmt.Errorf("len=%d want hftSizeFull=%d", len(raw), hftSizeFull)
		}
		if int(binary.LittleEndian.Uint16(raw[0:2])) != hftSizeFull || raw[2] != hftPktFull {
			return fmt.Errorf("header size=%d pkt=%d want %d/%d", binary.LittleEndian.Uint16(raw[0:2]), raw[2], hftSizeFull, hftPktFull)
		}
		if got := int32(binary.LittleEndian.Uint32(raw[4:8])); got != token {
			return fmt.Errorf("token=%d want %d", got, token)
		}
		if got := binary.LittleEndian.Uint32(raw[8:12]); got != 15050 {
			return fmt.Errorf("ltp=%d want 15050", got)
		}
		if got := binary.LittleEndian.Uint64(raw[64:72]); got != 100_000 {
			return fmt.Errorf("volume=%d want 100000", got)
		}
		if got := binary.LittleEndian.Uint64(raw[172:180]); got != 500_000 {
			return fmt.Errorf("oi=%d want 500000", got)
		}
		if got := binary.LittleEndian.Uint64(raw[180:188]); got != 1_752_540_000_000_000_000 {
			return fmt.Errorf("ts=%d want 1752540000000000000 (ns)", got)
		}
	case "ltp-tick":
		if len(raw) != hftSizeLTP {
			return fmt.Errorf("len=%d want hftSizeLTP=%d", len(raw), hftSizeLTP)
		}
		if int(binary.LittleEndian.Uint16(raw[0:2])) != hftSizeLTP || raw[2] != hftPktLTP {
			return fmt.Errorf("header size=%d pkt=%d want %d/%d", binary.LittleEndian.Uint16(raw[0:2]), raw[2], hftSizeLTP, hftPktLTP)
		}
		if got := int32(binary.LittleEndian.Uint32(raw[4:8])); got != token {
			return fmt.Errorf("token=%d want %d", got, token)
		}
		if got := binary.LittleEndian.Uint64(raw[24:32]); got != 1_752_540_000_000_000 {
			return fmt.Errorf("ltt=%d want 1752540000000000 (µs)", got)
		}
	case "response":
		if len(raw) != hftSizeResponse {
			return fmt.Errorf("len=%d want hftSizeResponse=%d", len(raw), hftSizeResponse)
		}
		if got := binary.LittleEndian.Uint32(raw[0:4]); got != hftSizeResponse {
			return fmt.Errorf("size=%d want %d", got, hftSizeResponse)
		}
		if raw[4] != hftPktResponse {
			return fmt.Errorf("pkt=%d want %d", raw[4], hftPktResponse)
		}
		if got := string(raw[6:13]); got != "SUCCESS" {
			return fmt.Errorf("error_code=%q want SUCCESS", got)
		}
		if binary.LittleEndian.Uint16(raw[536:538]) != 1 || binary.LittleEndian.Uint16(raw[538:540]) != 0 {
			return fmt.Errorf("success/error counts wrong")
		}
	case "unknown-packet":
		if len(raw) != 64 || int(binary.LittleEndian.Uint16(raw[0:2])) != 64 {
			return fmt.Errorf("unknown frame must stay a 64-byte frame")
		}
		if raw[2] == hftPktLTP || raw[2] == hftPktFull || raw[2] == hftPktResponse {
			return fmt.Errorf("unknown frame pkt=%d must stay unroutable", raw[2])
		}
	default:
		return fmt.Errorf("no self-check for frame %q", name)
	}
	return nil
}

func main() {
	outDir := flag.String("out", "testdata/golden", "output directory")
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir:", err)
		os.Exit(1)
	}

	// fixed received_ts_ms for determinism (2026-07-14 09:15:00 IST = 2026-07-14T03:45:00Z)
	received := time.Date(2026, 7, 14, 3, 45, 0, 0, time.UTC)
	receivedMs := received.UnixMilli()

	frames := []struct {
		name string
		raw  []byte
		gold *golden
	}{
		{
			name: "full-tick",
			raw:  buildFullFrame(),
			gold: &golden{
				Feed: "hft", Mode: "full", Token: token,
				LTP: 15050, Close: 15000, Open: 14900, High: 15100, Low: 14850,
				VWAP: 15040, LTQ: 10, Volume: 100_000, TBQ: 1_000, TSQ: 2_000,
				ATV: 1_000, BTV: 2_000, OI: 500_000, TS: 1_752_540_000_000,
				BidPx:      [5]int32{15000, 14990, 14980, 14970, 14960},
				AskPx:      [5]int32{15010, 15020, 15030, 15040, 15050},
				BidSize:    [5]int32{100, 200, 300, 400, 500},
				AskSize:    [5]int32{50, 100, 150, 200, 250},
				BidOrd:     [5]uint16{3, 4, 5, 6, 7},
				AskOrd:     [5]uint16{2, 3, 4, 5, 6},
				RecordType: "tick", ContractVersion: 2,
				ConnectionID: "ingestion-local/hft-0", ConnectionEpoch: 1,
				SlotID: "hft-0", FeedSequenceLocal: 1,
				ReceivedTsMs: receivedMs,
			},
		},
		{
			name: "ltp-tick",
			raw:  buildLTPFrame(),
			gold: &golden{
				Feed: "hft", Mode: "ltpc", Token: token,
				LTP: 15050, VWAP: 15040, Volume: 100_000,
				// P1-023: ltt is µs — /1e3 yields ms (was /1e6 = seconds,
				// 1000x below full ticks). 1_752_540_000_000_000 µs → ms.
				ATV: 1_000, BTV: 2_000, TS: 1_752_540_000_000,
				RecordType: "tick", ContractVersion: 2,
				ConnectionID: "ingestion-local/hft-0", ConnectionEpoch: 1,
				SlotID: "hft-0", FeedSequenceLocal: 2,
				ReceivedTsMs: receivedMs,
			},
		},
		{
			name: "response",
			raw:  buildResponseFrame(),
			gold: nil, // wire-only fixture: response frames are consumed by the
			// SDK subscription ack path and never reach EmitTick (main.go
			// routes them to responses <- r only), so no NDJSON record exists.
		},
		{
			name: "unknown-packet",
			raw:  buildUnknownFrame(),
			gold: nil, // not a decodable tick — no golden record
		},
	}

	for _, fr := range frames {
		raw := fr.raw
		// P1-146: decode every built frame at the parser's documented
		// offsets before writing — builder/parser drift fails fast here,
		// not as a poisoned testdata/golden corpus.
		if err := checkFrame(fr.name, raw); err != nil {
			fmt.Fprintf(os.Stderr, "gen-corpus self-check %s: %v\n", fr.name, err)
			os.Exit(1)
		}
		rawPath := filepath.Join(*outDir, fr.name+".frame")
		if err := os.WriteFile(rawPath, raw, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write raw:", err)
			os.Exit(1)
		}
		if fr.gold == nil {
			continue
		}
		g := fr.gold
		g.RawPayload = base64.StdEncoding.EncodeToString(raw)
		g.PayloadHash = sha256Hex(raw)
		b, err := json.Marshal(g)
		if err != nil {
			fmt.Fprintln(os.Stderr, "marshal golden:", err)
			os.Exit(1)
		}
		goldPath := filepath.Join(*outDir, fr.name+".golden")
		if err := os.WriteFile(goldPath, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write golden:", err)
			os.Exit(1)
		}
	}
	fmt.Printf("wrote %d golden frames to %s\n", len(frames), *outDir)
}
