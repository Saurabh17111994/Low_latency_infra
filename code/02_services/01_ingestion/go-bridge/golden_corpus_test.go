package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
	"github.com/gorilla/websocket"
)

// loadGoldenFrame reads a raw wire frame fixture from testdata/golden.
func loadGoldenFrame(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "golden", name+".frame"))
	if err != nil {
		t.Fatalf("read golden frame %s: %v", name, err)
	}
	return b
}

// loadGoldenRecord decodes the NDJSON-format .golden file into a map.
func loadGoldenRecord(t *testing.T, name string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "golden", name+".golden"))
	if err != nil {
		t.Fatalf("read golden record %s: %v", name, err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("parse golden record %s: %v", name, err)
	}
	return m
}

// goldenBroker is a wire-format fake broker that serves the golden frames.
// On each subscription it emits: response frame, then LTP frame, then full
// frame — the exact bytes from testdata/golden. The bridge's real path
// (SDK ConnectHFTDataStreamURL + runHFT) decodes them.
type goldenBroker struct {
	server      *httptest.Server
	url         string
	responses   int32
	withUnknown bool
}

func newGoldenBroker(t *testing.T) *goldenBroker {
	return newGoldenBrokerOpts(t, false)
}

func newGoldenBrokerOpts(t *testing.T, withUnknown bool) *goldenBroker {
	t.Helper()
	g := &goldenBroker{withUnknown: withUnknown}
	resp := loadGoldenFrame(t, "response")
	ltp := loadGoldenFrame(t, "ltp-tick")
	full := loadGoldenFrame(t, "full-tick")
	unknown := loadGoldenFrame(t, "unknown-packet")
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	g.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var sub map[string]any
			if json.Unmarshal(payload, &sub) != nil {
				continue
			}
			if code, _ := sub["code"].(string); code != "sub" {
				continue
			}
			g.responses++
			if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(resp)); err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(ltp)); err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(full)); err != nil {
				return
			}
			if g.withUnknown {
				if err := conn.WriteMessage(websocket.BinaryMessage, compressZstd(unknown)); err != nil {
					return
				}
			}
		}
	}))
	g.url = "ws" + strings.TrimPrefix(g.server.URL, "http")
	t.Cleanup(g.server.Close)
	return g
}

// runBridgeForGolden runs the real bridge path against the golden broker for
// the given duration and returns the emitted NDJSON.
func runBridgeForGolden(t *testing.T, g *goldenBroker, d time.Duration) string {
	t.Helper()
	old := bridgeEmitter
	out := newLockedBuffer()
	bridgeEmitter = newTestProtoEmitter(out)
	defer func() { bridgeEmitter = old }()

	client := arrow.NewClient("app", "secret")
	client.SetToken("token")
	t.Setenv("ARROW_HFT_URL", g.url)
	plan, err := BuildSubscriptionPlan([]int32{757614}, 1, MaxHFTTokensPerConnection, MaxHFTTokensPerRequest)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		runHFT(ctx, cancel, client, plan, 50, 10*time.Second, nil, t.Logf)
		close(done)
	}()
	time.Sleep(d)
	cancel()
	<-done
	flushTestEmitter()
	return out.String()
}

// TestGoldenCorpusBridgeDecodesGoldenFrames — the real bridge path against a
// broker serving the golden frames must emit ticks whose NDJSON fields match
// the golden records (full-depth ladder included), and whose raw_payload +
// payload_hash match the golden contract.
func TestGoldenCorpusBridgeDecodesGoldenFrames(t *testing.T) {
	g := newGoldenBroker(t)
	out := runBridgeForGolden(t, g, 1500*time.Millisecond)

	var fullTick, ltpTick map[string]any
	for _, m := range tickMapsFrom(t, out) {
		switch m["mode"] {
		case "full":
			fullTick = m
		case "ltpc":
			ltpTick = m
		}
	}
	if fullTick == nil {
		t.Fatalf("no full tick emitted\n%s", out)
	}
	if ltpTick == nil {
		t.Fatalf("no ltp tick emitted\n%s", out)
	}

	// Full-tick proto fields must match full-tick.golden (numeric values are
	// compared with type coercion: proto ints vs JSON float64).
	fullGolden := loadGoldenRecord(t, "full-tick")
	for _, k := range []string{"feed", "mode", "token", "ltp_paise", "close_paise",
		"open_paise", "high_paise", "low_paise", "vwap_paise", "ltq", "volume",
		"total_buy_qty", "total_sell_qty", "open_interest", "ts_ms"} {
		if !goldenEqual(fullTick[k], fullGolden[k]) {
			t.Fatalf("full %s = %v, golden %v", k, fullTick[k], fullGolden[k])
		}
	}
	// Depth ladders as JSON arrays.
	bidPx, _ := json.Marshal(fullTick["bid_px"])
	wantBidPx, _ := json.Marshal(fullGolden["bid_px"])
	if string(bidPx) != string(wantBidPx) {
		t.Fatalf("full bid_px = %s, golden %s", bidPx, wantBidPx)
	}

	// raw_payload + payload_hash must match the golden (frame bytes + SHA-256).
	if fullTick["raw_payload"] != fullGolden["raw_payload"] {
		t.Fatal("full raw_payload differs from golden")
	}
	if fullTick["payload_hash"] != fullGolden["payload_hash"] {
		t.Fatal("full payload_hash differs from golden")
	}

	// LTP-tick proto fields must match ltp-tick.golden.
	ltpGolden := loadGoldenRecord(t, "ltp-tick")
	for _, k := range []string{"feed", "mode", "token", "ltp_paise", "vwap_paise",
		"volume", "ts_ms"} {
		if !goldenEqual(ltpTick[k], ltpGolden[k]) {
			t.Fatalf("ltp %s = %v, golden %v", k, ltpTick[k], ltpGolden[k])
		}
	}
	if ltpTick["raw_payload"] != ltpGolden["raw_payload"] {
		t.Fatal("ltp raw_payload differs from golden")
	}
	if ltpTick["payload_hash"] != ltpGolden["payload_hash"] {
		t.Fatal("ltp payload_hash differs from golden")
	}
}

// TestGoldenCorpusUnknownPacketNeverEmitted — an unknown packet type on the
// wire must never become an NDJSON tick: the SDK's hftPacketMeta rejects it
// at decode, so the bytes never reach EmitTick. This is the Go half of the
// UNKNOWN_VERSION contract (Java-side quarantine is defense-in-depth only).
func TestGoldenCorpusUnknownPacketNeverEmitted(t *testing.T) {
	g := newGoldenBrokerOpts(t, true)
	out := runBridgeForGolden(t, g, 1500*time.Millisecond)

	// The unknown frame must produce no tick: count decoded proto ticks.
	tickCount := len(tickMapsFrom(t, out))
	// Exactly the ltp + full ticks (2), never a third from the unknown packet.
	if tickCount != 2 {
		t.Fatalf("expected exactly 2 ticks (ltp+full), got %d\n%s", tickCount, out)
	}
}

// TestGoldenCorpusHashPreservation — the golden raw_payload must be the exact
// frame bytes and payload_hash the SHA-256 of those bytes. This is the
// contract the Java PayloadHashValidator enforces at ingest time. (response is
// a wire-only fixture — it never becomes NDJSON — so only emitted tick frames
// are checked here.)
func TestGoldenCorpusHashPreservation(t *testing.T) {
	for _, name := range []string{"full-tick", "ltp-tick"} {
		frame := loadGoldenFrame(t, name)
		rec := loadGoldenRecord(t, name)
		raw, ok := rec["raw_payload"].(string)
		if !ok {
			t.Fatalf("%s: raw_payload missing", name)
		}
		got, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			t.Fatalf("%s: raw_payload not valid base64: %v", name, err)
		}
		if string(got) != string(frame) {
			t.Fatalf("%s: raw_payload != frame bytes (%d vs %d)", name, len(got), len(frame))
		}
		if hash := sha256Hex(frame); hash != rec["payload_hash"] {
			t.Fatalf("%s: payload_hash %q != sha256(frame) %q", name, rec["payload_hash"], hash)
		}
	}
}

// goldenEqual compares a proto-typed value against a JSON-decoded golden
// value with numeric type coercion (int32/int64/float64 all compare equal).
func goldenEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}
	af, aok := numFloat(a)
	bf, bok := numFloat(b)
	if aok && bok {
		return af == bf
	}
	return a == b
}

func numFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
