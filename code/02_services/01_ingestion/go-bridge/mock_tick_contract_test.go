package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestMockTickSampleDecodesStrictly (M5-2) pins the mock's dialect to the real
// one: the committed fixture (code/testdata/mock-tick-sample.json, the exact
// keys MockArrowServer emits) must decode into the same Tick the live feeds
// decode into, with DisallowUnknownFields rejecting any key the Go reader does
// not know. Before this, the mock spoke a private vocabulary
// (instrument_token/exchange_ts/ohlc_*/nested depth_*) that no in-repo consumer
// could read.
func TestMockTickSampleDecodesStrictly(t *testing.T) {
	path := filepath.Join("..", "..", "..", "testdata", "mock-tick-sample.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read mock tick fixture %s: %v", path, err)
	}

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var tick Tick
	if err := dec.Decode(&tick); err != nil {
		t.Fatalf("mock tick fixture must decode strictly into Tick: %v", err)
	}
	if tick.Feed != "token" || tick.Mode != "full" {
		t.Fatalf("feed/mode must be the canonical values, got %q/%q", tick.Feed, tick.Mode)
	}
	if tick.Token != 2885 || tick.TS == 0 || tick.LTP == 0 || tick.LTQ == 0 {
		t.Fatalf("core fields must be populated, got token=%d ts=%d ltp=%d ltq=%d",
			tick.Token, tick.TS, tick.LTP, tick.LTQ)
	}
	if tick.BidPx[4] == 0 || tick.AskPx[4] == 0 || tick.BidOrd[4] == 0 || tick.AskOrd[4] == 0 {
		t.Fatal("the five-element ladders must be populated (prices and order counts)")
	}

	var extra Tick
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("the fixture must carry exactly one tick object (extra decode: %v)", err)
	}
}
