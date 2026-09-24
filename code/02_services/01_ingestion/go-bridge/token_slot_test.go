package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// fakeTickSource is a hermetic stand-in for *arrow.DataStream.
type fakeTickSource struct {
	mu          sync.Mutex
	gotMode     arrow.StreamMode
	gotTokens   []int32
	subscribeEr error
	emit        []arrow.MarketTick
	emitRaw     [][]byte
	closed      bool
	readErr     error
}

func (f *fakeTickSource) Subscribe(mode arrow.StreamMode, tokens []int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotMode, f.gotTokens = mode, tokens
	return f.subscribeEr
}

func (f *fakeTickSource) ReadTicks(ctx context.Context, onTick func(arrow.MarketTick, []byte), onError func(error)) {
	for i, t := range f.emit {
		var raw []byte
		if i < len(f.emitRaw) {
			raw = f.emitRaw[i]
		}
		onTick(t, raw)
	}
	if f.readErr != nil && onError != nil {
		onError(f.readErr)
	}
}

func (f *fakeTickSource) StartKeepAlive(ctx context.Context, _ time.Duration, onError func(error)) {
	<-ctx.Done()
}

func (f *fakeTickSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func TestTokenStreamPassesSupervisorFullModeThrough(t *testing.T) {
	// The supervisor hardcodes "full" (main.go:554/560). The standard stream must
	// receive exactly that mode - it entitles full and delivers 5-level depth at about
	// 1 Hz (measured 2026-09-24). An earlier version downgraded it to "quote" silently,
	// so a caller asking for the book would have received none and never known.
	f := &fakeTickSource{}
	s := &tokenStream{src: f, ackCh: make(chan arrow.HFTResponsePacket, 1)}
	if err := s.SubscribeHFTTokens("full", arrow.HFTExchNSECM, []int32{757614}, -1); err != nil {
		t.Fatalf("subscribe returned error for HFT-only latency arg: %v", err)
	}
	if f.gotMode != arrow.StreamModeFull {
		t.Fatalf("mode = %q, want %q (must not be downgraded)", f.gotMode, arrow.StreamModeFull)
	}
	if len(f.gotTokens) != 1 || f.gotTokens[0] != 757614 {
		t.Fatalf("tokens = %v, want [757614]", f.gotTokens)
	}
}

func TestTokenStreamSubscribeModeMapping(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want arrow.StreamMode
	}{
		{"full", arrow.StreamModeFull},
		{"f", arrow.StreamModeFull},
		{"ltpc", arrow.StreamModeLTPC},
		{"ltp", arrow.StreamModeLTP},
		{"quote", arrow.StreamModeQuote},
	} {
		if got := subscribeMode(tc.in); got != tc.want {
			t.Errorf("subscribeMode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestTokenStreamSubscribeErrorPropagates(t *testing.T) {
	want := errors.New("E_ALL_INVALID rejected")
	s := &tokenStream{src: &fakeTickSource{subscribeEr: want}}
	if err := s.SubscribeHFTTokens("quote", 0, []int32{1}, 50); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// TestTokenStreamEmitsDecodedBeforeTickAndMapsFields pins the two invariants the emit
// path depends on: raw bytes must reach onDecoded before the tick, and the field
// mapping (including the seconds->nanoseconds conversion) must be exact.
func TestTokenStreamEmitsDecodedBeforeTickAndMapsFields(t *testing.T) {
	const rawLen = 93
	raw := make([]byte, rawLen)
	f := &fakeTickSource{
		emit: []arrow.MarketTick{{
			Token: 757614, Mode: arrow.StreamModeQuote,
			LTP: 14051, LTQ: 111, AvgPrice: 14202,
			Open: 14390, High: 14390, Low: 14050, Close: 14372,
			LowerLimit: 13000, UpperLimit: 15000,
			TotalBuyQuantity: 11, TotalSellQuantity: 22,
			Volume: 170851, OI: 33,
			Time: 1790224634, LTT: 1790224616,
			Bids: []arrow.DepthLevel{{Price: 14050, Quantity: 500, Orders: 3}, {Price: 14049, Quantity: 1 << 40}},
			Asks: []arrow.DepthLevel{{Price: 14051, Quantity: 700, Orders: 4}},
		}},
		emitRaw: [][]byte{raw},
	}
	s := &tokenStream{src: f}

	var decodingBeforeTick bool
	gotRaw, gotTicks := -1, 0
	var got arrow.HFTFullTick
	s.ReadHFTWithFrame(context.Background(),
		func(arrow.HFTLTPTick) {
			t.Error("onLTP must not fire on the standard stream")
		},
		func(tk arrow.HFTFullTick) {
			if gotRaw == rawLen {
				decodingBeforeTick = true
			}
			got, gotTicks = tk, gotTicks+1
		},
		func(arrow.HFTResponsePacket) {
			t.Error("onResponse must not fire on the standard stream")
		},
		nil,
		func(frame []byte) { gotRaw = len(frame) },
		func(err error) { t.Errorf("unexpected error: %v", err) })

	if gotTicks != 1 {
		t.Fatalf("ticks = %d, want 1", gotTicks)
	}
	if !decodingBeforeTick {
		t.Error("onDecoded did not run before onFull - raw_payload invariant broken")
	}
	if gotRaw != rawLen {
		t.Errorf("decoded bytes = %d, want %d", gotRaw, rawLen)
	}
	if got.Token != 757614 || got.LTP != 14051 || got.LTQ != 111 || got.VWAP != 14202 {
		t.Errorf("core fields wrong: %+v", got)
	}
	if got.Open != 14390 || got.High != 14390 || got.Low != 14050 || got.Close != 14372 {
		t.Errorf("ohlc wrong: %+v", got)
	}
	if got.TBQ != 11 || got.TSQ != 22 || got.Volume != 170851 || got.OI != 33 {
		t.Errorf("quantity fields wrong: %+v", got)
	}
	if got.DprL != 13000 || got.DprH != 15000 {
		t.Errorf("price band wrong: DprL=%d DprH=%d", got.DprL, got.DprH)
	}
	// The dangerous one: seconds in, nanoseconds out. main.go divides by 1e6 for ms.
	if want := uint64(1790224634) * 1_000_000_000; got.TS != want {
		t.Fatalf("TS = %d, want %d (seconds->nanoseconds)", got.TS, want)
	}
	if got.ExchSeg != arrow.HFTExchNSECM {
		t.Errorf("ExchSeg = %d, want NSECM", got.ExchSeg)
	}
	if got.BidPx[0] != 14050 || got.BidSize[0] != 500 || got.BidOrd[0] != 3 {
		t.Errorf("bid[0] wrong: %d %d %d", got.BidPx[0], got.BidSize[0], got.BidOrd[0])
	}
	if got.BidSize[1] != 1<<31-1 {
		t.Errorf("bid[1] quantity = %d, want saturation at MaxInt32", got.BidSize[1])
	}
	if got.AskPx[0] != 14051 || got.AskSize[0] != 700 || got.AskOrd[0] != 4 {
		t.Errorf("ask[0] wrong: %d %d %d", got.AskPx[0], got.AskSize[0], got.AskOrd[0])
	}
}

func TestTokenStreamWriteTextIsNoopAndCloseIsTracked(t *testing.T) {
	f := &fakeTickSource{}
	s := &tokenStream{src: f}
	if err := s.WriteText("PONG"); err != nil {
		t.Fatalf("WriteText = %v, want nil (heartbeat PONG is a no-op here)", err)
	}
	if s.IsClosed() {
		t.Fatal("IsClosed true before Close")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if !s.IsClosed() || !f.closed {
		t.Fatal("Close did not propagate to the tick source")
	}
}

func TestTokenStreamReadTicksErrorReachesOnError(t *testing.T) {
	want := errors.New("socket closed")
	f := &fakeTickSource{readErr: want}
	s := &tokenStream{src: f}
	var got error
	s.ReadHFTWithFrame(context.Background(), nil, func(arrow.HFTFullTick) {}, nil, nil, nil,
		func(err error) { got = err })
	if !errors.Is(got, want) {
		t.Fatalf("onError = %v, want %v", got, want)
	}
}

// TestFeedUsesTokenStreamDefaultsToHFT pins the compatibility contract: an unset (or
// unrecognised) ARROW_FEED must leave the HFT path in charge, so deploying this change
// cannot silently switch a running stack onto a different broker stream.
func TestFeedUsesTokenStreamDefaultsToHFT(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"", false}, {"hft", false}, {"HFT", false}, {"token", true},
		{"TOKEN", true}, {" token ", true}, {"tokens", false},
	} {
		t.Setenv("ARROW_FEED", tc.val)
		if got := feedUsesTokenStream(); got != tc.want {
			t.Errorf("ARROW_FEED=%q -> %v, want %v", tc.val, got, tc.want)
		}
	}
}

// TestTokenStreamAckSatisfiesTheSlotClassifier is the guard that matters: the packet
// this adapter fabricates must be accepted by the REAL decision function the slot uses.
// Measured 2026-09-24: without the synthesized ack the supervisor's 10s timer expired,
// the slot was declared PARTIAL and ingestion fail-closed every ~65s even though ticks
// were flowing - the unit tests were green throughout, because the failure lived in the
// interaction between this packet and the state machine, not in the tick path.
func TestTokenStreamAckSatisfiesTheSlotClassifier(t *testing.T) {
	src := &fakeTickSource{}
	ts := &tokenStream{src: src, ackCh: make(chan arrow.HFTResponsePacket, 1)}
	ids := []int32{1, 2, 3}
	got := make(chan arrow.HFTResponsePacket, 1)

	go ts.ReadHFTWithFrame(context.Background(), nil, nil,
		func(p arrow.HFTResponsePacket) { got <- p }, nil, nil, nil)

	if err := ts.SubscribeHFTTokens("full", arrow.HFTExchNSECM, ids, 50); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case p := <-got:
		if outcome := classifySubscriptionResponse(p.ErrorCode, int(p.SuccessCount), int(p.ErrorCount), len(ids)); outcome != subAccepted {
			t.Errorf("classifier outcome = %v, want subAccepted (packet would leave the slot stuck in SUBSCRIBING)", outcome)
		}
		if int(p.SuccessCount) != len(ids) {
			t.Errorf("SuccessCount = %d, want %d", p.SuccessCount, len(ids))
		}
		// ErrorCode carries the broker's status word, not just failures: the classifier
		// requires the literal "SUCCESS" here. An empty code is subTerminal, which is
		// how the first version of this fix would have stopped the slot outright.
		if p.ErrorCode != "SUCCESS" || p.ErrorCount != 0 {
			t.Errorf("status fields wrong: code=%q count=%d, want SUCCESS/0", p.ErrorCode, p.ErrorCount)
		}
		if p.RequestTypeStr != "subscribe" {
			t.Errorf("RequestTypeStr = %q, want subscribe", p.RequestTypeStr)
		}
		if p.ModeStr != "full" {
			t.Errorf("ModeStr = %q, want full (modes pass through unchanged; the free plan entitles full)", p.ModeStr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no ack delivered within 2s")
	}
}

// TestTokenStreamAckSurvivesSubscribeBeforeRead pins the ordering that made a plain
// field insufficient: the supervisor starts the read loop and subscribes afterwards,
// but the reverse order must not lose the ack either.
func TestTokenStreamAckSurvivesSubscribeBeforeRead(t *testing.T) {
	src := &fakeTickSource{}
	ts := &tokenStream{src: src, ackCh: make(chan arrow.HFTResponsePacket, 1)}
	if err := ts.SubscribeHFTTokens("full", arrow.HFTExchNSECM, []int32{7}, 50); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	got := make(chan arrow.HFTResponsePacket, 1)
	go ts.ReadHFTWithFrame(context.Background(), nil, nil,
		func(p arrow.HFTResponsePacket) { got <- p }, nil, nil, nil)
	select {
	case p := <-got:
		if int(p.SuccessCount) != 1 {
			t.Errorf("SuccessCount = %d, want 1", p.SuccessCount)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ack queued before the read loop was lost")
	}
}

// TestTokenStreamNoAckWhenSubscribeFails keeps the failure path honest: a rejected
// subscribe must produce NO ack, so the slot still fails closed on its own timeout
// instead of being told a subscription succeeded that never did.
func TestTokenStreamNoAckWhenSubscribeFails(t *testing.T) {
	src := &fakeTickSource{subscribeEr: errors.New("broker rejected subscription")}
	ts := &tokenStream{src: src, ackCh: make(chan arrow.HFTResponsePacket, 1)}
	got := make(chan arrow.HFTResponsePacket, 1)
	go ts.ReadHFTWithFrame(context.Background(), nil, nil,
		func(p arrow.HFTResponsePacket) { got <- p }, nil, nil, nil)
	if err := ts.SubscribeHFTTokens("full", arrow.HFTExchNSECM, []int32{1}, 50); err == nil {
		t.Fatal("subscribe error must propagate")
	}
	select {
	case p := <-got:
		t.Fatalf("ack delivered for a failed subscribe: %+v", p)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestTokenStreamDeliversOneAckPerSubscribeRequest mirrors how the supervisor actually
// drives this adapter: 1024 tokens arrive as TWO 512-token requests, and it blocks for a
// response after each one. An earlier version of the ack delivery was a single receive,
// so batch 1 was acknowledged, batch 2 was stranded in the channel, and the slot died on
// subscription_response_timeout - with every unit test green, because none of them
// exercised more than one request per reader.
func TestTokenStreamDeliversOneAckPerSubscribeRequest(t *testing.T) {
	src := &fakeTickSource{}
	ts := &tokenStream{src: src, ackCh: make(chan arrow.HFTResponsePacket, 1)}
	got := make(chan arrow.HFTResponsePacket, 4)
	go ts.ReadHFTWithFrame(context.Background(), nil, nil,
		func(p arrow.HFTResponsePacket) { got <- p }, nil, nil, nil)

	batch1 := make([]int32, 512)
	batch2 := make([]int32, 512)
	for i := range batch1 {
		batch1[i] = int32(i + 1)
		batch2[i] = int32(i + 100000)
	}
	for n, batch := range [][]int32{batch1, batch2} {
		if err := ts.SubscribeHFTTokens("full", arrow.HFTExchNSECM, batch, 50); err != nil {
			t.Fatalf("request %d: subscribe: %v", n+1, err)
		}
		select {
		case p := <-got:
			if int(p.SuccessCount) != len(batch) {
				t.Errorf("request %d: SuccessCount = %d, want %d", n+1, p.SuccessCount, len(batch))
			}
			if outcome := classifySubscriptionResponse(p.ErrorCode, int(p.SuccessCount), int(p.ErrorCount), len(batch)); outcome != subAccepted {
				t.Errorf("request %d: classifier = %v, want subAccepted", n+1, outcome)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("request %d: no ack - the batch strands and the slot times out", n+1)
		}
	}
}
