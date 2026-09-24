package main

// token_slot.go adapts the Arrow STANDARD token market-data stream (wss://ds.arrow.trade,
// StreamModeLTP/LTPC/Quote/Full + CAS) onto the hftStream interface the slot supervisor
// already drives, so the pipeline can keep ingesting on an account that has the free
// plan instead of HFT.
//
// Why an adapter and not a rewrite: everything downstream of the callbacks is already
// stream-agnostic. supervisor.go, subscription_plan.go (chunking, token-set hashing,
// sharding) and transport.go are reused untouched; main.go already declares the mode
// vocabulary "ltp" | "ltpc" | "quote" | "full" and maps HFTFullTick into the normalized
// record. Java ingestion, the DDL and compute need no change at all.
//
// Measured facts this file depends on (probe, 2026-09-24):
//   * MarketTick.Time and MarketTick.LTT are epoch SECONDS. The HFT path's LTT is
//     MICROSECONDS (P1-023) - the units differ per stream, so TS is converted
//     explicitly rather than copied.
//   * HFTFullTick.TS is NANOSECONDS: main.go does int64(t.TS / 1_000_000) for ms.
//   * MarketTick carries no ATV/BTV and no exchange segment, and in "quote" mode
//     Bids/Asks come back empty (depth needs "full"). None of those reach raw_table_1,
//     which stores only price, qty and time.
//   * The only WriteText caller is the heartbeat PONG (main.go); auth refresh goes
//     through client.AutoLogin, so a no-op here breaks nothing.
//   * In "quote" mode ticks arrive update-on-change, not on a fixed cadence.
//   * In "full" mode the stream delivers 5-level depth at about 1 Hz (measured);
//     "quote" carries no book and updates only on change. Depth is not persisted
//     (raw_table_1 has no depth columns) but the richer frames do reach raw_payload.
//   * The standard stream sends NO subscription-response packet. The slot state
//     machine requires one to leave SUBSCRIBING (main.go's subscribe loop), so this
//     adapter synthesizes it - see SubscribeHFTTokens.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// errNoDataStream is returned when the SDK yields an ArrowStreams without a token
// data stream; without this the failure would surface as a nil dereference on the
// first Subscribe instead of a diagnosable error.
var errNoDataStream = errors.New("arrow: token data stream unavailable")

// tickSource is the slice of *arrow.DataStream the adapter needs. It exists so the
// mapping can be tested hermetically, mirroring hftStreamFactory's seam.
type tickSource interface {
	Subscribe(mode arrow.StreamMode, tokens []int32) error
	ReadTicks(ctx context.Context, onTick func(arrow.MarketTick, []byte), onError func(error))
	StartKeepAlive(ctx context.Context, interval time.Duration, onError func(error))
	Close() error
}

// tokenStream implements hftStream on top of the standard token data stream.
type tokenStream struct {
	src    tickSource
	closed bool

	// ackCh carries the synthesized subscription acknowledgement from
	// SubscribeHFTTokens to the read loop. A buffered channel (not a plain field)
	// is required because of the supervisor's ordering: main.go starts the read
	// goroutine and only THEN calls SubscribeHFTTokens, so the ack can be produced
	// before the callback that consumes it exists.
	ackCh chan arrow.HFTResponsePacket
}

func newTokenStream(client *arrow.Client) (hftStream, error) {
	streams, err := client.NewStreams()
	if err != nil {
		return nil, err
	}
	if streams.DataStream == nil {
		return nil, errNoDataStream
	}
	return &tokenStream{src: streams.DataStream, ackCh: make(chan arrow.HFTResponsePacket, 1)}, nil
}

// subscribeMode maps the mode vocabulary the supervisor uses onto the standard
// stream's own modes. The two vocabularies coincide for ltp/ltpc/full, so they are
// passed through unchanged: an earlier version silently downgraded "full" to "quote",
// which was wrong (a caller asking for full and receiving quote would never know).
// Measured 2026-09-24: the free plan entitles "full" and delivers 5-level depth at
// about 1 Hz, versus "quote" which carries no book and updates only on change.
func subscribeMode(mode string) arrow.StreamMode {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "ltp", "l":
		return arrow.StreamModeLTP
	case "ltpc":
		return arrow.StreamModeLTPC
	case "full", "f":
		return arrow.StreamModeFull
	case "quote", "q":
		return arrow.StreamModeQuote
	case "":
		// The supervisor always names a mode; an empty one would otherwise subscribe
		// to the empty string, which the broker rejects with a confusing error.
		return arrow.StreamModeLTPC
	default:
		return arrow.StreamMode(strings.ToLower(strings.TrimSpace(mode)))
	}
}

// SubscribeHFTTokens satisfies hftStream. exchSeg and latencyMS are HFT-only concepts
// (exchange segmentation and broker-side tick latency): the standard stream has no
// equivalent, so they are ignored deliberately rather than silently misapplied.
func (s *tokenStream) SubscribeHFTTokens(mode string, _ int, ids []int32, _ int) error {
	m := subscribeMode(mode)
	if err := s.src.Subscribe(m, ids); err != nil {
		return err
	}
	// The standard stream acknowledges a subscription implicitly: a nil error plus
	// the arrival of ticks. The slot state machine, however, only leaves SUBSCRIBING
	// when a response packet arrives (main.go waits on this before it emits ACTIVE at
	// the end of its subscribe loop; the 10s responseTimeout otherwise ends the slot
	// as subscription_response_timeout). The HFT protocol sends a 540-byte response
	// frame for this; the standard protocol has no such packet, so synthesize one.
	// Measured 2026-09-24: without it the slot streams ~3000 real ticks and is then
	// torn down as PARTIAL, which is what this call fixes.
	success := len(ids)
	if success > math.MaxUint16 {
		// SuccessCount is uint16 on the wire; a wrapped count would read as a partial
		// subscription. Batches are capped far below this (512 per request today), so
		// this is a guard rather than a reachable path.
		success = math.MaxUint16
	}
	select {
	case s.ackCh <- arrow.HFTResponsePacket{
		PktType:        99, // hftPktResponse; not read downstream, set for shape fidelity
		ExchSeg:        uint8(arrow.HFTExchNSECM),
		RequestType:    0, // 0 = subscribe
		RequestTypeStr: "subscribe",
		ModeStr:        string(m),
		SuccessCount:   uint16(success),
		ErrorCount:     0,
		// "SUCCESS" is the literal classifySubscriptionResponse requires for
		// subAccepted - an empty code classifies as subTerminal, which would stop the
		// slot outright. Verified by TestTokenStreamAckSatisfiesTheSlotClassifier.
		ErrorCode: "SUCCESS",
	}:
		fmt.Fprintf(os.Stderr, "arrow-bridge: token subscribe ok mode=%s tokens=%d ack queued\n", m, len(ids))
	default:
		// Capacity 1 is sufficient in the supervisor's flow: it calls SubscribeHFTTokens
		// and waits for the response before issuing the next request, so acks are
		// consumed one at a time. Reaching here means that contract changed - report it
		// loudly rather than letting a slot wait out its response timeout.
		fmt.Fprintf(os.Stderr, "arrow-bridge: token stream: subscription ack dropped (previous ack unread)\n")
	}
	return nil
}

// deliverAck hands the queued subscription acknowledgement to onResponse, waiting
// for it if the subscribe has not happened yet. It exits on ctx cancellation so an
// epoch whose subscribe never succeeds does not leak this goroutine.
func (s *tokenStream) deliverAck(ctx context.Context, onResponse func(arrow.HFTResponsePacket)) {
	// One ack per subscription REQUEST, not per epoch: a 1024-token slot is split
	// into two 512-token batches, and the supervisor blocks on a response after each
	// SubscribeHFTTokens call. A single receive here delivered batch 1 and stranded
	// batch 2, so the loop timed out and tore the slot down every epoch (measured
	// 2026-09-24: "ack queued" twice, "ack delivered" once, then
	// subscription_response_timeout).
	for {
		select {
		case p := <-s.ackCh:
			onResponse(p)
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "arrow-bridge: token ack loop exiting (context done)\n")
			return
		}
	}
}

// WriteText satisfies hftStream. The single caller sends the heartbeat "PONG"; the
// standard stream's websocket manages its own keepalive, so this is intentionally a
// no-op. It must stay a no-op rather than an error: an error here would be read as a
// heartbeat failure and could trip the stall detector.
func (s *tokenStream) WriteText(string) error { return nil }

// ReadHFTWithFrame satisfies hftStream. onDecoded is invoked BEFORE onFull for each
// tick so the caller's lastDecoded holds the raw broker bytes, preserving the plan's
// raw_payload invariant (decoded JSON must never replace the original packet bytes).
// onLTP is left unused: the standard stream yields only MarketTick, which is
// full-shaped. onResponse carries the subscription ack synthesized in
// SubscribeHFTTokens; it is delivered from its own goroutine because the caller
// subscribes only after this read loop has started.
func (s *tokenStream) ReadHFTWithFrame(ctx context.Context,
	_ func(arrow.HFTLTPTick),
	onFull func(arrow.HFTFullTick),
	onResponse func(arrow.HFTResponsePacket),
	_ func(mt int, payload []byte),
	onDecoded func(frame []byte),
	onError func(err error)) {

	if onResponse != nil {
		go s.deliverAck(ctx, onResponse)
	}

	// The bridge's own heartbeat calls WriteText, which is a no-op here, so without
	// this the token path would have no keepalive at all: a silent websocket death
	// would only surface via the stall watchdog, and only once frames stop. The SDK
	// owns the correct ping for this stream, so delegate to it and surface failures
	// through the same onError channel the read loop uses.
	if onError != nil {
		go s.src.StartKeepAlive(ctx, 0, onError)
	}

	s.src.ReadTicks(ctx, func(t arrow.MarketTick, raw []byte) {
		if onDecoded != nil && len(raw) > 0 {
			onDecoded(raw)
		}
		if onFull != nil {
			onFull(toHFTFullTick(t))
		}
	}, onError)
}

func (s *tokenStream) Close() error {
	s.closed = true
	return s.src.Close()
}

// IsClosed reports whether Close has been called. The supervisor's fake stream exposes
// the same predicate, so slot health checks can be exercised identically on both paths.
func (s *tokenStream) IsClosed() bool { return s.closed }

// toHFTFullTick maps a standard MarketTick onto the HFTFullTick shape the existing
// emit path consumes. Fields with no standard-stream source are left zero and noted:
// ATV/BTV are HFT-only, and the HFT wire framing fields (Size/PktType) are unused
// by the emit path. The reverse also holds — ChangeFlag, OIDayHigh/Low and the CAS
// trio exist only here, so this adapter is their only producer.
func toHFTFullTick(t arrow.MarketTick) arrow.HFTFullTick {
	out := arrow.HFTFullTick{
		Token: t.Token,
		LTP:   t.LTP,
		LTQ:   t.LTQ,
		VWAP:  t.AvgPrice,
		Open:  t.Open,
		High:  t.High,
		Low:   t.Low,
		Close: t.Close,
		// LTT is deliberately left zero: this stream reports epoch SECONDS while the
		// HFT struct's LTT is an int32 that HFT fills with microseconds, so putting
		// seconds there would be a unit lie. The normalized value goes to LTTms.
		LTTms: func() int64 {
			ms, _ := arrow.LTTmsFromSeconds(int64(t.LTT))
			return ms
		}(),
		ChangeFlag: int32(t.ChangeFlag),
		OIDayHigh:  t.OIDayHigh,
		OIDayLow:   t.OIDayLow,
		// DprL/DprH are the day's price band; the standard stream names them lower/upper limit.
		DprL:   t.LowerLimit,
		DprH:   t.UpperLimit,
		TBQ:    t.TotalBuyQuantity,
		TSQ:    t.TotalSellQuantity,
		Volume: t.Volume,
		OI:     uint64(t.OI),
		// The standard stream carries no exchange segment; the pipeline only ever
		// subscribes NSECM (the manifest is NSE cash), so stamp it explicitly.
		ExchSeg: arrow.HFTExchNSECM,
		// Time is epoch SECONDS on this stream, TS is NANOSECONDS here (see header).
		TS: uint64(t.Time) * 1_000_000_000,
		// PktType/Size describe HFT wire framing and are unused by the emit path.
		// ATV/BTV have no standard equivalent; they stay zero, which the emit path
		// reports as absent so the HFT-only columns stay NULL on this feed.
	}
	if t.HasCAS {
		// The wire fields are int64/i32/i32; the row columns are BIGINT, so the two
		// prices widen here rather than costing a pointer-to-pointer copy downstream.
		imbalance := t.ImbalanceQty
		indicative := int64(t.IndicativeClose)
		ref := int64(t.RefPrice)
		out.ImbalanceQty, out.IndicativeClose, out.RefPrice = &imbalance, &indicative, &ref
	}
	for i := 0; i < len(t.Bids) && i < len(out.BidPx); i++ {
		out.BidPx[i] = t.Bids[i].Price
		out.BidSize[i] = clampQty(t.Bids[i].Quantity)
		out.BidOrd[i] = uint16(t.Bids[i].Orders)
	}
	for i := 0; i < len(t.Asks) && i < len(out.AskPx); i++ {
		out.AskPx[i] = t.Asks[i].Price
		out.AskSize[i] = clampQty(t.Asks[i].Quantity)
		out.AskOrd[i] = uint16(t.Asks[i].Orders)
	}
	return out
}

// clampQty converts a depth quantity to the int32 the HFT shape uses, saturating
// instead of wrapping: a wrapped quantity would be silently wrong data in the lake.
func clampQty(q int64) int32 {
	switch {
	case q > math.MaxInt32:
		return math.MaxInt32
	case q < math.MinInt32:
		return math.MinInt32
	default:
		return int32(q)
	}
}
