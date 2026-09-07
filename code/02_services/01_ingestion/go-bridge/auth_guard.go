package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/arrow-trade/go-arrow/arrow"
)

// P1-031/P1-035 own-side shield (vendored Client has no lock; upstream Bundle A
// adds it — see docs/plans/2026-09-08-wave9-upstream-bundles.md).
// The shared *arrow.Client fans out across slot goroutines
// (supervisor.go:165-179) while refreshAuth=client.AutoLogin (main.go:121-126)
// writes Config.Token and every dial reads it inside vendored code. Own code
// cannot lock vendored fields, but it CAN order its own calls: dials take
// RLock (concurrent with each other), refresh takes Lock (exclusive).
// A refresh during N parallel dials waits for the handshakes (~100s of ms,
// rare) — documented cost of excluding refresh-mid-dial-read.
//
// bridgeAuthMu guards the dial-vs-refresh pair process-wide.
var bridgeAuthMu sync.RWMutex

// wrapRefreshAuth serializes refresh-vs-refresh (no double-login stampede)
// and refresh-vs-dial (via bridgeAuthMu). Build ONCE in main() around the
// AutoLogin closure; all slot goroutines share the wrapped func.
func wrapRefreshAuth(refresh func(context.Context) error) func(context.Context) error {
	if refresh == nil {
		return nil
	}
	return func(ctx context.Context) error {
		bridgeAuthMu.Lock()
		defer bridgeAuthMu.Unlock()
		return refresh(ctx)
	}
}

// guardedDial runs a stream-factory dial under RLock so a concurrent refresh
// (Lock) cannot interleave the vendored token read inside Dial.
func guardedDial(factory hftStreamFactory) (hftStream, error) {
	bridgeAuthMu.RLock()
	defer bridgeAuthMu.RUnlock()
	return factory()
}

// singleReadStream enforces the P1-037 single-reader invariant structurally:
// runHFTEpoch starts exactly one ReadHFTWithFrame loop per stream; a second
// start (future regression) reports via onError instead of running two
// vendored read loops over one socket.
type singleReadStream struct {
	inner   hftStream
	reading atomic.Bool
}

// guardSingleReader wraps a dialed stream with the single-reader invariant.
// Nil passes through so the epoch's err branch handles dial failure.
func guardSingleReader(s hftStream) hftStream {
	if s == nil {
		return nil
	}
	return &singleReadStream{inner: s}
}

func (s *singleReadStream) SubscribeHFTTokens(mode string, exchSeg int, ids []int32, latencyMS int) error {
	return s.inner.SubscribeHFTTokens(mode, exchSeg, ids, latencyMS)
}

func (s *singleReadStream) WriteText(payload string) error {
	return s.inner.WriteText(payload)
}
func (s *singleReadStream) ReadHFTWithFrame(ctx context.Context,
	onLTP func(arrow.HFTLTPTick),
	onFull func(arrow.HFTFullTick),
	onResponse func(arrow.HFTResponsePacket),
	onFrame func(mt int, payload []byte),
	onDecoded func(frame []byte),
	onError func(err error)) {
	if s.reading.Swap(true) {
		// P1-037: second reader on one socket — refuse instead of running
		// two vendored read loops over the same connection.
		if onError != nil {
			onError(fmt.Errorf("bridge: second ReadHFTWithFrame on one stream refused"))
		}
		return
	}
	s.inner.ReadHFTWithFrame(ctx, onLTP, onFull, onResponse, onFrame, onDecoded, onError)
}

func (s *singleReadStream) Close() error {
	return s.inner.Close()
}
