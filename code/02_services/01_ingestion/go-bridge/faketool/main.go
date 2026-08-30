//go:build faketool

// faketool runs a standalone fake HFT broker for the full-stack E2E test.
// It speaks the real wire format: JSON `sub` inbound, zstd-compressed binary
// response + full-tick frames outbound, with an optional forced disconnect.
//
// Usage: go run -tags faketool main.go -port 8899 -disconnect-after 1
// (the -port / -disconnect-after flags replace the historical
// FAKE_HFT_PORT / FAKE_HFT_DISCONNECT_AFTER env vars, R-274).
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
)

const (
	hftSizeResponse = 540
	hftSizeFull     = 196
	hftPktResponse  = 99
	hftPktFull      = 2
)

func main() {
	port := flag.Int("port", 8899, "listen port")
	disconnect := flag.Int("disconnect-after", 0, "connection index to force-close (1-based); 0 = never")
	// Soak-marathon mode: close every Nth new connection after the
	// subscription response + first tick, so the real supervisor cycles
	// through reconnect/backoff with no external driver (run-full-suite.sh).
	disconnectEvery := flag.Int("disconnect-every", 0, "close every Nth new connection (1-based); 0 = never")
	// R-220: a connection due for a forced close must first serve the client's
	// complete subscription batch — the bridge splits a 1024-token plan into
	// 2x512 requests, so closing right after the first response would leave
	// every epoch half-subscribed (no ACTIVE ack ever emitted). Close only
	// after the connection has been idle for close-linger-ms.
	closeLinger := flag.Duration("close-linger", 500*time.Millisecond, "how long a forced-close connection keeps serving sub frames before closing")
	// Soak mode: keep sending a full-tick frame every N ms so a long soak has
	// continuous Fluss append traffic instead of one tick per connection.
	tickInterval := flag.Int("tick-interval-ms", 0, "if >0, send a full-tick frame every N ms until the client disconnects; 0 = single tick then idle")
	// Throughput bench mode (Phase 4): a real broker cadence — one full-tick
	// frame per subscribed token per interval at real-rate-hz. 1024 ids x
	// 20 Hz = 20,480 frames/s (the redesign's per-connection target).
	realRate := flag.Bool("real-rate", false, "emit one frame per subscribed id at -real-rate-hz (bench mode)")
	realRateHz := flag.Int("real-rate-hz", 20, "frames per second per subscribed id when -real-rate; must divide 1000")
	// Data-quality audit injection (F2/F3 live A/B, 2026-08-30): after the
	// tick stream has run for inject-after-ms, send a burst of crafted
	// frames for the first subscribed token and log the exact counts:
	//   - inject-dups D: D verbatim resends of the token's most recent
	//     normal frame. Same fingerprint (epoch|token|ts|price|qty) → the
	//     compute dedup operator MUST drop all D (compute.dedup.duplicates).
	//   - inject-late L with -inject-late-ms M: L frames with a fresh
	//     price/qty (unique fingerprint, passes dedup) but event timestamp
	//     M ms in the past (default 90000 — beyond the 15s window +
	//     lateness) → the candle window MUST late-drop all L
	//     (compute.candles.late.dropped).
	// Both kinds still land in raw_table_1 (ingestion writes raw before
	// compute dedups), so raw counts grow by D+L. One injection per
	// connection; repeats every inject-every-ms (0 = once).
	injectAfter := flag.Duration("inject-after-ms", 0, "start injection this long after the ticker starts; 0 = never")
	injectDups := flag.Int("inject-dups", 0, "verbatim duplicate frames to inject per round")
	injectLate := flag.Int("inject-late", 0, "late-timestamp unique frames to inject per round")
	injectLateMs := flag.Int("inject-late-ms", 90000, "how far in the past injected late frames' event time is")
	injectEvery := flag.Duration("inject-every-ms", 0, "repeat injection every this long; 0 = once")
	// Cap the number of rounds: the harness teardown lags the measurement
	// window by up to ~100s (poll-loop drift + evidence harvest), and a
	// round firing in that gap lands in raw but past the job's counters —
	// the analyzer then has to reason about post-counter rounds (observed
	// 2026-08-31: round 6 at +600s fired after the last Prometheus sample).
	injectMaxRounds := flag.Int("inject-max-rounds", 0, "stop after this many rounds; 0 = unlimited")
	flag.Parse()
	if *realRate && *tickInterval > 0 {
		fmt.Fprintln(os.Stderr, "faketool: -real-rate and -tick-interval-ms are mutually exclusive")
		os.Exit(2)
	}
	if *realRate && 1000%*realRateHz != 0 {
		fmt.Fprintf(os.Stderr, "faketool: -real-rate-hz %d must divide 1000\n", *realRateHz)
		os.Exit(2)
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	// R-094: each WebSocket handler runs in its own goroutine under net/http;
	// the shared connection counter must be atomic or concurrent/reconnecting
	// clients race on it.
	var connections atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		idx := int(connections.Add(1))
		// R-213: one zstd encoder per connection — constructing and closing a
		// fresh encoder (table initialization included) per frame was wasteful.
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "zstd encoder init failed:", err)
			return
		}
		defer enc.Close()
		logf := func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "faketool: conn=%d %s\n", idx, fmt.Sprintf(format, args...))
		}
		logf("accepted")
		// gorilla/websocket panics on concurrent writes ("concurrent write to
		// websocket connection"): the ticker goroutine's data frames raced the
		// library's automatic pong replies to the client's heartbeats. All
		// writes — data frames, control frames, and the pong handler — share
		// one per-connection mutex so nothing can interleave.
		var wmu sync.Mutex
		// Real-rate mode (Phase 4): every sub request contributes its ids to
		// the per-connection subscription set; the ticker emits one frame per
		// subscribed id per interval (the bridge splits its 1024-token plan
		// into 2x512 requests, so both halves must be collected).
		var subMu sync.Mutex
		var subscribed []uint32
		send := func(frame []byte) error {
			wmu.Lock()
			defer wmu.Unlock()
			// Compress inside the lock: EncodeAll returns a slice aliasing the
			// encoder's internal buffer, and the ticker goroutine can compress
			// while the handler serves a second sub batch — a concurrent
			// EncodeAll would overwrite this payload mid-write and corrupt the
			// stream ("hft unknown packet: N trailing bytes" on the bridge).
			return conn.WriteMessage(websocket.BinaryMessage, compressZstd(enc, frame))
		}
		sendControl := func(kind int, payload []byte) error {
			wmu.Lock()
			defer wmu.Unlock()
			return conn.WriteControl(kind, payload, time.Now().Add(time.Second))
		}
		conn.SetPingHandler(func(appData string) error {
			wmu.Lock()
			defer wmu.Unlock()
			return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})
		closePending := (*disconnect > 0 && idx == *disconnect) ||
			(*disconnectEvery > 0 && idx%*disconnectEvery == 0)
		tickerStarted := false
		for {
			if closePending {
				// R-220: linger for further subscription frames instead of
				// closing right after the first response — the client may split
				// its plan across several sub requests and must complete the
				// whole batch (and emit its ACTIVE ack) before the forced close.
				if err := conn.SetReadDeadline(time.Now().Add(*closeLinger)); err != nil {
					logf("set read deadline failed: %v", err)
					return
				}
			}
			_, payload, err := conn.ReadMessage()
			if err != nil {
				if closePending {
					var netErr net.Error
					if errors.As(err, &netErr) && netErr.Timeout() {
						logf("subscription batch idle — force closing")
					} else {
						logf("read end: %v", err)
					}
					_ = sendControl(websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseNormalClosure, "every"))
				} else {
					logf("read end: %v", err)
				}
				return
			}
			var sub map[string]any
			if json.Unmarshal(payload, &sub) != nil {
				logf("non-json frame (len=%d)", len(payload))
				continue
			}
			if code, _ := sub["code"].(string); code != "sub" {
				logf("non-sub frame code=%q (len=%d)", code, len(payload))
				continue
			}
			ids := []any{}
			if symIds, ok := sub["symIds"].([]any); ok && len(symIds) > 0 {
				if seg, ok := symIds[0].(map[string]any); ok {
					if segIDs, ok := seg["ids"].([]any); ok {
						ids = segIDs
					}
				}
			}
			logf("sub received (ids=%d)", len(ids))
			if *realRate {
				subMu.Lock()
				for _, id := range ids {
					if tok, ok := id.(float64); ok {
						subscribed = append(subscribed, uint32(int64(tok)))
					}
				}
				subMu.Unlock()
			}
			resp := make([]byte, hftSizeResponse)
			binary.LittleEndian.PutUint32(resp[0:4], hftSizeResponse)
			resp[4] = hftPktResponse
			copy(resp[6:22], "SUCCESS")
			resp[534] = 0
			resp[535] = 1
			binary.LittleEndian.PutUint16(resp[536:538], uint16(len(ids)))
			if err := send(resp); err != nil {
				logf("send response failed: %v", err)
				return
			}
			// Soak mode: keep emitting ticks until the client goes away. The
			// ticker starts right after the first sub response (before the
			// snapshot burst) so ticks flow continuously from subscription
			// time; it exits via write errors once the connection closes.
			// Real-rate mode (Phase 4) uses its own per-id cadence instead.
			if !closePending && !tickerStarted && (*tickInterval > 0 || *realRate) {
				tickerStarted = true
				if *realRate {
					logf("real-rate ticker start (hz=%d)", *realRateHz)
					// last frame per token, captured after each send, so a
					// duplicate injection resends the exact bytes the bridge
					// just accepted (identical fingerprint by construction).
					var lastFrames sync.Map // token -> []byte
					go func() {
						interval := time.Duration(1000 / *realRateHz) * time.Millisecond
						t := time.NewTicker(interval)
						defer t.Stop()
						frame := make([]byte, hftSizeFull)
						binary.LittleEndian.PutUint16(frame[0:2], hftSizeFull)
						frame[2] = hftPktFull
						// R-275: per-token mean-reverting random-walk LTP (paise).
						// A constant 15050 made every candle flat (o=h=l=c), so the
						// compute job's 20-candle breakout rule (close > open) could
						// never fire. The price walks around a token-distinct anchor
						// with ±10 paise noise: OHLC varies per candle, and a close
						// above the trailing-20 high stays a ~1-2% tail event instead
						// of a monotone drift that floods Signal_Candidates.
						prices := make(map[uint32]int32)
						for range t.C {
							subMu.Lock()
							ids := append([]uint32(nil), subscribed...)
							subMu.Unlock()
							for _, tok := range ids {
								p, ok := prices[tok]
								anchor := int32(15050 + tok%1000)
								if !ok {
									p = anchor
								}
								p += (anchor - p) / 20 // pull back toward the anchor
								p += int32(rand.IntN(21) - 10) // ±10 paise noise
								prices[tok] = p
								// Every frame carries a real traded quantity: LTQ (12:16)
								// and Volume (64:72). Without it every tick arrives as a
								// TRADE with last_qty=0 and the candle's tick_count/volume
								// stay zero (E2E assertion 2026-08-17).
								qty := uint32(1 + rand.IntN(50))
								binary.LittleEndian.PutUint32(frame[4:8], tok)
								binary.LittleEndian.PutUint32(frame[8:12], uint32(p))
								binary.LittleEndian.PutUint32(frame[12:16], qty)
								binary.LittleEndian.PutUint64(frame[64:72], uint64(qty))
								binary.LittleEndian.PutUint64(frame[180:188], uint64(time.Now().UnixNano()))
								lastFrames.Store(tok, append([]byte(nil), frame...))
								if err := send(frame); err != nil {
									logf("real-rate send failed: %v", err)
									return
								}
							}
						}
					}()
					// Injection goroutine (F2/F3 audit): waits for the first
					// subscribed token, then after inject-after-ms replays its
					// last frame verbatim (dups) and sends old-timestamp
					// unique frames (late). Counts logged as INJECT lines —
					// the harness parses them and asserts the counters match.
					if *injectAfter > 0 && (*injectDups > 0 || *injectLate > 0) {
						go func() {
							// wait for at least one subscribed token with a
							// captured frame
							var tok uint32
							var last []byte
							deadline := time.Now().Add(*injectAfter)
							for time.Now().Before(deadline) {
								subMu.Lock()
								n := len(subscribed)
								if n > 0 {
									tok = subscribed[0]
								}
								subMu.Unlock()
								if n > 0 {
									if v, ok := lastFrames.Load(tok); ok {
										last = v.([]byte)
										break
									}
								}
								time.Sleep(50 * time.Millisecond)
							}
							if last == nil {
								logf("INJECT skipped — no captured frame before deadline")
								return
							}
							time.Sleep(time.Until(deadline))
							for round := 1; ; round++ {
								// refresh the captured frame each round so
								// duplicates are always copies of a frame the
								// pipeline has just seen (dedup TTL is 60s)
								if v, ok := lastFrames.Load(tok); ok {
									last = v.([]byte)
								}
								for i := 0; i < *injectDups; i++ {
									if err := send(last); err != nil {
										logf("INJECT dup send failed: %v", err)
										return
									}
								}
								for i := 0; i < *injectLate; i++ {
									late := append([]byte(nil), last...)
									// fresh price/qty → unique fingerprint →
									// passes dedup; old timestamp → the candle
									// window late-drops it.
									binary.LittleEndian.PutUint32(late[8:12], uint32(15050+i))
									binary.LittleEndian.PutUint32(late[12:16], uint32(7+i))
									binary.LittleEndian.PutUint64(late[64:72], uint64(7+i))
									binary.LittleEndian.PutUint64(late[180:188],
										uint64(time.Now().UnixNano()-int64(*injectLateMs)*int64(time.Millisecond)))
									if err := send(late); err != nil {
										logf("INJECT late send failed: %v", err)
										return
									}
								}
								logf("INJECT round=%d token=%d dups=%d late=%d late_ms=%d",
									round, tok, *injectDups, *injectLate, *injectLateMs)
								if *injectEvery <= 0 {
									return
								}
								if *injectMaxRounds > 0 && round >= *injectMaxRounds {
									logf("INJECT done — max rounds reached (%d)", *injectMaxRounds)
									return
								}
								time.Sleep(*injectEvery)
							}
						}()
					}
				} else {
					logf("ticker start (every %dms)", *tickInterval)
					tickerFrame := make([]byte, hftSizeFull)
					binary.LittleEndian.PutUint16(tickerFrame[0:2], hftSizeFull)
					tickerFrame[2] = hftPktFull
					binary.LittleEndian.PutUint32(tickerFrame[4:8], 757614)
					binary.LittleEndian.PutUint32(tickerFrame[8:12], 15050)
					// Traded quantity so candles carry real volume/tick_count.
					binary.LittleEndian.PutUint32(tickerFrame[12:16], 25)
					binary.LittleEndian.PutUint64(tickerFrame[64:72], 25)
					go func() {
						t := time.NewTicker(time.Duration(*tickInterval) * time.Millisecond)
						defer t.Stop()
						for range t.C {
							binary.LittleEndian.PutUint64(tickerFrame[180:188], uint64(time.Now().UnixNano()))
							if err := send(tickerFrame); err != nil {
								logf("ticker send failed: %v", err)
								return
							}
						}
					}()
				}
			}
			// Initial snapshot: a real broker sends one full-tick frame per
			// subscribed token right after the sub response — the ingestion
			// side counts seen tokens toward subscription completeness
			// (all 1024 must appear or readiness stays false). Emit the
			// approved fake token 757614 first, then one frame per requested
			// id so the whole assignment is seen.
			//
			// The burst runs in its own goroutine so the read loop keeps
			// draining sub requests: the bridge splits a 1024-token plan into
			// 2x512 requests and waits for BOTH responses inside a 10s
			// window. Serving sub1's 513-frame burst inline would delay sub2's
			// response by the whole burst (minutes under backpressure), so a
			// real broker's promptness is simulated: every request gets its
			// response immediately, bursts proceed concurrently.
			//
			// Frames are paced (~300ms) so the burst never outruns the Java
			// pipeline's consumption of the bridge's stdout pipe. The bridge
			// emits each tick synchronously to that pipe; when the Java side
			// processes ticks slowly (per-tick Fluss append + resource
			// metrics, ~10/s), an unpaced 1026-frame burst fills the pipe,
			// blocks the bridge's read loop mid-burst, and its second
			// subscription response (in-stream after the first 513 frames)
			// arrives past the 10s window — terminating the subscription.
			// Two bursts (one per 512-token sub) run concurrently, so the
			// combined rate is 2x this pacing + the 2/s ticker: 2/0.3 + 2
			// = ~8.7/s, which stays under the pipe's ~10/s drain rate.
			// (120ms pacing measured 16.7/s combined — enough to back up
			// the pipe and quarantine every aged frame as STALE.)
			// In real-rate mode the burst is skipped entirely: the ticker's
			// per-id frames (one per subscribed token per interval) are the
			// subscription-completeness evidence.
			if !*realRate {
				go func(ids []any) {
					full := make([]byte, hftSizeFull)
					binary.LittleEndian.PutUint16(full[0:2], hftSizeFull)
					full[2] = hftPktFull
					binary.LittleEndian.PutUint32(full[4:8], 757614) // token present in approved CSV
					binary.LittleEndian.PutUint32(full[8:12], 15050)
					// Traded quantity so candles carry real volume/tick_count.
					binary.LittleEndian.PutUint32(full[12:16], 25)
					binary.LittleEndian.PutUint64(full[64:72], 25)
					// A real broker stamps every frame with send-time nanoseconds
					// (bridge ts_ms = ts/1e6). Without it ts_ms=0 and the
					// ingestion freshness gate quarantines every tick as
					// INVALID_VALUES.
					binary.LittleEndian.PutUint64(full[180:188], uint64(time.Now().UnixNano()))
					if err := send(full); err != nil {
						logf("send snapshot tick failed: %v", err)
						return
					}
					snapshotSent := 1
					for _, id := range ids {
						tok, ok := id.(float64)
						if !ok {
							continue
						}
						time.Sleep(300 * time.Millisecond)
						binary.LittleEndian.PutUint32(full[4:8], uint32(int64(tok)))
						// Per-token qty so every burst frame carries real volume.
						qty := uint32(1 + rand.IntN(50))
						binary.LittleEndian.PutUint32(full[12:16], qty)
						binary.LittleEndian.PutUint64(full[64:72], uint64(qty))
						binary.LittleEndian.PutUint64(full[180:188], uint64(time.Now().UnixNano()))
						if err := send(full); err != nil {
							logf("send snapshot tick failed at %d: %v", snapshotSent, err)
							return
						}
						snapshotSent++
					}
					logf("snapshot sent (%d ticks)", snapshotSent)
				}(ids)
			}
		}
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("fake HFT broker listening on %s (disconnect_after=%d disconnect_every=%d tick_interval_ms=%d real_rate=%v real_rate_hz=%d)\n",
		addr, *disconnect, *disconnectEvery, *tickInterval, *realRate, *realRateHz)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func compressZstd(enc *zstd.Encoder, payload []byte) []byte {
	return enc.EncodeAll(payload, nil)
}
