// capture-marketdata connects to the real Arrow HFT market-data feed
// (wss://socket.arrow.trade) with credentials from the environment and records
// raw wire frames to a JSONL file.
//
// Purpose: BROKER-MD-001 protocol evidence. The vendored parse layouts are
// validated against real broker bytes (HFT LTP 40 B / full 196 B, zstd).
//
// The tool is read-only: it only subscribes to market data and records what
// the broker sends. It never places orders or sends anything beyond the
// subscribe/unsubscribe messages.
//
// Environment:
//
//	ARROW_APP_ID + ARROW_APP_SECRET       → client.Login token exchange
//	ARROW_USER_ID + ARROW_PASSWORD + ARROW_TOTP_KEY → client.AutoLogin (TOTP only)
//	P1-019: the ARROW_REQUEST_TOKEN / ARROW_TOKEN device flows are removed —
//	the mandatory TOTP gate below made them unreachable, and TOTP is authoritative.
//	CAPTURE_HFT=1 (default)               → capture socket.arrow.trade
//	CAPTURE_TOKENS=2885,1333,3045         → instrument ids (NSE tokens)
//	CAPTURE_DURATION=20                   → seconds per feed
//	CAPTURE_OUT=marketdata-capture.jsonl  → output path
//
// JSONL record kinds:
//
//	{"kind":"connect","feed":"hft","ok":true}
//	{"kind":"subscribe","feed":"hft","mode":"ltpc","ok":true}
//	{"kind":"frame","feed":"hft","mt":2,"len":123}               (HFT frame, msg type + compressed size)
//	{"kind":"response","feed":"hft","errorCode":"","successCount":5,...}
//	{"kind":"error","feed":"hft","err":"..."}
//	{"kind":"raw","feed":"hft","len":196,"hex":"..."}            (decoded payload)
//	{"kind":"summary","hft":{...},"hftFrames":N}          (N counts every WS frame incl. keepalives; hft only decoded payloads)
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

type recorder struct {
	mu   sync.Mutex
	w    *bufio.Writer
	file *os.File
	// P1-020: evidence loss is counted, never swallowed — main exits
	// non-zero when failed > 0 so a truncated capture can't pose as clean.
	failed    int
	lostBytes int64
}

func (r *recorder) emit(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		r.mu.Lock()
		r.failed++
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.w.Write(b); err != nil {
		r.failed++
		r.lostBytes += int64(len(b) + 1)
		return err
	}
	return r.w.WriteByte('\n')
}

// stats reports evidence-write failures and estimated lost bytes.
func (r *recorder) stats() (failed int, lostBytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed, r.lostBytes
}

func (r *recorder) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// P1-020: bufio defers small writes to Flush — a flush failure loses
	// buffered evidence, so count it here too (emit-time counting alone
	// would miss it).
	pending := r.w.Buffered()
	if err := r.w.Flush(); err != nil {
		r.failed++
		r.lostBytes += int64(pending)
		return err
	}
	return nil
}

type lenCounts map[int]int

func (l lenCounts) add(n int) { l[n]++ }

// P1-145: the summary record must carry the total WS frame count alongside
// the decoded-length histogram — the emit once dropped HFTFrames, losing the
// total from the JSONL. Note the denominators differ: hftFrames counts every
// WS frame (incl. non-binary keepalives), hft only decoded LTP/full payloads.
func summaryRecord(hft lenCounts, hftFrames int) map[string]any {
	return map[string]any{"kind": "summary", "hft": hft, "hftFrames": hftFrames}
}

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s=%q not an int, using %d\n", name, v, def)
		return def
	}
	return n
}

func envBool(name string, def bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s=%q not a bool, using %v\n", name, v, def)
		return def
	}
	return b
}

func main() {
	if os.Getenv("ARROW_APP_ID") == "" {
		fmt.Fprintln(os.Stderr, "ARROW_APP_ID is required (see .env)")
		os.Exit(2)
	}
	if os.Getenv("ARROW_APP_SECRET") == "" {
		fmt.Fprintln(os.Stderr, "ARROW_APP_SECRET is required")
		os.Exit(2)
	}
	if os.Getenv("ARROW_USER_ID") == "" || os.Getenv("ARROW_PASSWORD") == "" || os.Getenv("ARROW_TOTP_KEY") == "" {
		fmt.Fprintln(os.Stderr, "need ARROW_USER_ID+PASSWORD+TOTP_KEY (ARROW_TOKEN removed 2026-08-24, TOTP only)")
		os.Exit(2)
	}

	outPath := os.Getenv("CAPTURE_OUT")
	if outPath == "" {
		outPath = "marketdata-capture.jsonl"
	}
	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", outPath, err)
		os.Exit(2)
	}
	rec := &recorder{w: bufio.NewWriter(f), file: f}
	defer func() {
		_ = rec.close()
		_ = f.Close()
	}()

	tokens := []int32{2885, 1333, 3045, 11536, 1594} // RELIANCE, HDFCBANK, SBIN, TCS, INFY
	if v := os.Getenv("CAPTURE_TOKENS"); v != "" {
		tokens = tokens[:0]
		for _, p := range strings.Split(v, ",") {
			n, err := strconv.ParseInt(strings.TrimSpace(p), 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bad token %q\n", p)
				os.Exit(2)
			}
			tokens = append(tokens, int32(n))
		}
	}
	duration := time.Duration(envInt("CAPTURE_DURATION", 20)) * time.Second
	// P1-285: fail fast on unvalidated CAPTURE_DURATION — <=0 makes
	// context.WithTimeout expire immediately (an empty capture still
	// reported "capture complete"), and huge values overflow time.Duration.
	if duration <= 0 || duration > 10*time.Minute {
		fmt.Fprintf(os.Stderr, "CAPTURE_DURATION=%v out of range (must be >0 and <=10m, e.g. 1..600s)\n", duration)
		os.Exit(2)
	}

	client := arrow.NewClient(os.Getenv("ARROW_APP_ID"), os.Getenv("ARROW_APP_SECRET"))

	// P1-019: TOTP-only — the request-token arm was dead (the gate above
	// exits unless TOTP creds exist, so token-only setups never got here).
	if err := client.AutoLogin(os.Getenv("ARROW_USER_ID"), os.Getenv("ARROW_PASSWORD"), os.Getenv("ARROW_TOTP_KEY")); err != nil {
		fmt.Fprintf(os.Stderr, "AutoLogin failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "AutoLogin OK, token len %d\n", len(client.GetToken()))

	if !envBool("CAPTURE_HFT", true) {
		fmt.Fprintln(os.Stderr, "CAPTURE_HFT=0 — nothing to capture")
		return
	}

	summary := struct {
		HFT       lenCounts `json:"hft"`
		HFTFrames int       `json:"hftFrames"`
	}{HFT: lenCounts{}}
	var smu sync.Mutex

	captureHFT(rec, client, tokens, duration, &summary, &smu)

	_ = rec.emit(summaryRecord(summary.HFT, summary.HFTFrames))
	// P1-020 fail-loud: flush explicitly (os.Exit skips the deferred
	// close) and refuse to report success when evidence was lost.
	if err := rec.close(); err != nil {
		fmt.Fprintf(os.Stderr, "capture: final flush failed: %v\n", err)
		os.Exit(1)
	}
	if n, lost := rec.stats(); n > 0 {
		fmt.Fprintf(os.Stderr, "capture: %d evidence writes failed (~%d bytes lost) -> %s is INCOMPLETE\n",
			n, lost, outPath)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "capture complete: hft=%v -> %s\n", summary.HFT, outPath)
}

func captureHFT(rec *recorder, client *arrow.Client, tokens []int32, dur time.Duration, summary *struct {
	HFT       lenCounts `json:"hft"`
	HFTFrames int       `json:"hftFrames"`
}, smu *sync.Mutex) {
	hds, err := client.ConnectHFTDataStream()
	if err != nil {
		_ = rec.emit(map[string]any{"kind": "connect", "feed": "hft", "ok": false, "err": err.Error()})
		fmt.Fprintf(os.Stderr, "hft connect failed: %v\n", err)
		return
	}
	defer hds.Close()
	_ = rec.emit(map[string]any{"kind": "connect", "feed": "hft", "ok": true})

	latency := envInt("ARROW_HFT_LATENCY_MS", 50)
	for _, mode := range []string{"ltpc", "full"} {
		err := hds.SubscribeHFTTokens(mode, 0, tokens, latency)
		_ = rec.emit(map[string]any{"kind": "subscribe", "feed": "hft", "mode": mode, "ok": err == nil, "err": errStr(err)})
		fmt.Fprintf(os.Stderr, "hft subscribe mode=%s ok=%v err=%v\n", mode, err == nil, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	hds.ReadHFTWithFrame(ctx,
		func(tick arrow.HFTLTPTick) {},
		func(tick arrow.HFTFullTick) {},
		func(pkt arrow.HFTResponsePacket) {
			_ = rec.emit(map[string]any{"kind": "response", "feed": "hft", "frameSize": pkt.FrameSize, "pktType": pkt.PktType, "errorCode": pkt.ErrorCode, "errorMsg": pkt.ErrorMsg, "requestType": pkt.RequestTypeStr, "mode": pkt.ModeStr, "successCount": pkt.SuccessCount, "errorCount": pkt.ErrorCount})
			fmt.Fprintf(os.Stderr, "hft response: type=%s mode=%s success=%d errors=%d code=%q msg=%q\n", pkt.RequestTypeStr, pkt.ModeStr, pkt.SuccessCount, pkt.ErrorCount, pkt.ErrorCode, pkt.ErrorMsg)
		},
		func(mt int, payload []byte) {
			smu.Lock()
			summary.HFTFrames++
			smu.Unlock()
			_ = rec.emit(map[string]any{"kind": "frame", "feed": "hft", "mt": mt, "len": len(payload)})
		},
		func(payload []byte) {
			smu.Lock()
			summary.HFT.add(len(payload))
			smu.Unlock()
			_ = rec.emit(map[string]any{"kind": "raw", "feed": "hft", "len": len(payload), "hex": hex.EncodeToString(payload)})
		},
		func(err error) {
			if ctx.Err() != nil {
				return
			}
			_ = rec.emit(map[string]any{"kind": "error", "feed": "hft", "err": err.Error()})
			fmt.Fprintf(os.Stderr, "hft read error: %v\n", err)
		})
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
