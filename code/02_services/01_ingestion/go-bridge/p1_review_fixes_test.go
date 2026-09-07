// Regression tests for P1 review findings P1-209, P1-156, P1-158, P1-212.
// Each test FAILS without its fix (proven via git stash of the non-test
// files) and passes with it.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// TestP1209FinalReportGuardedWhenDisabled — disabled tick counters must be
// side-effect free at shutdown: no counts file, no report.
func TestP1209FinalReportGuardedWhenDisabled(t *testing.T) {
	oldOn := tickCountsOn
	oldPath := tickCountsFilePath
	tickCountsOn = false
	tickCountsFilePath = filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	finalTickCountReport = sync.Once{}
	defer func() {
		tickCountsOn = oldOn
		tickCountsFilePath = oldPath
		finalTickCountReport = sync.Once{}
	}()

	maybeReportFinalTickCounts()

	if _, err := os.Stat(tickCountsFilePath); !os.IsNotExist(err) {
		t.Fatalf("P1-209: disabled shutdown must not write %s (err=%v)", tickCountsFilePath, err)
	}
}

// TestP1209FinalReportWrittenWhenEnabled — the guard must not break the
// enabled path: the shutdown report is still emitted exactly once.
func TestP1209FinalReportWrittenWhenEnabled(t *testing.T) {
	oldOn := tickCountsOn
	oldPath := tickCountsFilePath
	tickCountsOn = true
	tickCountsFilePath = filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	finalTickCountReport = sync.Once{}
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{7: 1}
	tickCountsMu.Unlock()
	defer func() {
		tickCountsOn = oldOn
		tickCountsFilePath = oldPath
		finalTickCountReport = sync.Once{}
		tickCountsMu.Lock()
		tickCounts = nil
		tickCountsMu.Unlock()
	}()

	maybeReportFinalTickCounts()

	raw, err := os.ReadFile(tickCountsFilePath)
	if err != nil {
		t.Fatalf("enabled shutdown must write the report: %v", err)
	}
	if !strings.Contains(string(raw), "arrow-tick-counts: total=1") {
		t.Fatalf("report missing totals, got %q", raw)
	}
}

// TestP1156MalformedCSVFailsClosed — a ragged manifest row (field-count
// mismatch) must fail startup (exitFatalStart), not silently truncate the
// token plan. os.Exit needs the exec-self subprocess pattern.
func TestP1156MalformedCSVFailsClosed(t *testing.T) {
	if os.Getenv("GO_P1156_HELPER") == "1" {
		loadTokensFromCSV("GO_P1156_TOKENS_UNSET", os.Getenv("GO_P1156_CSV"))
		return
	}
	os.Unsetenv("GO_P1156_TOKENS_UNSET")
	bad := filepath.Join(t.TempDir(), "ragged.csv")
	if err := os.WriteFile(bad, []byte("Exchange,Segment,ExchSeg,Token\nNSE,CM,1,1001\nNSE,CM\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestP1156MalformedCSVFailsClosed")
	cmd.Env = append(os.Environ(), "GO_P1156_HELPER=1", "GO_P1156_CSV="+bad)
	out, err := cmd.CombinedOutput()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != exitFatalStart {
		t.Fatalf("P1-156: ragged CSV must exit %d, got exit=%v output=%s", exitFatalStart, err, out)
	}
}

// TestP1156WellFormedCSVLoads — the fail-closed fix must not reject valid
// manifests.
func TestP1156WellFormedCSVLoads(t *testing.T) {
	os.Unsetenv("GO_P1156_TOKENS_UNSET")
	good := filepath.Join(t.TempDir(), "good.csv")
	if err := os.WriteFile(good, []byte("Exchange,Segment,ExchSeg,Token\nNSE,CM,1,1001\nNSE,CM,1,1002\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := loadTokensFromCSV("GO_P1156_TOKENS_UNSET", good)
	if len(got) != 2 || got[0] != 1001 || got[1] != 1002 {
		t.Fatalf("valid CSV must yield [1001 1002], got %v", got)
	}
}

// TestP1158SupervisorStopsMetricsTicker — supervisor return must stop the
// metrics ticker goroutine even while the parent context is still alive
// (otherwise a bridge_metrics frame can land after the bridge_shutdown
// drain marker, and each invocation leaks a goroutine).
func TestP1158SupervisorStopsMetricsTicker(t *testing.T) {
	term := newFakeHFTStream()
	term.response = hftResponse("E_ALL_INVALID", 0, 1)
	makeFactory := func(_ *arrow.Client, slotIdx int) func() (hftStream, error) {
		return func() (hftStream, error) { return term, nil }
	}
	plan := SubscriptionPlan{Slots: []SlotAssignment{
		{SlotID: "hft-0", ConnectionID: "hft-0", Tokens: []int32{1}, Requests: [][]int32{{1}}},
	}}
	client := arrow.NewClient("app", "secret")
	client.SetToken("token")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := runtime.NumGoroutine()
	captureBridge(t, func() {
		// All slots go terminal on their own — the supervisor returns
		// without any cancel, so a still-running ticker would linger.
		runHFTSupervisorWithFactory(ctx, makeFactory, client, plan, 50, 10*time.Second, nil, t.Logf)
	})
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Fatalf("P1-158: supervisor leaked %d goroutine(s) after return (before=%d now=%d)", got-before, before, got)
	}
}

// TestP1212BatchMaxBytesAtFrameCapFatal — 64MiB (== maxFrameLen) and anything
// above the 16MiB margin must be a FATAL startup error: such a batch plus
// wire overhead deterministically fails 'frame too large'.
func TestP1212BatchMaxBytesAtFrameCapFatal(t *testing.T) {
	if os.Getenv("GO_P1212_HELPER") == "1" {
		os.Setenv("BRIDGE_BATCH_MAX_BYTES", os.Getenv("GO_P1212_VALUE"))
		batchLimitsFromEnv(func(string, ...any) {})
		return
	}
	os.Unsetenv("BRIDGE_BATCH_MAX_BYTES")
	for _, v := range []string{"67108864", "16777217"} {
		cmd := exec.Command(os.Args[0], "-test.run=TestP1212BatchMaxBytesAtFrameCapFatal")
		cmd.Env = append(os.Environ(), "GO_P1212_HELPER=1", "GO_P1212_VALUE="+v)
		out, err := cmd.CombinedOutput()
		ee, ok := err.(*exec.ExitError)
		if !ok || ee.ExitCode() != exitFatalStart {
			t.Fatalf("P1-212: BRIDGE_BATCH_MAX_BYTES=%s must exit %d, got exit=%v output=%s", v, exitFatalStart, err, out)
		}
	}
}

// TestP1212BatchMaxBytesAtMarginAccepted — the clamp must still allow the
// documented 16MiB ceiling.
func TestP1212BatchMaxBytesAtMarginAccepted(t *testing.T) {
	t.Setenv("BRIDGE_BATCH_MAX_BYTES", "16777216")
	got := batchLimitsFromEnv(func(string, ...any) { t.Fatal("no fatal expected") })
	if got.MaxBytes != 16777216 {
		t.Fatalf("MaxBytes = %d, want 16777216", got.MaxBytes)
	}
}

// p1FailTickEmitter fails every EmitTick while delegating everything else to
// the real test emitter.
type p1FailTickEmitter struct {
	Transport
	tickErr   error
	tickCalls atomic.Int32
}

func (f *p1FailTickEmitter) EmitTick(tick Tick, connectionID, slotID string, epoch uint64, received time.Time, rawPayload []byte) error {
	f.tickCalls.Add(1)
	return f.tickErr
}

// TestP1212EmitTickErrorsAreLogged — EmitTick failures in the tick dispatch
// path must be surfaced, never silently dropped.
func TestP1212EmitTickErrorsAreLogged(t *testing.T) {
	old := bridgeEmitter
	out := newSyncBuffer()
	fail := &p1FailTickEmitter{Transport: newTestProtoEmitter(out), tickErr: errors.New("frame too large: 67108865 bytes")}
	bridgeEmitter = fail
	defer func() { bridgeEmitter = old }()

	stream := newFakeHFTStream()
	stream.response = hftResponse("SUCCESS", 1, 0)
	stream.onDecodedFrame = []byte{0x28, 0x00, 0x00, 0x00, 0x01}
	stream.onLTPTick = arrow.HFTLTPTick{Token: 1001, LTP: 15051}
	stream.onFullTick = arrow.HFTFullTick{Token: 1001, LTP: 15051}

	var mu sync.Mutex
	var msgs []string
	logf := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		msgs = append(msgs, fmt.Sprintf(format, args...))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan slotEpochResult, 1)
	go func() {
		done <- runHFTEpoch(ctx, func() (hftStream, error) { return stream, nil },
			slotAssignment(1001), 50, 10*time.Second, 1, nil, nil, logf)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for fail.tickCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if fail.tickCalls.Load() < 2 {
		t.Fatalf("expected LTP+full ticks to reach EmitTick, got %d calls", fail.tickCalls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, m := range msgs {
		if strings.Contains(m, "emit tick failed") {
			return
		}
	}
	t.Fatalf("P1-212: EmitTick errors were silently dropped (no 'emit tick failed' log)\nlogs: %q", msgs)
}
