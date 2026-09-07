// tickcounts.go — count-based losslessness evidence (ING-TCP-001).
//
// ARROW_TICK_COUNTS=<intervalSeconds> enables a per-token count of every
// emitted tick, incremented by the proto emitter on its way into the batcher.
// The wire carries no sequence numbers that the reconcile can trust across
// reconnects, so losslessness is verified by reconciling these counts (source
// of truth: the bytes read off the broker socket) against the rows actually
// stored in Fluss per token. The counts are reported to a file AND mirrored to
// stderr on the interval and once at shutdown as:
//
//	arrow-tick-counts: total=N chunk=0/52 t=TOKEN:n ...   (20 pairs per line)

package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

var (
	tickCountsOn bool
	tickCountsMu sync.Mutex
	tickCounts   map[int32]int64
)

// finalTickCountReport guards the shutdown report so it is emitted exactly
// once even though both the interval goroutine (on ctx.Done) and main (after
// runHFT returns) may reach for it. main's synchronous call is authoritative:
// a goroutine-only report can be killed mid-write when Go main returns on
// SIGTERM, which would lose the ING-TCP-001 epoch count.
var finalTickCountReport sync.Once

// tickCountsFilePath is where the final report is ALSO written (in addition
// to stderr). The Java parent closes the child's pipe streams as soon as its
// own JVM shutdown begins, so a stderr-only report can be lost to SIGPIPE
// (exit 141) before the reconcile can read it. A file survives the teardown.
// Env: ARROW_TICK_COUNTS_FILE (default /tmp/arrow-tick-counts.txt).
var tickCountsFilePath = "/tmp/arrow-tick-counts.txt"

func recordTickCount(token int32) {
	tickCountsMu.Lock()
	if tickCounts == nil {
		tickCounts = map[int32]int64{}
	}
	tickCounts[token]++
	tickCountsMu.Unlock()
}

// tickCountChunkSize bounds each report line: Java's log handler truncates
// bridge stderr lines (measured 603 B), so the per-token report is emitted as
// multiple bounded lines instead of one long one.
var tickCountChunkSize = 20

func reportTickCounts() {
	// P1-210: snapshot under the lock, sort + format AFTER unlock — the
	// report must never stall the per-tick recordTickCount hot path
	// (ProtoEmitter.EmitTick) while sorting/formatting thousands of lines.
	tickCountsMu.Lock()
	snapshot := make(map[int32]int64, len(tickCounts))
	for t, n := range tickCounts {
		snapshot[t] = n
	}
	tickCountsMu.Unlock()
	keys := make([]int32, 0, len(snapshot))
	for t := range snapshot {
		keys = append(keys, t)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	total := int64(0)
	for _, t := range keys {
		total += snapshot[t]
	}
	lines := (len(keys) + tickCountChunkSize - 1) / tickCountChunkSize
	// P1-048: never emit an empty report — zero keys must still produce the
	// total=0 header line, or the file is truncated to 0 bytes with no stderr
	// marker and the reconcile cannot tell "zero ticks" from "report lost".
	if lines == 0 {
		lines = 1
	}
	// Build the full report in memory first.
	var buf strings.Builder
	for c := 0; c < lines; c++ {
		fmt.Fprintf(&buf, "arrow-tick-counts: total=%d chunk=%d/%d", total, c, lines)
		lo, hi := c*tickCountChunkSize, (c+1)*tickCountChunkSize
		if hi > len(keys) {
			hi = len(keys)
		}
		for _, t := range keys[lo:hi] {
			fmt.Fprintf(&buf, " t=%d:n=%d", t, snapshot[t])
		}
		buf.WriteByte('\n')
	}
	// Persist to a file FIRST (while stderr is still open). The parent JVM
	// closes the child's pipe streams as soon as its own shutdown begins, so
	// a stderr write after that point dies with SIGPIPE (exit 141) and the
	// ING-TCP-001 reconcile would lose the epoch count. The file survives.
	if err := os.WriteFile(tickCountsFilePath, []byte(buf.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "arrow-tick-counts: WARN file write failed %s: %v\n", tickCountsFilePath, err)
	}
	// Then mirror to stderr (best-effort; may die on SIGPIPE if the parent
	// already closed the pipe, but the file above is already written).
	fmt.Fprint(os.Stderr, buf.String())
}
