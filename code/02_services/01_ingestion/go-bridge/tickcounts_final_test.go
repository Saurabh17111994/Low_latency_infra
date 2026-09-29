package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// L4-2: the tick-count report must always be a COMPLETE file. The interval
// ticker and the final Once can report concurrently; these tests pin the
// ordering (the newest snapshot wins), the atomic replace (rename, never a
// truncating in-place write) and reader-visible completeness.

// TestReportTickCountsFinalWinsDeterministically — the clobber case: report A
// snapshots, report B snapshots (newer), B writes, A writes → the file holds
// A's older counts. Taking the report lock BEFORE the snapshot serializes the
// reports, so A completes first and B's newer snapshot lands last.
//
// The hook makes the buggy interleaving deterministic: when a report can
// snapshot before taking the lock, B reaches the hook (bSnapped) while A is
// held, is released first, writes, and only then A writes its older snapshot.
// In the fixed order B never reaches the hook (it blocks on the lock), so the
// bSnapped/bDone waits time out and A finishes first — the assertion below is
// the same in both orders.
func TestReportTickCountsFinalWinsDeterministically(t *testing.T) {
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{7: 1}
	oldPath := tickCountsFilePath
	tickCountsFilePath = filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	tickCountsMu.Unlock()
	defer func() {
		tickReportAfterSnapshotHook = nil
		tickCountsMu.Lock()
		tickCountsFilePath = oldPath
		tickCounts = nil
		tickCountsMu.Unlock()
	}()

	aSnapped := make(chan struct{})
	bSnapped := make(chan struct{})
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	bDone := make(chan struct{})
	var calls atomic.Int32
	tickReportAfterSnapshotHook = func() {
		if calls.Add(1) == 1 {
			close(aSnapped)
			<-releaseA
			return
		}
		close(bSnapped)
		<-releaseB
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); reportTickCounts() }()
	<-aSnapped // A has snapshotted {7:1} and holds the report lock

	// Newer counts arrive while A is held mid-flight.
	tickCountsMu.Lock()
	tickCounts[7] = 2
	tickCountsMu.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()
		reportTickCounts()
		close(bDone)
	}()
	select {
	case <-bSnapped:
		// Pre-fix order: B snapshotted {7:2} before the lock; let it write first.
	case <-time.After(500 * time.Millisecond):
		// Fixed order: B is blocked on the lock A holds.
	}
	close(releaseB)
	select {
	case <-bDone:
	case <-time.After(500 * time.Millisecond):
	}
	close(releaseA)
	wg.Wait()

	raw, err := os.ReadFile(tickCountsFilePath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if !strings.Contains(string(raw), "t=7:n=2") {
		t.Fatalf("the file must hold the newest snapshot (L4-2 lock-first), got %q", raw)
	}
}

// TestReportTickCountsAtomicRenameChangesInode — the replace must be a rename,
// not an in-place rewrite: the old file is never truncated, and the new one
// appears whole.
func TestReportTickCountsAtomicRenameChangesInode(t *testing.T) {
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{7: 3}
	oldPath := tickCountsFilePath
	tickCountsFilePath = filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	tickCountsMu.Unlock()
	defer func() {
		tickCountsMu.Lock()
		tickCountsFilePath = oldPath
		tickCounts = nil
		tickCountsMu.Unlock()
	}()

	reportTickCounts()
	first, err := os.Stat(tickCountsFilePath)
	if err != nil {
		t.Fatalf("stat first report: %v", err)
	}
	tickCountsMu.Lock()
	tickCounts[7] = 4
	tickCountsMu.Unlock()
	reportTickCounts()
	second, err := os.Stat(tickCountsFilePath)
	if err != nil {
		t.Fatalf("stat second report: %v", err)
	}
	if os.SameFile(first, second) {
		t.Fatalf("the report must be replaced by rename (new inode), not rewritten in place")
	}
	raw, err := os.ReadFile(tickCountsFilePath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if !strings.Contains(string(raw), "t=7:n=4") {
		t.Fatalf("the new report must be complete, got %q", raw)
	}
}

// TestReportTickCountsFailedWriteKeepsPreviousReport — when the atomic write
// fails (here: an unwritable directory), the previous complete report survives
// and no truncated/0-byte file appears.
func TestReportTickCountsFailedWriteKeepsPreviousReport(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions; the fail path needs a non-root user")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "arrow-tick-counts.txt")
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{7: 3}
	oldPath := tickCountsFilePath
	tickCountsFilePath = path
	tickCountsMu.Unlock()
	defer func() {
		os.Chmod(dir, 0o755)
		tickCountsMu.Lock()
		tickCountsFilePath = oldPath
		tickCounts = nil
		tickCountsMu.Unlock()
	}()

	reportTickCounts()
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read first report: %v", err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	tickCountsMu.Lock()
	tickCounts[7] = 4
	tickCountsMu.Unlock()
	reportTickCounts() // must fail at temp creation; previous report stays

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the previous report must survive a failed write: %v", err)
	}
	if string(raw) != string(first) {
		t.Fatalf("a failed write must leave the previous complete report; got %q want %q", raw, first)
	}
}

// TestReportTickCountsCompleteFileReaderStress — readers must never observe a
// partial or empty report while writers replace it concurrently.
func TestReportTickCountsCompleteFileReaderStress(t *testing.T) {
	tickCountsMu.Lock()
	tickCounts = map[int32]int64{}
	oldPath := tickCountsFilePath
	tickCountsFilePath = filepath.Join(t.TempDir(), "arrow-tick-counts.txt")
	tickCountsMu.Unlock()
	defer func() {
		tickCountsMu.Lock()
		tickCountsFilePath = oldPath
		tickCounts = nil
		tickCountsMu.Unlock()
	}()

	reportTickCounts() // seed a complete report

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := int64(1); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			tickCountsMu.Lock()
			tickCounts[7] = i
			tickCountsMu.Unlock()
			reportTickCounts()
		}
	}()

	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 400; i++ {
				raw, err := os.ReadFile(tickCountsFilePath)
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
				s := string(raw)
				if !strings.HasPrefix(s, "arrow-tick-counts: total=") ||
					!strings.HasSuffix(s, "\n") ||
					!strings.Contains(s, "chunk=0/1") {
					t.Errorf("reader observed an incomplete report: %q", s)
					return
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	writer.Wait()
}
