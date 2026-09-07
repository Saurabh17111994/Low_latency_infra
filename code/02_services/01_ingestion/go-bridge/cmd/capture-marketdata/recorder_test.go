package main

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

// errWriter fails every write — the evidence-loss path (P1-020).
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("disk gone") }

// P1-020: evidence-write failures are counted with lost bytes, never
// swallowed — main exits non-zero off stats().
func TestRecorderCountsEvidenceLoss(t *testing.T) {
	r := &recorder{w: bufio.NewWriter(errWriter{})}
	// Overflow the 4 KiB bufio buffer so the write fails at emit time.
	big := map[string]any{"kind": "raw", "hex": strings.Repeat("ab", 5000)}
	if err := r.emit(big); err == nil {
		t.Fatal("emit must return the write error")
	}
	n, lost := r.stats()
	if n != 1 {
		t.Fatalf("failed = %d, want 1", n)
	}
	if lost <= 0 {
		t.Fatalf("lostBytes = %d, want > 0", lost)
	}
}

// P1-020: small writes sit in the bufio buffer — their loss surfaces at
// Flush, so close() must count it too, not just return the error.
func TestRecorderCountsFlushTimeLoss(t *testing.T) {
	r := &recorder{w: bufio.NewWriter(errWriter{})}
	if err := r.emit(map[string]any{"kind": "raw"}); err != nil {
		t.Fatalf("small emit buffers, want nil err, got %v", err)
	}
	if err := r.close(); err == nil {
		t.Fatal("close must return the flush error")
	}
	n, lost := r.stats()
	if n != 1 {
		t.Fatalf("failed = %d, want 1", n)
	}
	if lost <= 0 {
		t.Fatalf("lostBytes = %d, want > 0", lost)
	}
}
