package main

import "testing"

func TestVolumeDeltaTracker(t *testing.T) {
	v := newVolumeDeltaTracker()

	if d := v.deltaFor(101, 5_000); d != nil {
		t.Fatalf("first tick for a token must be unknown, got %d", *d)
	}
	if d := v.deltaFor(101, 5_000); d == nil || *d != 0 {
		t.Fatalf("repeat snapshot must be 0, got %v", d)
	}
	if d := v.deltaFor(101, 5_500); d == nil || *d != 500 {
		t.Fatalf("increase must be the difference, got %v", d)
	}
	if d := v.deltaFor(101, 300); d != nil {
		t.Fatalf("a backwards counter must be unknown, not negative, got %d", *d)
	}
	if d := v.deltaFor(101, 700); d == nil || *d != 400 {
		t.Fatalf("tracking resumes from the reset baseline, got %v", d)
	}
	// Tokens are tracked independently: one token's tick must not consume another's.
	if d := v.deltaFor(202, 9); d != nil {
		t.Fatalf("another token's first tick is unknown, got %d", *d)
	}
}
