//go:build faketool

package main

import "testing"

// TestValidateRealRateHz — P1-025: hz<=0 must exit cleanly, never panic
// (divide-by-zero at 0, negative ticker interval below 0).
func TestValidateRealRateHz(t *testing.T) {
	for _, hz := range []int{1, 2, 4, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000} {
		if err := validateRealRateHz(hz); err != nil {
			t.Fatalf("hz=%d: got %v, want nil", hz, err)
		}
	}
	// P1-025 cases: 0 panicked on 1000%hz; negatives slipped through to NewTicker.
	for _, hz := range []int{0, -1, -20, -25, 3, 7, 1001} {
		if err := validateRealRateHz(hz); err == nil {
			t.Fatalf("hz=%d: got nil, want error", hz)
		}
	}
}
