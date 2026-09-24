package main

// volumeDeltaTracker turns the feeds' CUMULATIVE day volume into a per-tick
// traded quantity (raw_table_1.volume_delta).
//
// Both feeds report cumulative volume, so a periodic snapshot row with no trade
// repeats the previous value. A consumer that treats every row as a trade then
// counts that snapshot as volume — measured on the standard stream: 5 of 6
// repeated tokens had a zero delta and still looked like trades. The delta makes
// "nothing traded" explicit (0) and "cannot be known" explicit (nil), so a
// candle can sum increments instead of guessing.
//
// Not safe for concurrent use: one tracker per epoch, driven only by that
// epoch's read goroutine (guardSingleReader enforces a single reader).
type volumeDeltaTracker struct {
	last map[int32]int64
}

func newVolumeDeltaTracker() *volumeDeltaTracker {
	return &volumeDeltaTracker{last: make(map[int32]int64)}
}

// deltaFor records cumulative for token and returns how much traded since the
// previous tick. nil means the baseline is unknown — that token's first tick in
// this epoch, or the counter moved backwards (an exchange reset would otherwise
// yield a negative or absurd delta). Unknown is reported as unknown, never as 0.
func (v *volumeDeltaTracker) deltaFor(token int32, cumulative int64) *int64 {
	prev, seen := v.last[token]
	v.last[token] = cumulative
	if !seen || cumulative < prev {
		return nil
	}
	d := cumulative - prev
	return &d
}
