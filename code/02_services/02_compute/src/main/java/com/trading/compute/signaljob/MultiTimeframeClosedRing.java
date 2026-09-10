package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Bounded ring of the last N closed candles for one (instrument, tf).
 *
 * <p>Capacity 15 per design Decision 6 (last 15 closed per TF for signal
 * lookback). Plain {@link Serializable} heap structure — NOT Flink managed
 * state. The enclosing {@link MultiTimeframeState} holds six of these, one
 * per {@link Timeframe}. Eviction is oldest-first when the 16th add arrives.
 *
 * <p>Backed by an {@link ArrayDeque} (fixed capacity, O(1) add/remove).
 * Snapshot order is newest-first (index 0 = most recent close), matching the
 * lookback requirement {@code closedRing[tf].size() >=15} for warm-up.
 */
public class MultiTimeframeClosedRing implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Design Decision 6: last 15 closed per TF. */
    public static final int CAPACITY = 15;

    private final ArrayDeque<ClosedCandle> deque;

    /** Public no-arg constructor for Flink serialization / POJO nesting. */
    public MultiTimeframeClosedRing() {
        this.deque = new ArrayDeque<>(CAPACITY);
    }

    /**
     * Append a closed candle, evicting the oldest when at capacity.
     *
     * @param candle closed lite to add (must be non-null)
     */
    public void add(ClosedCandle candle) {
        if (candle == null) {
            throw new NullPointerException("candle must not be null");
        }
        if (deque.size() >= CAPACITY) {
            deque.removeFirst();
        }
        deque.addLast(candle);
    }

    /** Number of candles currently buffered (0..15). */
    public int size() {
        return deque.size();
    }

    /** True when no candles have been added yet. */
    public boolean isEmpty() {
        return deque.isEmpty();
    }

    /**
     * Get candle by newest-first index.
     *
     * @param index 0 = newest, size-1 = oldest
     * @return the candle at that position
     * @throws IndexOutOfBoundsException if index out of range
     */
    public ClosedCandle get(int index) {
        if (index < 0 || index >= deque.size()) {
            throw new IndexOutOfBoundsException(
                    "index " + index + " out of bounds for size " + deque.size());
        }
        // deque is oldest-first (head=oldest). Convert newest-first index.
        int idxFromOldest = deque.size() - 1 - index;
        int i = 0;
        for (ClosedCandle c : deque) {
            if (i == idxFromOldest) {
                return c;
            }
            i++;
        }
        // Should never reach here given bounds check.
        throw new IndexOutOfBoundsException("index " + index);
    }

    /**
     * Snapshot of buffered candles in newest-first order (index 0 = newest).
     *
     * @return new mutable list, newest first; empty when ring is empty
     */
    public List<ClosedCandle> snapshotNewestFirst() {
        if (deque.isEmpty()) {
            return new ArrayList<>(0);
        }
        List<ClosedCandle> out = new ArrayList<>(deque.size());
        // Iterate deque in reverse (newest first). ArrayDeque has descendingIterator.
        var it = deque.descendingIterator();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    /**
     * Snapshot in oldest-first order (insertion order). Useful for debugging.
     *
     * @return new list, oldest first
     */
    public List<ClosedCandle> snapshotOldestFirst() {
        return new ArrayList<>(deque);
    }

    /** Clear all buffered candles. */
    public void clear() {
        deque.clear();
    }
}
