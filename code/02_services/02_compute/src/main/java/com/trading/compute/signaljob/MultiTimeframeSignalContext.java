package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * In-JVM signal context side-output from {@link MultiTimeframeAggregateFunction}.
 *
 * <p>Per-tick snapshot AFTER applying the trade (Decision 6: signals consume forming per-tick).
 * Carries 6 forming accumulators (live, mutable per TF) plus the last-15 closed rings per TF
 * (newest-first copies), plus instrument identity. Transport is a Flink side-output
 * {@code OutputTag&lt;MultiTimeframeSignalContext&gt;} — heap only, zero Fluss reads on the
 * hot path. Copies are deep: forming and closed lists do not alias live state (see §E.3).
 *
 * <p>Pinned API — data-quality suite reflects against this shape:
 * {@code instrumentToken(), eventTime(), timeframeCount()==6, frames()} and
 * {@code TimeframeContext(tf, forming, closedNewestFirst)}.
 */
public final class MultiTimeframeSignalContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long instrumentToken;
    private final String exchange;
    private final String symbol;
    private final long eventTime;
    private final List<TimeframeContext> frames;

    /**
     * @param instrumentToken instrument key
     * @param exchange exchange code (nullable)
     * @param symbol symbol code (nullable)
     * @param eventTime event_time of the tick that produced this snapshot (after mutation)
     * @param frames 6 entries, one per {@link Timeframe#values()} order
     */
    public MultiTimeframeSignalContext(long instrumentToken, String exchange, String symbol,
            long eventTime, List<TimeframeContext> frames) {
        this.instrumentToken = instrumentToken;
        this.exchange = exchange;
        this.symbol = symbol;
        this.eventTime = eventTime;
        Objects.requireNonNull(frames, "frames");
        if (frames.size() != Timeframe.values().length) {
            throw new IllegalArgumentException("frames must contain exactly 6 TimeframeContext entries, got " + frames.size());
        }
        // Defensive copy — mutable ArrayList for Kryo GenericType ser/de (unmodifiable fails Kryo copy)
        this.frames = new ArrayList<>(frames);
    }

    public long instrumentToken() {
        return instrumentToken;
    }

    public String exchange() {
        return exchange;
    }

    public String symbol() {
        return symbol;
    }

    public long eventTime() {
        return eventTime;
    }

    /** Always 6 (one per Timeframe). */
    public int timeframeCount() {
        return frames.size();
    }

    public List<TimeframeContext> frames() {
        return Collections.unmodifiableList(frames);
    }

    /**
     * Per-timeframe view: forming accumulator (live) plus newest-first closed ring snapshot.
     * Both are deep copies — mutating the returned objects never aliases operator state.
     */
    public static final class TimeframeContext implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Timeframe tf;
        private final CandleAccumulator forming;
        private final List<ClosedCandle> closedNewestFirst;

        public TimeframeContext(Timeframe tf, CandleAccumulator forming,
                List<ClosedCandle> closedNewestFirst) {
            this.tf = Objects.requireNonNull(tf, "tf");
            this.forming = Objects.requireNonNull(forming, "forming");
            Objects.requireNonNull(closedNewestFirst, "closedNewestFirst");
            // Defensive copy — mutable for Kryo (caller already deep-copies ClosedCandle)
            this.closedNewestFirst = new ArrayList<>(closedNewestFirst);
        }

        /** Timeframe discriminator (e.g. FIFTEEN_S). */
        public Timeframe tf() {
            return tf;
        }

        /** Alias for {@link #tf()} — some suites query via timeframe(). */
        public Timeframe timeframe() {
            return tf;
        }

        public Timeframe getTimeframe() {
            return tf;
        }

        /** Live forming accumulator snapshot for this TF (deep copy, may be empty if no tick yet in window). */
        public CandleAccumulator forming() {
            return forming;
        }

        public CandleAccumulator getForming() {
            return forming;
        }

        /** Last-15 closed candles, newest-first (index 0 = most recent). Deep copies, size 0..15. */
        public List<ClosedCandle> closedNewestFirst() {
            return Collections.unmodifiableList(closedNewestFirst);
        }

        public List<ClosedCandle> closed() {
            return Collections.unmodifiableList(closedNewestFirst);
        }

        public List<ClosedCandle> getClosedNewestFirst() {
            return Collections.unmodifiableList(closedNewestFirst);
        }
    }
}
