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
        // P2-150: validate identity/order/nulls BEFORE the copy — a list
        // with duplicates, wrong order or nulls would silently misalign
        // downstream ordinal indexing (frames.get(tf.ordinal())). Fails
        // before allocating 6 + up to 90 copies on the error path.
        Timeframe[] expected = Timeframe.values();
        for (int i = 0; i < expected.length; i++) {
            TimeframeContext f = frames.get(i);
            Objects.requireNonNull(f, "frames[" + i + "]");
            if (f.tf() != expected[i]) {
                throw new IllegalArgumentException(
                        "frames[" + i + "] must be " + expected[i] + ", got " + f.tf());
            }
        }
        // P2-044: single copy lives in the type — rebuild each frame via the
        // inner copy so later caller changes never touch sent data. The
        // caller passes live refs; no pre-copy needed (one copy per tick,
        // not two). Mutable ArrayList for Kryo GenericType ser/de
        // (unmodifiable fails Kryo copy).
        List<TimeframeContext> copy = new ArrayList<>(frames.size());
        for (TimeframeContext f : frames) {
            Objects.requireNonNull(f, "frames element");
            copy.add(new TimeframeContext(f.tf(), f.forming(), f.closedNewestFirst()));
        }
        this.frames = copy;
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
            Objects.requireNonNull(forming, "forming");
            // P2-045: field-by-field copy — CandleAccumulator fields are
            // public mutable, so retaining the caller ref is a live
            // corruption vector. The type enforces its own copy contract.
            CandleAccumulator f = new CandleAccumulator();
            f.exchange = forming.exchange;
            f.symbol = forming.symbol;
            f.openPaise = forming.openPaise;
            f.highPaise = forming.highPaise;
            f.lowPaise = forming.lowPaise;
            f.closePaise = forming.closePaise;
            f.volume = forming.volume;
            f.tickCount = forming.tickCount;
            f.firstEventTime = forming.firstEventTime;
            f.firstFingerprint = forming.firstFingerprint;
            f.lastEventTime = forming.lastEventTime;
            f.lastFingerprint = forming.lastFingerprint;
            f.lastIngestTs = forming.lastIngestTs;
            this.forming = f;
            Objects.requireNonNull(closedNewestFirst, "closedNewestFirst");
            // P2-046: deep-copy each element — ClosedCandle fields are public
            // mutable, so a shallow list copy still shares live objects.
            // Mutable ArrayList for Kryo (unmodifiable fails Kryo copy).
            List<ClosedCandle> copy = new ArrayList<>(closedNewestFirst.size());
            for (ClosedCandle c : closedNewestFirst) {
                Objects.requireNonNull(c, "closedNewestFirst element");
                copy.add(new ClosedCandle(c.windowStart, c.windowEnd, c.openPaise, c.highPaise,
                        c.lowPaise, c.closePaise, c.volume, c.tickCount, c.lastEventTime,
                        c.lastFingerprint));
            }
            this.closedNewestFirst = copy;
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
