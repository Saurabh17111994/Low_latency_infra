package com.trading.compute.signaljob;

import java.io.Serializable;

/**
 * In-window candle accumulator — the ONLY candle state that exists.
 *
 * <p>Deliberately compact: OHLCV plus the identity columns and the
 * {@code (event_time, fingerprint)} order keys used to pick open/close
 * deterministically under out-of-order arrival and replay. No tick list, no
 * raw payloads, no fingerprint lists are ever retained (Signal dossier: "no
 * tick collection exists in active state").
 *
 * <p>D1 (2026-08-30): class and fields MUST stay public. Flink POJO
 * recognition requires a public class with public fields (or getters);
 * anything less makes the type extractor fall back to GenericType/Kryo —
 * observed as a JM warning on every job submit and slower state
 * serialization. Guard: CandlePojoRecognitionTest asserts this class
 * extracts as a POJO, not a GenericType.
 */
public class CandleAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Public no-arg ctor required for Flink POJO extraction — must stay. */
    public CandleAccumulator() {}

    public String exchange;
    public String symbol;

    public long openPaise;
    public long highPaise;
    public long lowPaise;
    public long closePaise;
    public long volume;
    public long tickCount;

    /** True when no tick has been ingested (firstEventTime sentinel). Do not emit or merge OHLC when true. */
    public boolean isEmpty() {
        return firstEventTime == Long.MAX_VALUE;
    }

    /** Order key of the window's earliest event; open = its price. */
    public long firstEventTime = Long.MAX_VALUE;
    public String firstFingerprint = "";

    /** Order key of the window's latest event; close = its price. */
    public long lastEventTime = Long.MIN_VALUE;
    public String lastFingerprint = "";

    /**
     * Ingest wall-clock (raw {@code ingest_ts}) of the tick that set the
     * window's close ({@code lastEventTime}). Observability only (latency
     * probe): NOT written to any output row, NOT part of any table schema,
     * never used by candle math. It rides the accumulator through the heap
     * chain so a sink-side monitor can report
     * {@code output_now - lastIngestTs} = age of the newest tick that formed
     * the candle.
     *
     * <p>Unset sentinel: no close-setting tick with a non-null ingest_ts seen
     * yet. Monitors must treat this as unknown, not 1970.
     */
    public long lastIngestTs = Long.MIN_VALUE;

    /** Reset to sentinel defaults, preserving object identity. */
    public void clear() {
        exchange = null;
        symbol = null;
        openPaise = 0L;
        highPaise = 0L;
        lowPaise = 0L;
        closePaise = 0L;
        volume = 0L;
        tickCount = 0L;
        firstEventTime = Long.MAX_VALUE;
        firstFingerprint = "";
        lastEventTime = Long.MIN_VALUE;
        lastFingerprint = "";
        lastIngestTs = Long.MIN_VALUE;
    }
}
