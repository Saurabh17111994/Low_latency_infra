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

    public String exchange;
    public String symbol;

    public long openPaise;
    public long highPaise;
    public long lowPaise;
    public long closePaise;
    public long volume;
    public long tickCount;

    /** Order key of the window's earliest event; open = its price. */
    public long firstEventTime = Long.MAX_VALUE;
    public String firstFingerprint;

    /** Order key of the window's latest event; close = its price. */
    public long lastEventTime = Long.MIN_VALUE;
    public String lastFingerprint;
}
