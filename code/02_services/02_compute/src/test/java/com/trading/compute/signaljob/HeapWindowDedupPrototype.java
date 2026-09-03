package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Step-0 prototype for the dedup redesign (plan
 * {@code docs/plans/2026-09-03-dedup-redesign-plan.md}). Operator-local
 * count-windowed repeat filter. TEST TREE ONLY — not production code. The Step
 * 1 implementation will be modeled on this after the speed decision.
 *
 * <p>Per key (instrument token): insertion-ordered set of the last {@code
 * maxPerToken} fingerprint strings. Present = repeat (drop); absent = new
 * (add, auto-trim, pass). No Flink state, no TTL, no timers, no scans — the
 * bound is structural ({@code removeEldestEntry}), so expiry needs no
 * machinery at all.
 */
final class HeapWindowDedupPrototype extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** Fixed-capacity insertion-ordered set; trims itself on every insert. */
    private static final class Window extends LinkedHashMap<String, Boolean>
            implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int max;

        Window(int max) {
            super(Math.min(max * 2, 1 << 20), 0.75f, false);
            if (max < 1) {
                throw new IllegalArgumentException("max=" + max);
            }
            this.max = max;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > max;
        }
    }

    private final int maxPerToken;
    private final Map<Long, Window> windows = new HashMap<>();
    long firsts;
    long duplicates;

    HeapWindowDedupPrototype(int maxPerToken) {
        if (maxPerToken < 1) {
            throw new IllegalArgumentException("maxPerToken=" + maxPerToken);
        }
        this.maxPerToken = maxPerToken;
    }

    @Override
    public void open(OpenContext openContext) {
        windows.clear();
        firsts = 0;
        duplicates = 0;
    }

    @Override
    public void processElement(RowData row, Context ctx, Collector<RowData> out) {
        // Same identity contract as FingerprintDedupFunction: version + fingerprint,
        // scoped by the keyBy key (instrument token) via the per-key window.
        String fp = row.getString(RawTableColumns.FINGERPRINT_VERSION).toString()
                + "|" + row.getString(RawTableColumns.EVENT_FINGERPRINT).toString();
        Window w = windows.computeIfAbsent(ctx.getCurrentKey(), k -> new Window(maxPerToken));
        if (w.put(fp, Boolean.TRUE) != null) {
            duplicates++;
            return;
        }
        // G-DEDUP-1 pattern: the bound is structural, so reaching here with an
        // overgrown window means trimming broke — fail loud, never grow silent.
        if (w.size() > maxPerToken) {
            throw new IllegalStateException(
                    "dedup window overgrew bound: size=" + w.size() + " max=" + maxPerToken);
        }
        firsts++;
        out.collect(row);
    }
}
