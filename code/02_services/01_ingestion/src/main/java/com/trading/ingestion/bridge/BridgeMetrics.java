package com.trading.ingestion.bridge;

/**
 * Supervisor health snapshot (proto control record {@code "bridge_metrics"}).
 *
 * <p>Additive v2 extension of the bridge NDJSON contract: the Go supervisor
 * emits one line every 10s with the values it is authoritative for — the
 * largest per-slot consecutive-reconnect streak, the number of live HFT
 * sockets (orphaned streams stay visible because the Go side counts
 * open/close, not lifecycle events), and the bridge process goroutine count.
 * Ingestion maps these onto the process-level gauges
 * {@code bridge.reconnect_consecutive}, {@code bridge.active_sockets} and
 * {@code bridge.go_goroutines}.
 */
public record BridgeMetrics(long tsMs, int reconnectConsecutive, int activeSockets, long goGoroutines) {
    public BridgeMetrics {
        if (tsMs <= 0) throw new IllegalArgumentException("ts_ms must be positive");
        if (reconnectConsecutive < 0) throw new IllegalArgumentException("reconnect_consecutive must be non-negative");
        if (activeSockets < 0) throw new IllegalArgumentException("active_sockets must be non-negative");
        if (goGoroutines < 0) throw new IllegalArgumentException("go_goroutines must be non-negative");
    }
}
