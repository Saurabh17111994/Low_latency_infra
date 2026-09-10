package com.trading.compute.telemetry;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OTLP/HTTP log emitter for the Signal job's {@code trading_alerts} stream.
 *
 * <p><b>Scope after CHG-023 item 1 (2026-08-17):</b> the METRIC half of the old
 * {@code ComputeOtlpEmitter} was replaced by Flink's native {@code flink-metrics-otel}
 * reporter — every counter/gauge the emitter mirrored is now a Flink
 * {@code MetricGroup} metric exported by {@code OpenTelemetryMetricReporter}
 * (DELTA sums for counters, gauges for state sizes — same alert semantics).
 * This class survives ONLY for what a metrics reporter cannot do: deterministic,
 * synchronous, user-code-observable lifecycle LOG events (schema-preflight
 * failure, startup mode, restore start) that happen on the submitting client
 * BEFORE {@code env.execute} — a periodic reporter flush would never run for
 * them. Best-effort: a collector outage must never fail the job, so any
 * failure is logged and swallowed.
 *
 * <p>Runtime failure events (checkpoint failure, restart, sink failure) are
 * NOT hooked here — Flink 2.2.1 exposes no user-code checkpoint-failure/
 * restart callback; those are covered by the O2 alert rules (metrics path,
 * now via the native reporter) + the {@code flink_logs} stream.
 */
public final class ComputeAlertLogs {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeAlertLogs.class);

    /**
     * Extra resource attributes (tracker 14 P8.0/831), configured once at job
     * startup via {@link #configureResourceAttributes(String...)}: flat
     * {@code key1,value1,key2,value2,...} pairs appended after the fixed
     * {@code service.name}/{@code service.instance.id} attributes. Empty until
     * configured. Volatile so the config write from the job thread is visible
     * to any concurrent emit; values are static, immutable-after-configure
     * strings.
     */
    private static volatile String[] extraResourceAttributes = new String[0];

    private ComputeAlertLogs() {}

    /**
     * Configures the extra resource attributes (tracker 14 P8.0/831):
     * environment, host, deployment version, job name, execution mode — never
     * credentials (the caller passes only known-safe config fields). Must be
     * called before the first emit (from {@code SignalJob.run}, before the
     * preflight/startup events fire). An odd argument count fails fast.
     */
    public static void configureResourceAttributes(String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "resource attributes must be key,value pairs — got " + keyValuePairs.length
                            + " arguments");
        }
        extraResourceAttributes = keyValuePairs.clone();
    }

    /** TEST-ONLY: resets the configured extras to none. */
    static void resetResourceAttributesForTest() {
        extraResourceAttributes = new String[0];
    }

    /**
     * Synchronous OTLP/HTTP log emit for the {@code trading_alerts} stream
     * (tracker 14 P8.0 box 828). Best-effort: a collector outage must never
     * fail the job, so any failure is logged and swallowed.
     *
     * @param collectorHostPort host:port of the collector (OTLP HTTP 4318)
     * @param severity INFO/WARN/ERROR (maps to OTLP severity text + number)
     * @param event stable event name, e.g. {@code startup-mode}
     * @param detail human-readable one-line detail (no credentials)
     */
    public static void emitAlertLog(String collectorHostPort, String severity,
            String event, String detail) {
        try {
            // P2-069: the payload build runs inside the best-effort guard — the
            // never-fail contract covers the build itself, not just the POST.
            String json = buildLogsJson(severity, event, detail);
            // P2-191: fail closed on injection-shaped input — an unvalidated
            // host:port can smuggle a path/authority override (?/#/@//) into
            // the URL and forces cleartext HTTP.
            if (collectorHostPort == null
                    || !collectorHostPort.matches("[A-Za-z0-9._-]+(:\\d+)?")) {
                LOG.warn("compute-otlp: invalid collectorHostPort '{}', event={} skipped",
                        collectorHostPort, event);
                return;
            }
            // TODO: support https:// collector endpoint for production (P2-191).
            URL url = URI.create("http://" + collectorHostPort + "/v1/logs").toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            // P2-192: 2s/2s — a dead collector stalls startup ~4s per event,
            // not ~10s (async is overkill for the 3 pre-execute events).
            conn.setConnectTimeout(2_000);
            conn.setReadTimeout(2_000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }
            // P2-193: always disconnect, and drain the body so keep-alive
            // connections are reusable instead of leaking sockets per outage.
            try {
                int code = conn.getResponseCode();
                try (var in = code >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
                    if (in != null) {
                        in.transferTo(OutputStream.nullOutputStream());
                    }
                } catch (Exception ignored) {
                    // Draining is best-effort; the status code is what matters.
                }
                if (code >= 400) {
                    // Collector down must never fail the job — telemetry is off
                    // the critical path (observability dossier contract).
                    LOG.warn("compute-otlp: alert log HTTP {} (collector={}, event={})",
                            code, collectorHostPort, event);
                }
            } finally {
                conn.disconnect();
            }
        } catch (Exception e) {
            LOG.debug("compute-otlp: alert log emit failed (collector unreachable?): {}",
                    e.getMessage());
        }
    }

    /**
     * OTLP/JSON logs payload: one log record with the event name, severity,
     * and detail plus the same resource attributes as the metrics payload used
     * to carry (service.name/instance.id + configured extras). Package-visible
     * for the payload-shape tests.
     *
     * <p>Null-safe by contract (P2-070): pre-execute callers pass unvalidated
     * args, so null severity/event/detail (or a null extras pair) degrades to
     * defaults instead of throwing — the sink function guards, not the callers.
     */
    static String buildLogsJson(String severity, String event, String detail) {
        long now = System.currentTimeMillis() * 1_000_000L; // epoch nanos
        String safeSeverity = severity == null ? "INFO" : severity;
        String safeEvent = event == null ? "unknown" : event;
        String safeDetail = detail == null ? "" : detail;
        int severityNumber = switch (safeSeverity) {
            case "INFO" -> 9;
            case "WARN" -> 13;
            case "ERROR" -> 17;
            default -> 9;
        };
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"resourceLogs\":[{\"resource\":{\"attributes\":[");
        appendAttr(sb, "service.name", "compute");
        appendAttr(sb, "service.instance.id", "signal-job");
        String[] extras = extraResourceAttributes;
        for (int i = 0; i < extras.length; i += 2) {
            appendAttr(sb, extras[i], extras[i + 1]);
        }
        sb.setLength(sb.length() - 1); // drop trailing comma after last attribute
        sb.append("]},\"scopeLogs\":[{\"scope\":{\"name\":\"compute\"},\"logRecords\":[")
          .append("{\"timeUnixNano\":\"").append(now).append("\",")
          .append("\"severityText\":\"").append(escapeJson(safeSeverity)).append("\",")
          .append("\"severityNumber\":").append(severityNumber).append(",")
          .append("\"body\":{\"stringValue\":\"").append(escapeJson(safeDetail)).append("\"},")
          .append("\"attributes\":[");
        appendAttr(sb, "event", safeEvent);
        appendAttr(sb, "severity", safeSeverity);
        sb.setLength(sb.length() - 1); // drop trailing comma
        sb.append("]}]}]}]}");
        return sb.toString();
    }

    private static void appendAttr(StringBuilder sb, String key, String value) {
        sb.append("{\"key\":\"").append(escapeJson(key)).append("\",")
          .append("\"value\":{\"stringValue\":\"").append(escapeJson(value)).append("\"}},");
    }

    /**
     * Minimal JSON string escaping (quotes, backslash, control chars) — the
     * configured extras may carry hostnames/env values; an unescaped quote
     * would corrupt the whole OTLP payload.
     *
     * <p>Null-safe (P2-070 extension): a null extras value degrades to ""
     * instead of NPE-ing the whole payload.
     */
    private static String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder out = null;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            String esc;
            switch (c) {
                case '"' -> esc = "\\\"";
                case '\\' -> esc = "\\\\";
                case '\b' -> esc = "\\b";
                case '\f' -> esc = "\\f";
                case '\n' -> esc = "\\n";
                case '\r' -> esc = "\\r";
                case '\t' -> esc = "\\t";
                default -> {
                    if (c < 0x20) {
                        esc = String.format("\\u%04x", (int) c);
                    } else {
                        esc = null;
                    }
                }
            }
            if (esc != null) {
                if (out == null) {
                    out = new StringBuilder(s.length() + 16);
                    out.append(s, 0, i);
                }
                out.append(esc);
            } else if (out != null) {
                out.append(c);
            }
        }
        return out == null ? s : out.toString();
    }
}
