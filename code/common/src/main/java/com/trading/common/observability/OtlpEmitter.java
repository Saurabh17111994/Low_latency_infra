package com.trading.common.observability;

import java.util.Map;

/**
 * Typed emitter that serializes {@link StructuredLogEvent} and
 * {@link AlertThresholds.Alert} into OTLP/JSON logs records for the
 * OpenTelemetry Collector (docs/04_contracts/openobserve.md &sect;C, &sect;E).
 *
 * <p>Per the AI-agent rules, components emit OTLP via the collector and never
 * write observability data to local files except temporary buffering. This
 * class produces the record; forwarding to {@code otel-collector:4318} is done
 * by the runtime (outside {@code common}).
 */
public final class OtlpEmitter {

    /** OpenObserve stream that receives trading alerts. */
    public static final String TRADING_ALERTS_STREAM = "trading_alerts";

    private OtlpEmitter() {}

    /** Serialize a structured log event as an OTLP logs JSON document. */
    public static String emitLog(StructuredLogEvent event) {
        Map<String, String> attrs = event.toAttributes();
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resourceLogs\":[{\"resource\":{\"attributes\":[");
        // R-046/R-264: EVERY interpolated value AND key must be JSON-escaped.
        sb.append("{\"key\":\"service.name\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(event.service)).append("\"}}");
        sb.append("]},\"scopeLogs\":[{\"scope\":{\"name\":\"trading.common\"},\"logRecords\":[{");
        sb.append("\"timeUnixNano\":\"").append(event.timestampMs * 1_000_000L).append("\",");
        sb.append("\"severityText\":\"").append(escapeJson(event.level)).append("\",");
        sb.append("\"body\":{\"stringValue\":\"").append(escapeJson(event.message)).append("\"},");
        sb.append("\"attributes\":[");
        boolean first = true;
        for (Map.Entry<String, String> e : attrs.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{\"key\":\"").append(escapeJson(e.getKey()))
              .append("\",\"value\":{\"stringValue\":\"")
              .append(escapeJson(e.getValue())).append("\"}}");
        }
        sb.append("]}]}]}]}");
        return sb.toString();
    }

    /** Serialize an alert threshold breach as a {@code trading_alerts} OTLP record. */
    public static String emitAlert(AlertThresholds.Alert alert, String service, String host,
                                   String vmId, String environment, String correlationId,
                                   String category, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resourceLogs\":[{\"resource\":{\"attributes\":[");
        sb.append("{\"key\":\"service.name\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(service)).append("\"}}");
        sb.append("]},\"scopeLogs\":[{\"scope\":{\"name\":\"trading.common\"},\"logRecords\":[{");
        sb.append("\"timeUnixNano\":\"").append(System.currentTimeMillis() * 1_000_000L).append("\",");
        sb.append("\"severityText\":\"").append(escapeJson(category)).append("\",");
        sb.append("\"body\":{\"stringValue\":\"").append(escapeJson(message)).append("\"},");
        sb.append("\"attributes\":[");
        // R-265: use the stream constant so the stream name stays in sync.
        sb.append("{\"key\":\"stream\",\"value\":{\"stringValue\":\"")
          .append(TRADING_ALERTS_STREAM).append("\"}},");
        sb.append("{\"key\":\"alert.name\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(alert.name())).append("\"}},");
        sb.append("{\"key\":\"alert.condition\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(alert.condition)).append("\"}},");
        sb.append("{\"key\":\"alert.category\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(category)).append("\"}},");
        sb.append("{\"key\":\"service\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(service)).append("\"}},");
        sb.append("{\"key\":\"host\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(host)).append("\"}},");
        sb.append("{\"key\":\"vm_id\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(vmId)).append("\"}},");
        sb.append("{\"key\":\"environment\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(environment)).append("\"}},");
        sb.append("{\"key\":\"correlation_id\",\"value\":{\"stringValue\":\"")
          .append(escapeJson(correlationId)).append("\"}}");
        sb.append("]}]}]}]}");
        return sb.toString();
    }

    /**
     * R-046/R-047/R-078: RFC 8259 escaping for every interpolated field —
     * all control characters U+0000–U+001F (not just quote/backslash/newline/
     * carriage-return) so a free-text message or alert condition can never
     * produce an invalid JSON document.
     */
    private static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder o = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': o.append("\\\""); break;
                case '\\': o.append("\\\\"); break;
                case '\n': o.append("\\n"); break;
                case '\r': o.append("\\r"); break;
                case '\t': o.append("\\t"); break;
                case '\b': o.append("\\b"); break;
                case '\f': o.append("\\f"); break;
                default:
                    if (c < 0x20) {
                        o.append(String.format("\\u%04x", (int) c));
                    } else {
                        o.append(c);
                    }
            }
        }
        return o.toString();
    }
}
