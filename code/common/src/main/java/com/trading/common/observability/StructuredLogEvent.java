package com.trading.common.observability;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Structured log record carrying the OpenObserve contract
 * (docs/04_contracts/openobserve.md &sect;F).
 *
 * <p>Required fields: timestamp, level, service, component, subsystem, host,
 * vm_id, environment, correlation_id, trace_id, span_id, message.
 * Optional: symbol, timeframe, strategy, job_name, task_name, exception, stacktrace.
 *
 * <p>Plain-text logs are prohibited; this type is the only supported shape.
 */
public final class StructuredLogEvent {

    // Required
    public final long timestampMs;
    public final String level;
    public final String service;
    public final String component;
    public final String subsystem;
    public final String host;
    public final String vmId;
    public final String environment;
    public final String correlationId;
    public final String traceId;
    public final String spanId;
    public final String message;

    // Optional
    public final String symbol;
    public final String timeframe;
    public final String strategy;
    public final String jobName;
    public final String taskName;
    public final String exception;
    public final String stacktrace;

    private StructuredLogEvent(Builder b) {
        this.timestampMs = b.timestampMs;
        this.level = b.level;
        this.service = b.service;
        this.component = b.component;
        this.subsystem = b.subsystem;
        this.host = b.host;
        this.vmId = b.vmId;
        this.environment = b.environment;
        this.correlationId = b.correlationId;
        this.traceId = b.traceId;
        this.spanId = b.spanId;
        this.message = b.message;
        this.symbol = b.symbol;
        this.timeframe = b.timeframe;
        this.strategy = b.strategy;
        this.jobName = b.jobName;
        this.taskName = b.taskName;
        this.exception = b.exception;
        this.stacktrace = b.stacktrace;
    }

    /** Flat attribute map of every present field (required + non-null optional). */
    public Map<String, String> toAttributes() {
        Map<String, String> a = new LinkedHashMap<>();
        a.put("timestamp", String.valueOf(timestampMs));
        a.put("level", level);
        a.put("service", service);
        a.put("component", component);
        a.put("subsystem", subsystem);
        a.put("host", host);
        a.put("vm_id", vmId);
        a.put("environment", environment);
        a.put("correlation_id", correlationId);
        a.put("trace_id", traceId);
        a.put("span_id", spanId);
        a.put("message", message);
        putIfPresent(a, "symbol", symbol);
        putIfPresent(a, "timeframe", timeframe);
        putIfPresent(a, "strategy", strategy);
        putIfPresent(a, "job_name", jobName);
        putIfPresent(a, "task_name", taskName);
        putIfPresent(a, "exception", exception);
        putIfPresent(a, "stacktrace", stacktrace);
        return a;
    }

    private static void putIfPresent(Map<String, String> a, String k, String v) {
        if (v != null) {
            a.put(k, v);
        }
    }

    public static Builder builder(long timestampMs, String level, String service,
                                  String component, String subsystem, String host,
                                  String vmId, String environment, String correlationId,
                                  String traceId, String spanId, String message) {
        return new Builder(timestampMs, level, service, component, subsystem, host,
                vmId, environment, correlationId, traceId, spanId, message);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StructuredLogEvent e)) return false;
        // R-266: equality covers the identity/correlation fields too — two
        // records with the same level/message but different host, vm_id,
        // trace_id or span_id are distinct log records.
        return timestampMs == e.timestampMs
                && Objects.equals(level, e.level)
                && Objects.equals(service, e.service)
                && Objects.equals(component, e.component)
                && Objects.equals(subsystem, e.subsystem)
                && Objects.equals(host, e.host)
                && Objects.equals(vmId, e.vmId)
                && Objects.equals(environment, e.environment)
                && Objects.equals(correlationId, e.correlationId)
                && Objects.equals(traceId, e.traceId)
                && Objects.equals(spanId, e.spanId)
                && Objects.equals(message, e.message)
                && Objects.equals(symbol, e.symbol)
                && Objects.equals(timeframe, e.timeframe)
                && Objects.equals(strategy, e.strategy)
                && Objects.equals(jobName, e.jobName)
                && Objects.equals(taskName, e.taskName)
                && Objects.equals(exception, e.exception)
                && Objects.equals(stacktrace, e.stacktrace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMs, level, service, component, subsystem,
                host, vmId, environment, correlationId, traceId, spanId, message,
                symbol, timeframe, strategy, jobName, taskName, exception, stacktrace);
    }

    /** Builder for {@link StructuredLogEvent}. */
    public static final class Builder {
        private final long timestampMs;
        private final String level;
        private final String service;
        private final String component;
        private final String subsystem;
        private final String host;
        private final String vmId;
        private final String environment;
        private final String correlationId;
        private final String traceId;
        private final String spanId;
        private final String message;
        private String symbol;
        private String timeframe;
        private String strategy;
        private String jobName;
        private String taskName;
        private String exception;
        private String stacktrace;

        private Builder(long timestampMs, String level, String service, String component,
                        String subsystem, String host, String vmId, String environment,
                        String correlationId, String traceId, String spanId, String message) {
            this.timestampMs = timestampMs;
            this.level = level;
            this.service = service;
            this.component = component;
            this.subsystem = subsystem;
            this.host = host;
            this.vmId = vmId;
            this.environment = environment;
            this.correlationId = correlationId;
            this.traceId = traceId;
            this.spanId = spanId;
            this.message = message;
        }

        public Builder symbol(String v) { this.symbol = v; return this; }
        public Builder timeframe(String v) { this.timeframe = v; return this; }
        public Builder strategy(String v) { this.strategy = v; return this; }
        public Builder jobName(String v) { this.jobName = v; return this; }
        public Builder taskName(String v) { this.taskName = v; return this; }
        public Builder exception(String v) { this.exception = v; return this; }
        public Builder stacktrace(String v) { this.stacktrace = v; return this; }

        /**
         * R-130: the contract says 12 required fields — build() must not emit a
         * record with any of them missing or blank, the timestamp included: an
         * unset (0) or negative millisecond stamp becomes
         * {@code timeUnixNano: "0"} (or worse), which is not a time.
         */
        public StructuredLogEvent build() {
            String missing = missingRequired();
            if (!missing.isEmpty()) {
                throw new IllegalStateException(
                        "StructuredLogEvent missing required fields: " + missing);
            }
            return new StructuredLogEvent(this);
        }

        private String missingRequired() {
            StringBuilder sb = new StringBuilder();
            if (timestampMs <= 0) sb.append("timestamp ");
            if (isBlank(level)) sb.append("level ");
            if (isBlank(service)) sb.append("service ");
            if (isBlank(component)) sb.append("component ");
            if (isBlank(subsystem)) sb.append("subsystem ");
            if (isBlank(host)) sb.append("host ");
            if (isBlank(vmId)) sb.append("vmId ");
            if (isBlank(environment)) sb.append("environment ");
            if (isBlank(correlationId)) sb.append("correlationId ");
            if (isBlank(traceId)) sb.append("traceId ");
            if (isBlank(spanId)) sb.append("spanId ");
            if (isBlank(message)) sb.append("message ");
            return sb.toString().trim();
        }

        private static boolean isBlank(String s) {
            return s == null || s.isBlank();
        }
    }
}
