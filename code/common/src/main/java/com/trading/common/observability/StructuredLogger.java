package com.trading.common.observability;

import org.slf4j.MDC;

import java.util.Map;
import java.util.Objects;

/**
 * Structured logging with mandatory correlation context
 * (docs/08_implementation/01-foundation.md &rarr; "Observability invariant", orig L727).
 */
public final class StructuredLogger {

    private StructuredLogger() {}

    /**
     * Run an action with a correlation context on the SLF4J MDC, then restore
     * previous state.
     *
     * <p>The action and the context are validated <em>before</em> the MDC is
     * touched: {@code MDC.put(null, ...)} and a null value throw
     * {@link IllegalArgumentException} from the logging backend, and they would
     * throw after part of the context was already applied. A {@code null} map is
     * allowed and means "no extra context".
     */
    public static void withContext(Runnable action, Map<String, String> context) {
        Objects.requireNonNull(action, "action");
        if (context != null) {
            for (Map.Entry<String, String> entry : context.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank()) {
                    throw new IllegalArgumentException("MDC key must not be null or blank");
                }
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                            "MDC value for key '" + entry.getKey() + "' must not be null");
                }
            }
        }
        Map<String, String> previous = MDC.getCopyOfContextMap();
        try {
            if (context != null) {
                context.forEach(MDC::put);
            }
            action.run();
        } finally {
            if (previous != null) {
                MDC.setContextMap(previous);
            } else {
                MDC.clear();
            }
        }
    }

    /**
     * Stable correlation identity: {@code service/instance/version/traceId}.
     *
     * <p>The four parts are joined with {@code '/'}, so every part must be present
     * and must not itself contain {@code '/'}: a blank part or an embedded slash
     * would produce an id that cannot be split back into its fields (and two
     * different services could emit the same id).
     */
    public static String correlationId(String service, String instance, String version, String traceId) {
        return part("service", service) + "/" + part("instance", instance)
                + "/" + part("version", version) + "/" + part("traceId", traceId);
    }

    private static String part(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "correlation-id part '" + name + "' must not be null or blank");
        }
        if (value.indexOf('/') >= 0) {
            throw new IllegalArgumentException(
                    "correlation-id part '" + name + "' must not contain '/': " + value);
        }
        return value;
    }
}
