package com.trading.ingestion.telemetry;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Reads the Fluss client's own metrics out of the JVM's platform MBeanServer
 * (Task 3b, 2026-10-06).
 *
 * <p>Why this exists: every write-path number we had was our own
 * accept&rarr;ack histogram ({@code append.latency.ms}), which cannot split
 * "waited in the writer's batch queue" from "send + server + response". The
 * Fluss client already measures both ({@code batchQueueTimeMs},
 * {@code sendLatencyMs}, {@code recordsPerBatch}, the 64 MB pool gauges), but
 * it publishes nothing by default: {@code client.metrics.enabled} defaults to
 * false. When {@code FlussClientAdapter} turns it on with
 * {@code metrics.reporters=jmx} <em>and no JMX port</em>, the bundled JMX
 * reporter registers the client's beans in the platform MBeanServer (the RMI
 * route is deliberately avoided: it needs {@code metrics.reporter.jmx.port},
 * which makes Fluss load {@code JMXServer} and therefore an internal JDK class
 * that JDK 17 refuses to export).
 *
 * <p>Read-only and dependency-free: plain JDK JMX. Returns an empty map when
 * the reporter is off, so callers need no feature flag of their own.
 */
final class FlussClientMetrics {

    /**
     * Fluss registers client metrics under {@code org.apache.fluss.client.*}
     * with the metric name as the MBean <em>domain</em> tail (e.g.
     * {@code org.apache.fluss.client.writer.batchQueueTimeMs}); the attributes
     * are generic ({@code Value}, {@code Count}, {@code Rate}, ...).
     */
    static final String DOMAIN_PREFIX = "org.apache.fluss.client.";

    /**
     * Payload guard: an arm's OTLP line already carries ~80 metrics and the
     * client registers ~25 beans with a handful of attributes each. Capping
     * keeps one flush bounded even if a future client version adds more.
     */
    /**
     * Safety valve, not a selection rule: the client registers ~25 MBeans and the
     * reporter exposes several attributes (Value, Count, Rate, percentiles) per
     * metric, so the live population is ~150–200 names. The cap only exists to
     * bound a pathological registry; if it ever truncates, it truncates
     * alphabetically (TreeMap order) — raise it rather than live with a silent gap.
     */
    private static final int MAX_GAUGES = 256;

    private FlussClientMetrics() {}

    /**
     * @return metric-name &rarr; value for every numeric client attribute
     *     currently registered, or an empty map when nothing is registered
     *     (reporter disabled), the platform server is unreachable, or JMX
     *     throws. Names are {@code <domain tail>.<attribute>}, sorted, and
     *     capped at {@link #MAX_GAUGES}. Never null.
     */
    static Map<String, Double> snapshot() {
        final MBeanServer server;
        try {
            server = ManagementFactory.getPlatformMBeanServer();
        } catch (Throwable t) {
            return Collections.emptyMap();
        }

        Map<String, Double> out = new TreeMap<>();
        try {
            ObjectName pattern = new ObjectName(DOMAIN_PREFIX + "*:*");
            for (ObjectName name : server.queryNames(pattern, null)) {
                if (out.size() >= MAX_GAUGES) {
                    break;
                }
                String domain = name.getDomain();
                if (!domain.startsWith(DOMAIN_PREFIX)) {
                    continue;
                }
                String metric = domain.substring(DOMAIN_PREFIX.length());
                for (MBeanAttributeInfo info : server.getMBeanInfo(name).getAttributes()) {
                    if (out.size() >= MAX_GAUGES) {
                        break;
                    }
                    Double value = numeric(server, name, info);
                    if (value != null) {
                        out.put(metric + "." + info.getName(), value);
                    }
                }
            }
        } catch (Throwable t) {
            // A metric scrape must never disturb the write path.
            return out;
        }
        return out;
    }

    /** Numeric value of one attribute, or null when it is not a number. */
    private static Double numeric(MBeanServer server, ObjectName name, MBeanAttributeInfo info) {
        if (!info.isReadable()) {
            return null;
        }
        try {
            Object raw = server.getAttribute(name, info.getName());
            if (raw instanceof Number) {
                return ((Number) raw).doubleValue();
            }
            if (raw instanceof Boolean) {
                return ((Boolean) raw) ? 1.0 : 0.0;
            }
            if (raw instanceof String) {
                return Double.valueOf(((String) raw).trim());
            }
        } catch (Throwable t) {
            // One unreadable attribute (or a racing bean registration) must not
            // drop the rest of the snapshot.
            return null;
        }
        return null;
    }
}
