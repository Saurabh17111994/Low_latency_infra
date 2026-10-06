package com.trading.ingestion.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Task 3b (2026-10-06): the read half of the Fluss client metrics bridge.
 *
 * <p>The beans the real client registers are faked here, so the test needs no
 * Fluss connection and no cluster: it pins the two contract points that matter
 * downstream — the metric name is the MBean <em>domain</em> tail plus the
 * attribute name, and non-numeric attributes (and everything outside
 * {@code org.apache.fluss.client.*}) are skipped rather than emitted as junk.
 */
class FlussClientMetricsTest {

    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private final List<ObjectName> registered = new ArrayList<>();

    @AfterEach
    void unregisterBeans() {
        for (ObjectName name : registered) {
            try {
                server.unregisterMBean(name);
            } catch (Exception ignored) {
                // already gone — the assertion that matters already ran
            }
        }
        registered.clear();
    }

    @Test
    void noRegisteredClientBeansYieldsAnEmptySnapshot() throws Exception {
        assertTrue(FlussClientMetrics.snapshot().isEmpty(),
                "no Fluss client beans ⇒ nothing to emit (reporter off)");
    }

    @Test
    void numericClientAttributesAreExposedUnderTheirDomainTail() throws Exception {
        Map<String, Object> queue = new LinkedHashMap<>();
        queue.put("Value", 7L);
        queue.put("Name", "not-a-number");
        register("org.apache.fluss.client.writer.batchQueueTimeMs", queue);

        Map<String, Object> batches = new LinkedHashMap<>();
        batches.put("Value", 218L);
        register("org.apache.fluss.client.writer.recordsPerBatch", batches);

        Map<String, Object> pool = new LinkedHashMap<>();
        pool.put("Value", 67_108_864L);
        pool.put("WaitingThreads", 0.0);
        register("org.apache.fluss.client.writer.bufferAvailableBytes", pool);

        // A non-Fluss bean in the same server must not leak into the payload.
        Map<String, Object> foreign = new LinkedHashMap<>();
        foreign.put("Value", 42L);
        register("com.example.other.thing", foreign);

        Map<String, Double> snap = FlussClientMetrics.snapshot();

        assertEquals(7.0, snap.get("writer.batchQueueTimeMs.Value"), 1e-9);
        assertEquals(218.0, snap.get("writer.recordsPerBatch.Value"), 1e-9);
        assertEquals(67_108_864.0, snap.get("writer.bufferAvailableBytes.Value"), 1e-9);
        // BufferWaitingThreads is a real Fluss bean attribute (Double) and is kept.
        assertEquals(0.0, snap.get("writer.bufferAvailableBytes.WaitingThreads"), 1e-9);
        assertFalse(snap.containsKey("writer.batchQueueTimeMs.Name"),
                "a String attribute that is not a number must be skipped, not parsed");
        assertTrue(snap.keySet().stream().noneMatch(k -> k.contains("example")),
                "beans outside org.apache.fluss.client.* are ignored");
    }

    private void register(String domain, Map<String, Object> attributes) throws Exception {
        ObjectName name = new ObjectName(domain + ":type=task3b");
        server.registerMBean(new MapBackedBean(attributes), name);
        registered.add(name);
    }

    /** Minimal DynamicMBean backed by a map — the real beans are Fluss classes. */
    private static final class MapBackedBean implements DynamicMBean {
        private final Map<String, Object> values;

        MapBackedBean(Map<String, Object> values) {
            this.values = values;
        }

        @Override
        public Object getAttribute(String attribute) throws AttributeNotFoundException {
            if (!values.containsKey(attribute)) {
                throw new AttributeNotFoundException(attribute);
            }
            return values.get(attribute);
        }

        @Override
        public AttributeList getAttributes(String[] attributes) {
            AttributeList out = new AttributeList();
            for (String a : attributes) {
                if (values.containsKey(a)) {
                    out.add(new Attribute(a, values.get(a)));
                }
            }
            return out;
        }

        @Override
        public void setAttribute(Attribute attribute) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttributeList setAttributes(AttributeList attributes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object invoke(String actionName, Object[] params, String[] signature) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            MBeanAttributeInfo[] attrs = values.entrySet().stream()
                    .map(e -> new MBeanAttributeInfo(
                            e.getKey(), e.getValue().getClass().getSimpleName(),
                            e.getKey(), true, false, false))
                    .toArray(MBeanAttributeInfo[]::new);
            return new MBeanInfo(getClass().getName(), "test bean", attrs, null, null, null);
        }
    }
}
