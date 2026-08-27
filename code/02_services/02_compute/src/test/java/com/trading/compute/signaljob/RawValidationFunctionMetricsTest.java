package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tracker 14 box 906 (2026-08-12): {@link RawValidationFunction#open} must
 * register the container memory gauges on the operator metric group — the
 * registration path is the same one the PrometheusReporter scrapes on the
 * live dev cluster (P8.1 box 850 precedent), so a registered gauge with a
 * real cgroup-backed value IS the runtime proof that the PromQL series
 * {@code flink_taskmanager_job_task_operator_container_memory_usage_bytes}
 * (and {@code _limit_bytes}, O2 dot→underscore rename) will exist whenever
 * the job runs on a cgroup-backed host.
 *
 * <p>Deterministic on any host: the assertions mirror
 * {@link ContainerMemory#read()} — when the cgroup files are readable the
 * gauges MUST exist and carry live values (usage compared to a FRESH read
 * within a tolerance, since memory.current grows between probe and scrape);
 * when they are not readable the gauges MUST be absent (metric gap, never a
 * crash).
 *
 * <p>The recording proxy below captures every {@code gauge(name, g)} call on
 * the (real, unregistered) operator metric group: no mockito on this module's
 * test classpath, and {@code AbstractMetricGroup} keeps its metric map
 * private with no accessor, so interception is the only read path.
 */
@DisplayName("RawValidationFunction container-memory gauge registration (tracker 14 box 906)")
class RawValidationFunctionMetricsTest {

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        // ALLOW_FULL_REPLAY=true satisfies the startup-mode gate
        // (SignalJobConfig.validateStartupMode); the gauge then reads 1
        // (asserted in startupModeGaugeUnaffected).
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    /** Holds the proxy plus the gauges it observed being registered. */
    private record RecordingGroup(MetricGroup proxy, Map<String, Gauge<?>> gauges) {}

    private static RecordingGroup recordingGroup() {
        MetricGroup real =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        Map<String, Gauge<?>> gauges = new HashMap<>();
        // OperatorMetricGroup, not MetricGroup: Flink 2.2.1 declares
        // RuntimeContext.getMetricGroup() with the covariant OperatorMetricGroup
        // return type, so the RuntimeContext proxy below casts its result —
        // a MetricGroup-only proxy throws ClassCastException (observed 2026-08-12).
        MetricGroup proxy = (MetricGroup) Proxy.newProxyInstance(
                OperatorMetricGroup.class.getClassLoader(),
                new Class<?>[] {OperatorMetricGroup.class},
                (p, method, args) -> {
                    if (method.getName().equals("gauge")
                            && args.length == 2
                            && args[0] instanceof String
                            && args[1] instanceof Gauge<?>) {
                        gauges.put((String) args[0], (Gauge<?>) args[1]);
                    }
                    return method.invoke(real, args);
                });
        return new RecordingGroup(proxy, gauges);
    }

    private static RawValidationFunction openedFunction(MetricGroup group) throws Exception {
        RawValidationFunction fn = new RawValidationFunction(SignalJobConfig.from(env()));
        // No mockito on this module's test classpath: a dynamic Proxy that
        // answers only getMetricGroup() (the sole RuntimeContext method
        // RawValidationFunction.open uses) and fails loudly on anything else.
        RuntimeContext ctx = (RuntimeContext) Proxy.newProxyInstance(
                RuntimeContext.class.getClassLoader(),
                new Class<?>[] {RuntimeContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMetricGroup")) {
                        return group;
                    }
                    throw new UnsupportedOperationException(
                            "unexpected RuntimeContext call: " + method.getName());
                });
        fn.setRuntimeContext(ctx);
        fn.open(new OpenContext() {});
        return fn;
    }

    @Test
    @DisplayName("cgroup readable → usage/limit gauges registered with real values")
    void gaugesRegisteredWhenCgroupReadable() throws Exception {
        ContainerMemory.Snapshot snapshot = ContainerMemory.read();
        RecordingGroup rg = recordingGroup();
        openedFunction(rg.proxy());

        Gauge<?> usage = rg.gauges().get("container.memory.usage.bytes");
        Gauge<?> limit = rg.gauges().get("container.memory.limit.bytes");

        if (snapshot == null) {
            assertNull(usage, "no readable cgroup files → usage gauge must be absent, not crash");
            assertNull(limit, "no readable cgroup files → limit gauge must be absent, not crash");
            return;
        }
        // memory.current is a live value: it grows between the presence probe and
        // the gauge's first scrape (seen in the container suite: +151,552 B).
        // Assert liveness against a FRESH read with a tolerance, not byte-exact
        // equality with a stale snapshot; a broken gauge (0/garbage) still fails
        // by gigabytes. memory.max is stable, so the limit check stays exact.
        ContainerMemory.Snapshot fresh = ContainerMemory.read();
        assertNotNull(fresh, "cgroup readable at probe time must still be readable now");
        long usageDelta = Math.abs((long) usage.getValue() - fresh.usageBytes());
        assertTrue(usageDelta < 8L * 1024 * 1024,
                "usage gauge must carry a live cgroup value (delta vs fresh read: "
                        + usageDelta + " B, must be < 8 MiB)");
        assertEquals(fresh.limitBytes(), limit.getValue(),
                "limit gauge must carry the cgroup limit (-1 = unlimited)");
    }

    @Test
    @DisplayName("startup-mode gauge still registers alongside the container gauges")
    void startupModeGaugeUnaffected() throws Exception {
        RecordingGroup rg = recordingGroup();
        openedFunction(rg.proxy());

        Gauge<?> mode = rg.gauges().get("compute.startup.mode");
        assertEquals(1L, mode.getValue(),
                "ALLOW_FULL_REPLAY=true in the fixture → FULL_REPLAY startup mode → 1");
    }
}
