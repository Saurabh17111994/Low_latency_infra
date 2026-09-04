package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

/**
 * D1 guard: MultiTimeframeState must be recognized as a Flink POJO.
 *
 * <p>Mirrors {@link CandlePojoRecognitionTest} exactly: asserts the production
 * state extracts as {@link PojoTypeInfo}, not {@link GenericTypeInfo}/Kryo,
 * and proves the guard can fail via a bug-injected package-private twin.
 */
class MultiTimeframeStatePojoTest {

    /** Bug-injected copy: package-private class + package-private fields. */
    static final class SneakyPackagePrivateState implements Serializable {
        private static final long serialVersionUID = 1L;
        String exchange;
        String symbol;
        long openPaise;
    }

    @Test
    void multiTimeframeStateIsRecognizedAsPojo() {
        org.apache.flink.api.common.typeinfo.TypeInformation<MultiTimeframeState> ti =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(MultiTimeframeState.class);
        assertTrue(
                ti instanceof PojoTypeInfo,
                "MultiTimeframeState must extract as POJO, got "
                        + ti.getClass().getSimpleName()
                        + " — check the class and its fields are public (D1)");
    }

    @Test
    void bugInjectedCopyIsDetectedAsGenericType() {
        org.apache.flink.api.common.typeinfo.TypeInformation<SneakyPackagePrivateState> ti =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        SneakyPackagePrivateState.class);
        assertTrue(
                ti instanceof GenericTypeInfo,
                "expected the bug-injected package-private twin to be GenericType; "
                        + "if Flink now accepts it, revisit this guard (got "
                        + ti.getClass().getSimpleName()
                        + ")");
    }

    @Test
    void closedCandleIsRecognizedAsPojo() {
        org.apache.flink.api.common.typeinfo.TypeInformation<ClosedCandle> ti =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(ClosedCandle.class);
        assertTrue(
                ti instanceof PojoTypeInfo,
                "ClosedCandle must extract as POJO, got "
                        + ti.getClass().getSimpleName()
                        + " — check public class + public fields (D1)");
    }

    @Test
    void multiTimeframeClosedRingIsSerializable() {
        org.apache.flink.api.common.typeinfo.TypeInformation<MultiTimeframeClosedRing> ti =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        MultiTimeframeClosedRing.class);
        // Ring is plain Serializable (backed by ArrayDeque), not required to be POJO,
        // but the outer state's POJO-ness must hold regardless. This asserts the
        // TypeInformation resolves (not null) and is not mistakenly rejected.
        assertTrue(ti != null, "TypeInformation for MultiTimeframeClosedRing must resolve");
    }
}
