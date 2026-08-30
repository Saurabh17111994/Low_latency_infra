package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

/**
 * D1 guard (2026-08-30): CandleAccumulator must be recognized by Flink's
 * type extractor as a POJO, not fall back to GenericType/Kryo.
 *
 * <p>Background: the class was package-private with package-private fields,
 * so {@code TypeExtractor} returned GenericTypeInfo → Kryo serialized the
 * window-accumulator state (JM warned on every submit; slower checkpoints).
 *
 * <p>bug-injected proof: {@link SneakyPackagePrivateAccumulator} below is an
 * exact copy with the D1 bug (package-private class + fields). The test
 * asserts it FAILS POJO recognition — proving this test detects the bug it
 * guards against. If that assertion ever breaks, Flink changed its
 * extraction rules and this guard must be revisited.
 */
class CandlePojoRecognitionTest {

    /** Deliberate bug-injected copy: package-private class AND fields. */
    static final class SneakyPackagePrivateAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;
        String exchange;
        String symbol;
        long openPaise;
    }

    @Test
    void candleAccumulatorIsRecognizedAsPojo() {
        org.apache.flink.api.common.typeinfo.TypeInformation<CandleAccumulator> ti =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(CandleAccumulator.class);
        assertTrue(ti instanceof PojoTypeInfo,
                "CandleAccumulator must extract as POJO, got " + ti.getClass().getSimpleName()
                        + " — check the class and its fields are public (D1)");
    }

    @Test
    void bugInjectedCopyIsDetectedAsGenericType() {
        // Proof the guard can fail: a package-private twin must NOT be a POJO.
        org.apache.flink.api.common.typeinfo.TypeInformation<SneakyPackagePrivateAccumulator> ti =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        SneakyPackagePrivateAccumulator.class);
        assertTrue(ti instanceof GenericTypeInfo,
                "expected the bug-injected package-private twin to be GenericType; "
                        + "if Flink now accepts it, revisit this guard (got "
                        + ti.getClass().getSimpleName() + ")");
    }
}
