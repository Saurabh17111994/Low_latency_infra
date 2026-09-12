package com.trading.execution.gateway;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P3-319: the applier's per-eventId serialization must be <em>striped</em>. The
 * map it replaced held one lock per distinct eventId for the life of the process
 * while its comment already claimed the count was bounded, so the bound is the
 * property under test — not an implementation detail.
 */
class ProjectionApplierStripeTest {
    /** The stripe seam touches neither dependency, so they can stay unset here. */
    private final ProjectionApplier applier = new ProjectionApplier(null, null);

    @Test
    @DisplayName("the same eventId always maps to the same stripe")
    void sameEventIdAlwaysMapsToTheSameStripe() {
        assertSame(applier.stripeFor("evt-1"), applier.stripeFor("evt-1"));
        assertSame(applier.stripeFor(""), applier.stripeFor(""));
    }

    @Test
    @DisplayName("distinct eventIds share a bounded pool of stripes, not one lock each")
    void distinctEventIdsShareABoundedPoolOfStripes() {
        Set<Lock> distinct = new HashSet<>();
        IntStream.range(0, 10_000).forEach(i -> distinct.add(applier.stripeFor("evt-" + i)));
        assertTrue(distinct.size() > 1,
                "striping degenerated to a single lock: " + distinct.size());
        assertTrue(distinct.size() <= ProjectionApplier.STRIPE_COUNT,
                "one lock per eventId is the unbounded map again, not a stripe: " + distinct.size());
    }
}
