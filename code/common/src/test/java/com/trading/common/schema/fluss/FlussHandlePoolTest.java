package com.trading.common.schema.fluss;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * P3-065/P3-068/P3-072/P3-502: the pool exists to reuse Fluss handles that are
 * {@code @NotThreadSafe} and NOT Closeable, on stores with a concurrent request path. These
 * pin the three properties that make that sound.
 */
class FlussHandlePoolTest {

    @Test
    void aReleasedHandleIsReused() throws Exception {
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 2);
        Object first = pool.borrow();
        pool.release(first);

        assertThat(pool.borrow()).as("a returned handle must be handed back out").isSameAs(first);
        assertThat(pool.createdForTest()).as("no extra handle created").isEqualTo(1);
        assertThat(pool.reusedForTest()).isEqualTo(1);
    }

    @Test
    void aFailedOperationDoesNotPoolItsHandle() {
        // A writer whose call threw may be poisoned; reusing it would turn one transient
        // failure into a persistent one. Per-call creation discarded such handles implicitly.
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 4);

        assertThatThrownBy(() -> pool.with(h -> {
            throw new IllegalStateException("transient write failure");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(pool.idleForTest()).as("the failed handle must not be retained").isZero();
        assertThat(pool.borrow()).as("a fresh handle is created instead").isNotNull();
        assertThat(pool.reusedForTest()).as("nothing was reused").isZero();
    }

    @Test
    void aSuccessfulOperationReturnsItsHandleForReuse() throws Exception {
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 4);
        Object used = pool.with(h -> h);

        assertThat(pool.idleForTest()).isEqualTo(1);
        assertThat(pool.borrow()).isSameAs(used);
    }

    @Test
    void idleIsBoundedButTransientBorrowsStillSucceed() {
        // Steady state reuses; a burst degrades to per-call behaviour rather than blocking.
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 2);
        List<Object> borrowed = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            borrowed.add(pool.borrow());
        }
        borrowed.forEach(pool::release);

        assertThat(pool.idleForTest()).as("idle is capped at maxIdle").isEqualTo(2);
        assertThat(pool.createdForTest()).as("all five were created on demand").isEqualTo(5);
    }

    @Test
    void clearDropsRetainedHandles() {
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 4);
        pool.release(pool.borrow());
        assertThat(pool.idleForTest()).isEqualTo(1);

        pool.clear();

        assertThat(pool.idleForTest()).isZero();
    }

    @Test
    void closeDrainsRetainedHandlesAndRefusesNewBorrows() throws Exception {
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 4);
        Object used = pool.with(handle -> handle);
        assertThat(pool.idleForTest()).isEqualTo(1);

        pool.close();

        assertThat(pool.idleForTest()).as("close drops what was retained").isZero();
        assertThatThrownBy(pool::borrow)
                .as("a closed pool must not manufacture handles for a store that is shutting down")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThat(used).isNotNull();
    }

    @Test
    void aHandleReturnedAfterCloseIsDroppedInsteadOfPooled() throws Exception {
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 4);
        Object inFlight = pool.borrow();

        pool.close();
        pool.release(inFlight); // the call that was in flight when the store closed

        assertThat(pool.idleForTest())
                .as("pooling it would hand a closed store's handle to a later borrower")
                .isZero();
    }

    @Test
    void concurrentBorrowsNeverHandTheSameHandleToTwoCallers() throws Exception {
        // The property the whole design rests on: these handles are @NotThreadSafe, so even
        // with reuse the pool must never lend one instance to two callers at once.
        FlussHandlePool<Object> pool = new FlussHandlePool<>(Object::new, 2);
        Set<Object> inFlight = ConcurrentHashMap.newKeySet();
        AtomicInteger overlaps = new AtomicInteger();

        int threads = 16;
        int iterations = 300;
        ExecutorService pool2 = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool2.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        try {
                            pool.with(handle -> {
                                if (!inFlight.add(handle)) {
                                    overlaps.incrementAndGet();
                                }
                                inFlight.remove(handle);
                                return null;
                            });
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool2.shutdownNow();
        }

        assertThat(overlaps.get())
                .as("no handle may be lent to two callers at the same time")
                .isZero();
        assertThat(pool.createdForTest())
                .as("reuse must actually happen, not one handle per call")
                .isLessThan(threads * iterations);
    }
}
