package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.OrderLifecycleState;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the Group S in-memory store hardening (P4-321/322/323). */
class InMemoryStoresTest {

    private static OrderLifecycleSnapshot snap(String scope, String order) {
        return new OrderLifecycleSnapshot(scope, order, "instr-1", "att-1", "tc-1",
                OrderLifecycleState.PARTIAL, 10, 5, 1000L, "evt-1", 1L, 1000L, 1000L, "NEW", "2");
    }

    @Test
    void allSortsByBrokerOrderIdThenAccountScope() {
        // P4-321: same brokerOrderId in two scopes must still order totally.
        InMemoryLifecycleStore store = new InMemoryLifecycleStore();
        store.upsert(snap("acc-B", "b-1"));
        store.upsert(snap("acc-A", "b-1"));
        store.upsert(snap("acc-A", "b-0"));
        List<OrderLifecycleSnapshot> all = store.all();
        assertThat(all).extracting(s -> s.accountScopeId() + "|" + s.brokerOrderId())
                .containsExactly("acc-A|b-0", "acc-A|b-1", "acc-B|b-1");
    }

    @Test
    void auditAppendRejectsNullFast() {
        // P4-322: fail at write time, never poison later reads.
        InMemoryProjectionAuditStore audit = new InMemoryProjectionAuditStore();
        assertThatThrownBy(() -> audit.append(null))
                .isInstanceOf(NullPointerException.class);
        assertThat(audit.size()).isZero();
        assertThat(audit.all()).isEmpty();
    }

    @Test
    void ledgerStoreSurvivesConcurrentPutAndLookup() throws Exception {
        // P4-323: hammer put/lookup from 8 threads — no exception, no lost row.
        InMemoryProjectionLedgerStore ledger = new InMemoryProjectionLedgerStore();
        int threads = 8;
        int perThread = 500;
        List<Thread> workers = new java.util.ArrayList<>();
        List<Throwable> failures = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        for (int t = 0; t < threads; t++) {
            final int id = t;
            Thread w = new Thread(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        String key = "evt-" + id + "-" + i;
                        ledger.put(new ProjectionLedgerEntry(key,
                                PostbackProjectionLedger.State.RECEIVED, null, 0,
                                null, null, 1000L, null, "2"));
                        ledger.lookup(key).orElseThrow();
                    }
                } catch (Throwable e) {
                    failures.add(e);
                }
            });
            workers.add(w);
            w.start();
        }
        for (Thread w : workers) {
            w.join(30_000);
        }
        assertThat(failures).isEmpty();
        assertThat(ledger.size()).isEqualTo(threads * perThread);
    }
}
