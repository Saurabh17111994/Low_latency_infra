package com.trading.common.schema.projection;

import java.util.HashMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Pure-JVM {@link LifecycleStore} (offline tests/drills). */
public final class InMemoryLifecycleStore implements LifecycleStore {

    private final Map<String, OrderLifecycleSnapshot> rows = new HashMap<>();

    private static String key(String accountScopeId, String brokerOrderId) {
        return accountScopeId + "\u0000" + brokerOrderId;
    }

    @Override
    public Optional<OrderLifecycleSnapshot> lookup(String accountScopeId, String brokerOrderId) {
        return Optional.ofNullable(rows.get(key(accountScopeId, brokerOrderId)));
    }

    @Override
    public void upsert(OrderLifecycleSnapshot snapshot) {
        rows.put(key(snapshot.accountScopeId(), snapshot.brokerOrderId()), snapshot);
    }

    public int size() {
        return rows.size();
    }

    /** All rows sorted by broker order id, then account scope (deterministic introspection). */
    public List<OrderLifecycleSnapshot> all() {
        return rows.values().stream()
                .sorted(Comparator.comparing(OrderLifecycleSnapshot::brokerOrderId)
                        .thenComparing(OrderLifecycleSnapshot::accountScopeId))
                .toList();
    }
}
