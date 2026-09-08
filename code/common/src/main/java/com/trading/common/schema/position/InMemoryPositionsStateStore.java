package com.trading.common.schema.position;

import java.util.HashMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** {@link PositionsStateStore} backed by an in-memory map — pure-JVM twin of
 * the Fluss store for unit tests and drills. */
public final class InMemoryPositionsStateStore implements PositionsStateStore {

    private final Map<String, PositionSnapshot> byPositionId = new HashMap<>();

    @Override
    public PositionSnapshot lookup(String positionId) {
        // P4-312: fail fast like the Fluss twin (BinaryString.fromString NPEs
        // on null) instead of silently returning null via HashMap.get(null).
        return byPositionId.get(Objects.requireNonNull(positionId, "positionId"));
    }

    @Override
    public void upsert(PositionSnapshot snapshot) {
        // P4-312: guard the key as well as the snapshot — the record ctor
        // rejects blank ids today, but the twin parity must not rely on it.
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(snapshot.positionId(), "positionId");
        byPositionId.put(snapshot.positionId(), snapshot);
    }

    public int size() {
        return byPositionId.size();
    }

    /** All snapshots sorted by position id (deterministic introspection). */
    public List<PositionSnapshot> all() {
        return byPositionId.values().stream()
                .sorted(Comparator.comparing(PositionSnapshot::positionId))
                .toList();
    }
}
