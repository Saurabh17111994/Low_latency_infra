package com.trading.common.schema.audit;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Key-versioned master key material for the encrypted export pipeline. Rotation
 * = a new version added to the store; old versions are retained so previously
 * wrapped data keys stay decryptable (decrypt-old/write-new on rotation).
 * A version missing from the store fails closed — an unwrappable bundle is
 * never silently treated as verified.
 *
 * <p>The store owns its key material. {@link #of} snapshots the map and the key
 * bytes it is handed, and {@link #keyFor} hands back a copy, so a caller that
 * keeps (or zeroes) an array cannot corrupt key material every other user of
 * the store depends on (P6-285, P6-286). A store with no keys at all is
 * rejected: it would otherwise advertise a current version that cannot wrap
 * anything (P6-678).
 */
public interface MasterKeyStore {

    /** The version new bundles are wrapped with. */
    int currentVersion();

    /** The master key for {@code version} — absent versions fail closed. */
    byte[] keyFor(int version);

    /**
     * Static, test-friendly store from a version -> key map. The current
     * version is the highest present.
     */
    static MasterKeyStore of(Map<Integer, byte[]> keys) {
        Objects.requireNonNull(keys, "keys");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException(
                    "keys must not be empty — a store with no master key cannot wrap (fail-closed)");
        }
        Map<Integer, byte[]> snapshot = new HashMap<>();
        keys.forEach((version, key) -> snapshot.put(Objects.requireNonNull(version, "version"),
                Objects.requireNonNull(key, "key").clone()));
        Map<Integer, byte[]> owned = Collections.unmodifiableMap(snapshot);
        int current = owned.keySet().stream().mapToInt(Integer::intValue).max().orElseThrow();
        return new MasterKeyStore() {
            @Override
            public int currentVersion() {
                return current;
            }

            @Override
            public byte[] keyFor(int version) {
                byte[] key = owned.get(version);
                if (key == null) {
                    throw new IllegalArgumentException("master key version " + version
                            + " not present (rotation dropped it?)");
                }
                return key.clone();
            }
        };
    }
}
