package com.trading.common.schema.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A key store must own its key material: nothing the caller keeps can change
 * what the store wraps with later (P6-285, P6-286, P6-678).
 */
class MasterKeyStoreTest {

    private static byte[] key(int seed) {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) {
            k[i] = (byte) (seed + i);
        }
        return k;
    }

    @Test
    void laterChangesToTheCallersMapDoNotDesynchroniseTheStore() {
        Map<Integer, byte[]> keys = new HashMap<>();
        keys.put(1, key(1));
        MasterKeyStore store = MasterKeyStore.of(keys);

        keys.put(2, key(2));       // a rotation the store never saw
        keys.remove(1);            // and a key it still believes in

        assertThat(store.currentVersion()).isEqualTo(1);
        assertThat(store.keyFor(1)).isEqualTo(key(1));
        assertThatThrownBy(() -> store.keyFor(2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mutatingTheGivenArrayAfterConstructionCannotChangeTheStoredKey() {
        byte[] material = key(1);
        MasterKeyStore store = MasterKeyStore.of(Map.of(1, material));

        Arrays.fill(material, (byte) 0);

        assertThat(store.keyFor(1)).isEqualTo(key(1));
    }

    @Test
    void keyForHandsBackACopySoCallersCannotCorruptKeyMaterial() {
        byte[] material = key(1);
        MasterKeyStore store = MasterKeyStore.of(Map.of(1, material));

        byte[] handedOut = store.keyFor(1);
        Arrays.fill(handedOut, (byte) 0);
        Arrays.fill(material, (byte) 0);

        assertThat(store.keyFor(1)).isEqualTo(key(1));
    }

    @Test
    void emptyOrNullKeyMapsAreRejectedAtConstruction() {
        assertThatThrownBy(() -> MasterKeyStore.of(Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
        assertThatThrownBy(() -> MasterKeyStore.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keys");

        Map<Integer, byte[]> nullKey = new HashMap<>();
        nullKey.put(1, null);
        assertThatThrownBy(() -> MasterKeyStore.of(nullKey))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("key");

        Map<Integer, byte[]> nullVersion = new HashMap<>();
        nullVersion.put(null, key(1));
        assertThatThrownBy(() -> MasterKeyStore.of(nullVersion))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("version");
    }
}
