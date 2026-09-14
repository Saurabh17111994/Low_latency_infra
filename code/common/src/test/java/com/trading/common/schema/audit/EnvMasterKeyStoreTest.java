package com.trading.common.schema.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Environment-driven master keys: 32 bytes or nothing (P6-281), copies neither
 * way (P6-282), and rotation versions found through gaps (P6-674).
 */
class EnvMasterKeyStoreTest {

    private static byte[] key(int seed) {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) {
            k[i] = (byte) (seed + i);
        }
        return k;
    }

    private static String b64(byte[] raw) {
        return Base64.getEncoder().encodeToString(raw);
    }

    @Test
    void masterKeysMustBeExactly32Bytes() {
        for (int length : new int[] {16, 24, 31, 33}) {
            Map<String, String> env = new HashMap<>();
            env.put("EOD_EXPORT_MASTER_KEY_B64", b64(new byte[length]));
            assertThatThrownBy(() -> EnvMasterKeyStore.fromEnv(env::get))
                    .as("key length %d", length)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("32 bytes")
                    .hasMessageContaining(String.valueOf(length));
        }

        Map<String, String> env = new HashMap<>();
        env.put("EOD_EXPORT_MASTER_KEY_B64", b64(key(1)));
        assertThat(EnvMasterKeyStore.fromEnv(env::get).keyFor(1)).isEqualTo(key(1));
    }

    @Test
    void rotatedKeyWithAWrongLengthIsRejectedToo() {
        Map<String, String> env = new HashMap<>();
        env.put("EOD_EXPORT_MASTER_KEY_B64", b64(key(1)));
        env.put("EOD_EXPORT_MASTER_KEY_V2_B64", b64(new byte[16]));

        assertThatThrownBy(() -> EnvMasterKeyStore.fromEnv(env::get))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
    }

    @Test
    void keyForHandsBackACopySoCallersCannotCorruptTheStore() {
        Map<String, String> env = new HashMap<>();
        env.put("EOD_EXPORT_MASTER_KEY_B64", b64(key(1)));
        EnvMasterKeyStore store = EnvMasterKeyStore.fromEnv(env::get);

        byte[] handedOut = store.keyFor(1);
        Arrays.fill(handedOut, (byte) 0);

        assertThat(store.keyFor(1)).isEqualTo(key(1));
    }

    @Test
    void aVersionGapDoesNotHideHigherKeys() {
        Map<String, String> env = new HashMap<>();
        env.put("EOD_EXPORT_MASTER_KEY_B64", b64(key(1)));
        env.put("EOD_EXPORT_MASTER_KEY_V3_B64", b64(key(3)));   // v2 retired

        EnvMasterKeyStore store = EnvMasterKeyStore.fromEnv(env::get);

        assertThat(store.currentVersion()).isEqualTo(3);
        assertThat(store.keyFor(3)).isEqualTo(key(3));
        assertThat(store.keyFor(1)).isEqualTo(key(1));
        // A genuinely absent version still fails closed.
        assertThatThrownBy(() -> store.keyFor(2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theProbeIsBoundedAndSaysSo() {
        Map<String, String> env = new HashMap<>();
        env.put("EOD_EXPORT_MASTER_KEY_B64", b64(key(1)));
        env.put("EOD_EXPORT_MASTER_KEY_V" + EnvMasterKeyStore.MAX_PROBED_VERSION + "_B64",
                b64(key(9)));
        env.put("EOD_EXPORT_MASTER_KEY_V" + (EnvMasterKeyStore.MAX_PROBED_VERSION + 1) + "_B64",
                b64(key(10)));

        EnvMasterKeyStore store = EnvMasterKeyStore.fromEnv(env::get);

        assertThat(store.currentVersion()).isEqualTo(EnvMasterKeyStore.MAX_PROBED_VERSION);
        assertThatThrownBy(() -> store.keyFor(EnvMasterKeyStore.MAX_PROBED_VERSION + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
