package com.trading.common.schema.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Contract of {@link EnvelopeCrypto}: AES-256 only, inputs validated before the
 * cipher runs, and tamper evidence kept distinguishable from programming
 * errors (P6-283, P6-284, P6-675, P6-676, P6-677).
 */
class EnvelopeCryptoTest {

    private static final byte[] AAD =
            "2026-08-14__Trade_Decisions".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PLAIN = "the export payload".getBytes(StandardCharsets.UTF_8);

    private static byte[] key(int length) {
        byte[] k = new byte[length];
        for (int i = 0; i < length; i++) {
            k[i] = (byte) (i + 1);
        }
        return k;
    }

    @Test
    void sealAndOpenAcceptOnlyAes256Keys() {
        // 16/24-byte keys are valid AES-128/192 to the JDK: accepting them would
        // silently downgrade the class contract.
        for (int length : new int[] {16, 24, 31, 33}) {
            assertThatThrownBy(() -> EnvelopeCrypto.seal(key(length), PLAIN, AAD))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("AES-256")
                    .hasMessageContaining(length + " bytes");
        }
        assertThatThrownBy(() -> EnvelopeCrypto.seal(null, PLAIN, AAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dataKey");

        byte[] sealed = EnvelopeCrypto.seal(key(32), PLAIN, AAD);
        assertThatThrownBy(() -> EnvelopeCrypto.open(key(16), sealed, AAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AES-256");
        assertThatThrownBy(() -> EnvelopeCrypto.open(null, sealed, AAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dataKey");
    }

    @Test
    void wrapAndUnwrapAreAes256OnlyToo() {
        byte[] masterKey = key(32);
        byte[] dataKey = EnvelopeCrypto.newDataKey();
        byte[] wrapped = EnvelopeCrypto.wrap(masterKey, dataKey, AAD);

        assertThat(EnvelopeCrypto.unwrap(masterKey, wrapped, AAD)).isEqualTo(dataKey);
        assertThatThrownBy(() -> EnvelopeCrypto.wrap(key(16), dataKey, AAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AES-256");
        assertThatThrownBy(() -> EnvelopeCrypto.unwrap(key(24), wrapped, AAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AES-256");
    }

    @Test
    void emptyOrNullAadIsRefusedOnBothPaths() {
        byte[] dataKey = EnvelopeCrypto.newDataKey();
        byte[] sealed = EnvelopeCrypto.seal(dataKey, PLAIN, AAD);

        for (byte[] aad : new byte[][] {null, new byte[0]}) {
            assertThatThrownBy(() -> EnvelopeCrypto.seal(dataKey, PLAIN, aad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("record identity");
            assertThatThrownBy(() -> EnvelopeCrypto.open(dataKey, sealed, aad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("record identity");
        }
    }

    @Test
    void openRejectsPayloadsShorterThanIvPlusTag() {
        byte[] dataKey = EnvelopeCrypto.newDataKey();
        // Valid layout is iv(12) || ciphertext || tag(16): 28 bytes is the floor.
        for (int length : new int[] {0, 12, 13, 27}) {
            byte[] truncated = new byte[length];
            assertThatThrownBy(() -> EnvelopeCrypto.open(dataKey, truncated, AAD))
                    .as("length %d", length)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("too short");
        }
        assertThatThrownBy(() -> EnvelopeCrypto.open(dataKey, null, AAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short");

        // 28 bytes is long enough to reach the cipher, where the tag check decides.
        assertThatThrownBy(() -> EnvelopeCrypto.open(dataKey, new byte[28], AAD))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wrong key, tamper, or AAD");
    }

    @Test
    void programmingErrorsAreNotRewrappedAsTamperFindings() {
        byte[] dataKey = EnvelopeCrypto.newDataKey();
        byte[] sealed = EnvelopeCrypto.seal(dataKey, PLAIN, AAD);

        assertThatThrownBy(() -> EnvelopeCrypto.seal(null, PLAIN, AAD))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EnvelopeCrypto.seal(dataKey, null, AAD))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("plaintext");
        assertThatThrownBy(() -> EnvelopeCrypto.open(dataKey, null, AAD))
                .isInstanceOf(IllegalArgumentException.class);

        // Genuine tamper stays an IllegalStateException.
        assertThatThrownBy(() -> EnvelopeCrypto.open(key(32), sealed, AAD))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void roundTripBindsToTheRecordIdentity() {
        byte[] dataKey = EnvelopeCrypto.newDataKey();
        byte[] sealed = EnvelopeCrypto.seal(dataKey, PLAIN, AAD);

        assertThat(sealed).hasSizeGreaterThanOrEqualTo(28);
        assertThat(EnvelopeCrypto.open(dataKey, sealed, AAD)).isEqualTo(PLAIN);
        assertThatThrownBy(() -> EnvelopeCrypto.open(dataKey, sealed,
                "2026-08-14__Other_Table".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalStateException.class);
    }
}
