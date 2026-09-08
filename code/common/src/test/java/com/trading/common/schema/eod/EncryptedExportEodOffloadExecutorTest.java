package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.EodControllerState;
import com.trading.common.schema.audit.EnvelopeCrypto;
import com.trading.common.schema.audit.EnvMasterKeyStore;
import com.trading.common.schema.audit.MasterKeyStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Encrypted export pipeline (pure JVM): envelope sealing, key-versioned
 * wrapping, fail-closed verification (tamper / wrong key / missing version),
 * and key rotation keeping old bundles decryptable. The R2 push of the staging
 * Retained as the offload-encryption correctness proof.
 */
class EncryptedExportEodOffloadExecutorTest {

    @TempDir
    Path tmp;

    private static EodOffloadRecord record() {
        return new EodOffloadRecord("2026-08-14", "Trade_Decisions", "2",
                100L, 500L, 42L, 8_192L, "src-hash", "tgt-hash",
                "snap-1", EodControllerState.VERIFIED, 0, 0L,
                Long.MAX_VALUE, 1_700_000_000_000L);
    }

    /** Mirror the controller's withContent: the committed record carries the
     *  offload's evidence, which verify() binds against (P4-112). */
    private static EodOffloadRecord committedFrom(OffloadResult r) {
        EodOffloadRecord base = record();
        return new EodOffloadRecord(base.tradingDate(), base.tableName(), base.schemaVersion(),
                r.sourceOffsetStart(), r.sourceOffsetEnd(), r.rowCount(), r.byteCount(),
                r.sourceHash(), r.targetHash(), r.icebergSnapshotId(), base.state(),
                base.retryCount(), base.nextRetryAtMs(), base.earliestAllowedSourceExpiryMs(),
                base.updatedAtMs());
    }

    private static byte[] key() {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) {
            k[i] = (byte) (i + 1);
        }
        return k;
    }

    private EncryptedExportEodOffloadExecutor executor(MasterKeyStore store, Path src,
            Path staging, long rows) {
        return new EncryptedExportEodOffloadExecutor(staging, store,
                new FileBundleSource(src), rows);
    }

    @Test
    void offloadWritesSealedBundleAndVerifies() throws Exception {
        Path src = Files.createDirectories(tmp.resolve("src"));
        Path staging = Files.createDirectories(tmp.resolve("staging"));
        Files.writeString(src.resolve("2026-08-14__Trade_Decisions.plain"),
                "payload-rows-for-the-day");
        EncryptedExportEodOffloadExecutor ex = executor(
                MasterKeyStore.of(Map.of(1, key())), src, staging, 42L);

        OffloadResult r = ex.offload(record());
        assertThat(r.success()).isTrue();
        EodOffloadRecord committed = committedFrom(r);
        assertThat(r.rowCount()).isEqualTo(42L);
        assertThat(r.byteCount()).isEqualTo("payload-rows-for-the-day".length());
        assertThat(r.sourceHash()).isNotBlank();
        assertThat(r.targetHash()).isNotBlank();
        assertThat(r.icebergSnapshotId()).startsWith("env-export-v1:");

        assertThat(Files.exists(staging.resolve("2026-08-14__Trade_Decisions.enc"))).isTrue();
        assertThat(Files.exists(staging.resolve("2026-08-14__Trade_Decisions.key.v1"))).isTrue();
        assertThat(Files.exists(staging.resolve("2026-08-14__Trade_Decisions.manifest.json")))
                .isTrue();
        // The plaintext must never land on the staging dir.
        assertThat(Files.exists(staging.resolve("2026-08-14__Trade_Decisions.plain"))).isFalse();
        byte[] sealed = Files.readAllBytes(staging.resolve("2026-08-14__Trade_Decisions.enc"));
        assertThat(new String(sealed, StandardCharsets.ISO_8859_1))
                .doesNotContain("payload-rows-for-the-day");

        assertThat(ex.verify(committed)).isTrue();
    }

    @Test
    void tamperedCiphertextFailsVerify() throws Exception {
        Path src = Files.createDirectories(tmp.resolve("src"));
        Path staging = Files.createDirectories(tmp.resolve("staging"));
        Files.writeString(src.resolve("2026-08-14__Trade_Decisions.plain"), "payload");
        EncryptedExportEodOffloadExecutor ex = executor(
                MasterKeyStore.of(Map.of(1, key())), src, staging, 1L);
        assertThat(ex.offload(record()).success()).isTrue();

        Path enc = staging.resolve("2026-08-14__Trade_Decisions.enc");
        byte[] sealed = Files.readAllBytes(enc);
        sealed[sealed.length - 1] ^= 0x01; // flip one tag byte
        Files.write(enc, sealed);

        assertThat(ex.verify(record())).isFalse();
    }

    @Test
    void wrongKeyVersionFailsVerify() throws Exception {
        Path src = Files.createDirectories(tmp.resolve("src"));
        Path staging = Files.createDirectories(tmp.resolve("staging"));
        Files.writeString(src.resolve("2026-08-14__Trade_Decisions.plain"), "payload");
        EncryptedExportEodOffloadExecutor ex = executor(
                MasterKeyStore.of(Map.of(1, key())), src, staging, 1L);
        assertThat(ex.offload(record()).success()).isTrue();

        // Verify against a store that dropped v1 (rotation expired it) — the
        // wrapped key is unwrappable -> fail closed, never silent.
        EncryptedExportEodOffloadExecutor rotated =
                executor(MasterKeyStore.of(Map.of(2, key())), src, staging, 1L);
        assertThat(rotated.verify(record())).isFalse();
    }

    @Test
    void keyRotationKeepsOldBundlesDecryptableAndNewBundlesUseCurrent() throws Exception {
        Path src = Files.createDirectories(tmp.resolve("src"));
        Path staging = Files.createDirectories(tmp.resolve("staging"));
        byte[] v1 = key();
        byte[] v2 = new byte[32];
        System.arraycopy(v1, 0, v2, 0, 32);
        v2[0] ^= 0x7f;
        Files.writeString(src.resolve("2026-08-14__Trade_Decisions.plain"), "payload-v1");
        EncryptedExportEodOffloadExecutor v1Store =
                executor(MasterKeyStore.of(Map.of(1, v1)), src, staging, 1L);
        assertThat(v1Store.offload(record()).success()).isTrue();
        assertThat(Files.exists(staging.resolve("2026-08-14__Trade_Decisions.key.v1"))).isTrue();

        // Rotate: the store retains v1 and adds v2 as current.
        EncryptedExportEodOffloadExecutor rotated =
                executor(MasterKeyStore.of(Map.of(1, v1, 2, v2)), src, staging, 1L);
        OffloadResult vr = v1Store.offload(record());
        assertThat(rotated.verify(committedFrom(vr))).isTrue(); // v1 bundle still verifies

        Files.writeString(src.resolve("2026-08-14__Trade_Decisions.plain"), "payload-v2");
        assertThat(rotated.offload(record()).success()).isTrue();
        assertThat(Files.exists(staging.resolve("2026-08-14__Trade_Decisions.key.v2"))).isTrue();
    }

    @Test
    void missingSourceFailsClosed() throws Exception {
        Path src = Files.createDirectories(tmp.resolve("src-empty"));
        Path staging = Files.createDirectories(tmp.resolve("staging"));
        EncryptedExportEodOffloadExecutor ex = executor(
                MasterKeyStore.of(Map.of(1, key())), src, staging, 1L);
        OffloadResult r = ex.offload(record());
        assertThat(r.success()).isFalse();
        assertThat(r.error()).contains("bundle source missing");
    }

    @Test
    void envStoreRequiresMasterKeyFailClosed() {
        Map<String, String> empty = new HashMap<>();
        try {
            EnvMasterKeyStore.fromEnv(empty::get);
            assertThat(true).as("missing master key must fail closed").isFalse();
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains("EOD_EXPORT_MASTER_KEY_B64");
        }
    }

    @Test
    void envStoreReadsRotatedVersions() {
        Map<String, String> env = new HashMap<>();
        env.put("EOD_EXPORT_MASTER_KEY_B64",
                Base64.getEncoder().encodeToString(key()));
        env.put("EOD_EXPORT_MASTER_KEY_V2_B64",
                Base64.getEncoder().encodeToString(new byte[32]));
        EnvMasterKeyStore store = EnvMasterKeyStore.fromEnv(env::get);
        assertThat(store.currentVersion()).isEqualTo(2);
        assertThat(store.keyFor(1)).isEqualTo(key());
        assertThat(store.keyFor(2)).hasSize(32);
    }

    @Test
    void envelopeRoundTripAndAadBinding() throws Exception {
        byte[] dataKey = EnvelopeCrypto.newDataKey();
        byte[] master = key();
        byte[] aad = "record-1".getBytes(StandardCharsets.UTF_8);
        byte[] plain = "the export payload".getBytes(StandardCharsets.UTF_8);

        byte[] sealed = EnvelopeCrypto.seal(dataKey, plain, aad);
        assertThat(sealed).isNotEqualTo(plain);
        assertThat(EnvelopeCrypto.open(dataKey, sealed, aad)).isEqualTo(plain);

        byte[] wrapped = EnvelopeCrypto.wrap(master, dataKey, aad);
        byte[] unwrapped = EnvelopeCrypto.unwrap(master, wrapped, aad);
        assertThat(EnvelopeCrypto.open(unwrapped, sealed, aad)).isEqualTo(plain);

        // Wrong AAD must fail open — the bundle is bound to its record id.
        try {
            EnvelopeCrypto.open(dataKey, sealed, "other-record".getBytes(StandardCharsets.UTF_8));
            assertThat(true).as("wrong AAD must fail open").isFalse();
        } catch (Exception expected) {
            // expected — AEAD rejects the wrong AAD
        }
    }

    @Test
    void perRecordRowCountsAreStamped() throws Exception {
        Path src = Files.createDirectories(tmp.resolve("src2"));
        Path staging = Files.createDirectories(tmp.resolve("staging2"));
        Files.writeString(src.resolve("2026-08-14__Trade_Decisions.plain"), "payload");
        EncryptedExportEodOffloadExecutor ex = new EncryptedExportEodOffloadExecutor(
                staging, MasterKeyStore.of(Map.of(1, key())), new FileBundleSource(src),
                record -> record.tableName().equals("Trade_Decisions") ? 7L : 99L);
        OffloadResult r = ex.offload(record());
        assertThat(r.success()).isTrue();
        assertThat(r.rowCount()).isEqualTo(7L);
        assertThat(ex.verify(committedFrom(r))).isTrue();
    }

    @Test
    void swappedManifestFailsVerify() throws Exception {
        Path src = Files.createDirectories(tmp.resolve("src3"));
        Path staging = Files.createDirectories(tmp.resolve("staging3"));
        Files.writeString(src.resolve("2026-08-14__Trade_Decisions.plain"), "payload");
        EncryptedExportEodOffloadExecutor ex = executor(
                MasterKeyStore.of(Map.of(1, key())), src, staging, 1L);
        OffloadResult r = ex.offload(record());
        assertThat(r.success()).isTrue();
        EodOffloadRecord committed = committedFrom(r);

        // Attacker rewrites the manifest with self-consistent hashes for a
        // different payload — must fail against the committed evidence.
        Path manifest = staging.resolve("2026-08-14__Trade_Decisions.manifest.json");
        String json = Files.readString(manifest).replace("\"byte_count\":7", "\"byte_count\":8");
        Files.writeString(manifest, json);
        assertThat(ex.verify(committed)).isFalse();
    }
}
