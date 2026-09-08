package com.trading.common.schema.eod;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.schema.audit.EnvelopeCrypto;
import com.trading.common.schema.audit.MasterKeyStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.ToLongFunction;

/**
 * The encrypted offload half: envelope-encrypts a trading day's source data
 * into an immutable, key-versioned export bundle on the staging target and
 * verifies it by re-opening. Replaces the fail-closed
 * {@link NotConfiguredEodOffloadExecutor} / {@link MockEodOffloadExecutor}
 * when a master key is configured.
 *
 * <p><b>Pipeline</b> (per record): {@link BundleSource#read} acquires the
 * source payload (the Fluss reader is the documented integration — the
 * {@code FileBundleSource} twin serves drills); a fresh 256-bit data key
 * seals the payload (AAD = record id); the data key is wrapped with the
 * master key for {@link MasterKeyStore#currentVersion()}; the sealed payload,
 * the wrapped key, and a JSON manifest (version, key version, row/byte counts,
 * source + target hashes) land on the staging dir. {@link #verify} re-opens
 * with the manifest's key version and re-checks the source hash — fail-closed
 * on wrong key, tamper, or missing version.
 *
 * <p>The R2 push of the staging bundle rides the existing Python tooling
 * ({@code audit_r2.py}) — documented runbook step, not a Java S3 client.
 * {@code icebergSnapshotId} carries the manifest name so the controller's
 * evidence is content-addressed.
 */
public final class EncryptedExportEodOffloadExecutor implements EodOffloadExecutor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Acquires the plaintext bundle for one offload record. */
    public interface BundleSource {
        byte[] read(EodOffloadRecord record) throws Exception;
    }

    private final Path stagingDir;
    private final MasterKeyStore keyStore;
    private final BundleSource source;
    private final ToLongFunction<EodOffloadRecord> rowCountProvider;

    public EncryptedExportEodOffloadExecutor(Path stagingDir, MasterKeyStore keyStore,
            BundleSource source, ToLongFunction<EodOffloadRecord> rowCountProvider) {
        this.stagingDir = stagingDir;
        this.keyStore = keyStore;
        this.source = source;
        this.rowCountProvider = rowCountProvider;
    }

    /** Back-compat bridge for single-row-count callers (drills) — P4-108. */
    public EncryptedExportEodOffloadExecutor(Path stagingDir, MasterKeyStore keyStore,
            BundleSource source, long rowCount) {
        this(stagingDir, keyStore, source, ignored -> rowCount);
    }

    private static String recordId(EodOffloadRecord r) {
        // P4-109: unsanitized record fields become file names — a table name
        // with '/', '\', or '..' escapes stagingDir on write AND read.
        if (r.tradingDate() == null || r.tableName() == null
                || !r.tradingDate().matches("\\d{4}-\\d{2}-\\d{2}")
                || !r.tableName().matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("invalid record identity: "
                    + r.tradingDate() + "__" + r.tableName());
        }
        return r.tradingDate() + "__" + r.tableName();
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    private static byte[] sha256(byte[] payload) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(payload);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public OffloadResult offload(EodOffloadRecord record) {
        String id = recordId(record);
        try {
            byte[] payload = source.read(record);
            byte[] dataKey = EnvelopeCrypto.newDataKey();
            // P4-268: currentVersion()+keyFor() check-then-act — a rotation in
            // between yields a stale bundle or a spurious failure. Retry once
            // with the fresh version (atomic snapshot would need a keystore
            // API change; the retry covers the race without one).
            int keyVersion = keyStore.currentVersion();
            byte[] masterKey;
            try {
                masterKey = keyStore.keyFor(keyVersion);
            } catch (IllegalArgumentException rotated) {
                keyVersion = keyStore.currentVersion();
                masterKey = keyStore.keyFor(keyVersion);
            }
            byte[] aad = id.getBytes(StandardCharsets.UTF_8);

            byte[] sealed = EnvelopeCrypto.seal(dataKey, payload, aad);
            byte[] wrapped = EnvelopeCrypto.wrap(masterKey, dataKey, aad);

            // P4-110: sequential truncating writes leave a torn bundle on
            // crash (new payload + old manifest). Stage to temp, atomic-move
            // manifest-last so readers never see a mixed version.
            Files.createDirectories(stagingDir);
            Path sealedPath = stagingDir.resolve(id + ".enc");
            Path keyPath = stagingDir.resolve(id + ".key.v" + keyVersion);
            Path manifestPath = stagingDir.resolve(id + ".manifest.json");
            Path sealedTmp = Files.createTempFile(stagingDir, id, ".enc.tmp");
            Path keyTmp = Files.createTempFile(stagingDir, id, ".key.tmp");
            Path manifestTmp = Files.createTempFile(stagingDir, id, ".manifest.tmp");
            try {
                Files.write(sealedTmp, sealed);
                Files.write(keyTmp, wrapped);
                // P4-269: each digest/hex computed once (was 2x each + a fresh
                // ObjectMapper per call). Streaming to temp files for day-scale
                // payloads is deferred — current payloads are drill-scale;
                // revisit if a payload approaches heap (OOM: plaintext+sealed
                // both resident).
                String sourceHex = hex(sha256(payload));
                String targetHex = hex(sha256(sealed));
                Map<String, Object> manifest = new LinkedHashMap<>();
                manifest.put("record_id", id);
                manifest.put("trading_date", record.tradingDate());
                manifest.put("table_name", record.tableName());
                manifest.put("schema_version", record.schemaVersion());
                manifest.put("key_version", keyVersion);
                manifest.put("aad", id);
                manifest.put("sealed_file", sealedPath.getFileName().toString());
                manifest.put("wrapped_key_file", keyPath.getFileName().toString());
                long rows = rowCountProvider.applyAsLong(record);
                manifest.put("row_count", rows);
                manifest.put("byte_count", payload.length);
                manifest.put("source_sha256", sourceHex);
                manifest.put("target_sha256", targetHex);
                manifest.put("created_utc", Instant.now().toString());
                Files.write(manifestTmp, MAPPER.writeValueAsString(manifest)
                        .getBytes(StandardCharsets.UTF_8));
                Files.move(sealedTmp, sealedPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
                Files.move(keyTmp, keyPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
                Files.move(manifestTmp, manifestPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
                return new OffloadResult(true,
                        record.sourceOffsetStart(), record.sourceOffsetEnd(),
                        rows, payload.length, sourceHex, targetHex,
                        "env-export-v1:" + manifestPath.getFileName(), null);
            } finally {
                Files.deleteIfExists(sealedTmp);
                Files.deleteIfExists(keyTmp);
                Files.deleteIfExists(manifestTmp);
            }
        } catch (Exception e) {
            return OffloadResult.failure("encrypted export failed for " + id + ": " + e);
        }
    }

    @Override
    public boolean verify(EodOffloadRecord committed) {
        String id = recordId(committed);
        try {
            Path manifestPath = stagingDir.resolve(id + ".manifest.json");
            if (!Files.exists(manifestPath)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> manifest = MAPPER
                    .readValue(Files.readAllBytes(manifestPath), Map.class);
            // P4-111: the manifest is unauthenticated JSON — never trust it
            // for names or AAD. Pin filenames to id-derived names, recompute
            // AAD from id, and contain all resolved paths inside stagingDir.
            int keyVersion = ((Number) manifest.get("key_version")).intValue();
            String sealedName = (String) manifest.get("sealed_file");
            String wrappedName = (String) manifest.get("wrapped_key_file");
            if (!sealedName.equals(id + ".enc")
                    || !wrappedName.equals(id + ".key.v" + keyVersion)) {
                return false;
            }
            if (!id.equals(manifest.get("record_id")) || !id.equals(manifest.get("aad"))) {
                return false;
            }
            if (!committed.tradingDate().equals(manifest.get("trading_date"))
                    || !committed.tableName().equals(manifest.get("table_name"))) {
                return false;
            }
            Path staging = stagingDir.normalize().toAbsolutePath();
            Path sealedPath = stagingDir.resolve(sealedName).normalize().toAbsolutePath();
            Path wrappedPath = stagingDir.resolve(wrappedName).normalize().toAbsolutePath();
            if (!sealedPath.startsWith(staging) || !wrappedPath.startsWith(staging)) {
                return false;
            }
            byte[] wrapped = Files.readAllBytes(wrappedPath);
            byte[] sealed = Files.readAllBytes(sealedPath);
            byte[] aad = id.getBytes(StandardCharsets.UTF_8);

            byte[] dataKey = EnvelopeCrypto.unwrap(keyStore.keyFor(keyVersion), wrapped, aad);
            byte[] payload = EnvelopeCrypto.open(dataKey, sealed, aad);

            // P4-112: bind to the controller's committed evidence — a rewritten
            // manifest with self-consistent hashes must still fail when it
            // diverges from what offload committed.
            String expectedSource = (String) manifest.get("source_sha256");
            String expectedTarget = (String) manifest.get("target_sha256");
            if (!hex(sha256(payload)).equals(expectedSource)) return false;
            if (!hex(sha256(sealed)).equals(expectedTarget)) return false;
            if (!expectedSource.equals(committed.sourceHash())) return false;
            if (!expectedTarget.equals(committed.targetHash())) return false;
            if (((Number) manifest.get("byte_count")).longValue() != committed.byteCount()) {
                return false;
            }
            if (((Number) manifest.get("row_count")).longValue() != committed.rowCount()) {
                return false;
            }
            return ((Number) manifest.get("byte_count")).longValue() == payload.length;
        } catch (Exception e) {
            return false; // fail-closed: any mismatch is a failed verification
        }
    }
}
