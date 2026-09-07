package com.trading.ingestion;

import com.trading.ingestion.model.Instrument;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load the instrument manifest from the approved source.
 *
 * <p>Production path: Arrow {@code GET /nse} CSV, refreshed daily at 8 AM IST.
 * Development path: {@code code/01_platform/05_instruments/} directory.
 *
 * <p><b>Manifest enforcement (SCH-22):</b> one approved manifest version defines
 * the active subscription set. The loader validates the loaded instrument count
 * and a deterministic manifest fingerprint against the configured expected values.
 * If validation fails, readiness remains {@code false} and a warning is logged.
 * The synthetic fallback is only accepted when {@code ALLOW_SYNTHETIC_MANIFEST=true}.
 *
 * <p><b>Evidence-gated:</b> the production CSV parsing path is blocked until
 * the Arrow REST contract is verified ({@code TO_BE_VERIFIED} in
 * {@code versions.pin}). The development path loads from a local file or
 * falls back to a synthetic small instrument set for local testing.
 */
final class InstrumentManifestLoader {

    private static final Logger LOG = LoggerFactory.getLogger(InstrumentManifestLoader.class);

    private InstrumentManifestLoader() {}

    /**
     * Result of loading + validating the instrument manifest.
     * Carries the instrument list and whether the manifest was approved.
     */
    public record ManifestResult(List<Instrument> instruments, boolean approved,
                                  int version, int instrumentCount, String fingerprint) {}

    /**
     * Load instruments from the default source. In development, this loads
     * from {@code code/01_platform/05_instruments/} if available; falls back
     * to a small synthetic set for local Compose testing only when
     * {@code ALLOW_SYNTHETIC_MANIFEST=true}.
     */
    static ManifestResult loadDefault() {
        String manifestEnv = System.getenv("INSTRUMENT_MANIFEST_PATH");
        if (manifestEnv != null && !manifestEnv.isBlank()) {
            return loadFromPath(manifestEnv);
        }

        boolean allowSynthetic = "true".equalsIgnoreCase(
                System.getenv().getOrDefault("ALLOW_SYNTHETIC_MANIFEST", "false"));

        if (!allowSynthetic) {
            LOG.error("instrument-manifest: no manifest path configured and "
                    + "ALLOW_SYNTHETIC_MANIFEST is not true — ingestion cannot start");
            return new ManifestResult(List.of(), false, 0, 0, "");
        }

        // Fallback: synthetic 50-instrument set matching MockArrowServer defaults
        LOG.warn("instrument-manifest: using synthetic fallback set (50 instruments)");
        LOG.warn("instrument-manifest: production requires Arrow GET /nse CSV (TO_BE_VERIFIED)");
        List<Instrument> instruments = syntheticSet();
        String fp = computeFingerprint(instruments);
        LOG.info("instrument-manifest: approved=false, version=1, instruments={}, fingerprint={}",
                instruments.size(), fp.substring(0, Math.min(12, fp.length())));
        return new ManifestResult(instruments, false, 1, instruments.size(), fp);
    }

    static ManifestResult loadFromPath(String path) {
        return loadFromPath(path, System.getenv("INSTRUMENT_MANIFEST_VERSION"));
    }

    /** Version-aware variant (package-private for tests; null/blank = 1). */
    static ManifestResult loadFromPath(String path, String manifestVersion) {
        // P1-227: version from INSTRUMENT_MANIFEST_VERSION, not a hardcoded
        // 1 — a refreshed daily CSV is a NEW approved version (R-247); the
        // hardcode made the version check in isManifestApproved vacuous.
        // Unset/blank keeps 1 (today's behavior); explicit garbage refuses
        // the load fail-closed instead of silently versioning it as 1.
        int version;
        try {
            version = parseManifestVersion(manifestVersion);
        } catch (IllegalArgumentException e) {
            LOG.error("instrument-manifest: {}", e.getMessage());
            return emptyManifest();
        }
        return loadFromPath(path, version);
    }

    static int parseManifestVersion(String raw) {
        if (raw == null || raw.isBlank()) return 1;
        try {
            int v = Integer.parseInt(raw.trim());
            if (v <= 0) throw new IllegalArgumentException(
                    "INSTRUMENT_MANIFEST_VERSION must be a positive integer, got: " + raw);
            return v;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "INSTRUMENT_MANIFEST_VERSION must be a positive integer, got: " + raw);
        }
    }

    /**
     * Load and validate the manifest from {@code path} at {@code version}.
     *
     * <p>R-247: the manifest version is a parameter, not a hardcoded 1 — a
     * refreshed daily Arrow CSV is a NEW approved version; pinning 1 forever
     * made the version check in {@link #isManifestApproved} vacuous.
     *
     * <p>R-283: reads are pinned to UTF-8 (the platform default charset would
     * mangle an Excel-exported CSV on a non-UTF-8 host) and a UTF-8 BOM on
     * the header line is stripped.
     *
     * <p>R-061: a manifest that loaded ZERO data rows is NOT approved — an
     * empty approval would let readiness pass with no subscription set.
     */
    static ManifestResult loadFromPath(String path, int version) {
        LOG.info("instrument-manifest: loading from {} (version {})", path, version);
        try (BufferedReader reader = Files.newBufferedReader(Path.of(path), StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null || header.isBlank()) {
                LOG.error("instrument-manifest: CSV is empty");
                return emptyManifest();
            }
            // R-283: strip a UTF-8 BOM that Excel writes on exported CSVs.
            if (header.startsWith("\uFEFF")) {
                header = header.substring(1);
            }

            // Parse Arrow CSV: Exchange,Segment,ExchSeg,Token,FullName,...
            // P1-068: match case-insensitively and accept the documented
            // FullName alias - the old exact-case TradingSymbol-only match left
            // symbolIdx at -1 on real CSVs, failing every row downstream.
            List<String> columns = parseCsvRecord(header);
            int tokenIdx = -1, symbolIdx = -1, exchangeIdx = -1, lotSizeIdx = -1, segmentIdx = -1;
            for (int i = 0; i < columns.size(); i++) {
                switch (columns.get(i).trim().toLowerCase(java.util.Locale.ROOT)) {
                    case "token" -> tokenIdx = i;
                    case "tradingsymbol", "fullname" -> symbolIdx = i;
                    case "exchange" -> exchangeIdx = i;
                    case "lotsize" -> lotSizeIdx = i;
                    case "segment" -> segmentIdx = i;
                }
            }
            if (tokenIdx < 0) {
                LOG.error("instrument-manifest: CSV missing Token column");
                return emptyManifest();
            }
            // P1-068 fail-fast: same outcome as before (every row failed the
            // blank-symbol check and aborted the load per P1-069), but reported
            // at the header with the true cause instead of row 2.
            if (symbolIdx < 0) {
                LOG.error("instrument-manifest: CSV missing symbol column "
                        + "(want TradingSymbol or FullName); refusing to load");
                return emptyManifest();
            }
            // P1-086 (Batch-1 #9): lotSize is load-bearing (quantity math,
            // P1-070 hash input) and every live manifest carries it (all 3
            // cash CSVs, 5,777 rows, zero blanks — checked 2026-09-07), so a
            // missing column refuses the load instead of defaulting to 1.
            if (lotSizeIdx < 0) {
                LOG.error("instrument-manifest: CSV missing LotSize column; refusing to load");
                return emptyManifest();
            }

            List<Instrument> instruments = new ArrayList<>();
            String line;
            int lineNum = 1, skipped = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.isBlank()) continue;
                List<String> fields = parseCsvRecord(line);
                try {
                    if (tokenIdx >= fields.size()) throw new NumberFormatException("missing Token field");
                    long token = Long.parseLong(fields.get(tokenIdx).trim());
                    // P1-086: column presence proven above; a short row is
                    // malformed like a short token row (skip + count).
                    if (lotSizeIdx >= fields.size()) throw new NumberFormatException("missing LotSize field");
                    int lotSize = parseLotSize(fields.get(lotSizeIdx));
                    instruments.add(new Instrument.Builder()
                            .instrumentToken(token)
                            .tradingSymbol(symbolIdx >= 0 && symbolIdx < fields.size()
                                    ? fields.get(symbolIdx).trim() : "")
                            .exchange(exchangeIdx >= 0 && exchangeIdx < fields.size()
                                    ? fields.get(exchangeIdx).trim() : "NSE")
                            // P1-229: read the Segment column (live CSVs
                            // carry Segment/ExchSeg) instead of hardcoding
                            // "CM" — an FO file today would silently
                            // misclassify every row. Absent/blank keeps "CM".
                            .segment(segmentIdx >= 0 && segmentIdx < fields.size()
                                    && !fields.get(segmentIdx).isBlank()
                                    ? fields.get(segmentIdx).trim() : "CM")
                            .lotSize(lotSize)
                            .manifestVersion(version)
                            .build());
                } catch (NumberFormatException e) {
                    if (skipped == 0) {
                        LOG.debug("instrument-manifest: skipping malformed line {} — token parse failed",
                                lineNum, e);
                    }
                    skipped++;
                }
            }

            if (skipped > 0) {
                LOG.warn("instrument-manifest: skipped {} malformed lines out of {}", skipped, lineNum - 1);
            }

            // R-061: zero loaded rows is a FAILED load, not an approved one.
            if (instruments.isEmpty()) {
                LOG.error("instrument-manifest: loaded 0 instrument rows — refusing to approve");
                return emptyManifest();
            }

            List<Instrument> result = Collections.unmodifiableList(instruments);
            String fp = computeFingerprint(result);
            LOG.info("instrument-manifest: approved=true, version={}, instruments={}, fingerprint={}",
                    version, result.size(), fp.substring(0, Math.min(12, fp.length())));
            return new ManifestResult(result, true, version, result.size(), fp);

        } catch (IOException e) {
            LOG.error("instrument-manifest: failed to read {}: {}", path, e.getMessage());
            return emptyManifest();
        } catch (IllegalArgumentException e) {
            LOG.error("instrument-manifest: malformed CSV {}: {}", path, e.getMessage());
            return emptyManifest();
        }
    }

    /** Parse one RFC 4180-style CSV record without adding a dependency. */
    static List<String> parseCsvRecord(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    field.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (ch == ',' && !quoted) {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(ch);
            }
        }
        if (quoted) throw new IllegalArgumentException("unterminated quoted field");
        fields.add(field.toString());
        return fields;
    }

    private static int parseLotSize(String field) {
        String trimmed = field.trim();
        // P1-086: blank/non-positive lotSize is invalid CONTENT (like a blank
        // symbol) — IAE aborts the load per the P1-069 fail-closed posture,
        // instead of masking it as quantity 1. Garbage stays
        // NumberFormatException so the existing skip-and-count path treats it
        // like a bad token.
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("lotSize must not be blank");
        }
        int val = Integer.parseInt(trimmed);
        if (val <= 0) {
            throw new IllegalArgumentException("lotSize must be positive, got " + val);
        }
        return val;
    }

    private static ManifestResult emptyManifest() {
        return new ManifestResult(List.of(), false, 0, 0, "");
    }

    /**
     * Validate that a loaded manifest matches the expected approved version.
     *
     * <p>One approved manifest version defines the active subscription state.
     * If the loaded manifest count or fingerprint differs from expected,
     * ingestion readiness must remain false until reconfigured.
     */
    static boolean isManifestApproved(ManifestResult result,
                                       int expectedVersion,
                                       int expectedCount,
                                       String expectedFingerprint) {
        if (result == null || result.instruments().isEmpty()) {
            LOG.error("instrument-manifest: validation failed — empty manifest");
            return false;
        }

        if (result.version() != expectedVersion) {
            LOG.error("instrument-manifest: version mismatch — loaded={}, expected={}",
                    result.version(), expectedVersion);
            return false;
        }

        if (result.instrumentCount() != expectedCount) {
            LOG.error("instrument-manifest: instrument count mismatch — loaded={}, expected={}",
                    result.instrumentCount(), expectedCount);
            return false;
        }

        if (!result.fingerprint().equals(expectedFingerprint)) {
            LOG.error("instrument-manifest: fingerprint mismatch — manifest content differs from approved version");
            return false;
        }

        LOG.info("instrument-manifest: approved (version={}, instruments={}, fingerprint={})",
                result.version(), result.instrumentCount(),
                result.fingerprint().substring(0, Math.min(12, result.fingerprint().length())));
        return true;
    }

    /**
     * Compute a deterministic SHA-256 fingerprint over every load-bearing
     * Instrument field in token-sorted order. Used for manifest version
     * validation.
     * P1-070: tokens-only let a same-tokens/different-symbols manifest pass
     * approval. NOTE on Batch-2 #15 (expiry, tickSize): those fields do not
     * exist on Instrument (writer-side ManifestEntry only) and the CSV cannot
     * populate them, so the fingerprint covers the five fields that do.
     */
    static String computeFingerprint(List<Instrument> instruments) {
        MessageDigest md = sha256();
        List<Instrument> sorted = instruments.stream()
                .sorted(java.util.Comparator.comparingLong(Instrument::instrumentToken))
                .toList();
        for (Instrument in : sorted) {
            long token = in.instrumentToken();
            md.update((byte) (token >>> 56));
            md.update((byte) (token >>> 48));
            md.update((byte) (token >>> 40));
            md.update((byte) (token >>> 32));
            md.update((byte) (token >>> 24));
            md.update((byte) (token >>> 16));
            md.update((byte) (token >>> 8));
            md.update((byte) (token));
            md.update(in.tradingSymbol().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            md.update((byte) 0);
            md.update(in.exchange().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            md.update((byte) 0);
            md.update(in.segment().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            md.update((byte) 0);
            int lot = in.lotSize();
            md.update((byte) (lot >>> 24));
            md.update((byte) (lot >>> 16));
            md.update((byte) (lot >>> 8));
            md.update((byte) lot);
        }
        return HexFormat.of().formatHex(md.digest());
    }

    /**
     * Synthetic instrument set for local development — matches
     * MockArrowServer's default 50 instruments (tokens 100000-104900).
     *
     * <p>R-027: the formula must be exactly {@code 100_000L + i * 100L}.
     * MockArrowServer builds its default set the same way, so every tick the
     * fake broker emits resolves against the loaded manifest — otherwise the
     * ALLOW_SYNTHETIC_MANIFEST dev path quarantines ticks as
     * MISSING_INSTRUMENT and the subscription-completeness check never passes.
     */
    static List<Instrument> syntheticSet() {
        List<Instrument> instruments = new ArrayList<>();
        // 50 instruments matching MockArrowServer defaults
        for (int i = 0; i < 50; i++) {
            long token = 100_000L + i * 100L;
            instruments.add(new Instrument.Builder()
                    .instrumentToken(token)
                    .tradingSymbol("SYM" + (i + 1) + "-EQ")
                    .exchange("NSE")
                    .segment("CM")
                    .lotSize(1)
                    .manifestVersion(1)
                    .build());
        }
        return Collections.unmodifiableList(instruments);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
