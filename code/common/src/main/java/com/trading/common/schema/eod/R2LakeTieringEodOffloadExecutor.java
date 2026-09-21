package com.trading.common.schema.eod;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * R2LakeTieringEodOffloadExecutor (2026-08-31, Item 4): the lake-offload SPI
 * when the COPY is performed continuously by the Flink tiering job — so
 * offload()/verify() only READ EVIDENCE from R2 (via r2-list.sh, host-side,
 * the same SigV4 listing the smoke trusts) and report success once the
 * day's folder (event_day=yyyyMMdd) has data objects AND iceberg manifests
 * exist. Select with EOD_OFFLOAD=lake plus env R2_LIST_SCRIPT (absolute
 * path to r2-list.sh) and R2_LAKE_PREFIX (default "lake").
 */
public final class R2LakeTieringEodOffloadExecutor implements EodOffloadExecutor {

    private final Path r2ListScript;
    private final String lakePrefix;
    private final String database;

    public R2LakeTieringEodOffloadExecutor(Path r2ListScript, String lakePrefix) {
        this(r2ListScript, lakePrefix,
                System.getenv().getOrDefault("FLUSS_DATABASE", "default"));
    }

    public R2LakeTieringEodOffloadExecutor(Path r2ListScript, String lakePrefix, String database) {
        // P4-296: fail fast — null script NPEs late in bash concat, relative
        // script fails at offload time with a bare exit code, and a trailing-
        // slash prefix builds "lake//..." which matches nothing.
        this.r2ListScript = java.util.Objects.requireNonNull(r2ListScript, "r2ListScript");
        if (!r2ListScript.isAbsolute()) {
            throw new IllegalArgumentException(
                    "R2_LIST_SCRIPT must be absolute: " + r2ListScript);
        }
        if (lakePrefix == null || lakePrefix.isBlank()) {
            throw new IllegalArgumentException("lakePrefix required");
        }
        String p = lakePrefix.strip().replaceAll("/+$", "");
        if (p.contains("//") || p.contains(" ")) {
            throw new IllegalArgumentException("bad lakePrefix: " + lakePrefix);
        }
        this.lakePrefix = p;
        this.database = (database == null || database.isBlank()) ? "default" : database;
    }

    /** Evidence for one table-day parsed from r2-list.sh output (unit-testable). */
    record DayEvidence(long dataBytes, int dataObjects, int manifestFiles, String keysHash) {}

    static DayEvidence parseEvidence(List<String> lines, String prefix, String table, String day) {
        return parseEvidence(lines, prefix, "default", table, day);
    }

    static DayEvidence parseEvidence(List<String> lines, String prefix, String database,
            String table, String day) {
        String dataPrefix = prefix + "/" + database + "/" + table + "/data/event_day=" + day + "/";
        String metaPrefix = prefix + "/" + database + "/" + table + "/metadata/";
        String dayMarker = "event_day=" + day;
        long bytes = 0; int objects = 0; int manifests = 0;
        MessageDigest md;
        try { md = MessageDigest.getInstance("SHA-256"); } catch (Exception e) { throw new IllegalStateException(e); }
        // P4-140: sort key+size pairs and delimit before hashing — the raw
        // listing order is unstable and ["ab","c"] must not hash as ["a","bc"].
        // Sizes are bound into the hash so a rewritten/truncated object with
        // the same key changes the digest.
        java.util.List<String> hashed = new java.util.ArrayList<>();
        for (String line : lines) {
            int tab = line.indexOf('\t');
            String key = tab > 0 ? line.substring(0, tab) : line;
            long size = parseSize(line, tab);
            if (key.startsWith(dataPrefix)) { objects++; bytes += size; hashed.add(key + "\t" + size); }
            else if (key.startsWith(metaPrefix) && key.endsWith(".avro")
                    && key.contains(dayMarker)) { manifests++; }
        }
        java.util.Collections.sort(hashed);
        for (String k : hashed) { md.update(k.getBytes(StandardCharsets.UTF_8)); md.update((byte) 0); }
        return new DayEvidence(bytes, objects, manifests, HexFormat.of().formatHex(md.digest()));
    }

    /** Controlled parse of the r2-list.sh size column (P4-139): malformed or
     *  negative sizes fail closed with a named error so EodController maps the
     *  throw to FAILED_RETRYABLE with backoff instead of a bare NumberFormatException. */
    private static long parseSize(String line, int tab) {
        if (tab <= 0) return 0L;
        // r2-list.sh emits key<TAB>size<TAB>LastModified (see its _r2_list header;
        // the timestamp is what lets lake-guard.sh age-check a manifest). Only the
        // second column is the size: parsing the whole tail assumed a two-column
        // line the script never emits, so every live listing threw here and the
        // day became FAILED_RETRYABLE forever (proven 2026-09-21 against a TLS
        // S3 endpoint with a real listing).
        String tail = line.substring(tab + 1);
        int nextTab = tail.indexOf('\t');
        String sizeField = (nextTab >= 0 ? tail.substring(0, nextTab) : tail).trim();
        final long size;
        try {
            size = Long.parseLong(sizeField);
        } catch (NumberFormatException badSize) {
            throw new IllegalStateException("malformed r2-list.sh line (bad size): [" + line + "]", badSize);
        }
        if (size < 0) throw new IllegalStateException("malformed r2-list.sh line (negative size): [" + line + "]");
        return size;
    }

    private List<String> runList() throws Exception {
        // P4-141/299/300: the script path rides $1 (never interpolated into
        // -c text — env control must not become code execution); stderr is
        // merged so a verbose script cannot deadlock the 64KB pipe and its
        // tail survives into the failure message; the wait is bounded and the
        // child is always destroyed. P4-298: r2_list_lake takes no prefix arg
        // (shared with the smoke scripts), so scope = full listing filtered
        // in parseEvidence — the listing is one paginated pass, offload+verify
        // cost two passes per day by contract.
        Process p = new ProcessBuilder("bash", "-c", "source \"$1\" && r2_list_lake",
                        "bash", r2ListScript.toString())
                .redirectErrorStream(true).start();
        List<String> out = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String l; while ((l = r.readLine()) != null) out.add(l);
        }
        boolean done;
        try {
            done = p.waitFor(java.time.Duration.ofMinutes(5).toMillis(),
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            p.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("r2-list.sh interrupted", ie);
        }
        if (!done) {
            p.destroyForcibly();
            throw new IllegalStateException("r2-list.sh timed out after 5m");
        }
        int rc = p.exitValue();
        if (rc != 0) {
            int from = Math.max(0, out.size() - 20);
            throw new IllegalStateException("r2-list.sh exited " + rc + ": "
                    + String.join("\n", out.subList(from, out.size())));
        }
        return out;
    }

    private static String dayOf(EodOffloadRecord r) {
        // P4-301: validate via the record's canonical parser — a malformed
        // date fails closed here instead of silently building the wrong
        // event_day= prefix ("tiering behind" misreport).
        return r.tradingDateAsLocalDate()
                .format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
    }

    private DayEvidence evidenceFor(EodOffloadRecord record) throws Exception {
        return parseEvidence(runList(), lakePrefix, database, record.tableName(), dayOf(record));
    }

    @Override
    public OffloadResult offload(EodOffloadRecord record) throws Exception {
        String day = dayOf(record);
        DayEvidence e = evidenceFor(record);
        if (e.dataObjects() == 0) {
            return OffloadResult.failure("no R2 data objects for " + record.tableName()
                    + " day " + day + " — tiering job behind or day not yet tiered");
        }
        if (e.manifestFiles() == 0) {
            return OffloadResult.failure("R2 data objects exist for day " + day
                    + " but no iceberg manifests — snapshot not committed");
        }
        // P4-302 honesty: this is a read-only tiering check, not a copy —
        // there are no source offsets or Iceberg snapshot to populate, so
        // -1/"" are honest unknowns, not measured values. rowCount carries a
        // FILE count (dataObjects), not rows — verify() compares the same
        // semantic on both sides so the reconciliation is consistent.
        return new OffloadResult(true, -1L, -1L, e.dataObjects(), e.dataBytes(),
                "", e.keysHash(), "", null);
    }

    @Override
    public boolean verify(EodOffloadRecord committed) throws Exception {
        // P4-143: reconcile against the committed evidence — a day replaced,
        // truncated, or re-tiered between offload and verify must fail.
        DayEvidence e = evidenceFor(committed);
        if (e.dataObjects() == 0 || e.manifestFiles() == 0) return false;
        if (committed.targetHash() != null && !committed.targetHash().isEmpty()
                && !e.keysHash().equals(committed.targetHash())) return false;
        if (committed.byteCount() >= 0 && e.dataBytes() != committed.byteCount()) return false;
        // rowCount here is a file count (see offload), compared consistently.
        if (committed.rowCount() >= 0 && e.dataObjects() != committed.rowCount()) return false;
        return true;
    }
}
