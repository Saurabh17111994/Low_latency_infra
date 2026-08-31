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

    public R2LakeTieringEodOffloadExecutor(Path r2ListScript, String lakePrefix) {
        this.r2ListScript = r2ListScript;
        this.lakePrefix = lakePrefix;
    }

    /** Evidence for one table-day parsed from r2-list.sh output (unit-testable). */
    record DayEvidence(long dataBytes, int dataObjects, int manifestFiles, String keysHash) {}

    static DayEvidence parseEvidence(List<String> lines, String prefix, String table, String day) {
        String dataPrefix = prefix + "/default/" + table + "/data/event_day=" + day + "/";
        String metaPrefix = prefix + "/default/" + table + "/metadata/";
        long bytes = 0; int objects = 0; int manifests = 0;
        MessageDigest md;
        try { md = MessageDigest.getInstance("SHA-256"); } catch (Exception e) { throw new IllegalStateException(e); }
        for (String line : lines) {
            int tab = line.indexOf('\t');
            String key = tab > 0 ? line.substring(0, tab) : line;
            long size = tab > 0 ? Long.parseLong(line.substring(tab + 1).trim()) : 0L;
            if (key.startsWith(dataPrefix)) { objects++; bytes += size; md.update(key.getBytes(StandardCharsets.UTF_8)); }
            else if (key.startsWith(metaPrefix) && key.endsWith(".avro")) { manifests++; }
        }
        return new DayEvidence(bytes, objects, manifests, HexFormat.of().formatHex(md.digest()));
    }

    private List<String> runList() throws Exception {
        Process p = new ProcessBuilder("bash", "-c",
                "source '" + r2ListScript + "' && r2_list_lake")
                .redirectErrorStream(false).start();
        List<String> out = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String l; while ((l = r.readLine()) != null) out.add(l);
        }
        int rc = p.waitFor();
        if (rc != 0) throw new IllegalStateException("r2-list.sh exited " + rc);
        return out;
    }

    private static String dayOf(EodOffloadRecord r) { return r.tradingDate().replace("-", ""); }

    @Override
    public OffloadResult offload(EodOffloadRecord record) throws Exception {
        String day = dayOf(record);
        DayEvidence e = parseEvidence(runList(), lakePrefix, record.tableName(), day);
        if (e.dataObjects() == 0) {
            return OffloadResult.failure("no R2 data objects for " + record.tableName()
                    + " day " + day + " — tiering job behind or day not yet tiered");
        }
        if (e.manifestFiles() == 0) {
            return OffloadResult.failure("R2 data objects exist for day " + day
                    + " but no iceberg manifests — snapshot not committed");
        }
        return new OffloadResult(true, -1L, -1L, e.dataObjects(), e.dataBytes(),
                "", e.keysHash(), "", null);
    }

    @Override
    public boolean verify(EodOffloadRecord committed) throws Exception {
        DayEvidence e = parseEvidence(runList(), lakePrefix,
                committed.tableName(), dayOf(committed));
        return e.dataObjects() > 0 && e.manifestFiles() > 0;
    }
}
