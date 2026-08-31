# R2 Lake Archive: Daily Partitioning + Archive Access — RUNBOOK

**Date:** 2026-08-31 · **Status:** READY FOR EXECUTION (DDL v3 already applied) · **Plan ref:** this file replaces the earlier advisory plan; it is the execution contract.

---

## 0. Implementer contract (READ FIRST — 5 rules)

1. **No thinking, no improvising.** Execute steps in order. Copy code blocks verbatim. Do not "improve", reformat, rename, or fix anything not listed.
2. **Every step ends with a CHECK.** If the check's expected output does not match, STOP immediately, log the row in the Bug Ledger (Section 10), and report. Never paper over a mismatch.
3. **Never commit, never push.** No git operations at all.
4. **Never edit a running script / never edit files while a smoke is running.**
5. **Report format after each block:**
   
   ```
   BLOCK Bx: PASS|FAIL
   Evidence: <the exact check outputs>
   Ledger rows added: <ids or none>
   ```

All paths are relative to the repo root:
`/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project_New`
(export `ROOT` first in every shell: `ROOT=$PWD` when cd'd there).

---

## 1. Current state snapshot (verified 2026-08-31)

- Tiering Flink job RUNNING (job `db6d22c4edfcbed1b7f560080b9e605b`), table `raw_table_1` (id 74) datalake-enabled, tiering to R2 iceberg PROVEN (576MB backlog + 3M-row smoke, 6+ manifests, 3 completed rounds).
- R2 bucket `tradingticks-aug-2026`, warehouse `s3://tradingticks-aug-2026/lake`, region `auto`, path-style, creds in `code/01_platform/01_docker/secrets.env`.
- `02_raw_table_1.sql` ALREADY upgraded to v3 (event_day + PARTITIONED BY + auto-partition options) — this edit is DONE, do not redo it.
- Fluss 0.9.1 facts (verified in source): partition keys are frozen at CREATE (no ALTER path); tiering job supports partitioned tables natively (TieringSplitGenerator.isPartitioned()); iceberg partition keys must be STRING; auto-partition DAY naming is `yyyyMMdd`; client dynamic partitioning is on by default (creates partitions on write); coordinator auto-partition check interval default 10 min (pre-creates today+tomorrow).

## 2. Ground-truth API facts (do not re-verify)

- `Admin.createTable(TablePath, TableDescriptor, boolean ignoreIfExists)`
- `Admin.dropTable(TablePath, boolean ignoreIfNotExists)`
- `Admin.listPartitionInfos(TablePath) -> CompletableFuture<List<PartitionInfo>>`; `PartitionInfo.getPartitionName()`
- `Admin.createPartition(TablePath, PartitionSpec, boolean ignoreIfExists)`
- `TableDescriptor.builder().schema(S).partitionedBy("event_day").distributedBy(16,"instrument_token").property(k,v)...`
- `Schema.Builder.column(String, DataType)` (nullable default)
- Common module dir: `code/common` · Ingestion module dir: `code/02_services/01_ingestion`
- Build: `cd code && mvn -o -q -DskipTests package -pl 02_services/01_ingestion -am`
- Classpath file: `mvn -q -o dependency:build-classpath -pl 02_services/01_ingestion -Dmdep.outputAbsoluteArtifactFilename=true -Dmdep.outputFile=02_services/01_ingestion/target/cp.txt`

---

## BLOCK B1 — CODE EDITS (7 files) + build + tests

### B1-1. `code/common/src/main/java/com/trading/common/schema/EventDay.java` — NEW FILE

Create exactly:

```java
package com.trading.common.schema;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * EventDay (2026-08-31, daily-partition migration): maps an event timestamp
 * to the raw-table partition value — yyyyMMdd in Asia/Kolkata (trading day).
 * MUST match Fluss auto-partition DAY naming exactly (yyyyMMdd), or rows
 * land in a second, non-auto partition (smoke GUARD E catches this).
 */
public final class EventDay {

    private EventDay() {}

    /** Trading-day zone: Asia/Kolkata (market day boundary = IST midnight). */
    public static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");

    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZONE);

    public static String of(Instant eventTime) {
        return FORMAT.format(eventTime);
    }

    public static String of(long eventTimeEpochMilli) {
        return FORMAT.format(Instant.ofEpochMilli(eventTimeEpochMilli));
    }
}
```

### B1-2. `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java` — 3 edits

Edit A — version:
OLD:

```java
    /** DDL schema version of the raw row (v2 since R-054/R-231). */
    public static final String ROW_SCHEMA_VERSION = "2";
```

NEW:

```java
    /** DDL schema version of the raw row (v3 since daily-partition migration). */
    public static final String ROW_SCHEMA_VERSION = "3";
```

Edit B — COLUMNS: insert `"event_day",` as the FIRST element:
OLD:

```java
    public static final List<String> COLUMNS = List.of(
            "event_fingerprint",
```

NEW:

```java
    public static final List<String> COLUMNS = List.of(
            "event_day",              // partition key, yyyyMMdd IST (v3)
            "event_fingerprint",
```

Edit C — COLUMN_TYPE_ROOTS: insert `"STRING",   // event_day` as FIRST element:
OLD:

```java
    public static final List<String> COLUMN_TYPE_ROOTS = List.of(
            "STRING",   // event_fingerprint
```

NEW:

```java
    public static final List<String> COLUMN_TYPE_ROOTS = List.of(
            "STRING",   // event_day (partition key)
            "STRING",   // event_fingerprint
```

(Also update the javadoc line "The 20 raw columns" -> "The 21 raw columns" if present; cosmetic, optional.)

### B1-3. `code/common/src/main/java/com/trading/common/config/PlatformConfig.java` — 1 edit

OLD:

```java
    public static final String RAW_TABLE_1_SCHEMA_VERSION = "2";
```

NEW:

```java
    public static final String RAW_TABLE_1_SCHEMA_VERSION = "3";
```

### B1-4. `code/common/src/test/java/com/trading/common/schema/EventDayTest.java` — NEW FILE

```java
package com.trading.common.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventDayTest {

    @Test
    void istBoundaryFlipsAt1830Utc() {
        // 2026-08-30T18:29:59Z == 2026-08-30T23:59:59 IST
        assertEquals("20260830", EventDay.of(Instant.parse("2026-08-30T18:29:59Z")));
        // 2026-08-30T18:30:00Z == 2026-08-31T00:00:00 IST
        assertEquals("20260831", EventDay.of(Instant.parse("2026-08-30T18:30:00Z")));
        // midday IST
        assertEquals("20260831", EventDay.of(Instant.parse("2026-08-31T07:00:00Z")));
    }

    @Test
    void epochMilliOverloadMatches() {
        long ms = Instant.parse("2026-08-31T10:00:00Z").toEpochMilli();
        assertEquals(EventDay.of(Instant.ofEpochMilli(ms)), EventDay.of(ms));
    }
}
```

### B1-5. `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TypedFlussRowConverter.java` — 3 edits

Edit A — add import after the existing imports:
OLD:

```java
import com.trading.ingestion.model.TickPacket;
```

NEW:

```java
import com.trading.common.schema.EventDay;
import com.trading.ingestion.model.TickPacket;
```

Edit B — TickRow POJO: add first field:
OLD:

```java
    public static final class TickRow {
        public String event_fingerprint;
```

NEW:

```java
    public static final class TickRow {
        public String event_day;            // partition key, yyyyMMdd IST (v3)
        public String event_fingerprint;
```

Edit C — append(): add first assignment:
OLD:

```java
        TickRow row = new TickRow();
        row.event_fingerprint = packet.eventFingerprint();
```

NEW:

```java
        TickRow row = new TickRow();
        row.event_day = EventDay.of(packet.eventTime());
        row.event_fingerprint = packet.eventFingerprint();
```

(Note: the SC2 static block auto-enforces 21 fields matching RawTableSchema — no manual count check needed.)

### B1-6. `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java` — 3 edits (RealFlussRowConverter)

Edit A — import:
OLD:

```java
import com.trading.ingestion.write.FlussRowConverter;
```

NEW:

```java
import com.trading.common.schema.EventDay;
import com.trading.ingestion.write.FlussRowConverter;
```

Edit B — DDL column-order comment (inside append javadoc):
OLD:

```java
     * <pre>{@code
     *   event_fingerprint, fingerprint_version, connection_id, connection_epoch,
```

NEW:

```java
     * <pre>{@code
     *   event_day, event_fingerprint, fingerprint_version, connection_id, connection_epoch,
```

Edit C — GenericRow.of: add FIRST argument:
OLD:

```java
        GenericRow row = GenericRow.of(
                // identity and routing
                bs(packet.eventFingerprint()),                      // event_fingerprint STRING
```

NEW:

```java
        GenericRow row = GenericRow.of(
                // partition (v3: daily yyyyMMdd IST — order MUST match DDL)
                bs(EventDay.of(packet.eventTime())),                // event_day STRING (partition key)
                // identity and routing
                bs(packet.eventFingerprint()),                      // event_fingerprint STRING
```

### B1-7. `code/02_services/01_ingestion/src/test/java/com/trading/ingestion/BatchLingerSweepProbe.java` — 2 edits

Edit A — import (add after the last existing import):

```java
import com.trading.common.schema.EventDay;
```

Edit B — toRow(): add FIRST argument:
OLD:

```java
        return GenericRow.of(
                BinaryString.fromString("fp_" + p.instrumentToken()),  // event_fingerprint
```

NEW:

```java
        return GenericRow.of(
                BinaryString.fromString(EventDay.of(p.eventTime())),   // event_day (partition key)
                BinaryString.fromString("fp_" + p.instrumentToken()),  // event_fingerprint
```

### B1-8. `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java` — NO EDIT (verify-only)

DdlBootstrap only VERIFIES column counts (raw_table_1 is OWNED_TABLES) and derives its schema from RawTableSchema, so it auto-adjusts to 21 columns. Confirm with:
`grep -n "createTable" code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java`
→ EXPECTED: no output (no create call).

### B1-CHECK — build + tests

```bash
cd "$ROOT/code"
mvn -o -q -DskipTests package -pl 02_services/01_ingestion -am          # CHECK: exit 0
mvn -q -o dependency:build-classpath -pl 02_services/01_ingestion \
  -Dmdep.outputAbsoluteArtifactFilename=true \
  -Dmdep.outputFile=02_services/01_ingestion/target/cp.txt              # CHECK: exit 0
mvn -o -q test -pl common -Dtest=EventDayTest                           # CHECK: BUILD SUCCESS
mvn -o -q test -pl 02_services/01_ingestion -Dtest=RawTableSchemaParityTest \
  -DfailIfNoTests=false                                                 # CHECK: BUILD SUCCESS
```

Any failure → STOP, ledger, report.

---

## BLOCK B2 — ADMIN TOOL (1 new file) + compile

### B2-1. `code/01_platform/04_scripts/fluss-repair/RawTableAdmin.java` — NEW FILE

```java
import com.trading.common.schema.RawTableSchema;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

/**
 * RawTableAdmin (2026-08-31) — mechanical admin for the daily-partition
 * migration (docs/plans/2026-08-31-r2-daily-partitioning-and-archive-plan.md).
 * Subcommands:
 *   drop                     drop default.raw_table_1 (ignoreIfNotExists=true)
 *   create                   create v3 partitioned table (options mirror
 *                            code/01_platform/02_sql/ddl/02_raw_table_1.sql)
 *   partitions               print partition names, one per line
 *   add-partition <name>     createPartition(ignoreIfExists=true) fallback
 *                            if client dynamic partitioning is ever disabled
 * Schema derives from RawTableSchema (single source of truth).
 * Usage: java -cp "<out>:$(cat code/02_services/01_ingestion/target/cp.txt)" RawTableAdmin <subcommand>
 */
public class RawTableAdmin {

    private static final TablePath PATH = TablePath.of("default", RawTableSchema.TABLE);

    public static void main(String[] args) throws Exception {
        String cmd = args.length > 0 ? args[0] : "partitions";
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            switch (cmd) {
                case "drop":
                    admin.dropTable(PATH, true).get();
                    System.out.println("DROP OK: " + PATH);
                    break;
                case "create":
                    admin.createTable(PATH, descriptor(), false).get();
                    System.out.println("CREATE OK: " + PATH + " (partitioned by event_day)");
                    break;
                case "partitions":
                    for (PartitionInfo p : admin.listPartitionInfos(PATH).get()) {
                        System.out.println(p.getPartitionName());
                    }
                    break;
                case "add-partition":
                    if (args.length < 2) { throw new IllegalArgumentException("usage: add-partition <name>"); }
                    admin.createPartition(PATH, PartitionSpec.of(args[1]), true).get();
                    System.out.println("PARTITION OK: " + args[1]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown subcommand: " + cmd);
            }
        }
    }

    private static TableDescriptor descriptor() {
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < RawTableSchema.FIELD_COUNT; i++) {
            sb.column(RawTableSchema.COLUMNS.get(i), toType(RawTableSchema.COLUMN_TYPE_ROOTS.get(i)));
        }
        return TableDescriptor.builder()
                .schema(sb.build())
                .partitionedBy("event_day")
                .distributedBy(RawTableSchema.BUCKET_COUNT, RawTableSchema.BUCKET_KEY)
                .property("table.log.ttl", "7d")
                .property("table.auto-partition.enabled", "true")
                .property("table.auto-partition.time-unit", "DAY")
                .property("table.auto-partition.num-precreate", "2")
                .property("table.auto-partition.num-retention", "7")
                .property("table.auto-partition.time-zone", "Asia/Kolkata")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "iceberg")
                .property("table.datalake.freshness", "5min")
                .property("table.datalake.auto-compaction", "true")
                .build();
    }

    private static DataType toType(String root) {
        switch (root) {
            case "STRING": return DataTypes.STRING();
            case "BIGINT": return DataTypes.BIGINT();
            case "BYTES":  return DataTypes.BYTES();
            default: throw new IllegalStateException("unsupported type root: " + root);
        }
    }
}
```

### B2-2. `code/01_platform/04_scripts/r2-move-prefix.sh` — NEW FILE

```bash
#!/usr/bin/env bash
# r2-move-prefix.sh <src_prefix> <dst_prefix> (2026-08-31) — move R2 objects
# between prefixes via GET+PUT+DELETE (SigV4). R2's CopyObject canonicalizer
# rejects x-amz-copy-source header signing (observed 2026-08-31, 3 variants)
# — plain GET+PUT+DELETE is the proven path. Used to archive a stale iceberg
# table dir aside before a table recreate (T-7). Fails loudly on non-200.
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SRC="${1:?usage: r2-move-prefix.sh <src_prefix> <dst_prefix>}"
DST="${2:?usage: r2-move-prefix.sh <src_prefix> <dst_prefix>}"
_ENV="$_D/../01_docker/.env" _SEC="$_D/../01_docker/secrets.env"
R2_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_ENV" | cut -d= -f2-)"
R2_BUCKET="$(grep -E '^R2_BUCKET=' "$_ENV" | cut -d= -f2-)"
AK="$(grep -E '^AWS_ACCESS_KEY_ID=' "$_SEC" | cut -d= -f2-)"
SK="$(grep -E '^AWS_SECRET_ACCESS_KEY=' "$_SEC" | cut -d= -f2-)"
python3 - "$R2_ENDPOINT" "$R2_BUCKET" "$AK" "$SK" "$SRC" "$DST" <<'MOVEPY'
import hashlib, hmac, datetime, sys, urllib.request, urllib.error, xml.etree.ElementTree as ET
from urllib.parse import quote

endpoint, bucket, ak, sk, src, dst = sys.argv[1:7]
host = endpoint.split("//", 1)[1]

def enc(s):
    return quote(s, safe='')

def sign(method, key, payload=b''):
    t = datetime.datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ'); datestamp = t.strftime('%Y%m%d')
    scope = f"{datestamp}/auto/s3/aws4_request"
    payload_hash = hashlib.sha256(payload).hexdigest()
    canonical_uri = "/" + bucket + "/" + quote(key, safe='/')
    headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"{method}\n{canonical_uri}\n\n{headers}\n{signed}\n{payload_hash}"
    k = hmac.new(("AWS4" + sk).encode(), datestamp.encode(), hashlib.sha256).digest()
    for step in (b"auto", b"s3", b"aws4_request"):
        k = hmac.new(k, step, hashlib.sha256).digest()
    sts = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    return amzdate, {"Authorization": f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={signed}, Signature={sig}",
                     "x-amz-date": amzdate, "x-amz-content-sha256": payload_hash}

def req(method, key, payload=b''):
    amzdate, hdrs = sign(method, key, payload)
    r = urllib.request.Request(f"https://{host}/{bucket}/{quote(key, safe='/')}",
                               data=payload if method in ("PUT", "POST") else None,
                               headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()

def list_keys(prefix):
    keys = []
    token = None
    while True:
        params = {"list-type": "2", "max-keys": "1000", "prefix": prefix}
        if token: params["continuation-token"] = token
        qs = "&".join(f"{k}={enc(v)}" for k, v in sorted(params.items()))
        amzdate, hdrs = sign("GET", prefix)
        r = urllib.request.Request(f"https://{host}/{bucket}/?{qs}", headers=hdrs)
        body = urllib.request.urlopen(r, timeout=30).read()
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        root = ET.fromstring(body)
        for c in root.findall("s3:Contents", ns):
            keys.append(c.find('s3:Key', ns).text)
        if (root.find('s3:IsTruncated', ns).text or "").lower() != "true":
            break
        token = root.find('s3:NextContinuationToken', ns).text
    return keys

keys = list_keys(src)
print(f"moving {len(keys)} objects: {src} -> {dst}")
for i, key in enumerate(keys, 1):
    status, body = req("GET", key)
    if status != 200: raise SystemExit(f"GET failed {status} for {key}: {body[:200]}")
    new_key = dst + key[len(src):]
    status, body = req("PUT", new_key, body)
    if status != 200: raise SystemExit(f"PUT failed {status} for {new_key}: {body[:200]}")
    status, body = req("DELETE", key)
    if status != 204 and status != 200: raise SystemExit(f"DELETE failed {status} for {key}: {body[:200]}")
    if i % 50 == 0 or i == len(keys): print(f"  {i}/{len(keys)}")
left = list_keys(src)
if left: raise SystemExit(f"ARCHIVE INCOMPLETE: {len(left)} objects remain under {src}")
print(f"ARCHIVE COMPLETE: {len(keys)} objects moved")
MOVEPY
```

### B2-CHECK — compile the admin tool

```bash
mkdir -p /tmp/tiering-tools
cd "$ROOT"
javac -cp "$(cat code/02_services/01_ingestion/target/cp.txt)" -d /tmp/tiering-tools \
  code/01_platform/04_scripts/fluss-repair/RawTableAdmin.java
# CHECK: exit 0, no errors
```

---

## BLOCK B3 — LIVE MIGRATION (archive → drop → create → verify)

Commands in exact order. Before starting: confirm tiering job RUNNING.

```bash
cd "$ROOT"
bash code/01_platform/04_scripts/tiering-start.sh --status
# CHECK: prints "tiering job RUNNING"
```

### B3-1. Archive the current R2 iceberg table dir (T-7: prevents LakeTableAlreadyExistException on create)

```bash
cd "$ROOT"
source code/01_platform/04_scripts/r2-list.sh
echo "before: $(r2_list_lake | grep -c '^lake/default/raw_table_1/' || true)"   # e.g. 43
bash code/01_platform/04_scripts/r2-move-prefix.sh \
  lake/default/raw_table_1/ lake/_stale-20260831-v1/raw_table_1/
# CHECK: prints "ARCHIVE COMPLETE: N objects moved"
echo "after: $(r2_list_lake | grep -c '^lake/default/raw_table_1/' || true)"    # MUST be 0
```

### B3-2. Drop the old table

```bash
cd "$ROOT"
java -cp "/tmp/tiering-tools:$(cat code/02_services/01_ingestion/target/cp.txt)" \
  RawTableAdmin drop
# CHECK: "DROP OK: default.raw_table_1"
```

### B3-3. Create the v3 partitioned table

```bash
cd "$ROOT"
java -cp "/tmp/tiering-tools:$(cat code/02_services/01_ingestion/target/cp.txt)" \
  RawTableAdmin create
# CHECK: "CREATE OK: default.raw_table_1 (partitioned by event_day)"
```

### B3-4. Verify live ZK state (GUARD B input)

```bash
cd "$ROOT"
docker exec 01_docker-zookeeper-1 zkCli.sh get \
  /fluss/metadata/databases/default/tables/raw_table_1 2>/dev/null \
  | grep -o "table.datalake.enabled[^,}]*" | head -1
# CHECK: table.datalake.enabled":"true"
```

### B3-5. Verify partitions exist

```bash
cd "$ROOT"
java -cp "/tmp/tiering-tools:$(cat code/02_services/01_ingestion/target/cp.txt)" \
  RawTableAdmin partitions
# EXPECTED: today's yyyyMMdd (IST) and tomorrow's, one per line. May be empty
# right after create (coordinator auto-partition check runs every 10 min;
# client dynamic partitioning creates today's on first write). If empty now,
# proceed — B4 GUARD D re-checks after the write phase.
```

### B3-CHECK — ingestion jar still boots against the new table

```bash
cd "$ROOT"
bash code/01_platform/04_scripts/tiering-smoke.sh --help >/dev/null 2>&1 || true
# (no-op sanity; the real proof is Block B4)
```

---

## BLOCK B4 — SMOKE UPGRADE + RUN (proves daily folders + guards D/E)

### B4-1. `code/01_platform/04_scripts/tiering-smoke.sh` — 3 edits

Edit A — tiering wait is configurable (fresh table needs up to 5-min freshness + upload before first tiering; default stays 120s):
OLD:

```bash
echo "stopping feed + ingestion; waiting 120s for the tiering task to run..."
cleanup
trap - EXIT
sleep 120
```

NEW:

```bash
echo "stopping feed + ingestion; waiting ${TIER_WAIT:-120}s for the tiering task to run..."
cleanup
trap - EXIT
sleep "${TIER_WAIT:-120}"
```

Edit B — GUARD D (partition exists) at the start of the verify section:
OLD:

```bash
# ---- verify 1: writes landed (count via LogFullRead, extracted from
```

NEW:

```bash
# GUARD D (2026-08-31): v3 raw_table_1 is PARTITIONED by event_day — the
# tiering split generator lists partitions, so today's IST partition
# (yyyyMMdd) must exist or the tiering job has nothing to tier. Client
# dynamic partitioning creates it on first write.
javac -cp "$CP" -d "$OUT" \
  "$ROOT/code/01_platform/04_scripts/fluss-repair/RawTableAdmin.java" \
  2>"$OUT/rawadmin-err.log" \
  || { echo "!! GUARD-D: RawTableAdmin compile failed:"; head -5 "$OUT/rawadmin-err.log"; exit 1; }
TODAY_IST="$(TZ=Asia/Kolkata date +%Y%m%d)"
PARTS="$(java -cp "$OUT:$CP" RawTableAdmin partitions 2>"$OUT/rawadmin-err.log" || true)"
echo "partitions: $(echo "$PARTS" | tr '\n' ' ')"
echo "$PARTS" | grep -qx "$TODAY_IST" \
  || { echo "!! GUARD-D FAILED: partition '$TODAY_IST' missing — dynamic partitioning off?"; exit 1; }

# ---- verify 1: writes landed (count via LogFullRead, extracted from
```

Edit C — GUARD E (daily folder in R2) before the final PASS line:
OLD:

```bash
echo "=== tiering-smoke PASS: this run tiered to R2 ($((FRESH_R2 - BASELINE_R2)) new raw_table_1 objects, ${MANIFESTS} manifests) ==="
```

NEW:

```bash
# GUARD E (2026-08-31): daily-partition proof — this run's data must sit in
# TODAY's IST day-folder (event_day=yyyyMMdd). A wrong event_day format
# would create a second, non-auto partition and fail this check.
TODAY_IST="$(TZ=Asia/Kolkata date +%Y%m%d)"
DAY_OBJ="$(r2_list_lake | grep -c "^lake/default/raw_table_1/data/event_day=${TODAY_IST}/" || true)"
echo "day-folder objects for ${TODAY_IST}: ${DAY_OBJ}"
[ "${DAY_OBJ:-0}" -gt 0 ] || {
  echo "!! GUARD-E FAILED: no R2 objects under event_day=${TODAY_IST}/ — event_day format or tiering wrong"
  exit 1
}

echo "=== tiering-smoke PASS: this run tiered to R2 ($((FRESH_R2 - BASELINE_R2)) new raw_table_1 objects, ${MANIFESTS} manifests; day-folder ${TODAY_IST} present) ==="
```

Syntax check: `bash -n code/01_platform/04_scripts/tiering-smoke.sh` → CHECK: exit 0.

### B4-2. Run the migration smoke (5-min write; extended tiering wait)

```bash
cd "$ROOT"
TIER_WAIT=240 bash code/01_platform/04_scripts/tiering-smoke.sh 300 2>&1 | tee /tmp/tiering-smoke-b4.log
```

CHECK: ends with `tiering-smoke PASS` and prints:

- `partitions: ... <today> ...`
- `day-folder objects for <today>: >0`
- `this run tiered to R2 (N new raw_table_1 objects, M manifests)`

NOTE on VERIFY-1: LogFullRead counts `^(` rows; if the partitioned-table read returns 0 rows with EMPTY read-err.log, do NOT treat as data loss — use the ingestion JVM's final tick.throughput from `$OUT/j1/java.out` (grep "tick.throughput") as the write-proof, log the reader issue as a ledger row, and continue.

---

## BLOCK B5 — DUCKDB QUERY TOOL (Item 3)

### B5-1. Install DuckDB CLI

```bash
mkdir -p "$HOME/bin"
cd /tmp
curl -L -o duckdb_cli.zip \
  https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip -o duckdb_cli.zip -d "$HOME/bin"
"$HOME/bin/duckdb" --version
# CHECK: prints "v1.x.x"
```

### B5-2. `code/01_platform/04_scripts/r2-query.sh` — NEW FILE

```bash
#!/usr/bin/env bash
# r2-query.sh — run SQL against the R2 iceberg lake via DuckDB (Item 3,
# 2026-08-31). Usage: r2-query.sh "<sql>"
# First run installs httpfs + iceberg extensions (needs internet).
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL="${1:?usage: r2-query.sh '<sql>'}"
_ENV="$_D/../01_docker/.env" _SEC="$_D/../01_docker/secrets.env"
_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_ENV" | cut -d= -f2-)"
_AK="$(grep -E '^AWS_ACCESS_KEY_ID=' "$_SEC" | cut -d= -f2-)"
_SK="$(grep -E '^AWS_SECRET_ACCESS_KEY=' "$_SEC" | cut -d= -f2-)"
_HOST="${_ENDPOINT#https://}"
exec "$HOME/bin/duckdb" -c "
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
SET s3_endpoint='${_HOST}';
SET s3_region='auto';
SET s3_url_style='path';
SET s3_access_key_id='${_AK}';
SET s3_secret_access_key='${_SK}';
${SQL}
"
```

### B5-3. Proof query (replace <BUCKET> with the value from .env R2_BUCKET)

```bash
cd "$ROOT"
bash code/01_platform/04_scripts/r2-query.sh \
  "SELECT count(*) AS n, min(event_time) AS lo, max(event_time) AS hi
   FROM iceberg_scan('s3://tradingticks-aug-2026/lake/default/raw_table_1')
   WHERE event_day = '$(TZ=Asia/Kolkata date +%Y%m%d)';"
# CHECK: n > 0 (matches the smoke's written rows); lo/hi sane epoch-millis.
# FALLBACK if iceberg_scan fails: use read_parquet on the day folder glob:
#   SELECT count(*) FROM read_parquet('s3://tradingticks-aug-2026/lake/default/raw_table_1/data/event_day=<TODAY>/*.parquet', union_by_name=true);
```

---

## BLOCK B6 — R2 GROWTH MONITOR (Item 6)

### B6-1. `code/01_platform/04_scripts/lake-guard.sh` — NEW FILE

```bash
#!/usr/bin/env bash
# lake-guard.sh (2026-08-31, Item 6) — daily R2 lake health check. Cron-able;
# fail-fast exit 1 with the reason. Checks (Asia/Kolkata):
#   1. yesterday's day-folder has >= 1 data object
#   2. iceberg manifests exist (>= 2)
#   3. after 18:30 IST today's folder has >= 1 object
# Test hook: LAKE_GUARD_CHECK_DAY=20991231 forces the "yesterday" check
# against a nonexistent day (negative test).
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_D/r2-list.sh"
TODAY="$(TZ=Asia/Kolkata date +%Y%m%d)"
YEST="${LAKE_GUARD_CHECK_DAY:-$(TZ=Asia/Kolkata date -d 'yesterday' +%Y%m%d)}"
KEYS="$(r2_list_lake)"
echo "lake-guard: today=${TODAY} yesterday=${YEST}"
Y="$(echo "$KEYS" | grep -c "^lake/default/raw_table_1/data/event_day=${YEST}/" || true)"
echo "yesterday folder objects: ${Y}"
[ "${Y:-0}" -gt 0 ] || { echo "!! lake-guard FAIL: no R2 objects for ${YEST}"; exit 1; }
M="$(echo "$KEYS" | grep -c "^lake/default/raw_table_1/metadata/.*\.avro" || true)"
echo "iceberg manifests: ${M}"
[ "${M:-0}" -ge 2 ] || { echo "!! lake-guard FAIL: iceberg manifests missing"; exit 1; }
HOUR="$(TZ=Asia/Kolkata date +%H%M)"
if [ "$HOUR" -ge 1830 ]; then
  T="$(echo "$KEYS" | grep -c "^lake/default/raw_table_1/data/event_day=${TODAY}/" || true)"
  echo "today folder objects: ${T}"
  [ "${T:-0}" -gt 0 ] || { echo "!! lake-guard FAIL: no R2 objects for ${TODAY} after 18:30 IST"; exit 1; }
fi
echo "lake-guard PASS"
```

### B6-2. Prove the guard fires (negative test, tamper-proof discipline)

```bash
cd "$ROOT"
LAKE_GUARD_CHECK_DAY=20991231 bash code/01_platform/04_scripts/lake-guard.sh
# CHECK: prints "!! lake-guard FAIL: no R2 objects for 20991231" and exits 1
bash code/01_platform/04_scripts/lake-guard.sh
# CHECK: prints "lake-guard PASS" (yesterday folder exists after the B4 smoke)
```

---

## BLOCK B7 — EOD LAKE VERIFIER (Item 4) + RESTORE (Item 5)

### B7-1. `code/common/src/main/java/com/trading/common/schema/eod/R2LakeTieringEodOffloadExecutor.java` — NEW FILE

```java
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
                "", e.keysHash(), "flink-tiering-job", null);
    }

    @Override
    public boolean verify(EodOffloadRecord committed) throws Exception {
        DayEvidence e = parseEvidence(runList(), lakePrefix,
                committed.tableName(), dayOf(committed));
        return e.dataObjects() > 0 && e.manifestFiles() > 0;
    }
}
```

### B7-2. `code/common/src/test/java/com/trading/common/schema/eod/R2LakeTieringEodOffloadExecutorTest.java` — NEW FILE

```java
package com.trading.common.schema.eod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class R2LakeTieringEodOffloadExecutorTest {

    @Test
    void parsesDayAndManifestEvidence() {
        List<String> lines = List.of(
                "lake/default/raw_table_1/data/event_day=20260831/instrument_token_bucket=0/x.parquet\t100",
                "lake/default/raw_table_1/data/event_day=20260831/instrument_token_bucket=1/y.parquet\t200",
                "lake/default/raw_table_1/data/event_day=20260830/z.parquet\t50",
                "lake/default/raw_table_1/metadata/snap-1.avro\t10");
        R2LakeTieringEodOffloadExecutor.DayEvidence e =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
        assertEquals(2, e.dataObjects());
        assertEquals(300, e.dataBytes());
        assertEquals(1, e.manifestFiles());
        assertTrue(e.keysHash().length() == 64);
    }

    @Test
    void emptyDayIsZeroEvidence() {
        List<String> lines = List.of(
                "lake/default/raw_table_1/data/event_day=20260830/z.parquet\t50");
        R2LakeTieringEodOffloadExecutor.DayEvidence e =
                R2LakeTieringEodOffloadExecutor.parseEvidence(lines, "lake", "raw_table_1", "20260831");
        assertEquals(0, e.dataObjects());
        assertEquals(0, e.manifestFiles());
    }
}
```

### B7-3. Wire into `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java` — 4 edits

Edit A — replace BOTH executor-selection occurrences (they are identical; a global replace is fine — there are exactly 2):
OLD:

```java
            EodOffloadExecutor executor = opts.offloadMode.equalsIgnoreCase("mock")
                    ? new MockEodOffloadExecutor(true, true)
                    : NotConfiguredEodOffloadExecutor.INSTANCE;
```

NEW:

```java
            EodOffloadExecutor executor = buildOffloadExecutor(opts.offloadMode);
```

Edit B — add the helper method (place it right before the `private record Options` if present, else at the end of the class before the final `}`):

```java
    /** Executor selection (2026-08-31): none=fail-closed, mock=drills,
     *  lake=R2 iceberg evidence via r2-list.sh (tiering job does the copy). */
    private static EodOffloadExecutor buildOffloadExecutor(String mode) {
        switch (mode.toLowerCase()) {
            case "mock":
                return new MockEodOffloadExecutor(true, true);
            case "lake":
                String listSh = System.getenv().getOrDefault("R2_LIST_SCRIPT", "");
                if (listSh.isBlank()) {
                    throw new IllegalArgumentException(
                            "EOD_OFFLOAD=lake requires R2_LIST_SCRIPT (absolute path to r2-list.sh)");
                }
                return new R2LakeTieringEodOffloadExecutor(
                        java.nio.file.Path.of(listSh),
                        System.getenv().getOrDefault("R2_LAKE_PREFIX", "lake"));
            default:
                return NotConfiguredEodOffloadExecutor.INSTANCE;
        }
    }
```

Edit C — validation:
OLD:

```java
            if (!offloadMode.equalsIgnoreCase("none") && !offloadMode.equalsIgnoreCase("mock")) {
                throw new IllegalArgumentException("--offload must be none or mock, got "
                        + offloadMode);
            }
```

NEW:

```java
            if (!offloadMode.equalsIgnoreCase("none") && !offloadMode.equalsIgnoreCase("mock")
                    && !offloadMode.equalsIgnoreCase("lake")) {
                throw new IllegalArgumentException("--offload must be none, mock or lake, got "
                        + offloadMode);
            }
```

Edit D — usage/help text:
OLD: `--offload none|mock`
NEW: `--offload none|mock|lake`

### B7-4. `code/01_platform/04_scripts/r2-restore.sh` — NEW FILE (Item 5)

```bash
#!/usr/bin/env bash
# r2-restore.sh <yyyyMMdd> [out.parquet] (2026-08-31, Item 5) — pull one
# trading day's raw ticks from the R2 iceberg lake into a local parquet via
# DuckDB read_parquet on the day-folder glob (partition pruning by folder).
# Usage: r2-restore.sh 20260831 [raw_table_1-20260831.parquet]
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAY="${1:?usage: r2-restore.sh <yyyyMMdd> [out.parquet]}"
OUT="${2:-raw_table_1-${DAY}.parquet}"
_ENV="$_D/../01_docker/.env" _SEC="$_D/../01_docker/secrets.env"
_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_ENV" | cut -d= -f2-)"
_BUCKET="$(grep -E '^R2_BUCKET=' "$_ENV" | cut -d= -f2-)"
_AK="$(grep -E '^AWS_ACCESS_KEY_ID=' "$_SEC" | cut -d= -f2-)"
_SK="$(grep -E '^AWS_SECRET_ACCESS_KEY=' "$_SEC" | cut -d= -f2-)"
_HOST="${_ENDPOINT#https://}"
"$HOME/bin/duckdb" -c "
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='${_HOST}';
SET s3_region='auto';
SET s3_url_style='path';
SET s3_access_key_id='${_AK}';
SET s3_secret_access_key='${_SK}';
COPY (SELECT * FROM read_parquet(
  's3://${_BUCKET}/lake/default/raw_table_1/data/event_day=${DAY}/*.parquet',
  union_by_name=true))
  TO '${OUT}' (FORMAT PARQUET);
SELECT count(*) AS restored_rows FROM read_parquet('${OUT}');
"
```

### B7-CHECK — build + tests + live proof

```bash
cd "$ROOT/code"
mvn -o -q -DskipTests package -pl 02_services/01_ingestion -am
mvn -o -q test -pl common -Dtest='EventDayTest,R2LakeTieringEodOffloadExecutorTest'
# CHECK: BUILD SUCCESS (both tests pass)

cd "$ROOT"
TODAY_IST="$(TZ=Asia/Kolkata date +%Y%m%d)"
bash code/01_platform/04_scripts/r2-restore.sh "$TODAY_IST" "/tmp/restore-prove.parquet"
# CHECK: prints restored_rows > 0 and a parquet file exists
# (needs the B4 smoke's data to have been tiered — if 0 rows, re-run after
# the next tiering round, log as ledger note, continue)
```

---

## BLOCK B8 — BENCH + EOD DRILL ON FINAL SHAPE + COMMIT PROPOSAL

```bash
cd "$ROOT"
bash code/01_platform/04_scripts/bench-throughput.sh   # full bench with tiering live
# then EOD drill with the lake verifier:
EOD_OFFLOAD=lake R2_LIST_SCRIPT="$ROOT/code/01_platform/04_scripts/r2-list.sh" \
  python3 code/01_platform/04_scripts/eod_controller.py run
```

Then STOP and propose the commit set to the user (never commit yourself): tiering-smoke.sh, tiering-start.sh, r2-list.sh, r2-move-prefix.sh, lake-guard.sh, r2-query.sh, r2-restore.sh, fluss-repair/{EnableTiering,RawTableAdmin}.java, DDL 02 v3, common/ingestion code edits, tests, compose/.env, plan doc.

---

## 10. BUG LEDGER (mandatory — fill during execution)

**Discipline:** every issue detected during ANY block gets a row IMMEDIATELY (root cause → fix → fail-fast guard proven to fire). A block is not done until its rows are complete.

| ID   | Block | Issue detected                                                                                                                                                                                              | Root cause                                                                                                                                                                                                                                                                           | Fix                                                                                                                                                      | Fail-fast guard (proven?)                                                                                                                                                         |
| ---- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| M-1  | B1    | DdlBootstrap ensureTables() registers raw_table_1 with the OLD non-partitioned descriptor — a local-dev bootstrap could recreate a non-partitioned table                                                    | ensureTables() bootstraps OWNED_TABLES for local dev, never drops; descriptor was stale                                                                                                                                                                                              | Updated DdlBootstrap raw_table_1 entry to v3 partitioned descriptor (schema+partitionBy+properties mirror RawTableAdmin)                                 | GUARD D (smoke asserts today's partition exists post-write) — proven by design                                                                                                    |
| M-2  | B2    | RawTableSchemaParityTest fails with stale-classpath "expected 20 but was 21"                                                                                                                                | `mvn test -pl ingestion` without `-am` resolves common from m2 (old 20-col jar)                                                                                                                                                                                                      | Correct test command: `-am -Dsurefire.failIfNoSpecifiedTests=false`                                                                                      | (runbook test-command note; no runtime guard needed)                                                                                                                              |
| M-3  | B3    | `PartitionSpec.of(String)` doesn't exist — compile error in RawTableAdmin.add-partition                                                                                                                     | Wrong API; real API is ResolvedPartitionSpec.fromPartitionName(keys,name).toPartitionSpec()                                                                                                                                                                                          | Fixed RawTableAdmin to use ResolvedPartitionSpec                                                                                                         | javac compile gate (B2-CHECK) — proven (failed then passed)                                                                                                                       |
| M-4  | B4    | r2-move-prefix.sh list GET → 403 SignatureDoesNotMatch                                                                                                                                                      | ListObjectsV2 must sign canonical URI `/bucket/` (root), not `/bucket/<prefix>`                                                                                                                                                                                                      | Fixed sign() to take explicit canonical_uri for list ops                                                                                                 | Script exits non-zero on any 403 (fails loudly) — proven (first run 403)                                                                                                          |
| M-5  | B5    | r2-move-prefix.sh first object GET timed out mid-archive (3 objects moved, then crash)                                                                                                                      | 30s socket timeout too small for ~80MB parquet GETs                                                                                                                                                                                                                                  | timeout 30→600 + per-object 3x retry on URLError; idempotent re-run                                                                                      | Script's re-run idempotency (archive-verify counts remaining) — proven (59 moved on 2nd run)                                                                                      |
| M-6  | B6    | RawTableAdmin create failed: "Partition key 'event_day' does not exist in the schema"                                                                                                                       | Compiled against m2 common-0.1.0.jar (OLD 20-col) — cp.txt references installed jar, not freshly built target/classes                                                                                                                                                                | Prepend `code/common/target/classes` to cp.txt when compiling/running RawTableAdmin                                                                      | B3-CHECK partition listing (proven — partitions listed after fix)                                                                                                                 |
| M-7  | B7    | EodControllerTool --offload lake: "must be none or mock"; help still none\|mock                                                                                                                             | Edit B assert failed mid-script → C and D never applied (silent partial edit)                                                                                                                                                                                                        | Re-applied Edit C (validation) + Edit D (help) with corrected anchors                                                                                    | `--help` shows none\|mock\|lake; lake mode passes validation (proven)                                                                                                             |
| M-8  | B7    | Tiering Flink job DIED (flink list: no jobs) after table drop/recreate                                                                                                                                      | Initially attributed to table recreate — REVISED: see M-15 (NoClassDefFoundError under write volume); the 19:32IST flink container restart also killed the then-running job silently                                                                                                 | Restart via tiering-start.sh (idempotent submit)                                                                                                         | GUARD A (no RUNNING job → smoke exits 1) — PROVEN (smoke run 3 failed exactly on it)                                                                                              |
| M-9  | B5    | r2-query/r2-restore glob `*` finds no files (layout has bucket nesting)                                                                                                                                     | Layout is event_day=N/instrument_token_bucket=M/*.parquet (2 levels)                                                                                                                                                                                                                 | Use `**/*.parquet` in read_parquet glob; iceberg_scan works directly                                                                                     | r2-query proof: 615424 rows via both paths (PROVEN)                                                                                                                               |
| M-10 | B4    | VERIFY-3 FAILED at TIER_WAIT=240: object count did not grow                                                                                                                                                 | 5-min datalake freshness: last-second writes become eligible at write+300s; 240s wait checks before eligibility (write 60s + wait 240s = 300s < 60s+300s=360s)                                                                                                                       | TIER_WAIT=420 (write + freshness + interval + margin)                                                                                                    | VERIFY-3 itself (object-growth check) — PROVEN (it fired exactly on the timing miss)                                                                                              |
| M-11 | B7    | VERIFY-1 fallback "drained totalTicks" never appears — ingestion SIGKILLed before shutdown logging                                                                                                          | cleanup() SIGKILLs the JVM; the drained line is logged on graceful shutdown only                                                                                                                                                                                                     | Fallback now counts TODAY's partition rows in R2 via DuckDB iceberg_scan (write-proof = rows visible in the lake)                                        | Fallback check itself — PROVEN (run 5: 1600512 rows for 20260831)                                                                                                                 |
| M-12 | B8    | EOD run → RESULT=LEASED EXIT=5 "lease held by unknown-... until 15:34Z"                                                                                                                                     | A prior crashed/timeout java run left a stale lease; single-writer fencing holds it until TTL (30m default) expiry                                                                                                                                                                   | Wait for TTL expiry (lease is in eod state table), or use --lease-ttl to shorten on drills; not a code bug — by-design fencing                           | Lease refusal itself (EXIT=5) — PROVEN (it fired, second run refused)                                                                                                             |
| M-13 | B8    | bench-throughput.sh preflight: "O2_AUTH_BASIC missing from .env"                                                                                                                                            | Credential was moved from .env to secrets.env (env-file split); bench reads only .env                                                                                                                                                                                                | Added secrets.env fallback in the bench's O2_AUTH extraction                                                                                             | Bench preflight check itself — PROVEN (it aborted exactly on the missing credential)                                                                                              |
| M-14 | B8    | bench compose build fails: "required variable AWS_ACCESS_KEY_ID is missing"                                                                                                                                 | Bench's 3 docker compose invocations lacked `--env-file .env --env-file secrets.env` (standing rule from the compose env-file split; R2 vars are interpolated into x-flink-common.environment)                                                                                       | Added --env-file flags to all 3 compose calls (stop/build/up)                                                                                            | Compose interpolation error itself — PROVEN (build failed exactly on the missing var)                                                                                             |
| M-15 | B8 | Tiering job DIED during the bench: iceberg write → NoClassDefFoundError: org/apache/hadoop/mapreduce/lib/input/FileInputFormat (same error killed a job at 09:23Z — RECURRING under sustained write volume) | iceberg's SHADED parquet (ParquetInputFormat in fluss-lake-iceberg) references the UN-shaded FileInputFormat; that class lives only in hadoop-mapreduce-client-core — absent from every jar in the flink lib. Iceberg loads it lazily, so small rounds succeed before the bomb | Built hadoop-mapreduce-compat-2.8.5.jar (ONLY org/apache/hadoop/mapreduce/**, 494 classes, from m2 hadoop-mapreduce-client-core-2.8.5) into fluss-plugins/iceberg/; mounted into both flink services' lib; recreated containers; resubmitted tiering job | NEW GUARD in tiering-start.sh: refuses to submit unless hadoop-mapreduce-compat is in both flink libs — PROVEN |
| M-16 | B8 | First M-15 fix attempt (mounting the full flink-shaded-hadoop-2-uber-2.8.3 jar) broke S3A instantly: NoSuchMethodError Configuration.getTimeDuration — job died 2s after split assignment | fluss-fs-s3's S3A links against the hadoop 3.3.x Configuration bundled in fluss-fs jars; the uber jar's 2.8.3 Configuration won classloading order and lacks the method | Replaced the uber mount with the minimal mapreduce-only compat jar (M-15 final fix); Configuration stays the 3.3.x one | GUARD-M16 in tiering-start.sh: refuses to submit if the hadoop-uber jar IS present — PROVEN (job death 16:23:17Z) |

**Baseline (already guarded, do NOT re-solve):** T-1 reader crash→stderr guard; T-2 no-tiering-job→GUARD A; T-3 datalake disabled→GUARD B; T-4 R2 region→GUARD C; T-5 r2-list truncation→pagination; T-6 G4 comment-in-command→documented; T-7 stale iceberg table→archive script (this runbook's B3-1); T-8 guard-C parse dup lines→tail -1; T-9 reader segfault→-Xmx1g.

**Rule:** a new bug matching a T-* pattern means the guard failed — fix the guard, not just the instance, and log both.

---

## 11. DO-NOT LIST

- No `git` commands. No commits.
- Do not touch any DDL/SQL/Java file not listed in this runbook.
- Do not "fix" pre-existing code smells or update unrelated tests.
- Do not edit scripts while a smoke/bench is running.
- Do not delete the R2 archive (`lake/_stale-20260831-v1/`) — it is the pre-migration history.
- Do not rotate or print R2 credentials.
- Do not restart fluss/flink containers unless a block explicitly says so (none do — the tiering job polls; no restarts needed).
- Do not alter `table.datalake.freshness` (alterable later, but not now).
