# Lake Archive Ops — R2 Iceberg Tiering (raw_table_1)

> Runbook for the R2 lake tiering + daily-partition archive surface, live
> since 2026-08-31 (CHG-117). Commands assume repo root; docker compose
> needs `--env-file .env --env-file secrets.env` from `code/01_platform/01_docker/`.

## Scope

- **Table**: `default.raw_table_1` v3 — 21 columns, `event_day STRING`
  (yyyyMMdd, Asia/Kolkata) first, `PARTITIONED BY (event_day)`, auto-partition
  DAY (precreate 2 / retention 7d), bucket key `instrument_token` (16 buckets),
  `table.datalake.enabled=true`.
- **Lake layout**: `s3://<bucket>/lake/default/raw_table_1/data/event_day=<yyyyMMdd>/instrument_token_bucket=<N>/*.parquet`
- **Archive history**: `s3://<bucket>/lake/_stale-20260831-v1/raw_table_1/` —
  62 pre-migration objects. **Do NOT delete** (rollback source, CHG-117).
- The other nine 2d-TTL tables remain lake-disabled in dev (2026-08-13 note in
  `08_implementation/02-schema-storage.md`); only raw_table_1 is migrated.

## Daily / routine operations

| Task | Command |
|---|---|
| Health check (cron-able; yesterday's day-folder ≥1 object, manifests ≥2, today's folder after 18:30 IST) | `bash code/01_platform/04_scripts/lake-guard.sh` |
| Tiering job status (exit 0 = RUNNING) | `bash code/01_platform/04_scripts/tiering-start.sh --status` |
| Restart tiering job after table recreate / container restart (idempotent; guards the classpath first) | `bash code/01_platform/04_scripts/tiering-start.sh` |
| Query the lake (DuckDB, iceberg) | `bash code/01_platform/04_scripts/r2-query.sh "<sql>"` |
| Pull one trading day to local parquet | `bash code/01_platform/04_scripts/r2-restore.sh <yyyyMMdd> [out.parquet]` |
| EOD run with lake verification | `EOD_OFFLOAD=lake R2_LIST_SCRIPT="$PWD/code/01_platform/04_scripts/r2-list.sh" python3 code/01_platform/04_scripts/eod_controller.py run` |
| Full guarded E2E tiering proof (smoke) | `TIER_WAIT=420 bash code/01_platform/04_scripts/tiering-smoke.sh 300` |

Canonical proof query (row count for a day):

```sql
SELECT count(*) FROM iceberg_scan(
  's3://tradingticks-aug-2026/lake/default/raw_table_1', allow_moved_paths=true)
WHERE event_day='20260831';
```

## Recovery: tiering job died

1. `bash code/01_platform/04_scripts/tiering-start.sh --status` — confirm dead.
2. Check the cause: `docker logs 01_docker-flink-jobmanager-1 --since <ts> | grep -A5 "Caused by"`.
   - `NoClassDefFoundError ... mapreduce ... FileInputFormat` → the
     hadoop-mapreduce-compat jar is missing from a flink container — see the
     rebuild recipe in `code/01_platform/01_docker/fluss-plugins/iceberg/README.md`,
     recreate the container, then resubmit (the submit guard will verify).
3. `bash code/01_platform/04_scripts/tiering-start.sh` — idempotent resubmit
   (old offsets tied to a dropped table are not reusable; a fresh submit
   re-enumerates current tables).
4. Prove the round commits: watch `currentFinishedTables: {<tableId>=` appear
   in jobmanager logs, then object count grows via
   `source code/01_platform/04_scripts/r2-list.sh && r2_list_lake`.

## Recovery: restore a trading day

`r2-restore.sh <yyyyMMdd>` exports the day-folder parquet (partition-pruned
glob). Verified 2026-08-31: 2,293,048 rows → 313MB parquet.

The script refuses to write a restore it cannot trust (P6-165 / P6-498):

- `DAY` must be a real `yyyyMMdd` calendar date and `OUT` must be a plain
  filename (no quotes, `;`, `..`, newline). A day typed as `2026-08-31` used to
  export **zero rows** and still report success.
- A day whose folder contains Iceberg **delete files** aborts: DuckDB's
  `read_parquet` ignores them, so the export would silently contain deleted-away
  rows. Use `r2-query.sh` with `iceberg_scan` for those days (upgrade path:
  CHG-144).
- A restore that returns **zero rows** aborts and leaves any existing output
  file untouched. Output is written to `OUT.tmp.<pid>` and renamed only after
  both counts come back non-zero, so a failed run never leaves a half-written
  parquet behind.
- Schema drift now fails loudly: the export dropped its previous
  `union_by_name := true`, which had been quietly filling mismatched columns
  with NULLs.

Exit codes: `2` = refused before DuckDB ran (bad day, bad output path,
incomplete R2 config — nothing was read or written); `1` = DuckDB ran and the
result was not trustworthy. Secrets reach DuckDB inside a `0600` temp file on
stdin, never on the command line (`ps`/`/proc/<pid>/cmdline` is world-readable).

## Recovery: rollback the v3 migration (last resort)

1. Drop v3 table (`fluss-repair/RawTableAdmin.java drop`).
2. Recreate the v2 non-partitioned shape.
3. `r2-move-prefix.sh lake/_stale-20260831-v1/raw_table_1/ lake/default/raw_table_1/` back.
   (Post-migration rows are only re-derivable from upstream replay.)

`r2-move-prefix.sh <src> <dst>` normalizes both prefixes to a trailing slash
(`table` no longer also matches `table_backup/`) and refuses equal or
overlapping prefixes before it talks to R2. Per object it is
GET → PUT → **HEAD-verify** → DELETE: the source is deleted only after the
destination's size and MD5 ETag match the bytes that were just uploaded, so a
truncated or substituted copy leaves both copies in place and exits non-zero
(P6-013). An interrupted run is resumable — re-running copies the remaining
objects; already-moved ones are simply listed again on the destination side.
Transient 408/429/5xx responses are retried with exponential backoff (P6-161).

## Known failure modes (bug ledger, 2026-08-31 migration)

Discipline: a new bug matching one of these patterns means its guard failed —
fix the guard, not just the instance. Guards marked "proven" were observed
firing before the fix landed.

| ID | Failure mode | Root cause | Guard |
|---|---|---|---|
| M-1 | Local-dev bootstrap could recreate a non-partitioned raw_table_1 | stale DdlBootstrap descriptor | DdlBootstrap carries the v3 descriptor; smoke GUARD D asserts today's partition exists |
| M-2 | Parity test "expected 20 but was 21" | `mvn test -pl ingestion` without `-am` resolves stale m2 common jar | always `-am -Dsurefire.failIfNoSpecifiedTests=false` |
| M-3 | `PartitionSpec.of(String)` doesn't exist | real API: `ResolvedPartitionSpec.fromPartitionName(...).toPartitionSpec()` | javac compile gate |
| M-4 | R2 list 403 SignatureDoesNotMatch | ListObjectsV2 must sign canonical URI `/bucket/`, not `/bucket/<prefix>` | r2-list exits non-zero on 403 |
| M-5 | R2 GET timeout mid-archive (~80MB objects) | 30s socket timeout too small | 600s timeout + 3× retry + idempotent re-run |
| M-6 | RawTableAdmin: "partition key 'event_day' does not exist" | cp.txt pointed at stale m2 common jar | prepend `code/common/target/classes` to every RawTableAdmin/EodControllerTool classpath |
| M-7 | `--offload lake` rejected; help stale | silent partial file edit | `--help` shows none\|mock\|lake |
| M-8 | Tiering job gone after table recreate | job offsets tied to old table id; also container restarts kill jobs | smoke GUARD A (proven) + `tiering-start.sh --status` before any drill |
| M-9 | DuckDB glob `*.parquet` finds nothing | layout nests `instrument_token_bucket=N` → need `**/*.parquet` (or iceberg_scan) | r2-query proof queries |
| M-10 | VERIFY-3 fails at TIER_WAIT=240 | 5-min datalake freshness vs write+240s race | TIER_WAIT=420; VERIFY-3 itself (proven) |
| M-11 | ~~LogFullRead throws on partitioned tables~~ **FIXED 2026-09-01 (CHG-120)** | The reader used the non-partitioned `subscribe(bucket,offset)` overload after the v3 `event_day` migration; it now enumerates partition IDs and subscribes with `subscribe(partitionId,bucket,offset)`; the v3 audit projection also validates column names and uses the shifted indexes | compile + live 5s read returned 908,500 partitioned audit rows; smoke's R2 day-folder fallback remains for old/incompatible reader failures |
| M-12 | EOD run LEASED / exit 5 | stale lease held until 30m TTL — by-design fencing, not a bug | lease refusal itself (proven); `--lease-ttl` shortens drills |
| M-13 | bench preflight "O2_AUTH_BASIC missing" | credential moved to secrets.env | bench reads secrets.env fallback (proven) |
| M-14 | compose "required variable AWS_ACCESS_KEY_ID is missing" | compose calls missing `--env-file secrets.env` | compose interpolation error itself (proven) |
| M-15 | Tiering job dies under sustained volume: `NoClassDefFoundError ... FileInputFormat` | iceberg's shaded parquet references the un-shaded hadoop-mapreduce class, absent from every flink lib jar; loads lazily so small rounds pass first | hadoop-mapreduce-compat-2.8.5.jar (minimal, mapreduce-only) mounted in both flink services; tiering-start refuses to submit without it (proven — job survived the 3.7M-row bench backlog after fix) |
| M-18 | Full-suite ingestion run: `FuzzIngestionTest.validTicksAlwaysAppend` failed 197/200 with quarantine=0 — flaky under load, green in isolation and on rerun | The write path is async (processTickEvent → BoundedQueue → WriterWorker thread → converter.append); the test asserted appendCalls immediately after the feed loop, so under full-suite JVM load the worker legitimately lagged 3 packets — a test race, not a silent drop | Added `awaitPipelineDrain` (bounded 10s poll until appends+quarantines == fed, else fail with counts) before both fuzz tests' assertions | The drain guard itself — times out loudly with counts if the pipeline genuinely loses events (10s bound, no infinite wait) |
| M-17 | Docs fold + full-suite run surfaced 2 stale v2-pinned guard tests (`SchemaAgreementTest.rawTableV2ColumnCount` hardcoded 20 cols; `RawTable1DdlSchemaVersionTest` future-version fixture = 3 = now current) | B1 changed the schema but only the named tests were run, not the full suites — the v2 shape-pins went stale silently | Test list updated to v3 21 cols (renamed `rawTableV3ColumnCount`); future-version fixture bumped to 4 | `make docs-audit` C6 (surefire vs doc count) fired, forcing the full-suite run that exposed them — proven |
| M-16 | Full flink-shaded-hadoop-2-uber jar breaks S3A instantly (`NoSuchMethodError Configuration.getTimeDuration`) | uber's hadoop 2.8.3 Configuration beats fluss-fs's bundled 3.3.x one in classloading order | tiering-start refuses to submit if the uber jar is mounted (proven) |

Older baseline guards (pre-migration, still active): T-1 reader crash→stderr
guard; T-2 no-tiering-job→GUARD A; T-3 datalake disabled→GUARD B; T-4 R2
region→GUARD C; T-5 r2-list truncation→pagination; T-7 stale iceberg table
dir→archive before recreate (`LakeTableAlreadyExistException`); T-8 GUARD-C
dup lines→`tail -1`; T-9 reader segfault→`-Xmx1g`.

## Operational cautions

- Never edit a script while a smoke/bench is running; no comments inside
  backslash-continued shell commands.
- `table.datalake.freshness` stays at default (5 min) — changing it shifts
  the TIER_WAIT=420 math.
- R2 credentials are temporary (rotate per `05_deployment/04-secrets-rotation.md`).
- DuckDB first run installs httpfs + iceberg extensions (needs internet).
- Both R2 scripts take their credentials from the environment / `01_docker`
  config files, never from arguments — do not wrap them in a command that puts
  the secrets back on the command line (e.g. `env ... bash -c "..."` is fine,
  `r2-restore.sh` never needs a secret argument at all).
- Scripts and their guardrails are covered by
  `code/01_platform/04_scripts/tests/test_r2_move_prefix.py` (local mock R2) and
  `.../test_r2_restore_guardrails.py` (stub DuckDB) — both run in the Monday
  gate without network access.
- The EOD lake verifier only covers raw_table_1; the other nine tables fail
  EOD verification (FAILED_RETRYABLE) until they are migrated — expected.
