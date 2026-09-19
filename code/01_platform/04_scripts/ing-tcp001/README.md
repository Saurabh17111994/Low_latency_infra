# ING-TCP-001 — Count-based broker TCP losslessness tooling

Reusable tooling for the market-hours losslessness proof: prove that no tick
is lost between the broker socket and Fluss by reconciling per-token
emitted-tick counts from the Go bridge against per-token row counts stored in
Fluss.

The Arrow wire protocol has no sequence numbers, so the proof is **count-based**:
the bridge counts every tick it reads off the socket (`ARROW_TICK_COUNTS`),
and the probe counts every row actually stored per token. Equal counts per
token = zero loss.

## Components

| File | What it does |
| --- | --- |
| `TokenCountReconcile.java` | Standalone Fluss client probe. Reads per-token row counts from `raw_table_1` (lake-enabled LOG → `LogScanner`, `subscribeFromBeginning` per bucket) and `ingestion_quarantine` (plain LOG → `BatchScanner`). Token columns: raw_table_1 idx 4, quarantine idx 2. Output: `TOKEN <t> RAW=<n> QUAR=<n> TOTAL=<n>` lines + per-bucket and grand totals. |
| `reconcile-compare.py` | Compares bridge counts vs probe deltas. `--exact` requires `post − pre == bridge` per token (single-epoch market-hours proof); default requires `>=` (multi-epoch validation). `--sink quar|raw|total` selects which table the ticks should have landed in: `quar` for post-close/stale-window runs (a RAW *increase over the window* is a mismatch), `raw`/`total` for market-hours runs. An empty/truncated bridge, pre or post file fails closed (exit 1). |

## Bridge side

Enable the counters in the ingestion container (env already forwarded by
`docker-compose.yml`):

```bash
# in .env
ARROW_TICK_COUNTS=60   # report interval in seconds; final report at shutdown
ARROW_TICK_COUNTS_FILE_HOST=./ingestion-tick-counts.txt  # optional host path
```

The bridge writes `arrow-tick-counts: total=N chunk=i/m t=TOKEN:n ...`
(20 tokens/line, chunked to stay under the Java log-line cap) to stderr, which
the ingestion service logs verbatim. **The final report at shutdown is also
written to `ARROW_TICK_COUNTS_FILE` (default `/tmp/arrow-tick-counts.txt`) —
this file is the authoritative epoch count.** The parent JVM closes the child's
pipe streams the moment its own shutdown begins, so a stderr-only final report
dies with SIGPIPE (exit 141) and would be lost; the file write happens first
and survives the teardown. Fetch it after `docker compose stop ingestion`:

```bash
docker cp 01_docker-ingestion-1:/tmp/arrow-tick-counts.txt ./bridge.txt
```

## Probe build + run (against the live compose stack)

Build (requires the Fluss client jar — e.g. `~/.m2/repository/.../fluss-client-0.9.1-incubating.jar`):

```bash
javac -cp "$FLUSS_CLIENT_JAR" -d classes TokenCountReconcile.java
```

Run inside the compose network so Fluss service names resolve:

```bash
docker run --rm --network 01_docker_trading-net \
  -v "$HOME/.m2/repository:/m2:ro" \
  -v "$(pwd):/probe:ro" \
  -e FLUSS_BOOTSTRAP=fluss-coordinator:9123 \
  eclipse-temurin:17-jre \
  java --add-opens=java.base/java.nio=ALL-UNNAMED \
    -cp "/probe/classes:/m2/org/apache/fluss/fluss-client/0.9.1-incubating/fluss-client-0.9.1-incubating.jar" \
    TokenCountReconcile
```

## Market-hours proof procedure (15-min epoch)

1. Start Fluss + ingestion (HFT-1024 manifest, `ARROW_TICK_COUNTS` set). Wait for READY.
2. Run the probe → `pre.txt` (baseline; captures pre-existing rows for all tokens).
3. Record the epoch start time. Run for 15 min.
4. Stop the ingestion container (graceful shutdown) → copy the bridge's final
   report out: `docker cp 01_docker-ingestion-1:/tmp/arrow-tick-counts.txt ./bridge.txt`.
5. Run the probe again → `post.txt`.
6. Reconcile:

```bash
python3 reconcile-compare.py --bridge bridge.txt --pre pre.txt --post post.txt \
  --exact --sink total
```

`--sink total` (RAW+QUAR) is the safest choice for market-hours runs: fresh
ticks land in `raw_table_1`; the post-close snapshot tick at subscribe lands
in `ingestion_quarantine` (STALE). PASS = every token's sink delta exactly
equals its bridge count, no vanished/unexpected tokens.

## Evidence

2026-08-13 machinery validation (post-close, 3× HFT-1024 epochs): 3,072 bridge
emissions = 3,072 quarantine rows, per-token delta == 3, 0 mismatches.
Evidence: `logs/tracker-14/losslessness-validation-20260813.md`.
