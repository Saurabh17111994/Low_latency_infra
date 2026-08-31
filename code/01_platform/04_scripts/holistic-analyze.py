#!/usr/bin/env python3
"""holistic-analyze.py — per-phase latency metrics from real row timestamps.

Reads the preview + Signal_Candidates LOG tables from the EARLIEST offset
(the latest-offset tail probe missed rows while operators were actively
writing — observed 2026-08-30) and computes latency distributions.

Bugs fixed in v2 (all observed in the first validation run 2026-08-30):
  - Status classification MUST use the candidate_id suffix
    (...-TENTATIVE / ...-CONFIRM / ...-CANCEL). Substring matching
    misclassified all 83K CONFIRM rows as TENTATIVE because their
    validity_reason text also contains "TENTATIVE".
  - Rows must be filtered to the RUN'S EPOCH WINDOW: the from-earliest
    read returns the whole table history (previous runs, including
    full-replay bursts that made preview-cadence p50 = 0ms).
  - detection_ts/evaluation_ts on signal rows are EVENT-TIME stamps
    (window-aligned), not wall clock — "eval - window_start" is the
    constant 15000 and is NOT a latency. Wall-clock latency for the
    signal path comes from Flink's metrics.latency.interval histograms
    (collected separately); from rows we can derive:
      * preview cadence  — wall-clock gaps between consecutive preview
        rows per (token, window) — expected ~PREVIEW_INTERVAL_MS
      * preview freshness — wall-clock age of the newest preview per
        token at run end (consumer-visible staleness)
      * signal counts by status within the run window (volume check)

Usage: holistic-analyze.py <out_dir> <classpath> [run_start_epoch] [run_end_epoch]
"""

import subprocess
import sys
import os
import re
import json
from collections import defaultdict

LOG_READ_SRC = """
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import java.time.Duration;
import java.util.*;

public class LogFullRead {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:9123");
        TablePath tp = TablePath.of("default", args[0]);
        long runMs = args.length > 1 ? Long.parseLong(args[1]) : 20000L;
        // args[2] == "offset": prefix each row with its bucket:offset so the
        // caller can dedupe EXACTLY (the LogScanner re-delivers records —
        // line dedupe alone cannot distinguish a re-delivery from an
        // injected duplicate tick, which is the whole point of the F2 audit).
        // args[2] == "audit": offset mode PLUS a sanitized field projection
        // (tab-separated, no raw_payload/payload_hash). The raw table's
        // payload_hash VARBINARY prints as raw binary bytes — a 0x0A hash
        // byte split the line and a 0x2C byte shifted comma-split fields,
        // silently dropping ~23% of rows in the first G7 recount (2026-08-31).
        // The audit projection reads columns by INDEX and prints only the
        // ASCII-safe fields the audit needs.
        boolean audit = args.length > 2 && "audit".equals(args[2]);
        boolean withOffset = audit || (args.length > 2 && "offset".equals(args[2]));
        try (Connection c = ConnectionFactory.createConnection(conf);
             Table t = c.getTable(tp);
             LogScanner scanner = t.newScan().createLogScanner()) {
            int buckets = t.getTableInfo().getNumBuckets();
            for (int b = 0; b < buckets; b++) scanner.subscribe(b, 0L);
            long deadline = System.currentTimeMillis() + runMs;
            while (System.currentTimeMillis() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                if (audit || withOffset) {
                    // per-bucket iteration: ScanRecord exposes logOffset()
                    // but not its bucket, so the bucket must come from the
                    // records-per-bucket map (bucket:offset is the exact
                    // dedupe key).
                    for (var tb : records.buckets()) {
                        for (var r : records.records(tb)) {
                            if (audit) {
                                // raw_table_1 column indexes (RawTableColumns):
                                // 0 fingerprint, 4 token, 7 event_time, 8 ingest_ts,
                                // 11 price, 12 qty, 17 validity_state
                                var rw = r.getRow();
                                System.out.println(
                                    tb.getBucket() + ":" + r.logOffset() + "\t"
                                    + rw.getString(0) + "\t" + rw.getLong(4)
                                    + "\t" + rw.getLong(7) + "\t" + rw.getLong(8)
                                    + "\t" + rw.getLong(11) + "\t" + rw.getLong(12)
                                    + "\t" + rw.getString(17));
                            } else {
                                System.out.println("@" + tb.getBucket() + ":" + r.logOffset()
                                    + " " + r.getRow().toString());
                            }
                        }
                    }
                } else {
                    for (var r : records) System.out.println(r.getRow().toString());
                }
            }
        }
    }
}
"""

def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def fmt_ms(v):
    return "n/a" if v is None else f"{v:.0f}ms"


def collect_rows(table, cp, out_dir, run_ms=30000, with_offset=False):
    """Compile (once) and run the LOG reader; return parsed rows."""
    java_file = os.path.join(out_dir, "LogFullRead.java")
    class_file = os.path.join(out_dir, "LogFullRead.class")
    # regenerate + recompile when the source is newer than the class — a
    # stale class silently ignores new modes (observed 2026-08-31)
    if (not os.path.exists(class_file)
            or not os.path.exists(java_file)
            or os.path.getmtime(java_file) > os.path.getmtime(class_file)):
        with open(java_file, "w") as f:
            f.write(LOG_READ_SRC)
        r = subprocess.run(["javac", "-cp", cp, "-d", out_dir, java_file],
                           capture_output=True, text=True)
        if r.returncode != 0:
            print(f"!! LogFullRead compile failed for {table}: {r.stderr[:400]}")
            return []
    r = subprocess.run(
        ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED",
         "--add-opens=java.base/java.nio=ALL-UNNAMED",
         "-Dlog.dir=/tmp/fluss-probe-logs",
         "-cp", f"{out_dir}:{cp}", "LogFullRead", table, str(run_ms)]
        + (["offset"] if with_offset else []),
        capture_output=True, text=True, timeout=run_ms / 1000 + 90,
    )
    rows = [ln for ln in r.stdout.splitlines() if ln.strip()]
    # Never overwrite saved evidence with an empty read: re-analyzing an old
    # out_dir after its table was purged/recreated returns 0 live rows — the
    # old 'w' mode destroyed the original rows file (observed 2026-08-30:
    # runs 140907 and 173514 lost their evidence this way).
    rows_path = os.path.join(out_dir, f"latency-{table}.rows.txt")
    # A ROW starts with '(' (Fluss Row toString). When Fluss is DOWN the
    # reader prints connection errors to stdout — those lines are NOT rows,
    # and counting them as rows let an offline re-analysis overwrite 300MB
    # of preview evidence with 91 error lines (observed 2026-08-31).
    real_rows = [ln for ln in rows if ln.startswith("(")]
    if real_rows or not os.path.exists(rows_path):
        with open(rows_path, "w") as f:
            f.write("\n".join(rows) + "\n")
    return real_rows

def collect_rows_stream(table, cp, out_dir, run_ms=30000, mode="offset"):
    """Like collect_rows but streams stdout straight to the evidence file —
    for tables too big to hold in memory (raw_table_1 at ~6M rows/10-min
    phase ≈ 1.2 GB of text). Returns the rows file path; callers iterate
    the file line-by-line (two passes are fine — the file is the evidence).
    mode: "offset" (bucket:offset + full row toString) or "audit"
    (bucket:offset + sanitized tab-separated fields — see LogFullRead).
    """
    os.makedirs(out_dir, exist_ok=True)
    java_file = os.path.join(out_dir, "LogFullRead.java")
    # ALWAYS regenerate + recompile (~2s): a stale class silently ignores
    # new modes (observed 2026-08-31: an old class treated "audit" as plain
    # mode, re-read the whole table in full-row format — 8 wasted minutes,
    # 0 audit rows, and a false all-failures verdict)
    with open(java_file, "w") as f:
        f.write(LOG_READ_SRC)
    r = subprocess.run(["javac", "-cp", cp, "-d", out_dir, java_file],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print(f"!! LogFullRead compile failed for {table}: {r.stderr[:400]}")
        return None
    rows_path = os.path.join(out_dir, f"latency-{table}.rows.txt")
    with open(rows_path, "w") as fh:
        subprocess.run(
            ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED",
             "--add-opens=java.base/java.nio=ALL-UNNAMED",
             "-Dlog.dir=/tmp/fluss-probe-logs",
             "-cp", f"{out_dir}:{cp}", "LogFullRead", table, str(run_ms)]
            + ([mode] if mode else []),
            stdout=fh, stderr=subprocess.DEVNULL,
            timeout=run_ms / 1000 + 90,
        )
    # Evidence preservation (same rule as collect_rows): never let an empty
    # re-read destroy a previously captured file.
    # Same error-vs-row discipline as collect_rows: with Fluss down the
    # streamed output is connection errors, not rows — treat a file with
    # no '(' -prefixed lines as empty (protects evidence, 2026-08-31).
    n_real = 0
    try:
        with open(rows_path) as f:
            for ln in f:
                if ln.startswith("(") or ln[0:1].isdigit():
                    n_real += 1
                    break
    except OSError:
        n_real = 0
    if n_real == 0:
        return None
    return rows_path


def classify_status(candidate_id):
    """Status from the candidate_id suffix (NOT substring search: CONFIRM
    rows' validity_reason also contains 'TENTATIVE')."""
    if candidate_id.endswith("-TENTATIVE"):
        return "TENTATIVE"
    if candidate_id.endswith("-CONFIRM"):
        return "CONFIRM"
    if candidate_id.endswith("-CANCEL"):
        return "CANCEL"
    return "OTHER"


def main():
    failures = []  # collected by all guards; exit 1 if non-empty
    out_dir, cp = sys.argv[1], sys.argv[2]
    run_start = int(sys.argv[3]) * 1000 if len(sys.argv) > 3 else None
    run_end = int(sys.argv[4]) * 1000 if len(sys.argv) > 4 else None
    print("## Latency analysis (wall-clock from row timestamps, run-window filtered)")
    if run_start:
        print(f"- run window: [{run_start}, {run_end}] epoch-ms\n")

    ms = lambda v: v  # clarity: all row timestamps are epoch-ms

    # ---- previews: cadence + freshness (output_ts IS wall clock) ----
    # NOTE: the preview table is purged (drop+recreate) at RUN START by
    # pipeline-lib.sh (purge_preview_table) — never here, or the rows this
    # read needs would be destroyed before reading them.
    prev_rows = collect_rows("feature_candles_15s_preview", cp, out_dir)
    # Row (v2): (token,NSE,symbol,window_start,window_end,o,h,l,c,vol,tick_count,
    #            is_preview,output_ts,last_event_ts,ver)
    prev_re = re.compile(
        r"\((\d+),NSE,\d+,(\d+),(\d+),[\d.\-]+(?:,[\d.\-]+)*,\s*(true|false),(\d+),(\d+),\d+\)")
    per_key_ts = defaultdict(list)   # (token, window_start) -> [output_ts]
    buckets_1s = defaultdict(list)   # second-offset -> [e2e latency] (burst detection)
    e2e_lat = []                     # output_ts - last_event_ts per row (v2)
    last_ts_per_token = {}           # token -> newest output_ts in window
    for ln in prev_rows:
        m = prev_re.match(ln)
        if not m or m.group(4) != "true":
            continue
        token, ws, ots, lets = m.group(1), int(m.group(2)), int(m.group(5)), int(m.group(6))
        if run_start and not (run_start <= ots <= (run_end or ots)):
            continue
        per_key_ts[(token, ws)].append(ots)
        buckets_1s.setdefault((ots - (run_start or ots)) // 1000, []).append(ots - lets)
        if ots > last_ts_per_token.get(token, 0):
            last_ts_per_token[token] = ots
        if lets and ots >= lets:
            e2e_lat.append(ots - lets)

    # DEDUPE: the Fluss LogScanner re-delivers records across polls
    # (observed 2026-08-30: every preview row appeared exactly twice in a
    # from-earliest read; per-key timestamps were pairwise identical).
    # Distinct sorted timestamps per key = the true emission series.
    n_prev_in_window = sum(len(set(v)) for v in per_key_ts.values())
    cadence = []
    for ts_list in per_key_ts.values():
        distinct = sorted(set(ts_list))
        cadence.extend(b - a for a, b in zip(distinct, distinct[1:]))

    freshness = []
    if run_end:
        freshness = [run_end - t for t in last_ts_per_token.values()]

    print(f"### Preview path ({n_prev_in_window} preview rows in run window)")
    print(f"- e2e latency (last tick → preview row, output_ts - last_event_ts): "
          f"p50={fmt_ms(pct(e2e_lat,50))} p95={fmt_ms(pct(e2e_lat,95))} "
          f"p99={fmt_ms(pct(e2e_lat,99))} (n={len(e2e_lat)}, target <1000ms)")
    print(f"- preview cadence: p50={fmt_ms(pct(cadence,50))} p95={fmt_ms(pct(cadence,95))} "
          f"p99={fmt_ms(pct(cadence,99))} (n={len(cadence)}, expected ~1000ms)")
    print(f"- preview freshness at run end (staleness): p50={fmt_ms(pct(freshness,50))} "
          f"p95={fmt_ms(pct(freshness,95))} p99={fmt_ms(pct(freshness,99))} (n={len(freshness)})")

    # ---- Final candle path (2026-08-30): window-close → committed latency ----
    # feature_candles_15s (v2) has output_ts + window_end but no
    # last_event_ts, so the measurable latency is output_ts - window_end:
    # how long after a 15s window closed did its FINAL row land in the
    # table. (For context: the last tick of the window typically arrives
    # just before window_end, so broker→final-candle ≈ this value + tick
    # lead-in. This is the broker→FEATURE-TABLE headline number.)
    final_rows = collect_rows("feature_candles_15s", cp, out_dir)
    final_rows = list(dict.fromkeys(final_rows))  # same re-delivery dedupe
    close_lat = []
    for ln in final_rows:
        # DDL column order: token,exchange,symbol,window_start,window_end,
        # o,h,l,c,v,tick_count,algo_ver,config_ver,output_ts,schema_version
        # → output_ts is index 13 (15 fields; index 12 is config_version —
        # a first draft used 12 and silently read 0 rows).
        f = ln.strip("()").split(",")
        if len(f) >= 15:
            try:
                ots, wend = int(f[13]), int(f[4])
                # window_end is window_start+15000-1 style; the window
                # CLOSE is the end boundary. Negative = row stamped before
                # close (shouldn't happen); huge = replay of old rows.
                lat = ots - wend
                if run_start and not (run_start <= ots <= (run_end or ots)):
                    continue
                if 0 <= lat <= 60000:
                    close_lat.append(lat)
            except ValueError:
                continue
    print(f"\n### Final candle path ({len(close_lat)} final candles in run window)")
    if close_lat:
        print(f"- window-close → committed (output_ts - window_end): "
              f"p50={fmt_ms(pct(close_lat,50))} p95={fmt_ms(pct(close_lat,95))} "
              f"p99={fmt_ms(pct(close_lat,99))} (n={len(close_lat)})")
    else:
        print("- no final-candle rows in run window (final emission off?)")

    # ---- Signal_Candidates: volume by status + settlement balance ----
    # NOTE: detection_ts/evaluation_ts are EVENT-TIME (window-aligned), so
    # no wall-clock latency is derivable here — per-operator wall-clock
    # latency comes from Flink latency metrics (see throughput.tsv). We
    # report per-status volumes and the tentative→settlement balance.
    # G7_REUSE_RAW=1 (evidence replay) also covers the signal table: reuse
    # the saved rows file instead of re-reading live Fluss (the table may
    # have been purged, and re-reads cost 30s each). This is also the F4
    # tamper-proof path: bug-inject the rows file, re-run, guard fires.
    sig_reuse_path = os.path.join(out_dir, "latency-Signal_Candidates.rows.txt")
    if (os.environ.get("G7_REUSE_RAW") == "1"
            and os.path.exists(sig_reuse_path)):
        with open(sig_reuse_path) as f:
            sig_rows = [ln for ln in f.read().splitlines() if ln.strip()]
    else:
        sig_rows = collect_rows("Signal_Candidates", cp, out_dir)
    # DEDUPE: same LogScanner re-delivery (see preview note above).
    sig_rows = list(dict.fromkeys(sig_rows))
    status_counts = defaultdict(int)
    settle_by_status = defaultdict(int)
    candle_re = re.compile(r"candle:(\d{13}):(\d{13})")
    # F4 pairing (2026-08-31): group rows by candidate_id prefix (status
    # stripped) so a TENTATIVE can be matched to its CONFIRM/CANCEL
    # partner. Orphan check runs after the loop — see below.
    sig_groups = defaultdict(set)
    sig_group_ts = {}
    for ln in sig_rows:
        parts = ln.strip("()").split(",")
        if len(parts) < 12:
            continue
        try:
            eval_ts = int(parts[10])
        except ValueError:
            continue
        if run_start and not (run_start <= eval_ts <= (run_end or eval_ts)):
            continue
        st = classify_status(parts[0])
        status_counts[st] += 1
        # Settle rows come in TWO shapes (found via tamper proof 2026-08-31):
        # suffixed "-CONFIRM"/"-CANCEL", AND the unsuffixed base id with
        # validity_reason=VALID. Both are settlement partners — the pairing
        # must count either, or deleting one shape hides a dropped decision.
        m = re.match(r"(.*)-(TENTATIVE|CONFIRM|CANCEL)$", parts[0])
        if m:
            # in-run membership uses ANY row of the group inside the window
            sig_groups[m.group(1)].add(m.group(2))
            sig_group_ts[m.group(1)] = max(sig_group_ts.get(m.group(1), 0), eval_ts)
        else:
            sig_groups[parts[0]].add("BASE")
            sig_group_ts[parts[0]] = max(sig_group_ts.get(parts[0], 0), eval_ts)
        if st in ("CONFIRM", "CANCEL"):
            settle_by_status[st] += 1

    tent = status_counts.get("TENTATIVE", 0)
    conf = settle_by_status.get("CONFIRM", 0)
    canc = settle_by_status.get("CANCEL", 0)
    print(f"\n### Early-signal path ({sum(status_counts.values())} rows in run window)")
    print(f"- volumes by status: {dict(status_counts)}")
    if tent:
        settled = conf + canc
        print(f"- settlement balance: {settled}/{tent} tentatives settled "
              f"(confirm={conf} cancel={canc}) — unsettled tentatives belong to "
              f"windows still open at run end (expected)")
    # F4 guard (2026-08-31): "expected" above was an assumption — now
    # checked. A TENTATIVE whose window closed more than GRACE_MS before
    # run end MUST have a CONFIRM/CANCEL partner; without one the signal
    # path silently dropped a settlement (a trading decision vanished).
    # Verified on the 2026-08-31-022833 evidence: 205 in-run TENTATIVE-only
    # groups, ALL inside the last 15s window (open at run end) — 0 orphans.
    F4_GRACE_MS = 30000
    orphan_ts = [ts for pfx, sts in sig_groups.items()
                 if sts == {"TENTATIVE"}
                 for ts in [sig_group_ts[pfx]]
                 if (run_end or ts) - ts > F4_GRACE_MS]
    lonely_n = sum(1 for sts in sig_groups.values() if sts == {"TENTATIVE"})
    print(f"- F4 orphan check: {len(orphan_ts)} orphaned TENTATIVE(s) beyond "
          f"{F4_GRACE_MS // 1000}s grace ({lonely_n} in-run tentative-only "
          f"groups, all others inside the open-window tail)")
    if orphan_ts:
        failures.append(
            f"F4: {len(orphan_ts)} TENTATIVE signal(s) never settled — "
            f"windows closed >{F4_GRACE_MS // 1000}s before run end with no "
            f"CONFIRM/CANCEL partner (settlement path dropped decisions)")
    print("\n(Per-operator wall-clock latency: Flink latency histograms in "
          "main/latency-metrics.tsv; event-time row stamps cannot measure it.)")
    # ---- Signal-path latency (2026-08-31) -------------------------------
    # Flink latency tracking (metrics.latency.interval=2000, set at submit)
    # emits source->operator wall-clock latency histograms per operator.
    # Sampled by the harness since 2026-08-31 (tm-prom-latency.tsv) — this
    # is the only wall-clock signal-path measurement that exists: the signal
    # tables carry event-time stamps only, so the table rows themselves
    # cannot answer "how late did the signal land".
    try:
        lat_series = defaultdict(list)   # (opname, quantile) -> [values]
        with open(os.path.join(out_dir, "main", "tm-prom-latency.tsv")) as f:
            for ln in f:
                # "<epoch> flink_..._latency{...,task_name="op -> x",...,
                #  quantile="0.99",} 123.0" — the operator name is the
                # task_name LABEL (first path segment), not part of the
                # metric name; quantile is a label too (not first).
                parts = ln.split(" ", 1)
                if len(parts) != 2:
                    continue
                mq = re.match(r'(flink_\S*latency)\{(.*)\}\s+([\d.]+)$',
                              parts[1])
                if not mq or mq.group(1).endswith("_count"):
                    continue
                labels = dict(re.findall(r'(\w+)="([^"]*)"', mq.group(2)))
                task = labels.get("task_name", "")
                qtl = labels.get("quantile")
                if task and qtl:
                    op = task.split(" -> ")[0][:44]
                    lat_series[(op, float(qtl))].append(float(mq.group(3)))
        if lat_series:
            ops = sorted({op for op, _ in lat_series})
            print("\n### Signal-path latency (Flink source->operator, wall-clock ms)")
            print(f"{'operator':<28} {'p50':>8} {'p95':>8} {'p99':>8} {'n samples':>10}")
            for op in ops:
                def q(v):
                    return (pct(lat_series.get((op, v), []), 100 * v)
                            or 0)
                n = max(len(lat_series[(op, qq)]) for qq in (0.5, 0.95, 0.99))
                print(f"{op:<28} {q(0.5):>8.0f} {q(0.95):>8.0f} {q(0.99):>8.0f} {n:>10}")
        else:
            print("\n(signal-path latency: no tm-prom-latency.tsv samples — "
                  "pre-2026-08-31 evidence dir)")
    except OSError:
        print("\n(signal-path latency: tm-prom-latency.tsv absent)")

    # ---- Burst attribution (2026-08-30): correlate latency spikes with
    # GC pauses (TM + ingestion JVM) and slow checkpoints ----
    print("\n### Burst attribution (spikes vs GC pauses vs checkpoints)")
    run_start_ms = run_start or 0

    def gc_pauses(path, label):
        """Parse -Xlog:gc unified-log pause lines: start time + duration."""
        pauses = []
        try:
            with open(path) as f:
                for ln in f:
                    # e.g. [2026-08-30T03:10:30.123+05:30][uptime] GC(42) Pause
                    # Young (G1 Evacuation Pause) 512M->64M(2048M) 12.345ms
                    m = re.match(r"\[(\d{4}-\d{2}-\d{2}T[\d:.]+)", ln)
                    d = re.search(r"(\d+(?:\.\d+)?)ms\s*$", ln)
                    if m and d and "Pause" in ln:
                        import datetime as _dt
                        t = _dt.datetime.fromisoformat(m.group(1))
                        pauses.append((int(t.timestamp() * 1000), float(d.group(1))))
        except OSError:
            pass
        if pauses:
            big = [(t, d) for t, d in pauses if d >= 100]
            print(f"- {label}: {len(pauses)} pauses, {len(big)} >=100ms; "
                  f"max={max(d for _, d in pauses):.0f}ms")
        else:
            print(f"- {label}: no pause lines found (log absent or GC quiet)")
        return pauses

    tm_gc = gc_pauses(os.path.join(out_dir, "main", "tm-gc-final.log"), "TM JVM")
    ing_gc = gc_pauses(os.path.join(out_dir, "main", "j1", "gc.log"), "ingestion JVM")

    # Latency bursts: 1s slices with p95 > 2000ms
    burst_secs = sorted(k for k, v in buckets_1s.items()
                        if v and v[int(len(v) * 0.95)] > 2000) if buckets_1s else []
    print(f"- latency bursts (1s slices with p95>2s): {len(burst_secs)}")

    def overlaps(ts_ms, offsets_s, window_ms=3000):
        return any(abs(ts_ms - ((run_start_ms + off * 1000))) <= window_ms for off in offsets_s)

    if burst_secs:
        tm_hits = sum(1 for t, d in tm_gc if d >= 100 and overlaps(t, burst_secs))
        ing_hits = sum(1 for t, d in ing_gc if d >= 100 and overlaps(t, burst_secs))
        big_tm = [(t, d) for t, d in tm_gc if d >= 100]
        big_ing = [(t, d) for t, d in ing_gc if d >= 100]
        print(f"- TM GC pauses >=100ms within ±3s of a burst: {tm_hits}/{len(big_tm)}")
        print(f"- ingestion GC pauses >=100ms within ±3s of a burst: {ing_hits}/{len(big_ing)}")
        # checkpoints — correlate bursts with EVERY checkpoint's active
        # window [trigger_ts, trigger_ts+duration]. The old >5s-only filter
        # was computed from a single sample because a harness dedupe bug
        # (fixed 2026-08-30) collapsed checkpoints.jsonl to one line; the
        # one visible checkpoint took ~4s of a 10s interval.
        cps = []
        cp_path = os.path.join(out_dir, "main", "checkpoints.jsonl")
        try:
            with open(cp_path) as f:
                for ln in f:
                    try:
                        c = json.loads(ln)
                        if c.get("trigger_ts") and c.get("duration_ms") is not None:
                            cps.append(c)
                    except ValueError:
                        pass
        except OSError:
            pass
        if cps:
            durs = sorted(c["duration_ms"] for c in cps)
            print(f"- checkpoints completed: {len(cps)}, duration ms "
                  f"min/med/max: {durs[0]}/{durs[len(durs)//2]}/{durs[-1]} "
                  f"(interval 10s — durations approaching the interval mean "
                  f"near-continuous checkpoint pressure)")
            # A checkpoint overlaps a burst if the burst second falls inside
            # its [trigger, trigger+duration] window (+1s slack for
            # post-checkpoint catch-up emission).
            # burst_secs are OFFSETS from run_start (seconds) — convert
            # the checkpoint window to the same base (first attempt
            # compared epoch-seconds to offsets: 0/70 "overlap" was a units
            # bug, caught 2026-08-30 because 0 hits is statistically
            # impossible when checkpoints cover ~25% of wall time).
            cp_hits = sum(
                1 for c in cps
                if any((c["trigger_ts"] - run_start_ms)//1000 - 1 <= b <=
                       (c["trigger_ts"] + c["duration_ms"] - run_start_ms)//1000 + 1
                       for b in burst_secs))
            print(f"- checkpoints overlapping a burst second: "
                  f"{cp_hits}/{len(cps)}")
            # ---- B5 Phase-0 (2026-08-31): WHICH phase of a checkpoint
            # freezes emission? Per-task sync/async/alignment breakdown
            # from TM Prometheus vertex gauges. The REST history 'tasks'
            # map is EMPTY for regular checkpoints in Flink 2.2 (live
            # observation 2026-08-31) — the first attempt to capture it
            # produced empty task lists and would have silently answered
            # nothing. FAIL-FAST: a B5 analysis without gauge data is a
            # broken measurement, not a no-op.
            phases_path = os.path.join(out_dir, "main",
                                       "tm-prom-cp-phases.tsv")
            try:
                # gauges: "<epoch> flink_..._checkpointStartDelayNanos{...,
                # task_name="x",...} 1.2E7" (nanoseconds, per task)
                start_delay = defaultdict(list)   # task -> [ns]
                align_time = defaultdict(list)    # task -> [ns]
                n_phase_rows = 0
                with open(phases_path) as f:
                    for ln in f:
                        parts = ln.split(" ", 1)
                        if len(parts) != 2:
                            continue
                        body = parts[1]
                        try:
                            val = float(body[body.rindex("}") + 1:]
                                        .strip() or "nan")
                        except ValueError:
                            continue
                        labels = dict(re.findall(r'(\w+)="([^"]*)"',
                                                 body[:body.rindex("}") + 1]))
                        task = labels.get("task_name", "")
                        if not task:
                            continue
                        n_phase_rows += 1
                        if "checkpointStartDelayNanos" in body:
                            start_delay[task.split(" -> ")[0][:44]].append(val)
                        elif "checkpointAlignmentTime" in body:
                            align_time[task.split(" -> ")[0][:44]].append(val)
                if n_phase_rows == 0:
                    failures.append(
                        "B5: tm-prom-cp-phases.tsv has no parseable gauge "
                        "rows — checkpoint phase attribution is broken "
                        "(sampler not capturing vertex gauges?)")
                else:
                    # gauges are LAST-CHECKPOINT snapshots (ns). Report
                    # per-task maxima; verdict from the dominant family.
                    def _mx_ns(d):
                        return {k: max(v) for k, v in d.items() if v}
                    sd, al = _mx_ns(start_delay), _mx_ns(align_time)
                    print(f"- checkpoint phase gauges ({n_phase_rows} rows; "
                          f"start-delay / alignment, last-checkpoint ns):")
                    print(f"{'task':<46} {'start delay':>12} {'alignment':>12}")
                    for name in sorted(set(sd) | set(al),
                                       key=lambda n: -(sd.get(n, 0))):
                        print(f"{name:<46} {sd.get(name, 0)/1e6:>10.1f}ms "
                              f"{al.get(name, 0)/1e6:>10.1f}ms")
                    tot_sd = sum(sd.values()) / 1e6
                    tot_al = sum(al.values()) / 1e6
                    print(f"- totals (sum of per-task maxima): "
                          f"start-delay={tot_sd:.0f}ms "
                          f"alignment={tot_al:.0f}ms")
                    if tot_al > tot_sd:
                        print("- verdict: ALIGNMENT dominates → unaligned "
                              "checkpoints are the right lever")
                    else:
                        print("- verdict: start-delay (barrier arrival) "
                              "dominates over alignment → operators are "
                              "slow to REACH the barrier (busy threads / "
                              "sink flush before barrier), not stuck "
                              "aligning — unaligned checkpoints will NOT "
                              "fix this")
            except OSError:
                # pre-B5 evidence dir (file absent): not a failure UNLESS
                # the run was flagged as a B5 experiment
                if os.environ.get("B5_EXPECT_PHASES") == "1":
                    failures.append(
                        "B5: tm-prom-cp-phases.tsv absent but "
                        "B5_EXPECT_PHASES=1 — phase instrumentation did "
                        "not run (sampler broken or poll never fired)")
        else:
            cp_hits = 0
        cp_slow = [c for c in cps if (c.get("duration_ms") or 0) > 5000]
        print(f"- slow checkpoints (>5s): {len(cp_slow)}")
        verdict = []
        if big_tm and tm_hits >= max(1, len(big_tm) // 2):
            verdict.append("TM GC pauses CORRELATE with bursts — JVM tuning next")
        if big_ing and ing_hits >= max(1, len(big_ing) // 2):
            verdict.append("ingestion JVM GC pauses CORRELATE — JVM tuning next")
        if cps and cp_hits >= max(1, len(cps) // 2):
            verdict.append(
                f"checkpoints CORRELATE with bursts ({cp_hits}/{len(cps)} overlap) — "
                "checkpoint/barrier tuning next")
        elif cp_slow and cp_hits >= max(1, len(cp_slow) // 2):
            verdict.append("slow checkpoints CORRELATE — checkpoint tuning next")
        if not verdict:
            verdict.append("NO correlation found with GC or checkpoints — "
                           "investigate source/ingest flush cycles next")
        print("- verdict: " + "; ".join(verdict))

    # ---- A3 (2026-08-30): staged-latency attribution from ingestion
    # otlp-metrics-payload lines (logged every 10s when collector is down —
    # see pipeline-lib.sh). Which pipeline stage's p99 spikes during bursts?
    print("\n### Ingestion staged latencies (otlp-metrics-payload, 10s cadence)")
    java_out = os.path.join(out_dir, "main", "j1", "java.out")
    payloads = []  # (time_unix_nano, {stage_name: p99})
    try:
        with open(java_out) as f:
            for ln in f:
                if "otlp-metrics-payload: {" not in ln:
                    continue
                try:
                    payload = json.loads(ln.split("otlp-metrics-payload: ", 1)[1])
                except ValueError:
                    continue
                stages = {}
                tsn = 0
                for rm in payload.get("resourceMetrics", []):
                    for sm in rm.get("scopeMetrics", []):
                        for m in sm.get("metrics", []):
                            name = m.get("name", "")
                            dp = (m.get("histogram", {}).get("dataPoints") or [{}])[0]
                            if not dp:
                                continue
                            tsn = int(dp.get("timeUnixNano", 0))
                            if name.startswith("stage."):
                                p99 = dp.get("explicitBounds", [0, 0, 0])[2]
                                stages[name[len("stage."):]] = p99
                if stages:
                    payloads.append((tsn, stages))
    except OSError:
        pass

    if not payloads:
        print("- no otlp-metrics-payload lines found (collector was reachable, "
              "or java.out absent) — A3 attribution unavailable for this run")
    else:
        print(f"- payloads parsed: {len(payloads)}")
        stage_names = sorted({k for _, st in payloads for k in st})
        # Per-stage stats + spike/burst correlation
        print(f"- {'stage':<22} {'p99 med':>8} {'p99 max':>8} {'spikes':>6} {'@burst':>7}")
        attribution = {}
        for name in stage_names:
            vals = sorted(st.get(name, 0) for _, st in payloads if name in st)
            if not vals:
                continue
            med = vals[len(vals) // 2]
            mx = vals[-1]
            # spike = p99 above max(3x median, median+300ms)
            thr = max(3 * med, med + 300)
            spikes = [(t, v) for t, st in payloads
                      if (v := st.get(name)) is not None and v > thr]
            hits = 0
            if burst_secs:
                for t, _v in spikes:
                    t_s = (t // 10**9) - (run_start_ms // 1000)
                    if any(abs(t_s - b) <= 10 for b in burst_secs):
                        hits += 1
            attribution[name] = (med, mx, len(spikes), hits)
            print(f"- {name:<22} {med:>7}ms {mx:>7}ms {len(spikes):>6} {hits:>7}")
        # Verdict: the stage whose spikes best overlap bursts
        best = None
        if burst_secs:
            for name, (med, mx, ns, nh) in attribution.items():
                if ns >= 2 and nh >= max(1, ns // 2):
                    if best is None or nh > best[1]:
                        best = (name, nh)
        if best:
            print(f"- VERDICT: stage '{best[0]}' p99 spikes align with latency "
                  f"bursts ({best[1]} hits) — burst originates "
                  + ("in/before ingestion (bridge→decode→Fluss append path)"
                     if best[0] in ("ipc_latency", "routing_latency", "decode_latency",
                                    "batching_latency", "fluss_submit_latency",
                                    "fluss_ack_latency", "end_to_end_latency") else "upstream"))
        elif burst_secs:
            print("- VERDICT: no ingestion stage p99 spikes align with bursts — "
                  "the delay is DOWNSTREAM of ingestion (Fluss storage→Flink read path)")

    # ---- A2 (disk saturation) + A5 (CPU throttling) + A1 (RocksDB flush)
    # correlation with bursts (2026-08-30 batch) ----
    print("\n### A2 disk busy% / A5 CPU throttling / A1 RocksDB flush vs bursts")

    def near_bursts(t_s, window=10):
        return any(abs(t_s - b) <= window for b in burst_secs)

    # A2 v2: /proc/diskstats → busy%, await (ms/op), queue depth per interval.
    # Handles v1 (4-col: epoch reads writes io_ms) and v2 (8-col) formats.
    ds_path = os.path.join(out_dir, "main", "diskstats.tsv")
    try:
        ds = [tuple(int(x) for x in ln.split()) for ln in open(ds_path) if ln.strip()]
        busy, awaits, queues = [], [], []
        for a, b in zip(ds, ds[1:]):
            dt = b[0] - a[0]
            if dt <= 0:
                continue
            t_off = b[0] - run_start_ms // 1000
            io_ms = b[3] - a[3]          # v1 position == v2 field 13 (io_ms) by luck of layout
            if len(b) >= 8:
                # v2: epoch reads r_ms writes w_ms in_flight io_ms w_io_ms
                ops = (b[1] - a[1]) + (b[3] - a[3])
                await_ms = ((b[2] - a[2]) + (b[4] - a[4])) / ops if ops > 0 else 0.0
                # avg queue depth ≈ weighted_io_ms / elapsed_ms (dimensionless)
                qd = (b[7] - a[7]) / (dt * 1000)
                io_ms = b[6] - a[6]
            else:
                await_ms, qd = None, None
            busy.append((t_off, 100.0 * io_ms / (dt * 1000)))
            if await_ms is not None:
                awaits.append((t_off, await_ms))
                queues.append((t_off, qd))
        if busy:
            mx = max(v for _, v in busy)
            hot = [(t, v) for t, v in busy if v > 80]
            hits = sum(1 for t, v in hot if near_bursts(t))
            print(f"- disk busy%: max={mx:.0f}%, intervals>80%: {len(hot)}, "
                  f"within ±10s of a burst: {hits}")
            if awaits:
                aw_max = max(v for _, v in awaits)
                aw_hot = [(t, v) for t, v in awaits if v > 20]
                aw_hits = sum(1 for t, _ in aw_hot if near_bursts(t))
                print(f"- disk await: max={aw_max:.0f} ms/op, intervals>20ms: {len(aw_hot)}, "
                      f"within ±10s of a burst: {aw_hits}")
                qd_max = max(v for _, v in queues)
                print(f"- avg queue depth: max={qd_max:.1f}")
            if len(hot) >= 2 and hits >= max(1, len(hot) // 2):
                print("- A2 VERDICT: disk saturation CORRELATES with bursts")
            elif hot:
                print("- A2 VERDICT: disk pressure present but NOT conclusively burst-aligned")
            else:
                print("- A2 VERDICT: disk not saturated this run")
        else:
            print("- diskstats: no parseable samples")
    except OSError:
        print("- diskstats.tsv absent (A2 unavailable)")

    # A5: cgroup cpu.stat → throttling deltas
    th_path = os.path.join(out_dir, "main", "tm-throttle.tsv")
    try:
        snaps = {}
        for ln in open(th_path):
            parts = ln.split()
            if len(parts) == 3:
                snaps.setdefault(int(parts[0]), {})[parts[1]] = int(parts[2])
        keys = sorted(snaps)
        if keys:
            nr0 = snaps[keys[0]].get("nr_periods", 0)
            nrN = snaps[keys[-1]].get("nr_periods", 0)
            thr0 = snaps[keys[0]].get("nr_throttled", 0)
            thrN = snaps[keys[-1]].get("nr_throttled", 0)
            tu0 = snaps[keys[0]].get("throttled_usec", 0)
            tuN = snaps[keys[-1]].get("throttled_usec", 0)
            if nrN == 0:
                print("- A5 VERDICT: nr_periods=0 — no CPU quota configured on the TM; "
                      "CFS throttling structurally impossible (ruled out)")
            else:
                print(f"- throttled periods: {thrN-thr0}/{nrN-nr0}, "
                      f"throttled time: {(tuN-tu0)/1000:.0f}ms total")
                if thrN - thr0 == 0:
                    print("- A5 VERDICT: zero throttling (ruled out)")
                else:
                    # which intervals throttled?
                    thi = [(t, (snaps[t].get("throttled_usec", 0))) for t in keys]
                    th_ev = []
                    for a, b in zip(keys, keys[1:]):
                        d = snaps[b].get("throttled_usec", 0) - snaps[a].get("throttled_usec", 0)
                        if d > 100_000:  # >100ms in one interval
                            th_ev.append(b - run_start_ms // 1000)
                    hits = sum(1 for t in th_ev if near_bursts(t))
                    print(f"- heavy-throttle intervals (>100ms): {len(th_ev)}, within ±10s of burst: {hits}")
                    print("- A5 VERDICT: throttling CORRELATES with bursts" if hits >= max(1, len(th_ev)//2)
                          else "- A5 VERDICT: throttling present but NOT burst-aligned")
        else:
            print("- tm-throttle.tsv: no parseable samples")
    except OSError:
        print("- tm-throttle.tsv absent (A5 unavailable)")

    # A1: RocksDB gauges from TM Prometheus — flush events (memtable drop)
    pr_path = os.path.join(out_dir, "main", "tm-prom-rocksdb.tsv")
    try:
        samples = []  # (epoch, {metric_name: value})
        for ln in open(pr_path):
            parts = ln.split(None, 2)
            if len(parts) < 3:
                continue
            # format: "<epoch> <metric{labels}> <value>" — parts[1] is the
            # metric (possibly with {labels}), parts[2] the numeric value
            # (2026-08-30 bugfix: first version read the metric name from
            # parts[2] and parsed zero samples from 45840 captured lines).
            try:
                ep = int(parts[0])
                name = parts[1].split("{")[0]
                val = float(parts[2].strip())
            except ValueError:
                continue
            if not samples or samples[-1][0] != ep:
                samples.append((ep, {}))
            samples[-1][1][name] = val
        if samples:
            mt_key = None
            for k in samples[0][1]:
                if "cur_size_all_mem_tables" in k:
                    mt_key = k
                    break
            if mt_key:
                flushes = []
                for a, b in zip(samples, samples[1:]):
                    va, vb = a[1].get(mt_key), b[1].get(mt_key)
                    if va and vb and va > 0 and vb < 0.5 * va:
                        flushes.append(b[0] - run_start_ms // 1000)
                mx = max(s[1].get(mt_key, 0) for s in samples)
                print(f"- RocksDB memtable size: max={mx/1e6:.1f}MB, flush events "
                      f"(>50% drop): {len(flushes)}")
                if flushes:
                    hits = sum(1 for t in flushes if near_bursts(t))
                    print(f"- flush events within ±10s of a burst: {hits}/{len(flushes)}")
                    print("- A1 VERDICT: RocksDB flushes CORRELATE with bursts — "
                          "write-stall / flush tuning next" if hits >= max(1, len(flushes)//2)
                          else "- A1 VERDICT: flushes present but NOT burst-aligned")
            else:
                names = sorted({k for _, m in samples for k in m})[:5]
                print(f"- RocksDB metrics present but no memtable gauge; "
                      f"sample names: {names}")
        else:
            print("- tm-prom-rocksdb.tsv: no samples (job not stateful, or endpoint down)")
    except OSError:
        print("- tm-prom-rocksdb.tsv absent (A1 unavailable)")

    # ---- A2b (2026-08-30): name the disk WRITER — per-process write rates
    # (/proc/<pid>/io deltas) vs disk-saturation intervals and bursts ----
    print("\n### A2b per-process disk write rates (who saturates the disk?)")
    io_path = os.path.join(out_dir, "main", "proc-io.tsv")
    try:
        per = {}  # name -> [(epoch, read_bytes, write_bytes)]
        for ln in open(io_path):
            m = re.match(r"(\d+) (\w+) read_bytes: (\d+) write_bytes: (\d+)", ln)
            if not m:
                continue
            per.setdefault(m.group(2), []).append(
                (int(m.group(1)), int(m.group(3)), int(m.group(4))))
        if per:
            # v2 fix (2026-08-30): v1 parsed only read_bytes from the wrong
            # field and silently skipped distroless containers (minio,
            # openobserve) — the top tiering-writer candidates.
            print(f"- {'process':<12} {'rd avg':>7} {'rd max':>7} {'wr avg':>7} {'wr max':>7} {'wr@burst':>8}")
            ranked = []
            for name, series in per.items():
                series.sort()
                rates = []
                for a, b in zip(series, series[1:]):
                    dt = b[0] - a[0]
                    if dt > 0:
                        rates.append(((b[0] - run_start_ms // 1000),
                                      (b[1] - a[1]) / dt / 1e6,
                                      (b[2] - a[2]) / dt / 1e6))
                if not rates:
                    continue
                ravg = sum(r for _, r, _ in rates) / len(rates)
                rmax = max(r for _, r, _ in rates)
                wavg = sum(w for _, _, w in rates) / len(rates)
                wmax = max(w for _, _, w in rates)
                thr = max(3 * wavg, wavg + 5)
                spikes = [(t, w) for t, _, w in rates if w > thr]
                hits = sum(1 for t, _ in spikes if near_bursts(t, 12))
                ranked.append((wmax, name, ravg, rmax, wavg, len(spikes), hits))
            for wmax, name, ravg, rmax, wavg, ns, nh in sorted(ranked, reverse=True):
                print(f"- {name:<12} {ravg:>6.1f} {rmax:>6.1f} {wavg:>6.1f} {wmax:>6.1f} "
                      f"{nh}/{ns:>5}")
            top = sorted(ranked, reverse=True)[0]
            print(f"- A2b VERDICT: heaviest writer = '{top[1]}' (max {top[0]:.0f} MB/s) "
                  f"— {'and its spikes ALIGN with bursts' if top[6] >= max(1, top[5]//2) else 'but spikes do not align with bursts'}")
        else:
            print("- proc-io.tsv: no parseable samples")
    except OSError:
        print("- proc-io.tsv absent (A2b unavailable)")

    if not cadence and not status_counts:
        print("!! no rows parsed within the run window — raw rows kept in "
              "latency-*.rows.txt for diagnosis")

    # ---- G6 ASSERTIVE GUARDS (2026-08-30) --------------------------------
    # The tiering-off + preview-purge fixes are proven; these guards make a
    # regression FAIL the run (non-zero exit) instead of printing a verdict
    # nobody reads. Failures collected here, exit code set at the end.

    # ---- D6 guard (2026-08-31): ingestion JVM live-set leak alarm ----
    # Baseline measured over 17 runs (2026-08-31): post-GC heap 64-90 MB
    # (then at 2g heap; unchanged at 512m — live-set is driven by the
    # working set, not the heap size). A code change that leaks shows an
    # inflated live-set long before it OOMs — this turns the D6 one-off
    # analysis into a standing per-run check. Bound = 200 MB (~2.2x
    # baseline max): normal variance passes, a leak fails.
    print("\n### D6 ingestion JVM live-set (gc.log)")
    try:
        live = []
        with open(os.path.join(out_dir, "main", "j1", "gc.log")) as f:
            for ln in f:
                m = re.search(
                    r"GC\(\d+\) Pause \w+[^\n]*? \d+M->(\d+)M\(\d+M\) [\d.]+ms", ln)
                if m:
                    live.append(int(m.group(1)))
        if live:
            live_tail = live[len(live) // 2:]  # second half: warm, post-startup
            ls_max = max(live_tail)
            print(f"- post-GC live-set: n={len(live)} max(all)={max(live)}MB "
                  f"max(warm half)={ls_max}MB (guard bound 200MB)")
            if ls_max > 200:
                failures.append(
                    f"D6: ingestion JVM post-GC live-set reached {ls_max}MB "
                    f"(bound 200MB, baseline 64-90MB) - possible memory leak; "
                    f"compare with prior runs before shipping")
        else:
            print("- gc.log present but no pause lines parsed (format drift?)")
    except OSError:
        print("- gc.log absent (D6 guard skipped - legacy evidence dir?)")


    # G6a: preview e2e p95 MUST stay under 1s (REQ-FC-002 target, broker→
    # feature table). Proven achievable post-fix: bursts (tiering copies)
    # eliminated; p95 collapses toward p50 (~330ms).
    # Env escape hatch for deliberately-stressed diagnostic runs only:
    #   LATENCY_GUARD_OFF=1 holistic-measure.sh ...
    guard_off = os.environ.get("LATENCY_GUARD_OFF") == "1"
    if guard_off:
        print("- G6: LATENCY_GUARD_OFF=1 — latency guard SKIPPED (diagnostic run)")
    elif not e2e_lat:
        failures.append(
            "G6: no preview e2e samples — preview measurement broken "
            "(purge ran at run start? preview table recreated? sink alive?)")
    else:
        p95 = pct(e2e_lat, 95)
        if p95 is not None and p95 > 1000:
            failures.append(f"G6a: preview e2e p95={p95:.0f}ms > 1000ms target")

    # G6b: no NON-CHECKPOINT second may exceed p95=3s. Checkpoint-window
    # seconds are EXEMPT: at CHECKPOINT_INTERVAL_MS=60000 each checkpoint
    # freezes emission ~3.3s (tail to ~6s at 500ms emission) — a KNOWN,
    # accepted p99 tail documented in the levers map (next lever if it
    # becomes unacceptable: unaligned checkpoints). A flat threshold either
    # nagged on that known tail every run (3s) or went deaf to new stalls
    # (4s still tripped on the 6s checkpoint tail) — exemption keeps the
    # guard fully armed for NEW stall types (pre-fix tiering/checkpoint
    # bursts were caught exactly this way).
    if not guard_off and burst_secs:
        cp_windows = []
        try:
            with open(os.path.join(out_dir, "main", "checkpoints.jsonl")) as f:
                for ln in f:
                    try:
                        c = json.loads(ln)
                        if c.get("trigger_ts") and c.get("duration_ms") is not None:
                            cp_windows.append((c["trigger_ts"], c["duration_ms"]))
                    except ValueError:
                        pass
        except OSError:
            pass
        def in_cp_window(off_s):
            # Window: [trigger-1s, trigger+duration+6s]. The freeze lags
            # the checkpoint END by 0.5-5.0s (measured 2026-08-30, final
            # run) + 1s bucket rounding — the TM-side snapshot blocks the
            # emit timer, then the backlog drains after the JM-side
            # checkpoint completes.
            return any(
                t0 - 1000 <= run_start_ms + off_s * 1000 <= t0 + d + 6000
                for t0, d in cp_windows)
        bad = [b for b, v in buckets_1s.items()
               if len(v) >= 10 and pct(v, 95) > 3000
               and not in_cp_window(b)]
        exempt = [b for b, v in buckets_1s.items()
                  if len(v) >= 10 and pct(v, 95) > 3000 and in_cp_window(b)]
        if exempt:
            print(f"- G6b note: {len(exempt)} heavy second(s) inside checkpoint "
                  f"windows — known/accepted tail (offsets: {sorted(exempt)[:8]})")
        if bad:
            failures.append(
                f"G6b: {len(bad)} NON-checkpoint second(s) with per-second "
                f"p95 > 3000ms — new stall type (offsets: {sorted(bad)[:8]})")

    # G6c: tablet disk-read saturation must not burst-align — the tiering
    # fix (remote.log.task-interval-duration=0s on bench runs) is what
    # killed the ~90s burst period; if a run re-enables tiering (or another
    # tablet-side read storm appears) this catches it.
    # tablet spikes = intervals with tablet read > 50 MB/s (pre-fix: 200).
    try:
        io_path2 = os.path.join(out_dir, "main", "proc-io.tsv")
        per2 = {}
        for ln in open(io_path2):
            m = re.match(r"(\d+) (\w+) read_bytes: (\d+) write_bytes: (\d+)", ln)
            if m:
                per2.setdefault(m.group(2), []).append(
                    (int(m.group(1)), int(m.group(3)), int(m.group(4))))
        tser = per2.get("tablet", [])
        storm = []
        for a, b in zip(tser, tser[1:]):
            dt = b[0] - a[0]
            if dt > 0 and (b[1] - a[1]) / dt / 1e6 > 50:
                storm.append(b[0] - run_start // 1000)
        if storm:
            hits = sum(1 for t in storm if near_bursts(t, 12))
            if hits >= max(1, len(storm) // 2):
                failures.append(
                    f"G6c: tablet read-storm intervals ({len(storm)}) "
                    "burst-aligned — is log tiering re-enabled "
                    "(remote.log.task-interval-duration) or another "
                    "tablet read storm present?")
    except (OSError, NameError):
        pass  # proc-io absent (older harness) — not a failure

    # ---- G7 DATA-QUALITY GUARDS (F2/F3 audit, 2026-08-30) ----------------
    # The main phase injects duplicate ticks (verbatim frame resends - same
    # fingerprint) and late ticks (old event_time, unique fingerprint).
    # Three exactness assertions:
    #   G7a dedup:   compute.dedup.duplicates delta == dups sent == dups
    #                observed duplicated in raw_table_1 (repeated fingerprint)
    #   G7b late:    compute.candles.late.dropped delta == late sent == late
    #                observed in raw_table_1 (event_time lagging ingest_ts)
    #   G7c parity:  per (token, 15s window), the final candle's tick_count
    #                and volume == raw recount MINUS dropped dups and late
    #                ticks - zero loss, no over-drop, no double-count.
    print("\n### G7 data-quality audit (dedup / late-drop / tick-set parity)")
    g7_dir = os.path.join(out_dir, "main")
    want_dups = want_late = 0
    try:
        for ln in open(os.path.join(g7_dir, "faketool.log")):
            m = re.search(r"INJECT round=\d+ token=\d+ dups=(\d+) late=(\d+)", ln)
            if m:
                want_dups += int(m.group(1))
                want_late += int(m.group(2))
    except OSError:
        pass
    if want_dups == 0 and want_late == 0:
        print("- G7: no injection configured (no INJECT lines) - audit SKIPPED")
    else:
        # counter deltas: last sample per series minus first sample per
        # series, summed over subtask series (counters are cumulative per job).
        def counter_deltas(path):
            first, last = {}, {}
            try:
                for ln in open(path):
                    parts = ln.split()
                    # Prometheus text format: "<ts> name{labels} value".
                    # HELP/TYPE lines also match the name grep ("# HELP
                    # name (scope: taskmanager_job_task_operator)") — their
                    # last field is not a float; skip them (observed live:
                    # crashed the G7 audit on its first run).
                    if len(parts) < 3 or parts[1].startswith("#"):
                        continue
                    try:
                        val = float(parts[-1])
                    except ValueError:
                        continue
                    name = parts[1]
                    first.setdefault(name, val)
                    last[name] = val
            except OSError:
                pass
            names = set(first) | set(last)
            return {n: last.get(n, 0) - first.get(n, 0) for n in names}

        deltas = counter_deltas(os.path.join(g7_dir, "tm-prom-dedup-late.tsv"))
        dup_delta = sum(v for k, v in deltas.items() if "dedup_duplicates" in k)
        late_delta = sum(v for k, v in deltas.items() if "candles_late_dropped" in k)

        # raw recount with EXACT offset dedupe (the LogScanner re-delivers
        # records; line dedupe would erase the injected duplicates).
        # G7_REUSE_RAW=1: re-analyze the evidence file already in out_dir
        # instead of re-reading Fluss (test hook for the bug-injection
        # proof: tamper a COPY of the evidence and the guards must fire).
        if os.environ.get("G7_REUSE_RAW") == "1":
            raw_path = os.path.join(out_dir, "latency-raw_table_1.rows.txt")
            if not os.path.exists(raw_path):
                failures.append("G7: G7_REUSE_RAW=1 but no evidence file")
                raw_path = os.devnull
        else:
            print("- G7: reading raw_table_1 for full recount (this takes a few "
                  "minutes at ~6M rows)...")
            raw_path = collect_rows_stream("raw_table_1", cp, out_dir,
                                           run_ms=300000, mode="audit")
        if raw_path is None:
            failures.append("G7: raw_table_1 read failed — audit incomplete")
            raw_path = os.devnull
        # last counter sample timestamp: counter deltas only cover rounds
        # that fired BEFORE this moment (observed 2026-08-31: injection
        # round 6 fired after the final Prometheus sample — its frames
        # reached raw but the job's counters never saw them; the analyzer
        # originally misread that as dedup loss).
        last_sample_ms = 0
        try:
            for ln in open(os.path.join(g7_dir, "tm-prom-dedup-late.tsv")):
                try:
                    last_sample_ms = max(last_sample_ms, int(ln.split()[0]) * 1000)
                except ValueError:
                    pass
        except OSError:
            pass
        seen_offsets = set()
        fp_seen = set()
        win_ticks = defaultdict(int)         # (token, window_start) -> ticks
        win_vol = defaultdict(int)           # (token, window_start) -> qty sum
        dup_extras = late_rows = 0
        dup_in_window = late_in_window = 0   # ingested before last counter sample
        dup_ing_max = 0
        LATE_LAG_MS = 20000                  # > watermark(500) + lateness(5000) + margin
        # F8 (2026-08-31): per-token event-time ordering in LOG order. A
        # backward jump beyond LATE_LAG_MS cannot be accepted by the candle
        # window (watermark has passed it) — so every such jump must be one
        # of the late-observed rows. The two are INDEPENDENT measurements
        # (ingest-lag vs prev-event-time) of the same underlying ticks: a
        # bridge that rewrote timestamps would lag small but jump big →
        # mismatch → fire.
        prev_ev = {}                          # token -> last event_time seen
        f8_jumps = 0
        for ln in open(raw_path):
            # audit mode: "b:o\tfp\ttoken\tev\ting\tprice\tqty\tvalidity"
            # (sanitized projection — the full-row toString prints the
            # payload_hash VARBINARY as raw binary; 0x0A bytes split lines
            # and 0x2C bytes shifted comma-split fields, silently dropping
            # ~23% of rows in the first G7 recount, 2026-08-31)
            if "\t" not in ln:
                continue
            try:
                f = ln.rstrip("\n").split("\t")
                if len(f) < 8:
                    continue
                off = f[0]
                if off in seen_offsets:
                    continue  # scanner re-delivery
                seen_offsets.add(off)
                fp = f[1]
                token = int(f[2])
                ev_ms = int(f[3])
                ing_ms = int(f[4])
                qty = int(f[6])
                validity = f[7]
            except (ValueError, IndexError):
                continue
            if token in prev_ev and prev_ev[token] - ev_ms > LATE_LAG_MS:
                f8_jumps += 1
            prev_ev[token] = max(prev_ev.get(token, ev_ms), ev_ms)
            if ing_ms - ev_ms > LATE_LAG_MS:
                late_rows += 1
                if ing_ms <= last_sample_ms:
                    late_in_window += 1
                continue
            if not validity.startswith("VALID"):
                continue  # invalid ticks are quarantined pre-candle
            if run_start and ev_ms < run_start:
                continue  # pre-run history (raw table is not purged)
            if fp in fp_seen:
                # repeat fingerprint = duplicate the dedup operator drops;
                # exclude from the window recount (candles never saw it)
                dup_extras += 1
                if ing_ms <= last_sample_ms:
                    dup_in_window += 1
                continue
            fp_seen.add(fp)
            ws = (ev_ms // 15000) * 15000
            win_ticks[(token, ws)] += 1
            win_vol[(token, ws)] += qty
        print(f"- G7 raw recount: {len(seen_offsets)} logical rows, "
              f"{dup_extras} duplicate extras (repeated fingerprints), "
              f"{late_rows} late rows (event_time lag > {LATE_LAG_MS}ms)")
        print(f"- F8 ordering: {f8_jumps} per-token backward jump(s) beyond "
              f"{LATE_LAG_MS}ms in log order (must equal late rows)")
        if f8_jumps != late_rows:
            failures.append(
                f"F8: {f8_jumps} backward event-time jump(s) beyond "
                f"{LATE_LAG_MS}ms but {late_rows} late-observed rows — "
                f"timestamps and lag disagree (rewritten event times? "
                f"replayed frames?)")

        # final candles per (token, ws): tick_count (idx 10) + volume (idx 9)
        final_by_key = {}
        for ln in final_rows:
            f = ln.strip("()").split(",")
            if len(f) >= 15:
                try:
                    key = (int(f[0]), int(f[3]))
                    final_by_key[key] = (int(f[10]), int(f[9]))
                except ValueError:
                    continue

        # compare fully-closed windows inside the run window
        cutoff_end = (run_end or 0) - 10000
        mismatch = []
        compared = 0
        startup_skipped = 0
        # The SignalJob source starts in LATEST mode (2026-08-29 design:
        # skip the LOG backlog accumulated during job startup, measure only
        # the live path). The first windows after run_start may therefore
        # legitimately have NO candle. G7c tolerates a CONTIGUOUS STARTUP
        # PREFIX of candle-less windows — anything missing after the first
        # present candle is a real gap (observed 2026-08-31: the first G7
        # run misread the startup skip as 47k data-loss mismatches).
        first_candle_window = None
        for key in final_by_key:
            ws = key[1]
            if run_start and ws < run_start:
                continue
            if first_candle_window is None or ws < first_candle_window:
                first_candle_window = ws
        for (token, ws), exp_ticks in sorted(win_ticks.items()):
            wend = ws + 14999
            if run_start and ws < ((run_start + 14999) // 15000) * 15000:
                continue  # window straddles run start - partial raw history
            if wend > cutoff_end:
                continue  # window not fully closed + drained at run end
            key = (token, ws)
            if key not in final_by_key:
                if first_candle_window is None or ws < first_candle_window:
                    startup_skipped += 1
                else:
                    mismatch.append(f"window {ws} token {token}: NO final "
                                    f"candle (expected {exp_ticks} ticks)")
                continue
            fticks, fvol = final_by_key[key]
            compared += 1
            # The first present candle's window may be PARTIAL (the LATEST
            # source entered mid-window); every window after it must match
            # the raw recount exactly.
            if ws == first_candle_window:
                continue
            if fticks != exp_ticks or fvol != win_vol[key]:
                mismatch.append(
                    f"window {ws} token {token}: candle ticks={fticks} "
                    f"vol={fvol} vs raw recount ticks={exp_ticks} "
                    f"vol={win_vol[key]}")

        print(f"- G7a dedup: sent={want_dups} raw-observed-extras={dup_extras} "
              f"counter-delta={dup_delta:.0f} (rounds in counter window: "
              f"{dup_in_window})")
        print(f"- G7b late:  sent={want_late} raw-observed={late_rows} "
              f"counter-delta={late_delta:.0f} (rounds in counter window: "
              f"{late_in_window})")
        print(f"- G7c parity: {compared} fully-closed (token,window) pairs "
              f"compared candle-vs-raw, {len(mismatch)} mismatches")
        if startup_skipped:
            print(f"- G7c note: {startup_skipped} (token,window) pair(s) before "
                  f"the first final candle — LATEST-mode startup skip "
                  f"(documented design), not data loss")
        # counter exactness: the counter must equal the dups that INGESTED
        # before the last counter sample (rounds firing later land in raw
        # but can never reach the counters — the job is torn down).
        if want_dups and dup_delta != dup_in_window:
            failures.append(
                f"G7a: dedup duplicates counter {dup_delta:.0f} != "
                f"{dup_in_window} dups ingested inside the counter sampling "
                f"window - ticks are being lost or double-counted "
                f"downstream of dedup")
        if dup_extras != want_dups:
            failures.append(
                f"G7a: raw table shows {dup_extras} duplicate extras but "
                f"{want_dups} were injected - unexpected duplication "
                f"(bridge resend?) or dropped frames")
        if want_late and late_delta != late_in_window:
            failures.append(
                f"G7b: late-drop counter {late_delta:.0f} != "
                f"{late_in_window} late ticks ingested inside the counter "
                f"sampling window - late ticks are reaching candles "
                f"(wrong data) or being lost before the counter")
        if late_rows != want_late:
            failures.append(
                f"G7b: raw table shows {late_rows} late ticks but "
                f"{want_late} were injected - some late ticks never reached "
                f"raw_table_1 (quarantined? dropped?)")
        if mismatch:
            for msg in mismatch[:10]:
                failures.append(f"G7c: {msg}")
            if len(mismatch) > 10:
                failures.append(f"G7c: ...and {len(mismatch) - 10} more "
                                "window mismatches")
        else:
            print("- G7: data-quality guards passed (dedup exact, late-drop "
                  "exact, tick-set parity exact)")


    if failures:
        print()
        for f in failures:
            print(f"!! {f}")
        print("!! GUARD FAILED — this run FAILS (see failures above)")
        sys.exit(1)
    if not guard_off:
        print("- G6: latency guards passed (p95<1s, no burst seconds, "
              "no tablet read-storm alignment)")


if __name__ == "__main__":
    main()
