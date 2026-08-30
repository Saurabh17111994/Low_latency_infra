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
        try (Connection c = ConnectionFactory.createConnection(conf);
             Table t = c.getTable(tp);
             LogScanner scanner = t.newScan().createLogScanner()) {
            int buckets = t.getTableInfo().getNumBuckets();
            for (int b = 0; b < buckets; b++) scanner.subscribe(b, 0L);
            long deadline = System.currentTimeMillis() + runMs;
            while (System.currentTimeMillis() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (var r : records) System.out.println(r.getRow().toString());
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


def collect_rows(table, cp, out_dir, run_ms=30000):
    """Compile (once) and run the LOG reader; return parsed rows."""
    java_file = os.path.join(out_dir, "LogFullRead.java")
    if not os.path.exists(os.path.join(out_dir, "LogFullRead.class")):
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
         "-cp", f"{out_dir}:{cp}", "LogFullRead", table, str(run_ms)],
        capture_output=True, text=True, timeout=run_ms / 1000 + 90,
    )
    rows = [ln for ln in r.stdout.splitlines() if ln.strip()]
    # Never overwrite saved evidence with an empty read: re-analyzing an old
    # out_dir after its table was purged/recreated returns 0 live rows — the
    # old 'w' mode destroyed the original rows file (observed 2026-08-30:
    # runs 140907 and 173514 lost their evidence this way).
    rows_path = os.path.join(out_dir, f"latency-{table}.rows.txt")
    if rows or not os.path.exists(rows_path):
        with open(rows_path, "w") as f:
            f.write("\n".join(rows) + "\n")
    return rows


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
    sig_rows = collect_rows("Signal_Candidates", cp, out_dir)
    # DEDUPE: same LogScanner re-delivery (see preview note above).
    sig_rows = list(dict.fromkeys(sig_rows))
    status_counts = defaultdict(int)
    settle_by_status = defaultdict(int)
    candle_re = re.compile(r"candle:(\d{13}):(\d{13})")
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
    print("\n(Per-operator wall-clock latency: Flink latency histograms in "
          "main/latency-metrics.tsv; event-time row stamps cannot measure it.)")

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
    failures = []

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

    if failures:
        print()
        for f in failures:
            print(f"!! {f}")
        print("!! G6 GUARD FAILED — latency regression: this run FAILS")
        sys.exit(1)
    if not guard_off:
        print("- G6: latency guards passed (p95<1s, no burst seconds, "
              "no tablet read-storm alignment)")


if __name__ == "__main__":
    main()
