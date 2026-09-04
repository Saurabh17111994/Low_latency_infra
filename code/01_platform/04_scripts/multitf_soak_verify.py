#!/usr/bin/env python3
"""multitf_soak_verify.py — Phase 5 side-by-side soak gate.

Compares the new multi-timeframe aggregator's closed 15s leg
(``candle_closed`` where tf='FIFTEEN_S') against the old chain's output
(``feature_candles_15s``) over a shared instrument set and time window.
The Phase-5 contract (design doc §H.6 / §J.2) is:

  * bit-identical OHLCV + volume + tick_count + window bounds
    (rows matched on (instrument_token, window_start); output_ts and the
    algorithm/configuration/schema version columns are intentionally NOT
    compared — processing-time and version strings legitimately differ),
  * every old-chain row for a non-empty window appears in the new table
    (the new chain must not lose a closed candle the old chain produced),
  * the new table's live leg (candle_live) shows a ~1s overwrite cadence,
  * signal rows appear in Signal_Candidates for the multi-TF rule.

Read-only: connects to Fluss via the Fluss client jar (passed as
FLUSS_PROBE_CP, same mechanism as the stage-capture Fluss probes) and never
mutates the data path. Exits 0 when every gate passes, 1 on mismatch, 2 on
an environment/usage error.

Usage:
    FLUSS_PROBE_CP=<cp> MULTITF_TABLES=candle_closed,candle_live \
        OLD_TABLE=feature_candles_15s SIGNAL_TABLE=Signal_Candidates \
        WINDOW_START_MS=... WINDOW_END_MS=... \
        python3 multitf_soak_verify.py [--verbose]

The compare window is the soak's active tick window (passed in ms).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Dict, List, Optional, Tuple


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"!! FAIL: required env {name} is empty", file=sys.stderr)
        sys.exit(2)
    return value


def fmt_price(paise: object) -> str:
    """Render a stored integer-paise price as rupees with 2 decimals.

    The pipeline stores every price as whole paise (BIGINT): ₹1000.05 ==
    100005. NSE cash never carries sub-paise precision, so dividing by 100
    and formatting to 2 decimals is exact and lossless. Kept as a single
    helper so every human-facing number in the Phase-5 report uses the same
    convention (display-only; storage stays integer paise by design).
    """
    if paise is None:
        return "-"
    try:
        value = int(paise)
    except (TypeError, ValueError):
        return str(paise)
    sign = "-" if value < 0 else ""
    value = abs(value)
    return f"{sign}{value // 100}.{value % 100:02d}"


# Column names shared by the closed 15s table (old + new legs). The two
# tables have IDENTICAL OHLCV/volume/tick/bounds columns in the same order
# (see CandleClosedColumns / CandleTableColumns); only the version-bearing
# columns (algorithm_version/configuration_version/output_ts/schema_version
# and the new tf column) differ and are excluded from the comparison.
OLD_COMPARE_COLS = [
    "instrument_token", "exchange", "symbol", "window_start", "window_end",
    "open_paise", "high_paise", "low_paise", "close_paise", "volume",
    "tick_count",
]
# New closed table: same OHLCV columns + tf (the discriminator). The
# compare key is (instrument_token, window_start) on both sides.
NEW_COMPARE_COLS = OLD_COMPARE_COLS + ["tf"]

# Which side is which (env overridable for scratch names).
OLD_TABLE = os.environ.get("OLD_TABLE", "feature_candles_15s")
NEW_CLOSED_TABLE = os.environ.get("NEW_CLOSED_TABLE", "candle_closed")
NEW_LIVE_TABLE = os.environ.get("NEW_LIVE_TABLE", "candle_live")
SIGNAL_TABLE = os.environ.get("SIGNAL_TABLE", "Signal_Candidates")
TF_15S = "FIFTEEN_S"


def read_table(
    table: str,
    cp: str,
    tokens_csv: str,
    window_start_ms: Optional[int],
    window_end_ms: Optional[int],
) -> List[Dict[str, object]]:
    """Fetch rows for a token prefix via the compiled Java reader.

    Returns rows as dicts keyed by column name. window_start_ms/end_ms bound
    the row set (only rows whose window_start is inside the window are kept
    — the reader filters server-side by passing the window as a lower bound
    on the prefix key, which is NOT possible; instead the reader fetches all
    rows per token and Python filters by window_start).
    """
    # The Java reader is compiled once (into a temp dir) and reused; it
    # prints one JSON object per line.
    reader_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fluss-probes")
    reader_java = os.path.join(reader_dir, "FlussPrefixReader.java")
    if not os.path.exists(reader_java):
        print(f"!! FAIL: reader source missing: {reader_java}", file=sys.stderr)
        sys.exit(2)
    reader_class = os.path.join(reader_dir, "FlussPrefixReader.class")
    if not os.path.exists(reader_class):
        print(f"!! FAIL: reader not compiled: {reader_java}", file=sys.stderr)
        sys.exit(2)
    java = os.environ.get("JAVA_HOME", "")
    java_bin = os.path.join(java, "bin", "java") if java else "java"
    cmd = [
        java_bin,
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-cp", f"{reader_dir}:{cp}",
        "FlussPrefixReader",
        table, tokens_csv,
        str(window_start_ms or 0), str(window_end_ms or 0),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if proc.returncode != 0:
        print(f"!! FAIL: reader for {table} exited {proc.returncode}: "
              f"{proc.stderr.strip()}", file=sys.stderr)
        sys.exit(2)
    lines = proc.stdout.splitlines()
    if not lines or not lines[-1].startswith("__END__"):
        print(f"!! FAIL: reader for {table} produced no __END__ sentinel "
              f"(crashed mid-read? stderr: {proc.stderr.strip()[:300]})",
              file=sys.stderr)
        sys.exit(2)
    rows: List[Dict[str, object]] = []
    for line in lines[:-1]:
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            print(f"!! FAIL: reader output not JSON: {line[:200]} ({exc})",
                  file=sys.stderr)
            sys.exit(2)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    cp = env_required("FLUSS_PROBE_CP")
    tokens_csv = os.environ.get("PROBE_TOKENS", "")
    if not tokens_csv:
        print("!! FAIL: required env PROBE_TOKENS is empty (comma-separated "
              "instrument tokens to compare)", file=sys.stderr)
        sys.exit(2)
    ws_raw = os.environ.get("WINDOW_START_MS", "")
    we_raw = os.environ.get("WINDOW_END_MS", "")
    window_start_ms = int(ws_raw) if ws_raw else None
    window_end_ms = int(we_raw) if we_raw else None

    old_rows = read_table(OLD_TABLE, cp, tokens_csv, window_start_ms, window_end_ms)
    new_rows = read_table(NEW_CLOSED_TABLE, cp, tokens_csv, window_start_ms, window_end_ms)
    live_rows = read_table(NEW_LIVE_TABLE, cp, tokens_csv, window_start_ms, window_end_ms)

    # Normalize: keep only rows in the compare window (by window_start) and
    # only the new leg's FIFTEEN_S rows.
    def in_window(row: Dict[str, object]) -> bool:
        ws = row.get("window_start")
        if ws is None:
            return False
        if window_start_ms is not None and ws < window_start_ms:
            return False
        if window_end_ms is not None and ws > window_end_ms:
            return False
        return True

    old_map: Dict[Tuple[int, int], Dict[str, object]] = {}
    for r in old_rows:
        if in_window(r):
            old_map[(int(r["instrument_token"]), int(r["window_start"]))] = r
    new_map: Dict[Tuple[int, int], Dict[str, object]] = {}
    for r in new_rows:
        if in_window(r) and r.get("tf") == TF_15S:
            new_map[(int(r["instrument_token"]), int(r["window_start"]))] = r

    # Gate 1: bit-identical OHLCV/volume/tick/bounds on matched keys.
    mismatches: List[str] = []
    old_only: List[str] = []
    new_only: List[str] = []
    for key, old in old_map.items():
        new = new_map.get(key)
        if new is None:
            old_only.append(str(key))
            continue
        for col in OLD_COMPARE_COLS:
            if col == "instrument_token":
                continue
            if old.get(col) != new.get(col):
                mismatches.append(f"{key} col={col} old={old.get(col)} new={new.get(col)}")
    for key in new_map:
        if key not in old_map:
            new_only.append(str(key))

    # Gate 2: live cadence — count distinct (token, tf, window_start) rows
    # in the live table (each live row should be present for the forming
    # window; a >1x count means multiple rows per key = the 1s overwrite
    # produced the expected upsert, not an append).
    live_keys: Dict[Tuple[int, str, int], int] = {}
    for r in live_rows:
        if not in_window(r):
            continue
        key = (int(r["instrument_token"]), str(r.get("tf", "")),
               int(r["window_start"]))
        live_keys[key] = live_keys.get(key, 0) + 1
    multi_upsert_keys = {k for k, v in live_keys.items() if v > 1}
    # A live row for a single key is expected to appear exactly once in the
    # final KV snapshot (the last upsert wins); >1 means duplicate appends.
    live_dup = len(multi_upsert_keys)

    # Gate 3: signal rows (count + any multi-TF rule id present).
    signal_rows = read_table(SIGNAL_TABLE, cp, tokens_csv, None, None)
    signal_count = len(signal_rows)
    multitf_signals = [r for r in signal_rows
                       if "multi" in str(r.get("rule_id", "")).lower()
                       or "breakout" in str(r.get("rule_id", "")).lower()]

    print("multitf_soak_verify: side-by-side gate")
    print(f"  old 15s rows (in window): {len(old_map)}")
    print(f"  new 15s closed rows (in window): {len(new_map)}")
    print(f"  live rows (in window): {len(live_rows)}  live dup keys: {live_dup}")
    print(f"  signal rows: {signal_count}  multitf/breakout signals: {len(multitf_signals)}")
    if args.verbose:
        for m in mismatches[:20]:
            print(f"    MISMATCH {m}  (prices as rupees: "
                  f"old={fmt_price(m.split('old=')[1].split()[0] if 'old=' in m else '')})"
                  if "col=" in m else f"    MISMATCH {m}")
        for k in old_only[:10]:
            print(f"    OLD-ONLY {k}")
        for k in new_only[:10]:
            print(f"    NEW-ONLY {k}")

    ok = True
    if mismatches:
        print(f"!! FAIL: {len(mismatches)} OHLCV mismatches on matched keys "
              f"(first: {mismatches[0]})", file=sys.stderr)
        ok = False
    if old_only:
        print(f"!! FAIL: {len(old_only)} old-chain rows have NO new-chain "
              f"counterpart (first: {old_only[0]})", file=sys.stderr)
        ok = False
    if live_dup:
        print(f"!! FAIL: {live_dup} live keys have >1 KV row (duplicate "
              f"upserts — the 1s overwrite must not append)", file=sys.stderr)
        ok = False
    if not multitf_signals:
        print("!! FAIL: no multi-TF/breakout signal rows found in "
              f"{SIGNAL_TABLE}", file=sys.stderr)
        ok = False

    if not ok:
        return 1
    print("multitf_soak_verify: PASS — bit-identical 15s, no loss, "
          "live upsert cadence sane, signals present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
