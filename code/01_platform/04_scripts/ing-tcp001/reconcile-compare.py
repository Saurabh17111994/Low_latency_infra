#!/usr/bin/env python3
"""ING-TCP-001 count-based losslessness reconcile.

Compares bridge emitted-tick counts (ARROW_TICK_COUNTS reports, stderr lines
in the ingestion container logs) against Fluss sink-side per-token row counts
(TokenCountReconcile probe output).

Usage:
  reconcile-compare.py --bridge <file> --pre <file> --post <file> [--exact]
                       [--sink quar|raw|total]

--exact:  require post-pre delta == bridge count per token (single-epoch
          runs, e.g. the market-hours proof).
default:  require delta >= bridge count per token AND no unexpected/vanished
          tokens (multi-epoch validation runs where earlier epochs also wrote
          rows for the same tokens).

--sink:   which Fluss table the ticks should have landed in for this window.
          quar   = ingestion_quarantine (post-close / stale-window runs).
                   A RAW increase over the window is a mismatch in this mode
                   (rows already present before the window are not).
          raw    = raw_table_1 (market-hours runs: fresh ticks).
                   QUAR>0 is tolerated (stale edge ticks), RAW delta compared.
          total  = RAW + QUAR combined (use when rows may land in either).
          default = quar (preserves the original post-close semantics).

Exit 0 = pass, 1 = mismatch.
"""
import argparse
import re
import sys


def parse_bridge(path):
    m = {}
    for line in open(path):
        for t, n in re.findall(r"t=(\d+):n=(\d+)", line):
            m[int(t)] = int(n)
    return m


def parse_probe(path):
    m = {}
    for line in open(path):
        # P6-625: the token field must accept the Fluss sentinel -1 — with `\d+`
        # the row never entered the map, so the `t != -1` guards below never ran.
        mm = re.match(r"TOKEN (-?\d+) RAW=(\d+) QUAR=(\d+) TOTAL=(\d+)", line.strip())
        if mm:
            t, raw, quar, _ = map(int, mm.groups())
            m[t] = (raw, quar)
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bridge", required=True)
    ap.add_argument("--pre", required=True)
    ap.add_argument("--post", required=True)
    ap.add_argument("--exact", action="store_true")
    ap.add_argument("--sink", choices=["quar", "raw", "total"], default="quar")
    args = ap.parse_args()

    bridge = parse_bridge(args.bridge)
    pre = parse_probe(args.pre)
    post = parse_probe(args.post)
    print(f"bridge tokens: {len(bridge)}  bridge total: {sum(bridge.values())}")
    print(f"pre tokens: {len(pre)}  post tokens: {len(post)}  sink={args.sink}")

    # Fail closed: an empty/truncated counter file must never reconcile as a
    # pass — zero bridge emissions would vacuously match an empty sink (the
    # 'no evidence is not evidence of no loss' trap), and an empty PRE baseline
    # turns every delta into post-0, so a real loss still clears the >=
    # comparison. A missing file already crashes the parse; a truncated one
    # yields {} and must exit 1 here.
    if not bridge:
        print("MISMATCH bridge file empty or truncated — no emitted-tick counts to reconcile")
        return 1
    if not post:
        print("MISMATCH post probe file empty or truncated — no sink rows to reconcile")
        return 1
    if not pre:
        print("MISMATCH pre probe file empty or truncated — no baseline to diff against")
        return 1

    def sink_value(entry):
        raw, quar = entry
        return quar if args.sink == "quar" else raw if args.sink == "raw" else raw + quar

    bad = []
    for t in bridge:
        d = sink_value(post.get(t, (0, 0))) - sink_value(pre.get(t, (0, 0)))
        if (d < bridge[t]) if not args.exact else (d != bridge[t]):
            bad.append((t, bridge[t], d))
    vanished = set(pre) - set(post)
    extra = [t for t in post if t not in bridge and t != -1]
    # A RAW *increase* over the window is only unexpected when the chosen sink
    # is quarantine (post-close runs): rows already there before the window are
    # not this window's mismatch (a reused cluster keeps earlier runs' rows),
    # and at market hours fresh ticks belong in raw_table_1 by design.
    # P6-625: exclude the sentinel here exactly as `extra` does — now that -1 is
    # parsed, leaving it in would report the sentinel itself as a raw-row mismatch.
    raw_nonzero = (
        [t for t, (r, _) in post.items() if t != -1 and r != pre.get(t, (0, 0))[0]]
        if args.sink == "quar"
        else []
    )

    for t, want, got in bad[:20]:
        print(f"MISMATCH token={t} bridge={want} sink_delta={got}")
    if vanished:
        print(f"MISMATCH vanished tokens: {sorted(vanished)[:20]}")
    if extra:
        print(f"MISMATCH unexpected sink tokens: {sorted(extra)[:20]}")
    if raw_nonzero:
        print(f"MISMATCH tokens with new RAW rows (RAW>0 since pre): {sorted(raw_nonzero)[:20]}")

    ok = not bad and not vanished and not extra and not raw_nonzero
    print("RESULT " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
