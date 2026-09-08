#!/usr/bin/env python3
"""
T0 helper: split a single instrument CSV into N contiguous chunks
(1024+1024+952 for 3000) for the per-slot manifest list.

Usage:
  python3 split_manifest.py --input NSE_CM_EQUITY.csv --out-dir out --prefix slot --chunks 1024,1024,952
  python3 split_manifest.py --input NSE_CM_EQUITY.csv --out-dir . --chunks 1024,1024,952

Preserves header, sorts by Token column if present else row order.
Each output is written as <prefix><i>.csv (1-indexed) and validated to sum to input rows.
"""
import argparse, csv, pathlib, sys

def find_token_idx(header):
    # case-insensitive search for token-like column
    for i, h in enumerate(header):
        if h.strip().lower() in ("token", "instrument_token", "exchtoken", "exch_token"):
            return i
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="input CSV path")
    ap.add_argument("--out-dir", required=True, help="output directory")
    ap.add_argument("--prefix", default="slot", help="output filename prefix (slot -> slot1.csv)")
    ap.add_argument("--chunks", default="1024,1024,952", help="comma-separated chunk sizes, e.g. 1024,1024,952")
    args = ap.parse_args()
    inp = pathlib.Path(args.input)
    out = pathlib.Path(args.out_dir)
    chunks = [int(x) for x in args.chunks.split(",") if x.strip()]
    if not inp.is_file():
        print(f"input not found: {inp}", file=sys.stderr); sys.exit(2)
    out.mkdir(parents=True, exist_ok=True)
    with inp.open(newline="", encoding="utf-8-sig") as f:
        r = csv.reader(f)
        header = next(r, None)
        if header is None:
            print("empty CSV", file=sys.stderr); sys.exit(2)
        rows = list(r)
    total = sum(chunks)
    if len(rows) != total:
        # allow fewer chunks than rows only if user explicitly wants truncation; here we just warn
        print(f"warn: rows={len(rows)} != sum(chunks)={total}; will chunk contiguously by order", file=sys.stderr)
    token_idx = find_token_idx(header)
    if token_idx is not None:
        # P4-244: per-row fallback — one bad token sorts last with a warning
        # instead of silently cancelling the whole sort (was except: pass).
        def sort_key(row):
            if token_idx < len(row) and row[token_idx].strip():
                try:
                    return (0, int(row[token_idx].strip().replace(",", "")))
                except (ValueError, TypeError):
                    print(f"warn: non-numeric token {row[token_idx]!r}; sorting last",
                          file=sys.stderr)
            return (1, 0)
        rows.sort(key=sort_key)
    offset = 0
    for i, size in enumerate(chunks):
        end = min(offset + size, len(rows))
        chunk_rows = rows[offset:end]
        out_path = out / f"{args.prefix}{i+1}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as wf:
            w = csv.writer(wf)
            w.writerow(header)
            w.writerows(chunk_rows)
        print(f"wrote {out_path} rows={len(chunk_rows)} [{offset}:{end})")
        offset = end
        if offset >= len(rows):
            break
    if offset < len(rows):
        # P4-245: dropping instruments (highest tokens, post-sort) must fail
        # the run — a warn+exit-0 goes unnoticed downstream in per-slot lists.
        print(f"error: {len(rows)-offset} rows unassigned after chunks "
              f"(rows={len(rows)} != sum(chunks)={total}); refusing to drop instruments",
              file=sys.stderr)
        sys.exit(1)
    print(f"done: input rows={len(rows)} chunks={chunks}")

if __name__ == "__main__":
    main()
