"""Ledger keeper for docs/ENVIRONMENT.md (the verified-facts ledger).

Five small subcommands; stdlib only; no daemon, no network, no database:

  add      append a new LIVE FACT row (next id assigned automatically)
  retire   flip a LIVE row to DEAD (fact is wrong or stale)
  replace  retire + add in one step, with the supersede link filled both ways
  check    run every LIVE row's one-line `Check:` command, report drift
  prune    move DEAD rows out of the ledger into docs/ENVIRONMENT.archive.md

Every mutation is fail-closed: read the file, transform in memory, verify
anchors, then write via temp-file + atomic replace (a `.bak` is kept only
until the write succeeds). A fact added by hand (not via this CLI) still
works as long as it follows the skeleton in the ledger's "How to add" block.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
LEDGER = os.path.join(ROOT, "docs", "ENVIRONMENT.md")
ARCHIVE = os.path.join(ROOT, "docs", "ENVIRONMENT.archive.md")

HEADER_RE = re.compile(r"^### (FACT-(\d+)): (.+)$")
FIELD_RE = re.compile(r"^(Status|Verified|Check|Recheck when): (.*)$")
REQUIRED_FIELDS = ("Status", "Verified", "Check", "Recheck when")
VALID_STATUS = ("LIVE", "DEAD")
CHECK_TIMEOUT_S = 15


def die(msg):
    print(f"env_facts: error: {msg}", file=sys.stderr)
    return 1


def read_ledger(path=None):
    if path is None:  # resolved at call time, not def time (tests re-point LEDGER)
        path = LEDGER
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError as exc:
        return None


def parse_ledger(text):
    """Split ledger text into (preamble, rows, errors).

    rows: list of dicts {id, num, title, fields:{}, body, start, end}
    (start/end are line spans of the whole row, header included).
    errors: list of human-readable shape violations (empty == clean).
    """
    lines = text.splitlines()
    # Row headers only live under the "## Ledger" section.
    try:
        ledger_at = next(
            i for i, ln in enumerate(lines) if ln.strip() == "## Ledger")
    except StopIteration:
        return text, [], ["missing '## Ledger' section"]
    preamble = "\n".join(lines[:ledger_at + 1])
    rows, errors = [], []
    cur = None

    def close(cur):
        for f in REQUIRED_FIELDS:
            if f not in cur["fields"]:
                errors.append(f"{cur['id']}: missing '{f}:' line")
        st = cur["fields"].get("Status", "")
        if st not in VALID_STATUS and not st.startswith("DEAD "):
            errors.append(f"{cur['id']}: bad Status {st!r} (want LIVE or DEAD ...)")
        rows.append(cur)

    for idx in range(ledger_at + 1, len(lines)):
        ln = lines[idx]
        m = HEADER_RE.match(ln)
        if m and ln.startswith("### FACT-"):
            if cur is not None:
                close(cur)
            cur = {"id": m.group(1), "num": int(m.group(2)),
                   "title": m.group(3).strip(), "fields": {},
                   "body": [], "start": idx}
        elif cur is not None:
            fm = FIELD_RE.match(ln)
            if fm and fm.group(1) not in cur["fields"]:
                # A field line always wins on first sight, even after
                # continuation lines - Verified:/Check: values often wrap.
                cur["fields"][fm.group(1)] = fm.group(2).strip()
            else:
                cur["body"].append(ln)
    if cur is not None:
        close(cur)
    # Id hygiene: unique, sequential from 001.
    seen = set()
    for pos, r in enumerate(rows, start=1):
        if r["id"] in seen:
            errors.append(f"{r['id']}: duplicate id")
        seen.add(r["id"])
        if r["num"] != pos:
            errors.append(f"{r['id']}: want sequential id FACT-{pos:03d}")
        r["end"] = (rows[rows.index(r) + 1]["start"]
                    if rows.index(r) + 1 < len(rows) else len(lines))
    for r in rows:  # DEAD linkage must point somewhere real.
        st = r["fields"].get("Status", "")
        m = re.search(r"superseded by (FACT-\d+)", st)
        if m and m.group(1) not in seen:
            errors.append(f"{r['id']}: supersede target {m.group(1)} not in ledger")
    return preamble, rows, errors


def render_row(num, title, status, verified, check, recheck, body):
    return (f"### FACT-{num:03d}: {title}\n"
            f"Status: {status}\n"
            f"Verified: {verified}\n"
            f"Check: {check}\n"
            f"Recheck when: {recheck}\n"
            f"{body.rstrip()}\n")


def atomic_write(path, text):
    """Fail-closed write: temp file in same dir, then atomic replace."""
    bak = path + ".bak"
    shutil.copy2(path, bak)
    try:
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except BaseException:
        os.replace(bak, path)  # restore on any failure, then re-raise
        raise
    os.remove(bak)


def cmd_add(ns):
    text = read_ledger()
    if text is None:
        return die(f"cannot read {LEDGER}")
    _, rows, errors = parse_ledger(text)
    if errors:
        return die("ledger has shape errors - fix first:\n  " + "\n  ".join(errors))
    num = (max(r["num"] for r in rows) + 1) if rows else 1
    row = render_row(num, ns.title, "LIVE", ns.verified, ns.check,
                     ns.recheck, ns.body)
    if not text.endswith("\n"):
        text += "\n"
    atomic_write(LEDGER, text + "\n" + row)
    print(f"added FACT-{num:03d}: {ns.title}")
    return 0


def cmd_retire(ns):
    text = read_ledger()
    if text is None:
        return die(f"cannot read {LEDGER}")
    _, rows, errors = parse_ledger(text)
    if errors:
        return die("ledger has shape errors - fix first:\n  " + "\n  ".join(errors))
    hit = [r for r in rows if r["id"] == ns.fact]
    if len(hit) != 1:
        return die(f"{ns.fact}: found {len(hit)} rows (want exactly 1)")
    if hit[0]["fields"].get("Status") != "LIVE":
        return die(f"{ns.fact} is not LIVE - nothing to retire")
    anchor = f"### {ns.fact}: "
    assert text.count(anchor) == 1, "row header anchor is not unique"
    old_status = "Status: LIVE\n"
    assert text.count(old_status) >= 1
    # Replace only the Status line inside this row's span.
    lines = text.splitlines(keepends=True)
    span = lines[hit[0]["start"]:hit[0]["end"]]
    joined = "".join(span)
    assert joined.count("Status: LIVE\n") == 1, "status anchor not unique in row"
    new_span = joined.replace(
        "Status: LIVE\n", f"Status: DEAD ({ns.reason})\n", 1)
    lines[hit[0]["start"]:hit[0]["end"]] = [new_span]
    atomic_write(LEDGER, "".join(lines))
    print(f"retired {ns.fact}: {ns.reason}")
    return 0


def cmd_replace(ns):
    rc = cmd_retire(argparse.Namespace(fact=ns.fact,
                                       reason=f"superseded by next row: {ns.title}"))
    if rc:
        return rc
    return cmd_add(ns)


def cmd_check(ns):
    text = read_ledger()
    if text is None:
        return die(f"cannot read {LEDGER}")
    _, rows, errors = parse_ledger(text)
    if errors:
        print("shape errors:\n  " + "\n  ".join(errors))
        return 2
    live = [r for r in rows if r["fields"].get("Status") == "LIVE"]
    drift, ran = [], 0
    for r in live:
        chk = r["fields"].get("Check", "").strip()
        if len(chk) >= 2 and chk.startswith("`") and chk.endswith("`"):
            chk = chk[1:-1].strip()  # hand-written `code` formatting
        if chk.lower().startswith("manual"):
            print(f"{r['id']}: SKIP (manual: {chk})")
            continue
        ran += 1
        try:
            p = subprocess.run(["bash", "-c", chk], capture_output=True,
                               text=True, timeout=CHECK_TIMEOUT_S)
            if p.returncode == 0:
                print(f"{r['id']}: OK")
            else:
                print(f"{r['id']}: DRIFT (rc={p.returncode}): {chk}")
                drift.append(r["id"])
        except (OSError, subprocess.TimeoutExpired) as exc:
            print(f"{r['id']}: DRIFT ({exc}): {chk}")
            drift.append(r["id"])
    print(f"check: {ran} ran, {len(drift)} drift, "
          f"{len(live) - ran} manual-skip, {len(rows) - len(live)} dead-ignored")
    return 1 if drift else 0


def cmd_prune(ns):
    text = read_ledger()
    if text is None:
        return die(f"cannot read {LEDGER}")
    _, rows, errors = parse_ledger(text)
    if errors:
        return die("ledger has shape errors - fix first:\n  " + "\n  ".join(errors))
    dead = [r for r in rows if r["fields"].get("Status") != "LIVE"]
    if not dead:
        print("prune: no DEAD rows - ledger is already lean")
        return 0
    lines = text.splitlines(keepends=True)
    moved = "".join("".join(lines[r["start"]:r["end"]]) for r in dead)
    keep = [ln for i, ln in enumerate(lines)
            if not any(r["start"] <= i < r["end"] for r in dead)]
    atomic_write(LEDGER, "".join(keep).rstrip() + "\n")
    arch = read_ledger(ARCHIVE)
    head = ("# Environment Facts Archive\n\nDEAD rows pruned from "
            "docs/ENVIRONMENT.md. History only - do not treat as current.\n\n")
    atomic_write(ARCHIVE, (arch or head) + moved) if arch else _write_new(ARCHIVE, head + moved)
    print(f"prune: moved {len(dead)} DEAD row(s) to docs/ENVIRONMENT.archive.md")
    return 0


def _write_new(path, text):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def main(argv=None):
    ap = argparse.ArgumentParser(description="keeper for docs/ENVIRONMENT.md")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def fact_args(p):
        p.add_argument("--title", required=True)
        p.add_argument("--verified", required=True,
                       help="date + how, e.g. '2026-09-16 - docker info shows ...'")
        p.add_argument("--check", required=True,
                       help="one-line re-verification command, or 'manual (...)'")
        p.add_argument("--recheck", required=True,
                       help="the event that kills this fact")
        p.add_argument("--body", required=True, help="1-3 lines of claim detail")

    p_add = sub.add_parser("add", help="append a new LIVE fact")
    fact_args(p_add)
    p_add.set_defaults(fn=cmd_add)

    p_ret = sub.add_parser("retire", help="flip a LIVE fact to DEAD")
    p_ret.add_argument("fact", help="e.g. FACT-003")
    p_ret.add_argument("--reason", required=True,
                       help="why it died, e.g. 'superseded by FACT-011' or 'host rebuilt'")
    p_ret.set_defaults(fn=cmd_retire)

    p_rep = sub.add_parser("replace", help="retire a fact and add its successor")
    p_rep.add_argument("fact", help="e.g. FACT-003")
    fact_args(p_rep)
    p_rep.set_defaults(fn=cmd_replace)

    p_chk = sub.add_parser("check", help="run LIVE checks, report drift")
    p_chk.set_defaults(fn=cmd_check)

    p_pru = sub.add_parser("prune", help="archive DEAD rows out of the ledger")
    p_pru.set_defaults(fn=cmd_prune)

    ns = ap.parse_args(argv)
    return ns.fn(ns)


if __name__ == "__main__":
    sys.exit(main())
