#!/usr/bin/env python3
"""evidence_ownership_check.py — CI gate for the non-root ownership contract.

Every evidence record the ddl-apply CONTAINER writes must be GROUP-WRITABLE and
owned by the engine user's group: the entrypoint wrapper claims the evidence
root for the non-root engine user (uid/gid DDL_APPLY_UID/GID, default 10001)
with setgid 2775 + umask 002, so records land 664 (gid == the engine group) and
host automation in the shared group can read AND manage them. This gate fails
on any violation of that contract:

  EVIDENCE ROOT DIR (owner-scoped, not record-scoped: a root-owned evidence
  root fails even when every record inside it is host-owned, because the root's
  ownership is produced by the container's own repair step — root here means a
  container run escaped the contract and the next write would inherit it;
  P6-726):
    * owner == root (0)        -> FAIL (the wrapper's ownership repair never
                                   ran; the container ran as root)
    * owner == engine uid but the dir lacks setgid or group-write (expected
      2775)                   -> FAIL (descendants would not inherit the group)
    * owner == any other uid   -> out of scope (host-side run)

  RECORDS (apply.json):
    * owner == root (0)        -> FAIL (the container ran as root)
    * owner == engine uid but group != engine GID  -> FAIL (host automation in
      the shared group could not manage the record)
    * owner == engine uid but NOT group-writable   -> FAIL (umask/setgid repair
      regressed)

Records owned by any OTHER uid (host-side `make ddl` runs, which write with the
host user's own umask) are OUT OF SCOPE — that path is not governed by the
container non-root contract. Detection is by owner uid (the definitive
container-written marker).

Run:  python3 code/01_platform/04_scripts/evidence_ownership_check.py
Wired into: make docs-audit (C15), make evidence-ownership-check, the Monday
verification gate's DDL step, and the ddl-apply image (`evidence-check`
subcommand + auto-run at the end of every `apply`).
"""

import os
import sys

REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
EVIDENCE_DIR = os.environ.get("DDL_APPLY_EVIDENCE_DIR") or os.path.join(
    REPO_ROOT, "logs", "ddl-apply")


def _uid_env(name, default):
    """Read a uid/gid override; refuse a non-numeric value by name.

    int() at import time turned a typo into an uncaught ValueError traceback,
    which aborts the gate for a reason unrelated to ownership (P6-725). An
    absent or blank value keeps the documented default.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        # exit 2 = "did not run", distinct from main()'s 1 = findings
        print(f"evidence-ownership-check: FATAL — {name}='{raw}' is not an "
              f"integer (expected a uid/gid number, e.g. {default})",
              file=sys.stderr)
        sys.exit(2)


CONTAINER_UID = _uid_env("DDL_APPLY_UID", 10001)
CONTAINER_GID = _uid_env("DDL_APPLY_GID", 10001)

SETGID = 0o2000   # mode & SETGID -> setgid bit (descendants inherit the group)
GROUP_WRITE = 0o020  # mode & GROUP_WRITE -> group-writable


def _dir_problem(evidence_dir, st, container_uid):
    """Evidence-root-dir contract violations, or None. Scoped by owner uid:
    only root and the engine uid are governed. Root fails on its own, whatever
    the records inside say (P6-726)."""
    if st.st_uid == 0:
        return (f"{evidence_dir}: root-owned (uid 0) — the wrapper's ownership "
                "repair never ran; the container ran as root")
    if st.st_uid == container_uid:
        if not (st.st_mode & SETGID) or not (st.st_mode & GROUP_WRITE):
            return (f"{evidence_dir}: evidence root missing setgid+group-write "
                    f"(mode {oct(st.st_mode & 0o7777)}) — expected 2775 so "
                    "descendants inherit the shared group")
    return None


def check_evidence_dir(evidence_dir, container_uid, container_gid=CONTAINER_GID,
                       stat_fn=os.stat):
    """Scan the evidence corpus for ownership-contract violations.

    stat_fn is injectable for unit tests (real ownership can't be faked without
    root). Returns (problems, (checked, container_written, host_owned)).
    A missing corpus is a vacuous pass (nothing container-written to check).
    """
    problems = []
    checked = container_written = host_owned = 0
    if not os.path.isdir(evidence_dir):
        return problems, (checked, container_written, host_owned)
    try:
        dst = stat_fn(evidence_dir)
    except OSError as exc:
        problems.append(f"{evidence_dir}: cannot stat ({exc})")
    else:
        p = _dir_problem(evidence_dir, dst, container_uid)
        if p:
            problems.append(p)
    def _walk_error(exc):
        # os.walk swallows read errors by default, so an unreadable subtree
        # would hide every record inside it and the gate would still PASS
        # (P6-367). Fail closed instead.
        problems.append(f"{exc.filename or evidence_dir}: cannot read ({exc})")

    for root, _, files in os.walk(evidence_dir, onerror=_walk_error):
        for name in files:
            if name != "apply.json":
                continue
            path = os.path.join(root, name)
            try:
                st = stat_fn(path)
            except OSError as exc:
                problems.append(f"{path}: cannot stat ({exc})")
                continue
            checked += 1
            if st.st_uid == 0:
                problems.append(
                    f"{path}: root-owned (uid 0) — the container ran as root; "
                    "the non-root ownership contract is broken")
            elif st.st_uid == container_uid:
                container_written += 1
                if st.st_gid != container_gid:
                    problems.append(
                        f"{path}: container-written (uid {container_uid}) but "
                        f"group {st.st_gid} != engine GID {container_gid} — "
                        "host automation in the shared group cannot manage it")
                if not (st.st_mode & GROUP_WRITE):
                    problems.append(
                        f"{path}: container-written (uid {container_uid}) but NOT "
                        f"group-writable (mode {oct(st.st_mode & 0o777)}) — expected "
                        "664 (setgid 2775 evidence root + umask 002)")
            else:
                host_owned += 1
    return problems, (checked, container_written, host_owned)


def main():
    problems, (checked, cw, host) = check_evidence_dir(EVIDENCE_DIR, CONTAINER_UID)
    print(f"evidence-ownership-check: {EVIDENCE_DIR} — {checked} record(s), "
          f"{cw} container-written, {host} host-owned (out of scope)")
    if problems:
        for p in problems:
            print("  FAIL: " + p)
        print("evidence-ownership-check: FAIL — " + str(len(problems)) +
              " violation(s); remediate with, e.g.:")
        print("  docker run --rm -v \"$PWD/logs:/logs\" --entrypoint bash "
              "01_docker-ddl-apply -c 'chmod 2775 /logs/ddl-apply && "
              "find /logs/ddl-apply -name apply.json -exec chmod g+w {} +'")
        print("  (group mismatches: chgrp -R 10001 /logs/ddl-apply)")
        print("  (root-owned dir/records: chmod cannot change an owner — "
              "chown -R 10001:10001 /logs/ddl-apply, e.g. docker run --rm "
              "-v \"$PWD/logs:/logs\" --entrypoint bash 01_docker-ddl-apply "
              "-c 'chown -R 10001:10001 /logs/ddl-apply')")
        return 1
    print("evidence-ownership-check: PASS — evidence root setgid+group-writable, "
          "every container-written record group-writable with the engine GID")
    return 0


if __name__ == "__main__":
    sys.exit(main())
