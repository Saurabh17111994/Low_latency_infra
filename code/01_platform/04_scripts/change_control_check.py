#!/usr/bin/env python3
"""change_control_check.py — change-control reconciliation validator.

01-foundation.md "Change control" (orig L205): a change to an active decision,
requirement, DDL/schema, identity or event contract, Flink state/checkpoint
contract, broker/Arrow REST protocol adapter, execution gate or approval
behavior, retention/offload policy, or deployment topology/secret scope
requires a reconciliation review. The change record MUST identify:

    affected_artifacts   what files/schemas/contracts the change touches
    compatibility_class  one of the CompatibilityClass vocabulary
    savepoint_impact     state/savepoint/checkpoint effect
    test_updates         which tests are added or changed
    rollback_behavior    rollback path and state-readability
    plan_tasks           plan/tracker task references

This script validates every change record under docs/05_deployment/
change-records/ (one .md file per record, fields in a ```text fenced block,
`key: value` lines). It is also wired into docs-audit as C14.

Beyond field completeness, `plan_tasks` and `affected_artifacts` references
are resolved against the repository: `tracker-<n>` must match a dossier
docs/08_implementation/<n>-*.md; every `.md` path in the value must resolve to
an existing file (repo relative, change-record-dir relative, or a bare dossier
name under docs/08_implementation/ or docs/); every path-shaped artifact token
(known extension) must resolve the same way or by repo-wide basename search
(pruning target/.git/node_modules). A value of `none`/`N/A`/`-` needs no
reference. Reconciliation records cannot cite phantom tasks or artifacts.

A record may legitimately name an artifact that has since been deleted — the
record was accurate when filed, and rewriting it would erase what the change
touched. Such a token is accepted when immediately followed by
`(retired by <sha>)`, and only when that commit really removed the path (present
in its parent, absent in it, which also means a rename does not qualify: point
at the live path instead). The claim is therefore evidence, not a silencer.

Usage:
    python3 code/01_platform/04_scripts/change_control_check.py
    python3 code/01_platform/04_scripts/change_control_check.py --dir <dir>

Exit code: 0 = every change record names all required fields and its
plan_tasks references resolve; 1 otherwise (or the records directory is
missing entirely).
"""

import glob
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
DEFAULT_RECORDS_DIR = os.path.join(ROOT, "docs", "05_deployment", "change-records")

# The six required fields from 01-foundation.md "Change control" (orig L205).
REQUIRED_FIELDS = [
    "affected_artifacts",
    "compatibility_class",
    "savepoint_impact",
    "test_updates",
    "rollback_behavior",
    "plan_tasks",
]

# 01-foundation.md "Compatibility classifications" — the CompatibilityClass
# enum vocabulary (COMPATIBLE / COMPATIBLE_WITH_LIMITATION / INCOMPATIBLE /
# UNKNOWN / NOT_APPLICABLE).
COMPATIBILITY_CLASSES = {
    "COMPATIBLE",
    "COMPATIBLE_WITH_LIMITATION",
    "INCOMPATIBLE",
    "UNKNOWN",
    "NOT_APPLICABLE",
}

FENCED_TEXT_RE = re.compile(r"```text\n(.*?)\n```", re.S)
FIELD_RE = re.compile(r"^([a-z_]+):\s*(.+?)\s*$")

# plan_tasks / affected_artifacts reference resolution (see module docstring).
# A reference is a standalone `tracker-<n>` (optionally inside a path, e.g.
# logs/tracker-14/). The lookbehind keeps words that merely end in "tracker"
# out of the match: before it, "tasktracker-5", "non-tracker-5" and
# "backTracker_3" all counted as tracker references (P6-716).
TRACKER_RE = re.compile(r"(?<![A-Za-z0-9_-])tracker[\s_-]*(\d+)", re.I)
MD_TOKEN_RE = re.compile(r"[A-Za-z0-9_./-]+\.md")
ARTIFACT_TOKEN_RE = re.compile(r"[A-Za-z0-9_./-]+\.[A-Za-z0-9]+")
ARTIFACT_EXTENSIONS = {
    "md", "java", "sql", "json", "py", "sh", "yaml", "yml", "xml", "toml",
    "properties", "go", "pin", "txt", "csv", "frame", "golden", "sha256",
    "kts", "gradle", "pom",
}
NONE_RE = re.compile(r"^(?:none|n/a|na|-|\s)+$", re.I)
SKIP_DIRS = {"target", ".git", "node_modules", ".m2"}

# (basename -> relative path, sorted relative paths), built on first use. One
# record names many artifacts, so the walk is paid once per process, not once
# per token (P6-326). Sorted, so a name that repeats resolves the same way
# every run.
_REPO_INDEX = None

# A record may name an artifact that has since been deleted. That is not drift to
# paper over: the record was accurate when filed, and deleting the path from it
# would erase what the change touched. `(retired by <sha>)` keeps the path, adds
# the evidence, and is only accepted when the named commit really removed it —
# see retirement_verified() — so the annotation cannot work as a silencer.
ARTIFACT_RETIRED_RE = re.compile(r"^\(retired by ([0-9a-f]{7,40})\)", re.I)
_RETIREMENT_CACHE = {}


def parse_record(text):
    """Extract `key: value` fields from the fenced ```text block(s).

    First occurrence wins for duplicate keys; non-`key: value` lines (markdown
    prose, comments, blank lines) are ignored. Fields outside a fenced block
    are not parsed — the fenced block is the record's machine contract.
    """
    fields = {}
    for block in FENCED_TEXT_RE.findall(text):
        for line in block.splitlines():
            m = FIELD_RE.match(line)
            if m:
                fields.setdefault(m.group(1), m.group(2).strip())
    return fields


def validate_text(text):
    """Return a list of issue strings; empty list means the record is complete."""
    issues = []
    if not FENCED_TEXT_RE.search(text):
        return ["no fenced ```text record block"]
    fields = parse_record(text)
    for req in REQUIRED_FIELDS:
        if req not in fields or not fields[req]:
            issues.append(f"missing required field '{req}'")
    cc = fields.get("compatibility_class")
    if cc and cc not in COMPATIBILITY_CLASSES:
        issues.append(
            f"compatibility_class '{cc}' not in {sorted(COMPATIBILITY_CLASSES)}"
        )
    return issues


def _strip_anchor(token):
    """Drop a #anchor and surrounding punctuation from a path token."""
    tok = token.split("#", 1)[0].strip("` \t,;:)]}(")
    return tok[2:] if tok.startswith("./") else tok


def _resolve_candidates(ref, records_dir):
    """Path candidates for a reference token (repo/record-dir relative, or a
    bare name under docs/08_implementation/ or docs/)."""
    if "/" in ref:
        return [os.path.join(ROOT, ref), os.path.join(records_dir, ref)]
    return [
        os.path.join(ROOT, "docs", "08_implementation", ref),
        os.path.join(ROOT, "docs", ref),
    ]


def _repo_index():
    """Lazily walk the tree once: (by_basename, sorted relative paths)."""
    global _REPO_INDEX
    if _REPO_INDEX is None:
        rel_paths = []
        for root, dirs, files in os.walk(ROOT):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            for name in files:
                rel_paths.append(os.path.relpath(os.path.join(root, name), ROOT))
        rel_paths.sort()
        by_basename = {}
        for rel in rel_paths:
            by_basename.setdefault(os.path.basename(rel), rel)
        _REPO_INDEX = (by_basename, tuple(rel_paths))
    return _REPO_INDEX


def find_basename(name):
    """Repo-wide fallback for a token the explicit candidates missed (P6-325).

    A bare name matches that basename anywhere in the tree; a path-shaped token
    matches the repo-relative path itself or any path ending in "/<token>", so
    a record may name the full path or a suffix of it. None means genuinely
    unresolved, which the caller reports as an issue — never as a pass.
    """
    by_basename, rel_paths = _repo_index()
    if "/" in name:
        tail = "/" + (name[2:] if name.startswith("./") else name)
        return next((rel for rel in rel_paths if rel == name or rel.endswith(tail)), None)
    rel = by_basename.get(name)
    return os.path.join(ROOT, rel) if rel else None


def resolve_md_ref(ref, records_dir):
    """Return the first existing path for an .md reference, or None."""
    return next(
        (c for c in _resolve_candidates(ref, records_dir) if os.path.isfile(c)),
        None,
    )


def resolve_artifact_ref(ref, records_dir):
    """Resolve an artifact token: explicit candidates first, then a repo-wide
    search (cached index; basename or path suffix)."""
    hit = next(
        (c for c in _resolve_candidates(ref, records_dir) if os.path.isfile(c)),
        None,
    )
    return hit or find_basename(ref)


def plan_task_issues(value, records_dir):
    """Issues for a plan_tasks value whose references cannot resolve."""
    if not value or NONE_RE.match(value):
        return []
    issues = []
    for n in TRACKER_RE.findall(value):
        hits = glob.glob(
            os.path.join(ROOT, "docs", "08_implementation", f"{n}-*.md")
        )
        if not hits:
            issues.append(
                f"plan_tasks references tracker-{n} but no "
                f"docs/08_implementation/{n}-*.md exists"
            )
    for tok in MD_TOKEN_RE.findall(value):
        ref = _strip_anchor(tok)
        if not resolve_md_ref(ref, records_dir):
            issues.append(f"plan_tasks references unknown file '{ref}'")
    return issues


def _git_has_path(rev, ref):
    """Is `ref` present in revision `rev`? False whenever git cannot answer."""
    try:
        proc = subprocess.run(
            ["git", "-C", ROOT, "cat-file", "-e", f"{rev}:{ref}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def retirement_verified(ref, sha):
    """Did `sha` really delete `ref`?

    True only when the path is present in the commit's parent and gone in the
    commit itself. A rename therefore does NOT verify — a moved file should be
    re-pointed at its live path, not marked retired. Fails closed when git is
    unavailable: an unverifiable claim of removal is not evidence.
    """
    key = (ref, sha)
    if key not in _RETIREMENT_CACHE:
        _RETIREMENT_CACHE[key] = _git_has_path(f"{sha}^", ref) and not _git_has_path(
            sha, ref
        )
    return _RETIREMENT_CACHE[key]


def artifact_issues(value, records_dir):
    """Issues for an affected_artifacts value whose path-shaped tokens do not
    resolve to an existing file. Prose descriptions (no path shape, no known
    extension) are ignored; a dead path is accepted only when immediately
    followed by a verified `(retired by <sha>)` annotation."""
    if not value or NONE_RE.match(value):
        return []
    issues, seen = [], set()
    for match in ARTIFACT_TOKEN_RE.finditer(value):
        tok = match.group(0)
        ext = tok.rsplit(".", 1)[1].lower()
        if ext not in ARTIFACT_EXTENSIONS:
            continue
        ref = _strip_anchor(tok)
        if ref in seen:
            continue
        seen.add(ref)
        if resolve_artifact_ref(ref, records_dir):
            continue
        annotation = ARTIFACT_RETIRED_RE.match(value[match.end() :].lstrip())
        if annotation is None:
            issues.append(f"affected_artifacts references unknown artifact '{ref}'")
        elif not retirement_verified(ref, annotation.group(1)):
            issues.append(
                f"affected_artifacts marks '{ref}' retired by {annotation.group(1)},"
                " which did not remove it — annotate with `(retired by <sha>)`"
                " naming the commit that deleted the path, or point at the live"
                " path if it only moved"
            )
    return issues


def validate_file(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except OSError as exc:
        return [f"unreadable: {exc}"]
    issues = validate_text(text)
    rec_dir = os.path.dirname(path)
    pt = parse_record(text).get("plan_tasks")
    if pt:
        issues.extend(plan_task_issues(pt, rec_dir))
    fa = parse_record(text).get("affected_artifacts")
    if fa:
        issues.extend(artifact_issues(fa, rec_dir))
    return issues


def scan_records(records_dir):
    """Return (files, issues_by_file, dir_missing).

    Files starting with '_' (e.g. _template.md) are excluded — they are
    documentation, not records.
    """
    if not os.path.isdir(records_dir):
        return [], {}, True
    files = sorted(
        f for f in os.listdir(records_dir)
        if f.endswith(".md") and not f.startswith("_")
    )
    issues = {f: validate_file(os.path.join(records_dir, f)) for f in files}
    return files, issues, False


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    records_dir = DEFAULT_RECORDS_DIR
    if "--dir" in argv:
        i = argv.index("--dir")
        if i + 1 >= len(argv):
            print("change-control: --dir requires a path")
            return 2
        records_dir = argv[i + 1]

    files, issues, missing = scan_records(records_dir)
    if missing:
        print(f"[FAIL] change-records directory missing: {records_dir}")
        print(
            "change-control: create docs/05_deployment/change-records/ and file "
            "records as CHG-<N>.md (template: _template.md)"
        )
        return 1
    if not files:
        print("change-control: no change records on file — nothing to validate")
        return 0

    rc = 0
    for f in files:
        iss = issues[f]
        if iss:
            rc = 1
            print(f"[FAIL] {f} — {'; '.join(iss)}")
        else:
            print(f"[PASS] {f} names all {len(REQUIRED_FIELDS)} required fields "
                  f"and references resolve")
    if rc:
        print(f"change-control: {sum(1 for v in issues.values() if v)} record(s) "
              f"incomplete — reconciliation records must name all required fields "
              f"with resolvable plan_tasks")
    else:
        print(f"change-control: all {len(files)} change record(s) complete")
    return rc


if __name__ == "__main__":
    sys.exit(main())
