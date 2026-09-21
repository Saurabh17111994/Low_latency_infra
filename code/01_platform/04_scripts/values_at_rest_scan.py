#!/usr/bin/env python3
"""T2/T6 — no production value at rest, and none in a new transcript.

Two properties from `docs/plans/2026-09-21-post-verification-plan.md`, one
scanner:

* T2 — a names-plus-patterns scan over the workstation returns zero hits for
  the production secret names while the container is unmounted.
* T6 — the same scan over session transcripts created after the
  values-never-rest-here decision returns zero matches.

* T2 also has a value-equality mode (`--values-file`). Names and shapes answer
  "is this secret named here", which is not the question the leak asked: on
  2026-09-21 the four live Arrow values sat in 48 files with no name beside most
  of them. That mode compares values literally, prints no preview, fails on
  every hit — a supplied list of live values has no decoys in it — and does not
  skip large files, because a value does not care how large the file around it
  is.

Five rules make the result mean something:

1. The value is never printed. A hit prints the name, the file, the line, the
   length and a masked preview using `audit_r2.mask_secret`'s convention.
2. A hit inside this repository does not fail the scan by default: the tree is
   public and reviewed, and its fixtures and documented decoys are deliberate.
   `--strict` fails on those too. Either way every hit is printed — the class
   changes, never the visibility.
3. A missing scan root is rc=2, never "0 hits". The plan's own warning applies:
   absence of a scan must not read like a clean result.
4. Only regular files are opened. A named pipe has no reader, so opening one
   waits forever: the first full `$HOME` scan hung for 26 minutes on
   `~/.steam/steam.pipe` with `wchan=wait_for_partner`, which is the kernel
   function a process sleeps in while `open()` waits for a writer. Such a path
   is counted as `skipped_special` and never opened.
5. Value mode fails closed on its own inputs. A missing, unreadable or
   placeholder-only values file is rc=2, never "0 hits", and every value it
   could not search — too short, a placeholder, a duplicate — is counted in the
   report instead of being dropped.

The nine names come from `secrets-bootstrap.sh --print-names`, the single
authority the stack and its parity test already read, so a tenth secret cannot
escape this scan by being added somewhere else.

Pairs with CHG-289 and CHG-295.
"""

import argparse
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
from datetime import date

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
BOOTSTRAP = SCRIPT_DIR / "secrets-bootstrap.sh"

# Directories never worth reading, and file kinds that cannot hold a value we
# typed: build output, caches, vcs metadata, binaries.
SKIP_DIRS = {
    ".git", "target", "node_modules", "__pycache__", ".venv", "venv",
    ".mypy_cache", ".pytest_cache", "site-packages", ".cargo", ".rustup",
    ".npm", ".gradle", ".container-overlay", "dist", "build",
}
SKIP_EXT = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".pdf", ".zip", ".gz", ".xz",
    ".zst", ".tgz", ".jar", ".class", ".so", ".o", ".a", ".bin", ".woff",
    ".woff2", ".ttf", ".ico", ".mp4", ".db", ".sqlite", ".sqlite3", ".pyc",
    ".rlib", ".rmeta", ".dylib", ".exe",
}
MAX_BYTES = 2 * 1024 * 1024
# The floor for a value worth searching. Shared by `classify` and by the
# value-equality mode so the two cannot drift apart.
MIN_VALUE_LEN = 6

# A value we would never call a secret. The plan's must-fail decoy
# (`dummy1234567890`) is deliberately NOT here: it must trip the scan, which is
# the whole point of planting it.
PLACEHOLDER = re.compile(
    r"^(?:|\$\{.*\}|\{[^}]*\}|\$[A-Za-z_][A-Za-z0-9_]*|<[^>]*>|[*x]{3,}|\.{3}|"
    r"change[_-]?me|redacted|todo|none|null)$",
    re.IGNORECASE,
)
# A value that is an instruction to the reader rather than a secret. The token
# has to be a whole `_`/`-` separated word: `togetherenow` contains "here" but
# is a plausible password, while `PUT_YOUR_SECRET_HERE` is not a value at all.
INSTRUCTION_WORDS = frozenset({
    "your", "here", "placeholder", "example", "redact", "todo", "changeme",
    "change",
})
WORD_LIKE = re.compile(r"^[\w\-]+$")
PATH_LIKE = re.compile(
    r"^(?:/[^\s]*|\./[^\s]*|~/[^\s]*"
    # A relative path such as `code/01_platform/01_docker/.env`: a reference to
    # a file, not a value. The segment shape excludes base64's `+`, `/` and
    # `=`, so a real secret cannot be demoted by this rule.
    r"|(?:[\w.@-]+/)+[\w.@-]*\."
    r"(?:env|txt|json|sh|md|ya?ml|conf|toml|pem|key|crt|log|tsv|csv))$"
)


def is_placeholder(value):
    """A reference or a redaction, never a value.

    The prefix test matters as much as the whole-string one: a shell form such
    as `${O2_PASSWORD:?operator sets this at S7}` is cut at its first space, so
    only its prefix survives classification.
    """
    if not value:
        return True
    if value[0] in ("$", "<", "*"):
        return True
    if WORD_LIKE.match(value) and any(
            part.lower() in INSTRUCTION_WORDS
            for part in re.split(r"[_\-]+", value)):
        return True
    return bool(PLACEHOLDER.match(value))

# Name-free shapes. These catch a value whose name was written differently, or
# copied without its name at all.
SHAPES = (
    ("aws_access_key_id", re.compile(r"\b(AKIA[0-9A-Z]{16})\b")),
    ("github_token", re.compile(r"\b(ghp_[A-Za-z0-9]{36,})\b")),
    ("github_pat", re.compile(r"\b(github_pat_[A-Za-z0-9_]{30,})\b")),
    ("gitlab_token", re.compile(r"\b(glpat-[A-Za-z0-9_\-]{20,})\b")),
)

STAMP = re.compile(r"^(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}-\d{2}-\d{3}Z")

# The plan's controls plant values that are shaped like secrets on purpose
# (`dummy1234567890`, `dummy1234567890abcdef`), and its own documentation plus
# this file's docstring repeat them. They are still hits — that is what makes
# them controls — but a report that cannot separate them from a real value is
# unreadable, and the value itself is never printed to help. So the class is
# counted without ever looking at the value by eye.
DECOY_VALUE = re.compile(
    r"^(?:dummy|fixture|example|iosfodnn|sample|fake|smoke|test)",
    re.IGNORECASE,
)


def mask(value):
    """Same shape as audit_r2.mask_secret, so evidence reads consistently."""
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return value[:4] + "..." + value[-4:]


def fail(message):
    """rc=2, never a silent pass: absence of a scan is not a clean result."""
    print(f"values_at_rest_scan: {message}", file=sys.stderr)
    sys.exit(2)


def load_names(explicit):
    if explicit:
        names = [n.strip() for n in explicit.split(",") if n.strip()]
        if not names:
            fail("--names was empty")
        return names
    if not BOOTSTRAP.exists():
        fail(f"{BOOTSTRAP} not found; pass --names")
    proc = subprocess.run(
        ["bash", str(BOOTSTRAP), "--print-names"],
        capture_output=True, text=True,
    )
    names = [n.strip() for n in proc.stdout.split() if n.strip()]
    if proc.returncode != 0 or not names:
        fail(
            "could not read the secret names from "
            f"secrets-bootstrap.sh (rc={proc.returncode}); pass --names"
        )
    return names


def load_values(path):
    """`--values-file`: NAME=value lines, read once, never echoed.

    The file holds production values and lives outside this repository, so it is
    read into memory, compared, and never printed — only the names of its values
    and the paths of their hits reach the report.
    """
    try:
        text = pathlib.Path(path).expanduser().read_text(errors="ignore")
    except OSError as exc:
        fail(
            f"--values-file could not be read ({exc}); a scan without its "
            "values is not a clean result"
        )
    values, ignored = {}, {"placeholder": 0, "short": 0, "duplicate": 0}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, raw = line.split("=", 1)
        name = name.strip()
        value = raw.strip().strip("\"'")
        if not name or not value:
            continue
        if is_placeholder(value):
            ignored["placeholder"] += 1
            continue
        if len(value) < MIN_VALUE_LEN:
            ignored["short"] += 1
            continue
        if value in values:
            ignored["duplicate"] += 1
            continue
        values[value] = name
    if not values:
        fail(
            "--values-file supplied no usable values; a scan with nothing to "
            "compare cannot certify anything"
        )
    return values, ignored


def name_pattern(names):
    alt = "|".join(
        re.escape(n).replace("_", "[_-]")
        for n in sorted(names, key=len, reverse=True)
    )
    # `\\?` ahead of the quotes: a transcript stores JSON, so the value's own
    # quotes arrive backslash-escaped, and the escape must not become the first
    # character of the value.
    return re.compile(
        r"(?<![\w-])(?P<name>" + alt + r")(?:_FILE)?[\"']?\s*[:=]\s*\\?[\"']?"
        r"(?P<val>[^\"'\\\s,;)]+)",
        re.IGNORECASE,
    )


def classify(raw):
    """placeholder / path / short / value — only `value` is a hit."""
    value = raw.strip().strip("\"'")
    if is_placeholder(value):
        return "placeholder", value
    if PATH_LIKE.match(value):
        return "path", value
    # MIN_VALUE_LEN, not 8: a seven-character password is still a password. The
    # placeholder and path filters have already run, so the false-positive cost
    # is low, and the value-equality mode shares this floor so the two cannot
    # drift apart.
    if len(value) < MIN_VALUE_LEN:
        return "short", value
    return "value", value


def parse_since(text):
    try:
        return date.fromisoformat(text)
    except ValueError:
        fail(f"--since wants YYYY-MM-DD, got {text!r}")


def file_date(path):
    match = STAMP.match(path.name)
    if match:
        return date.fromisoformat(match.group(1))
    try:
        return date.fromtimestamp(path.stat().st_mtime)
    except OSError:
        return None


def name_matcher(pattern):
    """`find(line)` -> (label, value) pairs that survive classification."""
    def find(line):
        found = []
        for match in pattern.finditer(line):
            kind, value = classify(match.group("val"))
            if kind == "value":
                found.append((match.group("name"), value))
        for shape_name, shape in SHAPES:
            for match in shape.finditer(line):
                kind, value = classify(match.group(1))
                if kind == "value":
                    found.append((shape_name, value))
        return found
    return find


def value_matcher(value_map):
    """`find(line)` -> (name, value) pairs, matched literally.

    No classification here, deliberately. The two filters that make a *named*
    value trustworthy — placeholder-shaped and path-shaped — are exactly the
    ones that would drop a real value the operator supplied because it happens
    to look like a reference or a path.
    """
    def find(line):
        return [(name, value) for value, name in value_map.items() if value in line]
    return find


def scan_file(path, find):
    hits = []
    try:
        with path.open("r", errors="ignore") as handle:
            for lineno, line in enumerate(handle, 1):
                for label, value in find(line):
                    hits.append((lineno, label, value))
    except OSError:
        return hits, False
    return hits, True


def walk(root, since, names=None, values=None, max_bytes=MAX_BYTES):
    find = (value_matcher(values) if values is not None
            else name_matcher(name_pattern(names)))
    stats = {"files": 0, "skipped_big": 0, "skipped_ext": 0, "unreadable": 0,
             "skipped_old": 0, "skipped_special": 0}
    results = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for filename in filenames:
            path = pathlib.Path(dirpath) / filename
            if path.suffix.lower() in SKIP_EXT:
                stats["skipped_ext"] += 1
                continue
            try:
                info = path.stat()
            except OSError:
                stats["unreadable"] += 1
                continue
            # A fifo, socket, or device: opening it is not reading it. This is
            # the check that keeps a scan from waiting forever on a path that
            # will never produce a byte.
            if not stat.S_ISREG(info.st_mode):
                stats["skipped_special"] += 1
                continue
            # Value mode passes max_bytes=None: a value can sit in a file of any
            # size, and skipping large files is exactly the gap that left 243
            # files unread in the 2026-09-21 leak scan.
            if max_bytes is not None and info.st_size > max_bytes:
                stats["skipped_big"] += 1
                continue
            if since is not None:
                stamp = file_date(path)
                if stamp is not None and stamp < since:
                    stats["skipped_old"] += 1
                    continue
            hits, read_ok = scan_file(path, find)
            stats["files"] += 1
            if not read_ok:
                stats["unreadable"] += 1
                continue
            for lineno, label, value in hits:
                cls = "in-repo" if REPO_ROOT in path.parents else "outside"
                results.append({
                    "class": cls, "path": str(path), "line": lineno,
                    "name": label, "length": len(value),
                    "preview": "" if values is not None else mask(value),
                    "decoy": bool(DECOY_VALUE.match(value)),
                })
    return results, stats


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="T2/T6 — scan for production secret values at rest.",
    )
    parser.add_argument("--root", action="append", default=None,
                        help="root to scan; repeatable (default: $HOME)")
    parser.add_argument("--names", default=None,
                        help="comma-separated names (default: secrets-bootstrap.sh)")
    parser.add_argument("--values-file", default=None, dest="values_file",
                        help="NAME=value file outside this repository: compare values "
                             "instead of names, and fail on every hit")
    parser.add_argument("--since", default=None,
                        help="only files dated YYYY-MM-DD or later (T6)")
    parser.add_argument("--strict", action="store_true",
                        help="also fail on hits inside this repository")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    roots = [pathlib.Path(r).expanduser() for r in (args.root or [str(pathlib.Path.home())])]
    missing = [str(r) for r in roots if not r.is_dir()]
    if missing:
        fail(f"not a directory: {', '.join(missing)}")

    since = parse_since(args.since) if args.since else None
    if args.values_file and args.names:
        fail("--values-file and --names are two modes; pass one")
    values, ignored = None, {"placeholder": 0, "short": 0, "duplicate": 0}
    if args.values_file:
        values, ignored = load_values(args.values_file)
        names = sorted(values.values())
    else:
        names = load_names(args.names)

    hits, stats = [], {"files": 0, "skipped_big": 0, "skipped_ext": 0,
                       "unreadable": 0, "skipped_old": 0,
                       "skipped_special": 0}
    for root in roots:
        root_hits, root_stats = walk(
            root, since,
            names=None if values is not None else names,
            values=values,
            max_bytes=None if values is not None else MAX_BYTES,
        )
        hits.extend(root_hits)
        for key in stats:
            stats[key] += root_stats[key]

    outside = [h for h in hits if h["class"] == "outside"]
    in_repo = [h for h in hits if h["class"] == "in-repo"]
    decoys = [h for h in hits if h["decoy"]]
    failed = bool(outside) or (args.strict and bool(in_repo))
    if values is not None:
        # A supplied list holds live values, not the plan's documented decoys, so
        # every hit is a finding — inside this repository as much as outside it.
        failed = bool(hits)

    mode = "values" if values is not None else "names"
    if args.as_json:
        print(json.dumps({
            "mode": mode,
            "roots": [str(r) for r in roots],
            "names": names,
            "since": args.since,
            "hits": hits,
            "counts": {"total": len(hits), "outside": len(outside),
                       "in_repo": len(in_repo), "decoys": len(decoys),
                       "searched": len(values) if values is not None else 0,
                       "ignored_placeholder": ignored["placeholder"],
                       "ignored_short": ignored["short"],
                       "ignored_duplicate": ignored["duplicate"],
                       **stats},
            "failed": failed,
        }, indent=2))
    else:
        for hit in sorted(hits, key=lambda h: (h["class"], h["path"], h["line"])):
            # Value mode prints no preview: the name mode's masked preview is
            # four characters of a live value, which is four too many when the
            # value is real rather than a documented decoy.
            preview = "" if values is not None else f" preview={hit['preview']}"
            print(f"HIT {hit['class']:<7} {hit['path']}:{hit['line']} "
                  f"{hit['name']} len={hit['length']}{preview} "
                  f"decoy={'yes' if hit['decoy'] else 'no'}")
        print(
            f"values_at_rest_scan: mode={mode} files={stats['files']} "
            f"searched={len(values) if values is not None else 0} hits={len(hits)} "
            f"outside={len(outside)} in-repo={len(in_repo)} decoys={len(decoys)} "
            f"ignored_placeholder={ignored['placeholder']} "
            f"ignored_short={ignored['short']} "
            f"ignored_duplicate={ignored['duplicate']} "
            f"skipped_old={stats['skipped_old']} skipped_big={stats['skipped_big']} "
            f"skipped_ext={stats['skipped_ext']} "
            f"skipped_special={stats['skipped_special']} "
            f"unreadable={stats['unreadable']} "
            f"since={args.since or 'none'} strict={args.strict} "
            f"rc={1 if failed else 0}"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
