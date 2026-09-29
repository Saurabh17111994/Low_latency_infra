#!/usr/bin/env python3
"""L2-1: the static checks lint exactly the tracked shell scripts — one enumeration.

WHY THIS EXISTS
---------------
`make static-check` and gate step 1 each enumerated shell scripts their own way,
from `code/`: the three root entry scripts (start-all.sh, run-ingestion.sh,
show-ticks.sh) were tracked but never linted by either, and the two lists could
drift apart. L2-1 moved both onto one repo-rooted enumeration
(`lint-enumeration.sh`, `git ls-files '*.sh'` minus target/third_party) that
fails closed on an empty list.

WHAT IS CHECKED (offline; runs git and the script, nothing else)
---------------------------------------------------------------
* The enumeration equals `git ls-files '*.sh'` minus the exclusions.
* Both sites call the shared enumeration and keep no code-rooted find/fallback.
* The root entry scripts are inside the checked set.
* An empty enumeration fails closed (exit != 0) in a sandbox repo.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "code/01_platform/04_scripts/lint-enumeration.sh"
MAKEFILE = ROOT / "Makefile"
GATE = ROOT / "code/01_platform/04_scripts/run-monday-gates.sh"
EXCLUDE = re.compile(r"(^|/)(target|third_party)/")


def tracked_scripts() -> list[str]:
    out = subprocess.run(["git", "ls-files", "*.sh"], cwd=ROOT,
                         capture_output=True, text=True, check=True).stdout
    return sorted(line for line in out.splitlines() if line and not EXCLUDE.search(line))


def enumerated() -> list[str]:
    out = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return sorted(out.stdout.splitlines())


def test_the_enumeration_is_the_tracked_set_minus_exclusions():
    assert enumerated() == tracked_scripts()


def test_the_root_entry_scripts_are_in_the_checked_set():
    names = set(enumerated())
    for name in ("start-all.sh", "run-ingestion.sh", "show-ticks.sh"):
        assert name in names, f"{name} must be linted (it was the blind spot L2-1 fixed)"


def test_both_sites_use_the_shared_enumeration():
    assert "lint-enumeration.sh" in MAKEFILE.read_text(), \
        "make static-check must call the shared enumeration"
    assert "lint-enumeration.sh" in GATE.read_text(), \
        "gate step 1 must call the shared enumeration"


def test_no_site_keeps_a_code_rooted_enumeration():
    makefile = MAKEFILE.read_text()
    gate = GATE.read_text()
    assert not re.search(r"find\s+code\b", makefile), "the code-rooted find is back"
    assert not re.search(r"find\s+\.\s+-name\s+'\*\.sh'", gate), \
        "the code-rooted find is back in the gate"
    assert "$CODE_DIR/$s" not in gate, "the gate must check repo-rooted paths"


def test_an_empty_enumeration_fails_closed(tmp_path):
    repo = tmp_path / "repo"
    scripts = repo / "code/01_platform/04_scripts"
    scripts.mkdir(parents=True)
    (scripts / "lint-enumeration.sh").write_text(SCRIPT.read_text())
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    (repo / "README.md").write_text("no shell scripts here\n")
    r = subprocess.run(["bash", str(scripts / "lint-enumeration.sh")],
                       cwd=repo, capture_output=True, text=True)
    assert r.returncode != 0, "an empty enumeration must fail, not print nothing and pass"
    assert "empty" in (r.stdout + r.stderr).lower()


def test_a_non_git_tree_fails_closed(tmp_path):
    scripts = tmp_path / "code/01_platform/04_scripts"
    scripts.mkdir(parents=True)
    (scripts / "lint-enumeration.sh").write_text(SCRIPT.read_text())
    r = subprocess.run(["bash", str(scripts / "lint-enumeration.sh")],
                       cwd=tmp_path, capture_output=True, text=True)
    assert r.returncode != 0
    assert "not a git work tree" in (r.stdout + r.stderr)
