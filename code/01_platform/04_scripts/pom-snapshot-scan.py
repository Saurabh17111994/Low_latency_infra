#!/usr/bin/env python3
"""pom-snapshot-scan.py — CI gate (foundation L554: "CI rejects mutable image
tags and unpinned dependencies").

Fails when any pom.xml pins an EXTERNAL dependency to a SNAPSHOT version
(mutable). The workspace's own com.trading SNAPSHOT is the module-build
contract (common -> ingestion/compute) and is explicitly allowed.

Scanned: <dependency> elements (in <dependencies> and <dependencyManagement>)
and <parent> blocks — both are build-time dependencies on another artifact.
Not scanned: <plugin>/<pluginManagement> entries, a different kind of pin that
the earlier element-sequence match reported as a dependency (P6-484).

POM_SCAN_ROOT overrides the scanned root (tests only; unset in production).

Usage: python3 pom-snapshot-scan.py
  exit 0 = clean, 1 = violations, 2 = the scan could not verify anything (no
  pom.xml found, or one could not be read).
"""

import glob
import os
import re
import sys

# <repo root>/code/**/pom.xml. This file sits three directories below `code`, so
# the repo root is four levels up: three resolved to `<repo>/code`, turning the
# glob into `<repo>/code/code/**/pom.xml`, which never exists — so the gate
# always printed "no external SNAPSHOT dependencies" and passed (P6-012).
ROOT = os.environ.get("POM_SCAN_ROOT") or os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


def _element_re(tag):
    """groupId/artifactId/version inside one <tag> element.

    The tempered dots keep the match inside its own element, so a <plugin> or
    <pluginManagement> pin cannot satisfy a <dependency> or <parent> match.
    """
    return re.compile(
        rf"<{tag}>(?:(?!</{tag}>).)*?<groupId>([^<]+)</groupId>"
        rf"(?:(?!</{tag}>).)*?<artifactId>([^<]+)</artifactId>"
        rf"(?:(?!</{tag}>).)*?<version>([^<]+)</version>"
        rf"(?:(?!</{tag}>).)*?</{tag}>",
        re.DOTALL,
    )


DEP_RE = _element_re("dependency")
PARENT_RE = _element_re("parent")


def find_poms(root):
    """Every pom.xml under <root>/code — the workspace this gate is about."""
    return sorted(glob.glob(os.path.join(root, "code", "**", "pom.xml"), recursive=True))


def scan(root):
    """Return (violations, unreadable). Read failures are reported, not skipped.

    An unreadable pom used to be dropped on the floor, so a permission or
    encoding error silently bypassed the check (P6-762).
    """
    violations, unreadable = [], []
    for pom in find_poms(root):
        try:
            with open(pom, encoding="utf-8") as fh:
                text = fh.read()
        except OSError as exc:
            print(f"WARN: cannot read {os.path.relpath(pom, root)}: {exc}", file=sys.stderr)
            unreadable.append(os.path.relpath(pom, root))
            continue
        for rx in (DEP_RE, PARENT_RE):
            for match in rx.finditer(text):
                group, version = match.group(1), match.group(3)
                if "SNAPSHOT" in version and not group.startswith("com.trading"):
                    violations.append(f"{os.path.relpath(pom, root)}: {group}:{version}")
    return violations, unreadable


def main():
    poms = find_poms(ROOT)
    if not poms:
        print(
            f"FAIL: no pom.xml found under {os.path.join(ROOT, 'code')} — "
            "the scan verified nothing",
            file=sys.stderr,
        )
        return 2
    violations, unreadable = scan(ROOT)
    if violations:
        for v in violations:
            print(f"FAIL: external SNAPSHOT dependency: {v}")
        print(
            "pom-snapshot-scan: external SNAPSHOT dependencies are mutable — pin a release"
        )
        return 1
    if unreadable:
        print(
            f"pom-snapshot-scan: {len(unreadable)} pom.xml could not be read — "
            "result is inconclusive",
            file=sys.stderr,
        )
        return 2
    print(
        f"pom-snapshot-scan: no external SNAPSHOT dependencies "
        f"({len(poms)} pom.xml scanned)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
