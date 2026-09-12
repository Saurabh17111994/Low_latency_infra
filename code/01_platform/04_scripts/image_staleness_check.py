#!/usr/bin/env python3
"""image_staleness_check.py — CHG-101: fail when a compose `build:` image is
older than the last change to the source it packages.

WHY: 2026-08-24 the gateway+bridge images were still the 2026-08-20 builds
while their source had changed (CHG-092 execution flag, CHG-096 TOTP-only) —
the stale gateway jar even reported readyz 200 with the OLD fail-open
`EXECUTION_ENABLED=false` semantics. This check makes that impossible to
miss: an image that predates its source's last commit is a deployment bug.

METHOD (per compose service with `build:`):
  * source paths: the Dockerfile's COPY inputs (SERVICE_SOURCES overlay;
    fallback = build context + dockerfile path)
  * CONTENT STAMP (primary): sha256 over those inputs' contents (plus the
    Dockerfile), which `make images` exports so compose bakes it into the
    image as the `com.trading.build-stamp` label (build.labels). FRESH iff the
    label equals the stamp recomputed from the tree — content-addressed, so a
    `touch`, a `git checkout`, or a fully-cached rebuild cannot move it.
  * TIMESTAMP PROXY (fallback for images built before the stamp existed):
    source epoch from `git log -1 --format=%ct -- <paths>` vs image epoch from
    `docker image inspect -f '{{.Created}}' <image>`. Proved unsound in both
    directions on 2026-09-12 — it reported STALE for an image built from
    exactly the committed content (built 13 min before the commit, so the
    image contains the change while predating it), and a cache-hit rebuild
    can never clear it. Hence the stamp is the primary signal.
  * verdict: FRESH | STALE | MISSING | NO-HISTORY | DIRTY-WARN

Skipped: services without `build:` (digest-pinned pulls) — they never embed
repo source.

Limitations (documented, not silent): untracked source paths have no git
history and are skipped; uncommitted working-tree changes are reported as
DIRTY WARN (the image cannot include them) but do not fail the gate — the
monday gate runs the committed truth; rebuild images with `make images`
(computes every stamp, builds, then re-verifies).

Exit: 0 = all fresh; 1 = STALE/MISSING; 2 = config/usage error.

Usage:
  image_staleness_check.py [--compose <docker-compose.yml>]
                           [--git-root <repo root>] [--project <name>]
                           [--service <name> [--service ...]]
                           [--print-services | --print-stamps-env]

Matches the pytest pattern of prod_node_check.py: pure helpers + injected
values so unit tests need no docker.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import os
import subprocess
import sys
from pathlib import Path

import yaml

# Repo-root-relative source paths per compose service (Dockerfile COPY inputs;
# a dir = whole subtree). Keep in sync with the build Dockerfiles.
def _platform_sources() -> list[str]:
    return [
        "code/pom.xml", "code/common", "code/01_platform/02_sql/ddl",
        "code/01_platform/04_scripts/ddl_apply.py",
        "code/01_platform/04_scripts/ddl_apply_smoke.py",
        "code/01_platform/04_scripts/evidence_ownership_check.py",
        "code/01_platform/04_scripts/eod_controller.py",
        "code/01_platform/01_docker/ddl-apply",
        "code/02_services/01_ingestion/pom.xml",
        "code/02_services/06_execution_gateway/pom.xml",
    ]


SERVICE_SOURCES: dict[str, list[str]] = {
    "ingestion": [
        "code/pom.xml", "code/common",
        "code/02_services/01_ingestion",
        "code/02_services/06_execution_gateway/pom.xml",
    ],
    "execution-bridge": [
        "code/02_services/06_execution_bridge",
        "code/02_services/01_ingestion/go-bridge/third_party",
    ],
    "execution-gateway": [
        "code/pom.xml", "code/common",
        "code/02_services/06_execution_gateway",
        "code/02_services/01_ingestion",
    ],
    "nautilus": ["code/02_services/04_executor"],
    # CHG-122 loadgen (2026-09-02): built from the reactor root with
    # Dockerfile.loadgen, which compiles the Go bridge and the ingestion sources
    # into the image, so those are its real COPY inputs. Without this entry the
    # compose-derived fallback hands git compose-relative strings ("../..") as
    # if they were repo-relative, and the service reports NO-HISTORY however
    # fresh its image is.
    "loadgen": [
        "code/pom.xml", "code/common",
        "code/02_services/01_ingestion",
        "code/02_services/06_execution_gateway/pom.xml",
    ],
    # Native split: the compute image is the PLATFORM + launcher only — the
    # jar is a host artifact (mounted/submitted at runtime). Sources are the
    # Dockerfile + submit-jobs.sh; the 02_services job code is deliberately
    # EXCLUDED so a code change does not flag the image STALE (no rebuild).
    "compute": [
        "code/02_services/02_compute/Dockerfile",
        "code/02_services/02_compute/submit-jobs.sh",
    ],
    "ddl-apply": _platform_sources(),
    "eod-controller": _platform_sources(),
}


def load_compose(path: Path) -> dict:
    """Parse the compose YAML; raise ValueError on structural errors."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("services"), dict):
        raise ValueError(f"{path}: no services section")
    return data


def build_services(compose: dict) -> dict[str, dict]:
    """Return {service: {context, dockerfile, profile, image, labels}} for
    build: services. `image` is compose's own declaration, or None when compose
    will tag the build with its default <project>-<service> name. `labels` are
    the image-config labels compose applies (build.labels)."""
    out: dict[str, dict] = {}
    for name, svc in compose["services"].items():
        build = svc.get("build") if isinstance(svc, dict) else None
        if isinstance(build, dict):
            out[name] = {
                "context": build.get("context", "."),
                "dockerfile": build.get("dockerfile", "Dockerfile"),
                "profile": svc.get("profiles"),
                "image": svc.get("image"),
                "labels": build.get("labels") or {},
            }
    return out


def image_ref(project: str, service: str, entry: dict | None = None) -> str:
    """Image reference this service is inspected under.

    A compose `image:` declaration wins: a service that pins one (loadgen ->
    pipeline-loadgen:1.0.0) is never tagged with compose's <project>-<service>
    default, so looking up the default reports a false MISSING. Absent a
    declaration, the default is exactly what compose itself builds.
    """
    declared = (entry or {}).get("image")
    return declared or f"{project}-{service}"


# Content-addressed freshness (the G27b pattern, generalised in CHG-124):
# compose bakes sha256(inputs) into the image under this label, and the checker
# recomputes it from the tree. Content, not clock.
STAMP_LABEL = "com.trading.build-stamp"
# Directories that never enter an image even when they sit inside a source
# subtree. Hashing build outputs would move the stamp for reasons the image
# does not care about (and `target/` is rewritten by every local build).
_STAMP_SKIP_DIRS = frozenset(
    {".git", "__pycache__", "target", "node_modules", ".mypy_cache",
     ".pytest_cache", ".idea"}
)
_STAMP_SKIP_SUFFIXES = (".pyc", ".class", ".jar", ".so", ".o", ".swp")


def _add_stamp_files(git_root: Path, rel_path: str, files: set[str]) -> None:
    """Add one source path (file or whole subtree) to `files`."""
    abs_path = git_root / rel_path
    if abs_path.is_file():
        files.add(rel_path)
        return
    if not abs_path.is_dir():
        return
    for child in abs_path.rglob("*"):
        if not child.is_file():
            continue
        rel_child = child.relative_to(abs_path)
        if any(part in _STAMP_SKIP_DIRS for part in rel_child.parts[:-1]):
            continue
        if child.suffix in _STAMP_SKIP_SUFFIXES:
            continue
        files.add(str(child.relative_to(git_root)))


def stamp_inputs(git_root: Path, service: str, entry: dict | None = None,
                 compose_dir: Path | None = None) -> list[str]:
    """Repo-relative files whose contents define this service's image.

    SERVICE_SOURCES is the curated COPY-input list; the compose-declared
    Dockerfile is added on top, because a Dockerfile change is an image change
    even when the file sits outside a listed subtree.
    """
    sources = list(SERVICE_SOURCES.get(service) or [])
    entry = entry or {}
    context, dockerfile = entry.get("context"), entry.get("dockerfile")
    if context and dockerfile and compose_dir is not None:
        try:
            abs_dockerfile = (compose_dir / context / dockerfile).resolve()
            sources.append(str(abs_dockerfile.relative_to(git_root.resolve())))
        except ValueError:
            pass  # context outside the repo: timestamp fallback covers it
    files: set[str] = set()
    for rel_path in sources:
        _add_stamp_files(git_root, rel_path, files)
    return sorted(files)


def input_stamp(git_root: Path, service: str, entry: dict | None = None,
                compose_dir: Path | None = None) -> str | None:
    """The stamp of this service's inputs: sha256 over (path, content-hash)
    pairs. Content-only, so `touch` and `git checkout` are invisible to it
    while a one-byte edit is not. None when nothing was hashed."""
    files = stamp_inputs(git_root, service, entry, compose_dir)
    if not files:
        return None
    digest = hashlib.sha256()
    for rel_path in files:
        digest.update(rel_path.encode("utf-8"))
        digest.update(b"\0")
        try:
            content = (git_root / rel_path).read_bytes()
            digest.update(hashlib.sha256(content).hexdigest().encode("ascii"))
        except OSError:
            # Listed but unreadable/vanished: stay sensitive to that fact
            # instead of silently treating it as unchanged.
            digest.update(b"<unreadable>")
        digest.update(b"\n")
    return digest.hexdigest()


def image_label(image: str, key: str = STAMP_LABEL) -> str | None:
    """Value of an image-config label; None when absent, empty, or the image
    is missing / docker unavailable."""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", "-f",
             '{{index .Config.Labels "' + key + '"}}', image],
            capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    # compose interpolates an unset stamp as the literal "none".
    return None if value in ("", "none") else value


def env_var_name(service: str) -> str:
    """Env var (compose-interpolated) that carries a service's stamp."""
    return service.upper().replace("-", "_") + "_BUILD_STAMP"


def _run_git(git_root: Path, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git"] + args, cwd=str(git_root), capture_output=True, text=True,
        timeout=30,
    )


def source_epoch(git_root: Path, rel_paths: list[str]) -> int | None:
    """Epoch (seconds) of the last commit touching any rel_path; None if the
    paths have no history (untracked / never committed)."""
    if not rel_paths:
        return None
    result = _run_git(git_root, ["log", "-1", "--format=%ct", "--"] + rel_paths)
    if result.returncode != 0:
        return None
    line = result.stdout.strip()
    if not line:
        return None
    try:
        return int(line)
    except ValueError:
        return None


def have_git_history(git_root: Path, rel_paths: list[str]) -> bool:
    """True when every path exists and at least one has git history."""
    for p in rel_paths:
        if (git_root / p).exists():
            return True
    return False


def worktree_dirty(git_root: Path, rel_paths: list[str]) -> bool:
    """True when any of the paths has uncommitted changes or untracked files."""
    if not rel_paths:
        return False
    result = _run_git(git_root, ["status", "--porcelain", "--"] + rel_paths)
    return bool(result.returncode == 0 and result.stdout.strip())


def image_created_epoch(image: str) -> int | None:
    """Epoch of `docker image` Created; None when the image is missing or
    docker is unavailable."""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", "-f", "{{.Created}}", image],
            capture_output=True, text=True, timeout=30,
        )
    except OSError:
        return None
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    raw = result.stdout.strip()
    if not raw:
        return None
    try:
        created = datetime.datetime.fromisoformat(raw)
    except ValueError:
        # Docker prints RFC3339 with nanoseconds; strip suffix on failure.
        created = datetime.datetime.fromisoformat(raw.split(".")[0])
    if created.tzinfo is None:
        created = created.replace(tzinfo=datetime.timezone.utc)
    return int(created.timestamp())


def verdict(created_epoch: int | None, source_epoch: int | None,
            dirty: bool, stamp_image: str | None = None,
            stamp_sources: str | None = None) -> tuple[str, str]:
    """(status, detail): FRESH|STALE|MISSING|NO-HISTORY|DIRTY-WARN.

    When both the image and the tree carry a content stamp, that decides
    FRESH/STALE (see the module docstring); the timestamp comparison is the
    fallback for images built before the stamp existed.
    """
    if dirty:
        return "DIRTY-WARN", "uncommitted source changes (image may be behind)"
    if stamp_image and stamp_sources:
        if stamp_image == stamp_sources:
            return "FRESH", f"stamp {stamp_image[:12]}... matches the sources"
        return "STALE", (f"stamp {stamp_image[:12]}... != sources "
                         f"{stamp_sources[:12]}... (rebuild: make images)")
    if source_epoch is None:
        return "NO-HISTORY", "source paths have no git history (untracked)"
    if created_epoch is None:
        return "MISSING", "image not built (docker compose build <service>)"
    if created_epoch < source_epoch:
        return "STALE", (f"image {created_epoch} < source {source_epoch} "
                         f"(rebuild with make images)")
    return "FRESH", (f"image {created_epoch} >= source {source_epoch} "
                     f"(timestamp proxy: no build stamp)")


def check_service(service: str, image: str, git_root: Path,
                  compose_services: dict | None = None,
                  compose_dir: Path | None = None) -> dict:
    """Full check for one service; compose_services override is for tests."""
    entry = (compose_services or {}).get(service) or {}
    sources = SERVICE_SOURCES.get(service)
    if sources is None:
        if entry:
            sources = [entry["context"], entry["dockerfile"]]
        else:
            sources = []
    epoch = source_epoch(git_root, sources) if have_git_history(git_root, sources) else None
    dirty = worktree_dirty(git_root, sources)
    created = image_created_epoch(image)
    stamp_image = image_label(image)
    stamp_sources = input_stamp(git_root, service, entry, compose_dir)
    status, detail = verdict(created, epoch, dirty, stamp_image=stamp_image,
                             stamp_sources=stamp_sources)
    return {
        "service": service, "image": image, "status": status,
        "detail": detail, "source_epoch": epoch, "created_epoch": created,
        "stamp_image": stamp_image, "stamp_sources": stamp_sources,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose", type=Path,
                        default=Path("code/01_platform/01_docker/docker-compose.yml"))
    parser.add_argument("--git-root", type=Path, default=Path("."))
    parser.add_argument("--project", default=None,
                        help="compose project name (default: compose dir basename)")
    parser.add_argument("--service", action="append", default=None,
                        help="check only this service (repeatable)")
    parser.add_argument("--print-services", action="store_true",
                        help="print the build services (space separated) and exit")
    parser.add_argument("--print-stamps-env", action="store_true",
                        help="print <SERVICE>_BUILD_STAMP=<sha256> lines for the "
                             "services and exit (eval before `compose build`)")
    args = parser.parse_args(argv)

    git_root = args.git_root.resolve()
    compose_path = args.compose
    if not compose_path.is_absolute():
        # Resolve relative to the git root (script may run from anywhere).
        compose_path = git_root / compose_path
    try:
        compose = load_compose(compose_path)
    except (OSError, ValueError) as exc:
        print(f"image-stale: can not read compose file: {exc}", file=sys.stderr)
        return 2

    services = build_services(compose)
    if not services:
        print("image-stale: no build: services in compose", file=sys.stderr)
        return 2
    if args.service:
        wanted = set(args.service)
        unknown = wanted - set(services)
        if unknown:
            print(f"image-stale: unknown service(s): {sorted(unknown)}",
                  file=sys.stderr)
            return 2
        services = {k: v for k, v in services.items() if k in wanted}

    if args.print_services:
        print(" ".join(sorted(services)))
        return 0
    if args.print_stamps_env:
        # The builders' side of the content stamp: compose interpolates these
        # into build.labels, so an image records the inputs it was built from.
        for name in sorted(services):
            stamp = input_stamp(git_root, name, services[name],
                                compose_path.parent)
            print(f"{env_var_name(name)}={stamp or 'none'}")
        return 0

    project = args.project or compose_path.parent.name
    failures = 0
    warn = 0
    for name in sorted(services):
        image = image_ref(project, name, services[name])
        result = check_service(name, image, git_root, services,
                               compose_path.parent)
        status = result["status"]
        mark = "OK " if status == "FRESH" else ("WARN" if status == "DIRTY-WARN"
                                                else "FAIL")
        if status in ("STALE", "MISSING"):
            failures += 1
        elif status == "DIRTY-WARN":
            warn += 1
        print(f"image-stale: [{mark}] {name} ({image}) {status}: {result['detail']}")

    if failures:
        print(f"image-stale: FAIL — {failures} stale/missing "
              f"(rebuild: make images)")
        return 1
    if warn:
        print(f"image-stale: PASS with {warn} DIRTY-WARN (committed truth "
              f"clean; rebuild after committing source)")
        return 0
    print(f"image-stale: PASS — {len(services)} image(s) current")
    return 0


if __name__ == "__main__":
    sys.exit(main())
