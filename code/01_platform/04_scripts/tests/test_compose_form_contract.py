#!/usr/bin/env python3
"""Pin the one compose form at every invocation site in this repo.

Provenance: the monday gate failed at step 11 because `ddl_apply_smoke.py` invoked
the stack as `docker compose -f <file>` while the running stack had been created as
`docker compose --env-file .env --env-file secrets.env -f <file>`. The bare form
resolves a different config (it does not load `secrets.env`, and it interpolates
from the ambient environment), so compose decided the coordinator had drifted,
recreated it mid-drill, and the DDL apply then failed on evidence ownership. The
fix was one form everywhere; this test is what keeps it that way.

The rule, for shell and Makefile invocation sites of the 01_docker stack:

  FLAGS      carries both `--env-file .env` and `--env-file secrets.env`, and names
             the stack file (a literal `docker-compose.yml` path, or `-f "$VAR"`
             where the same file assigns VAR to that path).
  FLAGS_DIR  carries both env files and runs from the compose directory, where
             compose's default file resolution picks the same docker-compose.yml
             and derives the same project name (the directory) — equivalent, so no
             `-f` is required.
  VAR        uses a compose variable/array whose definition is itself checked here
             (Makefile COMPOSE, pipeline-lib.sh COMPOSE, catalog-guard.sh COMPOSE).
  TEXT       prose in a message, docstring or log line — no invocation.
  DIVERGENT  a known, deliberate deviation: registered so it cannot grow silently
             and so this test notices if someone fixes it. See COMPOSE_DEBT below.

Any site that matches none of these fails the test with file:line:text.
"""

from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
assert (ROOT / "Makefile").is_file(), f"repo root mis-derived: {ROOT}"

VERBS = (
    r"(up|down|ps|stop|start|restart|build|config|exec|run|logs|pull|kill|rm|create"
    r"|cp|events|images|port|top|version|wait|watch)"
)
CALL = re.compile(r"docker[ -]compose\s+.*?\b" + VERBS + r"\b")
COMPOSE_VAR = re.compile(r"(\$\(COMPOSE\)|\$\{COMPOSE\[@\]\}|\$COMPOSE\b)")


def _tracked_scripts() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "*.sh", "*.py", "Makefile", "*.mk"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout.split()
    # Not this file: its registry rows quote compose commands they do not run.
    self_rel = str(Path(__file__).resolve().relative_to(ROOT))
    return [f for f in out if f != self_rel and (f.startswith(("code/", "start-all")) or f == "Makefile")]


def _sites() -> list[tuple[str, int, str]]:
    """Every non-comment, non-echo line that invokes compose (backslash-joined)."""
    found: list[tuple[str, int, str]] = []
    for rel in _tracked_scripts():
        raw = (ROOT / rel).read_text(encoding="utf-8", errors="replace").split("\n")
        i = 0
        while i < len(raw):
            cmd, start = raw[i], i + 1
            while cmd.rstrip().endswith("\\") and i + 1 < len(raw):
                i += 1
                cmd = cmd.rstrip()[:-1] + " " + raw[i].strip()
            cmd = cmd.strip()
            if not cmd.startswith("#") and not re.match(r"@?echo\b", cmd):
                if CALL.search(cmd) or COMPOSE_VAR.search(cmd):
                    found.append((rel, start, cmd))
            i += 1
    return found


def _both_env_files(cmd: str) -> bool:
    return len(re.findall(r"--env-file\s+\S*\.env\b(?<!secrets\.env)", cmd)) >= 1 and "secrets.env" in cmd


def _stack_file(cmd: str, rel: str) -> bool:
    if re.search(r"(?:-f|--file)[= ]\S*docker-compose\.yml", cmd):
        return True
    # `-f "$VAR"`: accept when this file assigns VAR to a .../docker-compose.yml.
    text = (ROOT / rel).read_text(encoding="utf-8", errors="replace")
    for var in re.findall(r"(?:-f|--file)\s+\"?\$?\{?([A-Z_][A-Z0-9_]*)", cmd):
        if re.search(rf"^\s*{var}=.*docker-compose\.yml", text, re.M):
            return True
    return False


def _runs_in_compose_dir(cmd: str) -> bool:
    return bool(re.search(r"\(\s*cd\s+\"\$[A-Z_]+_DIR\"", cmd) or re.search(r"cd\s+\"\$\{?[A-Z_]+_DIR\}?\"", cmd))


# (file, distinctive substring of the command, kind, note)
SITES: list[tuple[str, str, str, str]] = [
    ("Makefile", "$(COMPOSE) up -d", "VAR", ""),
    ("Makefile", "$(COMPOSE) down", "VAR", ""),
    ("Makefile", "$(COMPOSE) logs -f", "VAR", ""),
    ("Makefile", "$(COMPOSE) build ddl-apply", "VAR", "ddl-image"),
    ("Makefile", "image_staleness_check.py --git-root", "TEXT", "message text"),
    ("code/01_platform/04_scripts/bench-throughput.sh", "secrets.env build ingestion", "FLAGS_DIR", ""),
    ("code/01_platform/04_scripts/bench-throughput.sh", "-f docker-compose.bench.yml stop ingestion", "FLAGS", ""),
    ("code/01_platform/04_scripts/bench-throughput.sh", "-f docker-compose.bench.yml up -d ingestion", "FLAGS", ""),
    ("code/01_platform/04_scripts/catalog-guard.sh", '"${COMPOSE[@]}" run --rm', "VAR", "array"),
    ("code/01_platform/04_scripts/catalog-guard.sh", '"${COMPOSE[@]}" ps -q zookeeper', "VAR", "array"),
    ("code/01_platform/04_scripts/ddl_apply_smoke.py", "the FULL containerized apply", "TEXT", "docstring"),
    ("code/01_platform/04_scripts/ddl_apply_smoke.py", "interpolate secrets:", "TEXT", "docstring"),
    ("code/01_platform/04_scripts/ddl_apply_smoke.py", "docker compose config invalid", "TEXT", "error string"),
    ("code/01_platform/04_scripts/gate_preflight.py", "The one compose form for this stack", "TEXT", "docstring"),
    ("code/01_platform/04_scripts/image_staleness_check.py", "image not built (docker compose build", "TEXT", "message"),
    ("code/01_platform/04_scripts/image_staleness_check.py", "--print-stamps-env) docker compose", "TEXT", "message"),
    ("code/01_platform/04_scripts/image_staleness_check.py", "reach docker compose (rebuild: make images)", "TEXT", "message"),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "a foreign or corrupt image", "TEXT", "G27 message"),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "is STALE — its build stamp", "TEXT", "G27 message"),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "$COMPOSE restart flink-taskmanager", "VAR", ""),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "$COMPOSE build loadgen", "VAR", ""),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "recreate the containers (a plain", "TEXT", "hint text"),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "$COMPOSE exec -T flink-taskmanager", "VAR", ""),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "$COMPOSE exec -T  -e ALLOW_FULL_REPLAY", "VAR", ""),
    ("code/01_platform/04_scripts/pipeline-lib.sh", "$COMPOSE exec -T flink-jobmanager flink cancel", "VAR", ""),
    ("code/01_platform/04_scripts/rollout-savepoint.sh", "DRY: docker compose -f $COMPOSE_FILE", "TEXT", "dry-run log text"),
    ("code/01_platform/04_scripts/run-full-suite.sh", "secrets.env build ingestion)", "FLAGS_DIR", ""),
    ("code/01_platform/04_scripts/run-full-suite.sh", "docker-compose.soak.yml up -d --force-recreate ingestion",
     "FLAGS_DIR", "soak override — --force-recreate so a leaked container cannot serve stale state"),
    ("code/01_platform/04_scripts/run-full-suite.sh", "ps -q ingestion", "FLAGS_DIR",
     "container discovery (P6-513): preflight double-run guard and post-`up` id lookup"),
    ("code/01_platform/04_scripts/run-full-suite.sh", "docker-compose.soak.yml stop ingestion", "FLAGS_DIR", "soak override"),
    ("code/01_platform/04_scripts/run-monday-gates.sh", "config >/dev/null 2>>\"$STATIC_LOG\"", "FLAGS", "gate step 2"),
    ("code/01_platform/04_scripts/run-monday-gates.sh", "docker compose version", "TEXT",
     "gate step 18 capability probe — asks whether the compose plugin exists, never invokes the stack"),
    ("code/01_platform/04_scripts/stage-a2-baseline.sh", "secrets.env up -d flink-jobmanager", "FLAGS_DIR", ""),
    ("code/01_platform/04_scripts/stage-a2-baseline.sh", "$COMPOSE exec -T flink-taskmanager", "VAR", ""),
    ("code/01_platform/04_scripts/stage-soak-e2e.sh", "secrets.env exec -T flink-jobmanager flink cancel", "FLAGS_DIR", ""),
    ("code/01_platform/04_scripts/stage-soak-e2e.sh", "secrets.env restart flink-taskmanager", "FLAGS_DIR", ""),
    ("code/01_platform/04_scripts/stage-soak-e2e.sh", "is STALE (build stamp", "TEXT", "message"),
    ("code/01_platform/04_scripts/stage-soak-e2e.sh", "secrets.env exec -T flink-taskmanager sh -c", "FLAGS_DIR", ""),
    ("code/01_platform/04_scripts/t9_order_sandbox.py", "probes go through", "TEXT", "docstring"),
    ("code/01_platform/04_scripts/tests/test_08_local_compose_l0.py", "config must succeed (YAML parses", "TEXT", "docstring"),
    ("code/01_platform/04_scripts/tests/test_08_local_compose_l0.py", "config must not leak secret values", "TEXT", "docstring"),
    ("code/01_platform/04_scripts/tests/test_08_local_compose_l2.py", "invocation, and", "TEXT", "comment in docstring"),
    ("code/01_platform/04_scripts/tests/test_gate_preflight.py", "The preflight's compose form must stay identical", "TEXT", "docstring"),
    ("start-all.sh", "secrets.env up -d zookeeper", "FLAGS_DIR", ""),
    ("start-all.sh", "check docker compose logs", "TEXT", "error message"),
]

# Compose variables: the definitions this test trusts (checked by test_definitions).
DEFS: list[tuple[str, str, str]] = [
    ("Makefile", "COMPOSE := docker compose", "COMPOSE"),
    ("code/01_platform/04_scripts/catalog-guard.sh", "COMPOSE=(docker compose", "COMPOSE"),
    ("code/01_platform/04_scripts/pipeline-lib.sh", 'COMPOSE="docker compose', "COMPOSE"),
]

COMPOSE_DEBT = """
Compose-form debt: none. The four bare-form sites listed here until 2026-09-13
(run-full-suite.sh x3, start-all.sh) now carry both --env-file flags, so the soak
path interpolates the same config as the stack it drives. The registry stays for
both directions: a DIVERGENT row asserts the site is still divergent, and a
canonical row asserts the site still carries both flags — so this cannot silently
come back either.
"""


class ComposeFormContract(unittest.TestCase):
    def test_every_site_is_registered(self) -> None:
        unmatched = []
        for rel, line, cmd in _sites():
            if not any(rel == f and pat in cmd for f, pat, _kind, _n in SITES):
                unmatched.append(f"{rel}:{line}: {cmd[:150]}")
        self.assertEqual(unmatched, [], "unregistered compose invocation site(s):\n  " + "\n  ".join(unmatched))

    def test_no_dead_registry_entries(self) -> None:
        sites = _sites()
        dead = [
            f"{f}: {pat!r}"
            for f, pat, kind, _n in SITES
            if not any(rel == f and pat in cmd for rel, _line, cmd in sites)
        ]
        self.assertEqual(dead, [], "registry entries that no longer match any site:\n  " + "\n  ".join(dead))

    def test_canonical_sites_carry_both_env_files(self) -> None:
        for rel, line, cmd in _sites():
            for f, pat, kind, _n in SITES:
                if rel != f or pat not in cmd:
                    continue
                if kind == "FLAGS":
                    self.assertTrue(_both_env_files(cmd), f"{f}:{line}: missing an --env-file\n{cmd[:200]}")
                    self.assertTrue(_stack_file(cmd, f), f"{f}:{line}: does not name the stack file\n{cmd[:200]}")
                elif kind == "FLAGS_DIR":
                    self.assertTrue(_both_env_files(cmd), f"{f}:{line}: missing an --env-file\n{cmd[:200]}")
                    self.assertTrue(_runs_in_compose_dir(cmd), f"{f}:{line}: not run from the compose dir\n{cmd[:200]}")
                elif kind == "VAR":
                    self.assertRegex(cmd, r"(\$\(COMPOSE\)|COMPOSE\[@\]|\$COMPOSE\b)",
                                     f"{f}:{line}: not using the canonical compose variable")

    def test_definitions_are_canonical(self) -> None:
        text = {rel: (ROOT / rel).read_text(encoding="utf-8") for rel, _p, _v in DEFS}
        for rel, pat, var in DEFS:
            hits = [l for l in text[rel].split("\n") if l.strip().startswith(pat.split()[0]) and pat in l]
            self.assertEqual(len(hits), 1, f"{rel}: expected one `{pat}` definition, found {len(hits)}")
            self.assertTrue(_both_env_files(hits[0]), f"{rel}: {var} lacks an --env-file: {hits[0][:160]}")
            self.assertTrue(_stack_file(hits[0], rel), f"{rel}: {var} does not name the stack file: {hits[0][:160]}")

    def test_known_divergences_are_still_divergent(self) -> None:
        debt = [(f, pat, n) for f, pat, kind, n in SITES if kind == "DIVERGENT"]
        for rel, line, cmd in _sites():
            for f, pat, note in debt:
                if rel == f and pat in cmd:
                    self.assertFalse(
                        _both_env_files(cmd),
                        f"{f}:{line}: this site now carries both env files — remove it from "
                        f"the DIVERGENT registry and the debt report:\n{cmd[:200]}",
                    )
        print(COMPOSE_DEBT)


if __name__ == "__main__":
    unittest.main()
