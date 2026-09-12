#!/usr/bin/env python3
"""implementation_gate.py — mandatory implementation order gate.

01-foundation.md "Mandatory implementation order" (orig L60): the 7 fixed
tasks run in strict sequence and no downstream task invents an upstream
contract. This gate executes each task's acceptance checks IN ORDER and
refuses to run task N+1 while task N is red (failing check) or missing
(required evidence absent). It is the enforcement mechanism for a rule
that previously existed only as documentation ("Location: _not implemented_").

Usage:
    python3 code/01_platform/04_scripts/implementation_gate.py   # run the gate
    python3 code/01_platform/04_scripts/implementation_gate.py --list
    make gate-order

Exit code: 0 = all 7 tasks pass in order; 1 = a task failed or is missing
evidence, and every downstream task was blocked (not run).

Check primitives (each check is a dict with "type" + "desc"):
    run            {"cmd": ...}                shell command must exit 0
    file           {"path": ...}               file must exist
    contains       {"path": ..., "needle": ...} substring must appear in file
    tree-contains  {"dir": ..., "needle": ...} some *.java under dir must
                                               reference the needle

The runner is injectable so the gate logic is unit-testable without
executing any mvn command (tests/code/01_platform/04_scripts/tests/).
"""

import os
import subprocess
import sys

ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

TASKS = [
    {
        "seq": 1,
        "title": "Make the mock broker per-instrument, variable, and deterministic",
        "dossier": "docs/08_implementation/11-testing-and-release.md#performance-benchmark-procedure",
        "checks": [
            {
                "type": "file",
                "path": "code/02_services/05_mock_arrow/src/test/java/com/trading/mockarrow/SyntheticWorkloadTest.java",
                "desc": "MOCK-UNIT-001..003 / MOCK-PERF-001 harness (SyntheticWorkloadTest)",
            },
            # 05_mock_arrow is a standalone module (not in the parent reactor,
            # which lists only common + 01_ingestion), so its suite runs from
            # the module dir (common is not a dependency here).
            {
                "type": "run",
                "cmd": "cd code/02_services/05_mock_arrow && mvn -q test",
                "desc": "05_mock_arrow acceptance suite green",
            },
        ],
    },
    {
        "seq": 2,
        "title": "Implement immediate, bounded ingestion writes",
        "dossier": "docs/08_implementation/03-ingestion.md",
        "checks": [
            {
                "type": "contains",
                "path": "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java",
                "needle": "INGESTION_MAX_BATCH_RECORDS",
                "desc": "immediate no-batch pins (INGESTION_MAX_BATCH_RECORDS=1)",
            },
            {
                "type": "contains",
                "path": "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java",
                "needle": "MAX_PENDING_APPEND_RECORDS",
                "desc": "bounded-backpressure pins (MAX_PENDING_APPEND_RECORDS/BYTES)",
            },
            {
                "type": "run",
                "cmd": "cd code && mvn -q test -pl 02_services/01_ingestion -am",
                "desc": "ingestion acceptance suite green",
            },
        ],
    },
    {
        "seq": 3,
        "title": "Implement compact, bounded Signal-job state",
        "dossier": "docs/08_implementation/04-signal-job.md",
        "checks": [
            {
                "type": "file",
                "path": "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FingerprintDedupFunction.java",
                "desc": "DEC-038 externalized dedup (FingerprintDedupFunction)",
            },
            {
                "type": "file",
                # CandleEmitFunction was retired by 0f3e595 (15s candle path);
                # CandleAccumulator carries the same pin today — its own header
                # calls it "the ONLY candle state that exists", deliberately
                # compact (OHLCV plus identity columns).
                "path": "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleAccumulator.java",
                "desc": "OHLCV-only candle state (CandleAccumulator)",
            },
            # 02_compute is also standalone; common must be resolvable (installed
            # in ~/.m2 or built first).
            {
                "type": "run",
                "cmd": "cd code/02_services/02_compute && mvn -q test",
                "desc": "signal-job acceptance suite green (STATE-DEDUP-001 / STATE-CANDLE-001)",
            },
        ],
    },
    {
        "seq": 4,
        "title": "Bound candidate work and preserve in-job ranking",
        "dossier": "docs/08_implementation/04-signal-job.md (Ranking section)",
        "checks": [
            {
                "type": "contains",
                "path": "code/common/src/main/java/com/trading/common/config/PlatformConfig.java",
                "needle": "MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT = 1",
                "desc": "MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT=1 pin",
            },
        ],
    },
    {
        "seq": 5,
        "title": "Pin job recovery and container-memory settings",
        "dossier": "docs/08_implementation/09-production-swarm.md",
        "checks": [
            {
                "type": "contains",
                "path": "code/common/src/main/java/com/trading/common/config/PlatformConfig.java",
                "needle": "CHECKPOINT_INTERVAL_MS = 10_000L",
                "desc": "checkpoint interval 10 s pin",
            },
            {
                "type": "contains",
                "path": "code/common/src/main/java/com/trading/common/config/PlatformConfig.java",
                "needle": "CHECKPOINT_TIMEOUT_MS = 30_000L",
                "desc": "checkpoint timeout 30 s pin",
            },
            {
                "type": "contains",
                "path": "code/common/src/main/java/com/trading/common/config/PlatformConfig.java",
                "needle": "MAX_CONCURRENT_CHECKPOINTS = 1",
                "desc": "MAX_CONCURRENT_CHECKPOINTS=1 pin",
            },
            {
                "type": "contains",
                "path": "code/common/src/main/java/com/trading/common/config/PlatformConfig.java",
                "needle": "JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65",
                "desc": "JVM heap 65% pin",
            },
            {
                "type": "contains",
                "path": "code/common/src/main/java/com/trading/common/config/PlatformConfig.java",
                "needle": "NON_HEAP_MEMORY_RESERVE_PERCENT = 35",
                "desc": "non-heap reserve 35% pin",
            },
            {
                "type": "file",
                "path": "code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/SignalJobObjectStoreCheckpointIntegrationTest.java",
                "desc": "S3 checkpoint storage evidence test",
            },
        ],
    },
    {
        "seq": 6,
        "title": "Keep Babysitter state minimal",
        "dossier": "docs/08_implementation/05-execution-core.md",  # Babysitter absorbed (2026-08-18)
        "checks": [
            {
                "type": "contains",
                "path": "code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java",
                "needle": "POSITION_ACTIONS_ENABLED = false",
                "desc": "POSITION_ACTIONS_ENABLED hard-coded false",
            },
            {
                "type": "contains",
                "path": "code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java",
                "needle": "POSITION_ACTIONS_ENABLED must be false in MVP",
                "desc": "startup rejects POSITION_ACTIONS_ENABLED=true",
            },
            {
                "type": "tree-contains",
                "dir": "code/02_services/02_compute/src/test",
                "needle": "BabysitterJob",
                "desc": "BAB-* acceptance test references BabysitterJob (BABYSITTER-001)",
            },
        ],
    },
    {
        "seq": 7,
        "title": "Implement required alerts and safe-stop conditions",
        "dossier": "docs/08_implementation/10-observability.md",
        "checks": [
            {
                "type": "file",
                "path": "code/02_services/02_compute/src/main/java/com/trading/compute/safetyhalt/SafetyHaltJob.java",
                "desc": "Safety_Halt_Request publisher (SafetyHaltJob)",
            },
            {
                "type": "file",
                "path": "code/02_services/02_compute/src/test/java/com/trading/compute/safetyhalt/SafetyHaltLiveIntegrationTest.java",
                "desc": "safe-stop acceptance test",
            },
            {
                "type": "contains",
                "path": "docs/08_implementation/10-observability.md",
                "needle": "60-second consecutive breach",
                "desc": "60 s consecutive breach window for alerts",
            },
            {
                "type": "contains",
                "path": "docs/08_implementation/10-observability.md",
                "needle": "Do not resume automatically",
                "desc": "no auto-resume (reconciliation/approval only)",
            },
        ],
    },
]


class GateRunner:
    """Executes the check primitives against the repository. The command
    runner is injectable so unit tests never invoke mvn."""

    def __init__(self, root, run_cmd=None):
        self.root = root
        self._run_cmd = run_cmd or self._default_run

    def _default_run(self, cmd):
        try:
            p = subprocess.run(
                cmd, shell=True, cwd=self.root, capture_output=True, text=True,
                timeout=1800,
            )
        except subprocess.TimeoutExpired:
            return False, "command timed out after 1800 s"
        out = ((p.stdout or "")[-1500:] + (p.stderr or "")[-1500:]).strip()
        return p.returncode == 0, (out or f"exit {p.returncode}")

    def run(self, cmd):
        return self._run_cmd(cmd)

    def file(self, path):
        full = os.path.join(self.root, path)
        ok = os.path.isfile(full)
        return ok, (full if ok else f"missing evidence: {path}")

    def contains(self, path, needle):
        full = os.path.join(self.root, path)
        try:
            with open(full, encoding="utf-8", errors="replace") as fh:
                txt = fh.read()
        except OSError as exc:
            return False, f"unreadable: {path} ({exc})"
        ok = needle in txt
        return ok, (
            f"'{needle}' present in {path}" if ok else f"'{needle}' NOT present in {path}"
        )

    def tree_contains(self, rel_dir, needle):
        base = os.path.join(self.root, rel_dir)
        if not os.path.isdir(base):
            return False, f"missing evidence dir: {rel_dir}"
        for root, _, files in os.walk(base):
            for f in files:
                if not f.endswith(".java"):
                    continue
                p = os.path.join(root, f)
                try:
                    with open(p, encoding="utf-8", errors="replace") as fh:
                        txt = fh.read()
                except OSError:
                    continue
                if needle in txt:
                    rel = os.path.relpath(p, self.root)
                    return True, f"{needle} referenced by {rel}"
        return False, f"'{needle}' NOT referenced under {rel_dir}"

    def check(self, chk):
        t = chk["type"]
        if t == "run":
            return self.run(chk["cmd"])
        if t == "file":
            return self.file(chk["path"])
        if t == "contains":
            return self.contains(chk["path"], chk["needle"])
        if t == "tree-contains":
            return self.tree_contains(chk["dir"], chk["needle"])
        raise ValueError(f"unknown check type: {t}")


def run_gate(tasks, runner, out=print):
    """Run tasks strictly in sequence; stop at the first failing or missing check."""
    blocked = None
    for task in tasks:
        out(f"[gate] Task {task['seq']}: {task['title']}")
        failed_check = None
        for chk in task["checks"]:
            ok, detail = runner.check(chk)
            out(f"  [{'PASS' if ok else 'FAIL'}] {chk['desc']}")
            if not ok:
                out(f"        {detail}")
                failed_check = chk
                break
        if failed_check:
            blocked = task
            break
        out(f"  -> task {task['seq']} acceptance green")
    if blocked:
        nxt = blocked["seq"] + 1
        out(
            f"\nimplementation-gate: task {blocked['seq']} FAILED — downstream "
            f"tasks {nxt}..{len(tasks)} BLOCKED (not run); fix task {blocked['seq']} "
            f"evidence before proceeding."
        )
        return 1
    out(
        f"\nimplementation-gate: all {len(tasks)} tasks pass in order — "
        f"implementation order gate GREEN"
    )
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if "--list" in argv:
        for task in TASKS:
            print(f"{task['seq']}. {task['title']}  [{task['dossier']}]")
            for chk in task["checks"]:
                print(f"     - {chk['desc']}")
        return 0
    runner = GateRunner(ROOT)
    return run_gate(TASKS, runner)


if __name__ == "__main__":
    sys.exit(main())
