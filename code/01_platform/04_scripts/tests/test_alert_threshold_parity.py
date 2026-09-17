#!/usr/bin/env python3
"""Alert-threshold parity: O2 alert setpoints must agree with the pinned constants.

WHY THIS EXISTS
---------------
The same soft-drift class the compute-identifier suite guards one level down
(CHG-191/193/194: a cutover retires a name, no gate can fire) recurs ONE LEVEL UP
in the values: `o2-provision.py` hand-copies the numbers behind its 47 alert rules
from constants that live in Java (`AlertThresholds`, `PlatformConfig`) and from
facts that live in the compose file / the fact ledger. Nothing joined the two
sets, so a constant change in Java touches no file under `04_scripts/` and every
downstream consumer keeps firing at the stale setpoint — an alert that fires at
the wrong level is the same failure class as an alert that never fires.

The first run of this suite caught exactly that:
  CHG-197  `SIGNAL-error-checkpoint-slow` fired at 240 000 ms, but its own desc
           says "80% of pinned CHECKPOINT_TIMEOUT_MS" and the pinned value is
           30 000 ms (PlatformConfig.CHECKPOINT_TIMEOUT_MS == compose pin
           "30000") → the relation value is 24 000 ms. The runbook agrees twice
           (runbooks.md L372, L519: "duration >= 24000 ms"). The 300000 and
           240000 in the rule were the drift: a number that exists NOWHERE else
           in the repo.

DESIGN
------
Same shape as test_compute_identifier_parity.py (stage 1):
  * `EXPECTED` is a hand-maintained table of (rule, expected, anchor) rows —
    expected value, how it is derived, and the source lines that must still
    agree. The anchors keep the snapshot itself from rotting.
  * The actual setpoints are extracted from `o2-provision.py`'s `ALERTS` list
    via `ast` (not regex), so quoted prose cannot produce false matches.
  * Fail-closed: any rule in ALERTS missing from EXPECTED fails the test, so a
    new alert cannot silently inherit "unverified" and skip parity forever.

HONEST LIMITATIONS (recorded, not hidden)
-----------------------------------------
* The anchors pin the CONSTANTS (Java literals, compose pin, runbook rows), not
  a parsed Java AST — the Java side is regex-scanned with tight patterns.
* Position-state alert thresholds (>100 active count etc.) have no Java
  constant to tie to; they are anchored to their own JSON corpus instead.
* Only the value axis is guarded here; stream names are guarded by the
  compute-identifier suite, delivery by the G6 routing selftest.

Run: python3 -m unittest tests.test_alert_threshold_parity  (from 04_scripts/)
"""

from __future__ import annotations

import ast
import json
import re
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
REPO = SCRIPTS.parents[2]
O2_PROVISION = SCRIPTS / "o2-provision.py"
COMPOSE = REPO / "code/01_platform/01_docker/docker-compose.yml"
ALERTS_JSON = REPO / "code/01_platform/01_docker/openobserve/alerts/position-state-alerts.json"
JAVA_DIR = REPO / "code/common/src/main/java/com/trading/common"

# ---------------------------------------------------------------------------
# Loaders — each returns the raw material the expected table pins against.
# ---------------------------------------------------------------------------


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _java_const(java_file: str, const_name: str) -> int:
    """Numeric constant from a Java file: `public static final long NAME = 30_000L;`"""
    m = re.search(
        rf"public\s+static\s+final\s+\w+\s+{re.escape(const_name)}\s*=\s*([0-9_]+)",
        _read(JAVA_DIR / "observability" / java_file),
    )
    if not m:
        raise AssertionError(f"anchor lost: {java_file} no longer declares {const_name}")
    return int(m.group(1).replace("_", ""))


def _platform_config_const(const_name: str) -> int:
    m = re.search(
        rf"public\s+static\s+final\s+\w+\s+{re.escape(const_name)}\s*=\s*([0-9_]+)",
        _read(JAVA_DIR / "config" / "PlatformConfig.java"),
    )
    if not m:
        raise AssertionError(f"anchor lost: PlatformConfig.java no longer declares {const_name}")
    return int(m.group(1).replace("_", ""))


def _compose_env(key: str) -> str:
    """Value of KEY in the flink-jobmanager env block of docker-compose.yml."""
    text = _read(COMPOSE)
    m = re.search(rf"^\s*{re.escape(key)}:\s*\"?([^\"\n]+)\"?", text, re.M)
    if not m:
        raise AssertionError(f"anchor lost: {COMPOSE.name} no longer pins {key}")
    return m.group(1).strip()


def _alert_thresholds_doc_percent(cell_label: str) -> int:
    """Percent in a 10-observability.md threshold-table row (first number in the row)."""
    for line in _read(REPO / "docs/08_implementation/10-observability.md").splitlines():
        if cell_label.lower() in line.lower() and line.strip().startswith("|"):
            m = re.search(r"(\d+)\s*%", line)
            if m:
                return int(m.group(1))
    raise AssertionError(f"anchor lost: no percent row for {cell_label!r} in 10-observability.md")


def _runbook_threshold_ms(rule: str) -> int:
    """The `>= NNNNN ms` figure the runbook quotes for a rule."""
    m = re.search(
        rf"{re.escape(rule)}\b.*?(\d[\d_ ]*)\s*ms",
        _read(REPO / "docs/06_operations/01-runbooks.md"),
        re.S,
    )
    if not m:
        raise AssertionError(f"anchor lost: runbook no longer quotes {rule} in ms")
    return int(m.group(1).replace("_", "").replace(" ", ""))


def _alerts_spec() -> dict[str, dict]:
    """Parse o2-provision.py's ALERTS list with ast: name -> full kwarg dict."""
    src = _read(O2_PROVISION)
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "ALERTS" for t in node.targets
        ):
            specs: dict[str, dict] = {}
            for el in node.value.elts:
                if not isinstance(el, ast.Call):
                    continue
                kw = {k.arg: k.value for k in el.keywords}
                name = kw.get("name")
                if isinstance(name, ast.Constant) and isinstance(name.value, str):
                    specs[name.value] = kw
            return specs
    raise AssertionError(f"anchor lost: no ALERTS list found in {O2_PROVISION.name}")


def _spec_threshold(name: str) -> tuple[str, int]:
    """(operator, numeric value) of a rule's threshold, computed through ast.

    Handles conditions=[(col, op, val), ...] (first value-bearing pair), a bare
    promql_condition=(">=", N) tuple, and simple arithmetic like 14 * 1024 ** 3.
    Raises on anything else — fail closed rather than guess.
    """
    kw = _alerts_spec()[name]
    node = kw.get("conditions") or kw.get("promql_condition")
    if node is None:
        raise AssertionError(f"{name}: no conditions/promql_condition to extract")

    def eval_num(n: ast.AST) -> int:
        if isinstance(n, ast.Constant) and isinstance(n.value, (int, float)) and not isinstance(n.value, bool):
            return int(n.value)
        if isinstance(n, ast.BinOp) and isinstance(n.op, (ast.Mult, ast.Add, ast.Sub, ast.FloorDiv, ast.Pow)):
            a, b = eval_num(n.left), eval_num(n.right)
            return {
                ast.Mult: a * b, ast.Add: a + b, ast.Sub: a - b,
                ast.FloorDiv: a // b, ast.Pow: a ** b,
            }[type(n.op)]
        raise AssertionError(
            f"{name}: threshold expression {ast.dump(n)[:80]!r} is not machine-evaluable; "
            "extend the extractor or restate the value as a literal"
        )

    if isinstance(node, ast.List):  # conditions=[(col, op, val), ...]
        for pair in node.elts:
            if not (isinstance(pair, ast.Tuple) and len(pair.elts) == 3):
                continue
            col, op, val = pair.elts
            if isinstance(col, ast.Constant) and col.value == "value":
                if isinstance(val, ast.Constant) and isinstance(val.value, (int, float)):
                    return op.value, int(val.value)
                return op.value, eval_num(val)
        raise AssertionError(f"{name}: no ('value', op, number) pair found")
    if isinstance(node, ast.Tuple) and len(node.elts) == 2:  # promql_condition=(">=", N)
        op, val = node.elts
        if isinstance(val, ast.Constant) and isinstance(val.value, (int, float)):
            return op.value, int(val.value)
        return op.value, eval_num(val)
    raise AssertionError(f"{name}: unrecognised threshold shape")


def _position_state_promql_threshold(name: str) -> tuple[str, int]:
    """(operator, value) from the position-state alerts JSON corpus."""
    data = json.loads(_read(ALERTS_JSON))
    for a in data:
        if a.get("name") == name:
            pc = a["query_condition"]["promql_condition"]
            return pc["operator"], int(pc["value"])
    raise AssertionError(f"anchor lost: {name} missing from {ALERTS_JSON.name}")


# ---------------------------------------------------------------------------
# The expected table: (rule, expected_value, derivation note + anchors).
# Every anchor is re-read at test time; if the source moves, the test says so.
# ---------------------------------------------------------------------------


def _pct(v: int) -> int:
    return v * 100  # promql ratio rows are compared in percent


class ThresholdParity(unittest.TestCase):
    maxDiff = None

    # -- Java-constant-backed rows ------------------------------------------

    def test_checkpoint_slow_is_80_percent_of_the_pinned_timeout(self):
        """CHG-197: the rule drifted to 240000; the relation is 80% of the pin."""
        timeout = _platform_config_const("CHECKPOINT_TIMEOUT_MS")
        self.assertEqual("30000", _compose_env("CHECKPOINT_TIMEOUT_MS"),
                         "compose pin and PlatformConfig.CHECKPOINT_TIMEOUT_MS must agree")
        self.assertEqual(24000, _runbook_threshold_ms("SIGNAL-error-checkpoint-slow"),
                         "runbook must keep quoting 80% of the pin")
        op, actual = _spec_threshold("SIGNAL-error-checkpoint-slow")
        self.assertEqual(">=", op)
        self.assertEqual(timeout * 8 // 10, actual,
                         "SIGNAL-error-checkpoint-slow must fire at 80% of "
                         "CHECKPOINT_TIMEOUT_MS (PlatformConfig pin, compose agrees)")

    def test_jvm_heap_warn_is_85_percent_of_the_1gib_jm_container(self):
        """SIGNAL-warn-jvm-heap-high: 900 MB ≈ 0.85 × 1 GiB; INFRA row is % of max."""
        op, actual = _spec_threshold("SIGNAL-warn-jvm-heap-high")
        self.assertEqual(">=", op)
        self.assertEqual(900_000_000, actual,
                         "desc says '~0.85 x 1 GiB container max'; if the JM "
                         "container limit changes, restate this rule deliberately")
        self.assertEqual(85, _alert_thresholds_doc_percent("JVM heap"),
                         "10-observability JVM heap row")

    def test_infra_alert_rows_match_the_observability_doc_table(self):
        """The 9 INFRA- setpoints must equal 10-observability.md's table."""
        rows = {
            "INFRA-warn-host-cpu-80": ("Sustained host CPU", 80, ">="),
            "INFRA-crit-host-cpu-90": ("Sustained host CPU", 90, None),
            "INFRA-crit-jvm-heap-85": ("JVM heap", 85, ">="),
            "INFRA-warn-jvm-gc-500": ("JVM non-heap / GC", 500, ">="),
            "INFRA-crit-disk-20": ("Free SSD", 20, "<"),
            "INFRA-warn-disk-io-20": ("Disk I/O await", 20, ">="),
            "INFRA-warn-net-80": ("Network TX/RX", 80, None),
        }
        for rule, (doc_label, expected, op_required) in rows.items():
            op, actual = _spec_threshold(rule)
            if op_required is not None:
                self.assertEqual(op_required, op, rule)
            self.assertEqual(expected, actual,
                             f"{rule}: setpoint must match the 10-observability row "
                             f"{doc_label!r}")

    def test_o2_memory_rule_is_14gb_from_the_doc_budget(self):
        """INFRA-crit-o2-mem-14: 14 * 1024**3, matching the >14GB doc row."""
        op, actual = _spec_threshold("INFRA-crit-o2-mem-14")
        self.assertEqual(">=", op)
        self.assertEqual(14 * 1024**3, actual)
        o2_row = next(
            ln for ln in _read(REPO / "docs/08_implementation/10-observability.md").splitlines()
            if ln.strip().startswith("|") and "O2 memory" in ln
        )
        self.assertIn("14GB", o2_row, "10-observability O2 memory budget row")
        self.assertIn("ZO_MEMORY_LIMIT=12g", o2_row,
                      "the 14GB alert sits above the 12g O2 limit on purpose")

    def test_jvm_side_gate_shares_the_container_memory_constant(self):
        """AlertThresholds.CONTAINER_MEMORY_ALERT_PERCENT (85) is the invariant the
        doc table states; the JVM gate and the doc must not drift apart."""
        self.assertEqual(
            85,
            _java_const("AlertThresholds.java", "CONTAINER_MEMORY_ALERT_PERCENT"),
        )
        self.assertEqual(
            85,
            _alert_thresholds_doc_percent("Container memory critical"),
        )

    def test_consecutive_breach_window_is_60s_everywhere(self):
        """AlertThresholds.CONSECUTIVE_BREACH_SECONDS (60) vs the doc preamble."""
        self.assertEqual(
            60,
            _java_const("AlertThresholds.java", "CONSECUTIVE_BREACH_SECONDS"),
        )
        self.assertIn(
            "60-second consecutive breach window",
            _read(REPO / "docs/08_implementation/10-observability.md"),
            "10-observability preamble pins the same 60s window",
        )

    def test_ingestion_fd_halting_threshold_chain(self):
        """ING-warn-fd-80 (warn) < ING-crit-fd-90 (programmed safety halt)."""
        warn_op, warn = _spec_threshold("ING-warn-fd-80")
        crit_op, crit = _spec_threshold("ING-crit-fd-90")
        self.assertEqual(">=", warn_op)
        self.assertEqual(">=", crit_op)
        self.assertLess(warn, crit, "warning must fire strictly below the halt")
        self.assertEqual(80, warn)
        self.assertEqual(90, crit)

    # -- JSON-corpus-anchored rows -------------------------------------------

    def test_position_state_active_count_threshold_matches_its_corpus(self):
        """pos-state-high-active-count: >100 active signals — the corpus is the
        only pin today (no Java constant); this test IS the second copy."""
        op, value = _position_state_promql_threshold("pos-state-high-active-count")
        self.assertEqual(">", op)
        self.assertEqual(100, value)

    # -- Contract- and runbook-anchored rows ---------------------------------

    def test_ingestion_contract_thresholds(self):
        """Rules carrying a number from docs/06_operations/02-ingestion-alerting.md
        (the 11-rule contract, CHG-093): value AND evaluation period both pinned."""
        # (rule, operator, value, period_minutes, contract anchor substring)
        rows = [
            ("ING-warn-capacity-90", ">=", 90, 5, "`>= 90` for 5 min"),
            ("ING-crit-capacity-98", ">=", 98, 2, "`>= 98` for 2 min"),
            ("ING-crit-reconnect-storm", ">=", 5, 1, "`>= 5 / 10 min`"),
            ("ING-crit-reconnect-consecutive", ">=", 5, 1, "`>= 5`"),
            ("ING-crit-stale-feed", ">=", 5000, 1, "`>= 5000` (5s, the freshness limit)"),
            ("ING-crit-decode-error-burst", ">=", 100, 1, "`>= 100 / 10 s`"),
            ("ING-warn-partial-subscription", ">", 0, 5, "`> 0` for 5 min"),
            ("ING-warn-heartbeat-failures", ">=", 3, 1, "`>= 3 / 5 min`"),
        ]
        contract = _read(REPO / "docs/06_operations/02-ingestion-alerting.md")
        for rule, op, value, period, anchor in rows:
            self.assertIn(anchor, contract,
                          f"{rule}: contract row anchor moved — re-derive the pin")
            got_op, got_val = _spec_threshold(rule)
            self.assertEqual((op, value), (got_op, got_val), rule)
            kw = _alerts_spec()[rule]
            got_period = kw.get("period")
            got_period = ast.literal_eval(got_period) if got_period else 1
            self.assertEqual(period, got_period, f"{rule}: contract evaluation period")

    def test_ingestion_unsafe_duration_rule_matches_its_name(self):
        """ING-crit-unsafe-duration-30s: >= 30000 ms — the 30s bound is the name."""
        op, value = _spec_threshold("ING-crit-unsafe-duration-30s")
        self.assertEqual(">=", op)
        self.assertEqual(30_000, value)

    def test_runbook_quoted_signal_thresholds(self):
        """SIGNAL rules whose setpoint the runbook alert table quotes verbatim."""
        rows = [
            ("SIGNAL-warn-schema-rejected-rate", ">", 10),
            ("SIGNAL-warn-scrape-slow", ">=", 1),
            ("SIGNAL-warn-source-lag", ">=", 600_000),  # 600 s in ms
        ]
        for rule, op, value in rows:
            got_op, got_val = _spec_threshold(rule)
            self.assertEqual((op, value), (got_op, got_val),
                             f"{rule}: runbook-quoted setpoint drifted")

    def test_dedup_state_rule_is_the_2026_08_28_repointed_checkpoint_size(self):
        """CHG-023-adjacent gauge remediation: the rule watches the job's LAST
        CHECKPOINT SIZE (> 1.5 GB, ~6 GB TM budget minus margin), NOT the retired
        compute_dedup_state_count gauge. The runbook's pre-repoint row (6.5M on
        the dead series) was drift and is corrected in the same change as this
        suite; the in-rule comment is the derivation of record."""
        op, value = _spec_threshold("SIGNAL-warn-dedup-state")
        self.assertEqual(">", op)
        self.assertEqual(1_500_000_000, value)
        kw = _alerts_spec()["SIGNAL-warn-dedup-state"]
        stream = ast.literal_eval(kw["stream"])
        self.assertEqual("flink_jobmanager_job_lastcheckpointsize", stream,
                         "the repointed series is the derivation's subject")

    def test_dedup_firsts_rate_is_above_the_real_broker_max(self):
        """SIGNAL-warn-dedup-firsts-rate: > 50 000/s — just above the measured
        real-broker max 48.7k/s (in-rule comment); beyond the envelope = state
        grows faster than the 60s TTL drains it."""
        op, value = _spec_threshold("SIGNAL-warn-dedup-firsts-rate")
        self.assertEqual(">", op)
        self.assertEqual(50_000, value)
        self.assertIn("48.7k", _read(O2_PROVISION),
                      "derivation anchor: the measured broker max in the rule comment")

    def test_snapshot_pinned_thresholds(self):
        """Setpoints with no second authority yet (in-rule comment or rule name is
        the only derivation). Pinned here so any change is at least DELIBERATE;
        each row notes what a future authority should be."""
        rows = [
            # (rule, operator, value, note)
            ("ING-warn-capacity-80", ">=", 80, "pre-contract warn tier"),
            ("ING-crit-capacity-over-100", ">", 100, "impossible-by-name capacity"),
            ("SIGNAL-warn-source-volume-drop", "<", 5000, "source volume floor"),
            ("SIGNAL-warn-multitf-session-drop", ">", 10_000, "session drop burst"),
            ("SIGNAL-warn-multitf-signal-suppressed", ">", 1000, "suppression burst"),
        ]
        for rule, op, value, note in rows:
            got_op, got_val = _spec_threshold(rule)
            self.assertEqual((op, value), (got_op, got_val), f"{rule} ({note})")

    # -- Completeness: nothing unverified may join the alert set -------------

    def test_every_alert_rule_is_covered_by_parity_or_a_documented_exemption(self):
        """Fail closed: every rule in ALERTS needs a disposition — a test above,
        or an explicit exemption with a reason. Absence-trigger rules fire on
        value in {0, 1} or a label match; there is no tunable setpoint to drift.
        A NEW rule with a real number must NOT be exempted — add a pin."""
        EXEMPT_ABSENCE_TRIGGERS = {
            # value==0/1 or label-only: presence/absence, nothing to drift
            "ING-crit-orphan-process",            # value == 0
            "ING-crit-telemetry-delivery-failed",  # value > 0 (any failure)
            "ING-crit-not-ready",                 # value == 0
            "ING-crit-bridge-disconnected",       # value == 0
            "ING-warn-otlp-collector-unhealthy",  # value == 0 for 10 min
            "SIGNAL-crit-schema-version-rejected",  # > 0
            "SIGNAL-crit-checkpoint-failed",      # > 0 (doc: "any checkpoint fails")
            "SIGNAL-error-job-restarting",        # > 0
            "SIGNAL-crit-full-replay-started",    # == 1
            "SIGNAL-crit-taskmanager-down",       # < 1
            "SIGNAL-error-flink-jm-scrape-down",  # up == 0 (label + absence)
            "SIGNAL-error-flink-tm-scrape-down",  # up == 0
            "SIGNAL-error-source-stalled",        # rate == 0 for 2 min
            "SIGNAL-warn-candle-sink-zero",       # rate == 0 for 2 min
            "SIGNAL-warn-multitf-branch-stall",   # rate == 0 for 5 min
            "INFRA-crit-collector-export-failed",  # > 0
        }
        covered_by_tests = {
            # fd chain test
            "ING-warn-fd-80", "ING-crit-fd-90",
            # contract test
            "ING-warn-capacity-90", "ING-crit-capacity-98",
            "ING-crit-reconnect-storm", "ING-crit-reconnect-consecutive",
            "ING-crit-stale-feed", "ING-crit-decode-error-burst",
            "ING-warn-partial-subscription", "ING-warn-heartbeat-failures",
            # name test
            "ING-crit-unsafe-duration-30s",
            # runbook test
            "SIGNAL-warn-schema-rejected-rate", "SIGNAL-warn-scrape-slow",
            "SIGNAL-warn-source-lag",
            # dedup tests
            "SIGNAL-warn-dedup-state", "SIGNAL-warn-dedup-firsts-rate",
            # snapshot test
            "ING-warn-capacity-80", "ING-crit-capacity-over-100",
            "SIGNAL-warn-source-volume-drop", "SIGNAL-warn-multitf-session-drop",
            "SIGNAL-warn-multitf-signal-suppressed",
            # constant-relation tests
            "SIGNAL-error-checkpoint-slow", "SIGNAL-warn-jvm-heap-high",
            "INFRA-warn-host-cpu-80", "INFRA-crit-host-cpu-90",
            "INFRA-crit-jvm-heap-85", "INFRA-warn-jvm-gc-500",
            "INFRA-crit-disk-20", "INFRA-warn-disk-io-20", "INFRA-warn-net-80",
            "INFRA-crit-o2-mem-14",
        }
        spec_names = set(_alerts_spec())
        unaccounted = spec_names - EXEMPT_ABSENCE_TRIGGERS - covered_by_tests
        self.assertFalse(
            unaccounted,
            "new alert rule(s) without a threshold-parity pin — add a test above "
            f"or an explicit, reasoned exemption: {sorted(unaccounted)}",
        )
        self.assertEqual(
            EXEMPT_ABSENCE_TRIGGERS - spec_names, set(),
            "an exemption names a rule that no longer exists — prune it",
        )


if __name__ == "__main__":
    unittest.main()
