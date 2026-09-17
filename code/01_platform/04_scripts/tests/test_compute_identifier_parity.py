#!/usr/bin/env python3
"""Compute-identifier parity: 04_scripts must only name identifiers the job graph has.

WHY THIS EXISTS
---------------
`code/01_platform/04_scripts/` refers to Flink operator names and metric leaf names
that exist only because `02_compute` declares them. Nothing joined the two sets, so
three topology cutovers orphaned the script side silently, each from a commit that
touched NO file under `04_scripts/`:

  CHG-191  `0f3e5952` ("retire 15s candle path") moved `compute.candles.late.dropped`
           behind MULTITF_ENABLED. The harness kept asserting a counter its own job
           graph no longer created, so `late_delta` was structurally 0 and the smoke
           inject gate could never pass (run 8, 2026-09-17).
  CHG-193  `pipeline_metric_input_progress`'s fallback matched a hand-written operator
           set two-thirds of which `0f3e5952` had deleted; run 10 called a healthy
           pipeline dead while `multi-tf-aggregator` held 458 132 records.
  CHG-194  G7c's zero-loss parity proof read a table the cutover had emptied, so all
           22 528 closed windows were reported as "NO final candle".

The failure is always soft: Flink REST answers `[]` for an unknown metric, so a gate
compares 0 against 0, a dashboard tile renders empty, and an alert on a dead stream
never fires.

WHAT THIS SUITE IS (staged)
---------------------------
Stage 1 — this file. The authoritative set is a **hardcoded snapshot with provenance
anchors** (`EXPECTED`). The script side is scanned. So it fails immediately on the
current tree instead of waiting for a parser to be written.
Stage 2 — generalize: derive `EXPECTED` from `02_compute/src/main/java` (operator
names, uids, and metric literals incl. the next-line `gauge(\\n "name"` form, with
each metric's flag condition resolved from the `if` blocks in SignalJob.java).
The `EXPECTED` rows are shaped so that swap is mechanical: (kind, name, condition,
anchor_file, anchor_literal) is exactly the record an extractor produces.

HONEST LIMITATIONS (recorded, not hidden)
-----------------------------------------
* A **rename in Java is caught** by the anchor leg, not by inference: every row names
  the declaring file and the literal that must still be in it. A second copy of the
  truth cannot rot silently — including this one.
* Operator **names** are not auto-extracted; they are a hand registry
  (`OPERATOR_REFERENCES`), because operator names are hyphenated and share their shape
  with infra names (`trading-net`, `go-bridge`, `execution-webhook`). A registry of 7
  rows beats a 100-row ignore list. A NEW hyphenated reference in a script is therefore
  not detected until stage 2.
* The suite proves a *reference resolves*. It cannot prove the *value* is meaningful —
  a metric that exists and is always 0 still passes. That is what the live gates
  (G7/G8, the smoke inject gate) are for.
* Bare identifiers are ignored by design: only **quoted string literals** are scanned,
  which is what drops `compute_env` and `compute_manifest_entries` (they are Python
  names, never quoted) without an allowlist.

Refs: CHG-191/193/194; docs/plans/2026-09-17-compute-identifier-parity.md.
"""

from __future__ import annotations

import re
import unittest
from collections import namedtuple
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
REPO = SCRIPTS.parents[2]

COMPUTE_MAIN = REPO / "code/02_services/02_compute/src/main/java/com/trading/compute"
SIGNAL_JOB = "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java"
MTF_AGG = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "MultiTimeframeAggregateFunction.java"
)
MTF_SINKS = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "MultiTimeframeSinks.java"
)
STRATEGY_HOST = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "StrategyHostFunction.java"
)
N7_STRATEGY = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "N7RangeBreakoutStrategy.java"
)
STUB_STRATEGY = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "StubSmokeStrategy.java"
)
INTENT_PRODUCER = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "ExecutionIntentProducerFunction.java"
)
RAW_VALIDATION = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "RawValidationFunction.java"
)
FINGERPRINT_DEDUP = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "FingerprintDedupFunction.java"
)
LATENCY_MONITOR = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "IngestLatencyMonitorFunction.java"
)
CANONICAL_FILTER = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "CanonicalSignalFilterFunction.java"
)
TRADE_DECISIONS = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/"
    "TradeDecisionsSinks.java"
)
BABYSITTER_JOB = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/BabysitterJob.java"
)
POSITIONS_OBS = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/"
    "PositionsObservationOperator.java"
)
POSITIONS_DESER = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/babysitter/"
    "PositionsRowDeserializer.java"
)
SAFETY_HALT_JOB = (
    "code/02_services/02_compute/src/main/java/com/trading/compute/safetyhalt/SafetyHaltJob.java"
)

# ── flag conditions ──────────────────────────────────────────────────────────
# The three compute topology flags, and only those (see plan decision 3): they are
# the only env inputs whose value decides whether an identifier EXISTS.
U = "unconditional"
MTF = "MULTITF_ENABLED=true"
HOST = "MULTITF_ENABLED=true + STRATEGY_HOST_ENABLED=true"
INTENT = "MULTITF_ENABLED=true + STRATEGY_HOST_ENABLED=true + EXECUTION_INTENT_ENABLED=true"
BAB = "job=BabysitterJob"
SAF = "job=SafetyHaltJob"
UNWIRED = "unwired: declared but not reached by any buildTopology()"

Row = namedtuple("Row", "kind name condition anchor literal")


def _rows(kind, condition, anchor, names, literal_of=None):
    """Build rows sharing one anchor file. `literal` defaults to the name itself."""
    return [
        Row(kind, n, condition, anchor, literal_of(n) if literal_of else n)
        for n in names
    ]


# ── EXPECTED: the authoritative set (stage 1 = hand snapshot + provenance) ────
# Every row names the declaring file AND the literal that must still be present in
# it, which is what stops this snapshot from rotting silently.
EXPECTED = []

# SignalJob — operators. Anchors are the .name(...)/.uid(...) literal pairs.
EXPECTED += _rows("operator", U, SIGNAL_JOB, ["raw-table-1"], lambda n: f'"{n}"')
EXPECTED += _rows("uid", U, SIGNAL_JOB, ["raw-table-1"], lambda n: f'"{n}"')
EXPECTED += _rows("operator", U, SIGNAL_JOB, ["raw-validation", "fingerprint-dedup",
                                             "ingest-latency-monitor"], lambda n: f'"{n}"')
EXPECTED += _rows("uid", U, SIGNAL_JOB, ["fingerprint-dedup-v2"], lambda n: f'"{n}"')
EXPECTED += _rows("operator", MTF, SIGNAL_JOB, ["multi-tf-aggregator"], lambda n: f'"{n}"')
EXPECTED += _rows("uid", MTF, SIGNAL_JOB, ["multi-tf-aggregator-v1"], lambda n: f'"{n}"')
EXPECTED += _rows("operator", HOST, SIGNAL_JOB,
                  ["strategy-host", "strategy-host-candidates-sink",
                   "canonical-signal-filter-strategy-host",
                   "strategy-host-candidates-current-sink"], lambda n: f'"{n}"')
EXPECTED += _rows("uid", HOST, SIGNAL_JOB, ["strategy-host-v1"], lambda n: f'"{n}"')
EXPECTED += _rows("operator", INTENT, SIGNAL_JOB,
                  ["execution-intent-producer", "execution-intent-sink"], lambda n: f'"{n}"')

# Metrics — unconditional
EXPECTED += _rows("metric", U, FINGERPRINT_DEDUP,
                  ["compute.dedup.first", "compute.dedup.duplicates"])
EXPECTED += _rows("metric", U, RAW_VALIDATION,
                  ["compute.invalid.rows", "compute.startup.mode"])
EXPECTED += _rows("metric", U, LATENCY_MONITOR, ["compute.latency.ingest_to_monitor"])
EXPECTED += _rows("metric", U, CANONICAL_FILTER,
                  ["compute.signal.kv.filtered.noncanonical"])

# Metrics — MULTITF_ENABLED (registered inside MultiTimeframeAggregateFunction#open)
EXPECTED += _rows("metric", MTF, MTF_AGG,
                  ["compute.candles.emitted", "compute.candles.late.dropped",
                   "compute.candles.gap.detected", "compute.candles.restored_timer_noop",
                   "compute.candles.live.emitted", "compute.session.filtered.pre_open",
                   "compute.session.filtered.post_close"])
EXPECTED += _rows("metric", MTF, MTF_SINKS, ["compute.candles.multitf.duplicate_window"])

# The two dynamic families. Leaves are static literals, so they get real rows; the
# exported O2 stream flattens `strategy.<ruleId>.<leaf>` to `strategy_<leaf>`.
# compute.invalid.byReason.<reason> — RawValidationFunction#invalidReason return literals
EXPECTED += _rows(
    "metric", U, RAW_VALIDATION,
    [
        "compute.invalid.byReason.rowkind-not-insert",
        "compute.invalid.byReason.blank-fingerprint",
        "compute.invalid.byReason.blank-fingerprint-version",
        "compute.invalid.byReason.schema-version",
        "compute.invalid.byReason.validity-state",
        "compute.invalid.byReason.non-positive-price",
        "compute.invalid.byReason.negative-qty",
        "compute.invalid.byReason.non-positive-event-time",
        "compute.invalid.byReason.event-time-overflow-window",
    ],
    literal_of=lambda n: '"' + n.rsplit(".", 1)[1] + '"',
)

# Metrics — STRATEGY_HOST_ENABLED
EXPECTED += _rows(
    "metric", HOST, STRATEGY_HOST,
    ["compute.strategy.suppressed", "compute.strategy.failed",
     "compute.strategy.dropped.unkeyed", "compute.strategy.dropped.oversize"],
)
# strategy.<ruleId>.<leaf> — one row per leaf literal
EXPECTED += _rows("metric", HOST, STRATEGY_HOST, ["strategy.emitted"],
                  lambda n: '"emitted"')
EXPECTED += _rows("metric", HOST, N7_STRATEGY,
                  ["strategy.suppressed", "strategy.rejected_invalid",
                   "strategy.invalidated_gap", "strategy.armed"],
                  literal_of=lambda n: '"' + n.rsplit(".", 1)[1] + '"')
EXPECTED += _rows("metric", HOST, STUB_STRATEGY, ["strategy.skipped_poison_tf"],
                  literal_of=lambda n: '"skipped_poison_tf"')

# Metrics — EXECUTION_INTENT_ENABLED
EXPECTED += _rows("metric", INTENT, INTENT_PRODUCER,
                  ["compute.execution_intent.rejected", "compute.latency.tick_to_intent",
                   "compute.latency.signal_to_intent"])

# Babysitter job (separate graph)
EXPECTED += _rows("operator", BAB, BABYSITTER_JOB,
                  ["babysitter-rowkind-filter", "babysitter-position-deserialize",
                   "babysitter-position-observation", "babysitter-discard"],
                  lambda n: f'"{n}"')
EXPECTED += _rows("metric", BAB, POSITIONS_OBS,
                  ["babysitter.positions.observed", "babysitter.positions.applied",
                   "babysitter.positions.duplicate", "babysitter.positions.stale",
                   "babysitter.positions.conflict", "babysitter.positions.stale_arrival",
                   "babysitter.positions.latest_observed_version"])
EXPECTED += _rows("metric", BAB, POSITIONS_DESER,
                  ["babysitter.positions.rows.observed", "babysitter.positions.rows.malformed"])

# Safety-halt job (separate graph)
EXPECTED += _rows("operator", SAF, SAFETY_HALT_JOB, ["safety-halt-tracker"],
                  lambda n: f'"{n}"')
EXPECTED += _rows("metric", SAF, SAFETY_HALT_JOB,
                  ["transitions.applied", "rows.malformed", "rows.skipped"])

EXPECTED_NAMES = {r.name for r in EXPECTED}

# ── UNWIRED: declared in the tree but absent from every job graph ─────────────
# A script reference to one of these is an error even though the literal exists in
# the source tree — the literal is not evidence that the identifier is produced.
UNWIRED_NAMES = {
    # TradeDecisionsSinks: the class is referenced nowhere outside its own file and
    # SignalJob never mentions it (the trade-decisions LOG/KV hits in SignalJob are a
    # different method validating table *contracts*). No buildTopology reaches it.
    "trade-decisions-sink",
    "trade-instruction-index-map",
    "trade-instruction-state-sink",
    "trade-instruction-state-first-write-wins",
    "compute.trade_decisions.duplicate_instruction",
}

# ── OPERATOR_REFERENCES: hand registry (stage 1) ─────────────────────────────
# name -> scripts that reference it. Both halves are asserted: the name exists in
# EXPECTED, and each listed script really contains it.
OPERATOR_REFERENCES = {
    "raw-table-1": ("loadtest-collect.sh",),
    "raw-validation": ("stage-a2-baseline.sh", "loadtest-collect.sh", "holistic-analyze.py",
                       "holistic-measure.sh"),
    "fingerprint-dedup": ("pipeline-lib.sh", "stage-a2-baseline.sh", "rollout-savepoint.sh",
                          "stage-capture.sh", "holistic-measure.sh"),
    "ingest-latency-monitor": ("holistic-measure.sh",),
    "multi-tf-aggregator": ("pipeline-lib.sh", "o2-provision.py", "holistic-measure.sh"),
    "strategy-host": ("fluss-probes/FlussRuleCounter.java",),
}

# ── foreign metric allowlist (decision 4: checked in, with provenance) ───────
# Names a script may legitimately reference that this repo does not produce. Each
# family names its source and the event that would invalidate it.
#
#   flink_taskmanager_* core metrics  source: Flink 2.2.1 built-ins
#                                     recheck when: Flink dist re-pinned (runtime.lock)
#   fluss_client_* / fluss_reader_*   source: the Fluss connector's own metrics
#                                     recheck when: Fluss connector version changes
#   bridge_* / process_* / go_* / jvm_* / otelcol_* / container.*
#                                     source: Go bridge + agent/otelcol/cAdvisor exporters
#   append_latency_ms / decode_errors_*
#                                     source: ingestion's own OTLP emitter
ALLOWLIST_STREAM_EXACT = {
    "flink_taskmanager_job_task_operator_currentinputwatermark",
    "flink_taskmanager_job_task_operator_currentfetcheventtimelag",
    "flink_taskmanager_job_task_operator_numlaterecordsdropped",
    "flink_taskmanager_job_task_operator_sourceidletime",
    "flink_taskmanager_job_task_operator_watermarklag",
    "flink_taskmanager_job_task_operator_fluss_reader_bucket_currentoffset",
    "flink_jobmanager_numregisteredtaskmanagers",
    "flink_jobmanager_job_numberoffailedcheckpoints",
    "flink_taskmanager_job_task_numrecordsinpersecond",
}
ALLOWLIST_STREAM_PREFIXES = (
    "flink_taskmanager_job_task_operator_fluss_",
    "flink_taskmanager_job_task_operator_numrecords",
)

# ── script scanning ─────────────────────────────────────────────────────────
# Quoted string literals only: a bare identifier (`compute_env`,
# `compute_manifest_entries`) is Python code, not a metric name, and requiring the
# quotes drops it without an allowlist entry.
QUOTED = re.compile(r'"((?:[^"\\\n]|\\.)*)"' + r"|'([^'\n]*)'")
# Dotted namespaces, deliberately WITHOUT `safety` and `strategy`: both are common
# config words as well as metric groups (`strategy.type` / `strategy.fixed` are Fluss
# restart-strategy keys in tiering-start.sh, `safety.state` is a command-spec stream),
# so a dotted `strategy.*` / `safety.*` is ambiguous and cannot be enforced. The
# stream form disambiguates those two because it carries the full prefix.
DOTTED_NS = r"compute|babysitter|preview|rows|transitions"
DOTTED = re.compile(rf"\b(?:{DOTTED_NS})(?:\.[A-Za-z][A-Za-z0-9_]*)+\.?")
STREAM = re.compile(r"\bflink_taskmanager_job_task_operator_[A-Za-z0-9_.-]+")
# Bare form limited to `compute_`: an O2 stream may be written without the
# flink_taskmanager_ prefix (o2-provision.py L442 writes
# `compute_kv_filtered_noncanonical` for the live `compute.signal.kv.filtered...`).
# Limiting it to `compute_` keeps `rows_s` / `safety_2` / `strategy_type` — shell and
# Python variable names — out without an allowlist entry.
BARE = re.compile(r"\bcompute_[a-z0-9_]+")
STREAM_PREFIX = "flink_taskmanager_job_task_operator_"

# A candidate is not a metric when its last segment is a file extension/verb. Kept
# explicit so a NEW false positive fails closed instead of being silently dropped.
# Observed false positives these rules exist for: `compute.jar` (the shaded build
# artifact), `compute.md` / `babysitter.md` (dossier filenames in docs_audit.py),
# `compute.X.xml` (a surefire glob), `compute.signaljob.SignalJob` (a Java FQCN in
# rollout-savepoint.sh).
EXCLUDED_FINAL = {"jar", "md", "java", "sh", "py", "yaml", "yml", "json", "xml",
                  "get", "txt", "class", "jvm", "log", "sum"}

SCAN_SUFFIXES = {".sh", ".py", ".java"}
SKIP_DIRS = {"tests", "__pycache__", "target", "stubs", "third_party"}


def _norm(name: str) -> str:
    """O2 renames '.' -> '_' and preserves everything else; compare on [a-z0-9]."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def scan_scripts():
    """Yield (relative_path, token, line) for every metric-shaped script reference."""
    found = []
    for path in sorted(SCRIPTS.rglob("*")):
        if path.suffix not in SCAN_SUFFIXES or not path.is_file():
            continue
        if SKIP_DIRS & set(path.relative_to(SCRIPTS).parts):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        rel = str(path.relative_to(SCRIPTS))
        for lineno, line in enumerate(text.splitlines(), 1):
            for m in QUOTED.finditer(line):
                literal = m.group(1) or m.group(2) or ""
                for tok in _candidates(literal):
                    found.append((rel, tok, lineno))
    return found


def _candidates(literal):
    """Metric-shaped tokens inside one quoted literal."""
    out = []
    for m in DOTTED.finditer(literal):
        tok = m.group(0)
        final = tok.rstrip(".").rsplit(".", 1)[-1]
        if final and (final in EXCLUDED_FINAL or final[0].isupper()):
            continue
        out.append(tok)
    for m in STREAM.finditer(literal):
        tail = m.group(0)[len(STREAM_PREFIX):]
        if tail.rsplit("_", 1)[-1] in EXCLUDED_FINAL:
            continue
        out.append(STREAM_PREFIX + tail)
    for m in BARE.finditer(literal):
        if m.group(0).rsplit("_", 1)[-1] in EXCLUDED_FINAL:
            continue
        out.append(m.group(0))
    return out


def _norm_expected():
    return {_norm(r.name) for r in EXPECTED}


def _matches_expected(want: str) -> bool:
    """Normalised match, exact or as a family head. A grep fragment (`compute_dedup_`)
    is a legitimate family head; a wrong leaf (`compute_dedup_state_count`) is neither
    an exact name nor a head of one, so it still fails."""
    return any(n == want or n.startswith(want) for n in _norm_expected())


def _resolves(token: str) -> bool:
    """Does this script token name something a job graph actually produces?"""
    if token.startswith(STREAM_PREFIX):
        if _norm(token) in {_norm(n) for n in ALLOWLIST_STREAM_EXACT}:
            return True
        if any(token.startswith(p) for p in ALLOWLIST_STREAM_PREFIXES):
            return True
        # After the export flattens the group path, `strategy_emitted` is
        # `strategy.<ruleId>.emitted` with the ruleId elided, and
        # `strategy_host_candidates_current_sink` is an operator-derived stream.
        return _matches_expected(_norm(token[len(STREAM_PREFIX):]))
    if token.startswith("compute_"):
        return _matches_expected(_norm(token))
    if token.endswith("."):
        token = token.rstrip(".")
    if any(r.name == token or r.name.startswith(token + ".") for r in EXPECTED):
        return True
    # A dotted O2 stream spelling (`compute.invalid.byreason.schema_version` for the
    # registered `compute.invalid.byReason.schema-version`) matches only after the
    # same [^a-z0-9] strip the exporter applies.
    want = _norm(token)
    return any(_norm(r.name) == want or _norm(r.name).startswith(want) for r in EXPECTED)


class ScriptReferencesResolve(unittest.TestCase):
    """LEG 1 — every identifier a script names must be produced by a job graph."""

    def test_no_script_names_a_dead_identifier(self):
        bad = []
        for rel, token, lineno in scan_scripts():
            if token in UNWIRED_NAMES:
                bad.append(f"{rel}:{lineno}  {token}  (UNWIRED — declared but reached by "
                           f"no buildTopology(); see UNWIRED_NAMES)")
            elif not _resolves(token):
                bad.append(f"{rel}:{lineno}  {token}  (not in the compute job graph)")
        self.assertFalse(
            bad,
            "scripts reference identifiers the job graph does not produce — every one of "
            "these is a gate/dashboard/alert that silently measures nothing:\n  "
            + "\n  ".join(sorted(set(bad))),
        )


class ExpectedSnapshotIsLive(unittest.TestCase):
    """LEG 2 — the hand snapshot cannot rot silently: each row's literal must exist."""

    def test_every_anchor_literal_is_still_declared(self):
        stale = []
        for row in EXPECTED:
            text = (REPO / row.anchor).read_text(encoding="utf-8")
            if row.literal not in text:
                stale.append(f"{row.kind} {row.name}: {row.literal!r} gone from {row.anchor}")
        self.assertFalse(
            stale,
            "EXPECTED is stale — the compute source changed under it. Update the row (and "
            "therefore every script that names it):\n  " + "\n  ".join(stale),
        )

    def test_every_row_states_a_condition(self):
        for row in EXPECTED:
            self.assertIn(row.condition, {U, MTF, HOST, INTENT, BAB, SAF},
                          f"{row.name} has no condition — a new identifier must state the "
                          f"flag (or 'unconditional') that makes it exist")

    def test_unwired_names_are_not_also_expected(self):
        overlap = UNWIRED_NAMES & EXPECTED_NAMES
        self.assertFalse(overlap, f"a name cannot be both wired and unwired: {overlap}")


class OperatorRegistry(unittest.TestCase):
    """LEG 3 — hand registry of hyphenated operator names, asserted both ways."""

    def test_registered_operators_exist_in_the_graph(self):
        names = {r.name for r in EXPECTED if r.kind == "operator"}
        for name in OPERATOR_REFERENCES:
            self.assertIn(name, names, f"{name} is referenced by a script but is not an "
                                       f"operator in the job graph")

    def test_each_registered_script_really_contains_the_name(self):
        for name, scripts in OPERATOR_REFERENCES.items():
            for rel in scripts:
                text = (SCRIPTS / rel).read_text(encoding="utf-8")
                self.assertIn(name, text, f"{name} is registered against {rel}, which no "
                                          f"longer names it — update the registry or the script")

    def test_operator_names_are_names_not_uids(self):
        """Flink REST embeds the operator DISPLAY name; restore keys on the uid. A
        uid-only pin (SignalJobOperatorUidTest) cannot see a .name() rename."""
        names = {r.name for r in EXPECTED if r.kind == "operator"}
        uids = {r.name for r in EXPECTED if r.kind == "uid"}
        for name in OPERATOR_REFERENCES:
            self.assertIn(name, names, f"{name} is not a .name() literal")
        # The known divergence must stay visible: name != uid for this operator.
        self.assertIn("fingerprint-dedup", names)
        self.assertIn("fingerprint-dedup-v2", uids)


# A script that sources pipeline-lib.sh chooses the topology it submits.
SOURCE_RE = re.compile(r"^[ \t]*(?:source|\.)[ \t]+\S*pipeline-lib\.sh", re.M)
# The artifact that turns absence into evidence, written by stage-capture.sh.
CAPTURE_RECORD_MARKER = "metric-availability"

# Consumers that legitimately reference a gated identifier without recording
# availability, each with the reason it is not a silent-measurement risk.
GATED_CONSUMER_EXEMPT = {
    # Dashboard/alert provisioning: a dead stream here is a dead tile, not a
    # measurement read as zero. Task 3's dead-stream cleanup plus extending
    # validate_command_spec() (o2-provision.py L123) own this case.
    "o2-provision.py",
}


class GatedReferencesOptIn(unittest.TestCase):
    """LEG 4 — the CHG-191 leg, split by what a script is allowed to do.

    A **driver** sources `pipeline-lib.sh`, so it chooses the topology it submits:
    it must opt the flag in or refuse an explicit opt-out — the contract
    `holistic-measure.sh` L105-129 writes by hand.

    A **capture** attaches to a job it did not start and cannot change, so "opt the
    flag in" would be a lie. Its honest contract is to RECORD the observed branch
    state and per-name availability (`stage-capture.sh`, 2026-09-17).

    Either way the outcome stops being "silently measures a topology without that
    operator" — a refusal, or evidence.
    """

    FLAG_OF_CONDITION = {MTF: "MULTITF_ENABLED", HOST: "STRATEGY_HOST_ENABLED",
                         INTENT: "EXECUTION_INTENT_ENABLED"}

    @staticmethod
    def _is_driver(text):
        return bool(SOURCE_RE.search(text))

    def _gated_references(self):
        """(rel, token, lineno, row, flag) for every gated metric a script reads."""
        by_name = {r.name: r for r in EXPECTED}
        out = []
        for rel, token, lineno in scan_scripts():
            row = by_name.get(token)
            if row is None or row.condition in (U, BAB, SAF):
                continue
            flag = self.FLAG_OF_CONDITION.get(row.condition)
            if flag:
                out.append((rel, token, lineno, row, flag))
        return out

    def test_a_driver_opts_the_flag_in_or_refuses(self):
        bad = []
        for rel, token, lineno, row, flag in self._gated_references():
            text = (SCRIPTS / rel).read_text(encoding="utf-8")
            if not self._is_driver(text):
                continue
            if flag not in text:
                bad.append(f"{rel}:{lineno}  {token}  needs {flag}=true ({row.condition}) "
                           f"but {rel} never mentions {flag}")
            elif not re.search(rf"{flag}=(?:\"?true\"?|\$\{{{flag}:-true\}})", text):
                bad.append(f"{rel}:{lineno}  {token}  names {flag} but never opts it IN "
                           f"(no {flag}=true / ${{{flag}:-true}})")
        self.assertFalse(
            bad,
            "a driver reads a gated identifier without running the topology that creates "
            "it — this is CHG-191, the counter exists but the operator is not wired:\n  "
            + "\n  ".join(sorted(set(bad))),
        )

    def test_a_capture_records_the_observed_availability(self):
        bad = []
        for rel, token, lineno, row, _flag in self._gated_references():
            if rel in GATED_CONSUMER_EXEMPT:
                continue
            text = (SCRIPTS / rel).read_text(encoding="utf-8")
            if self._is_driver(text):
                continue
            if CAPTURE_RECORD_MARKER not in text:
                bad.append(f"{rel}:{lineno}  {token}  ({row.condition}) — {rel} attaches to "
                           f"a job it cannot change but records no "
                           f"{CAPTURE_RECORD_MARKER}, so this absence is silent")
        self.assertFalse(
            bad,
            "a gated identifier is read by a script that can neither run the topology nor "
            "record that the identifier was absent — the CHG-191 failure mode:\n  "
            + "\n  ".join(sorted(set(bad))),
        )

    def test_the_driver_capture_split_holds(self):
        """Pin the classification itself: a rename of the lib or a new `source` line
        would silently reclassify a script and weaken the legs above."""
        self.assertTrue(self._is_driver((SCRIPTS / "holistic-measure.sh").read_text()),
                        "holistic-measure.sh must classify as a driver (it submits the job)")
        self.assertFalse(self._is_driver((SCRIPTS / "stage-capture.sh").read_text()),
                         "stage-capture.sh must classify as a capture (it attaches to a job)")


if __name__ == "__main__":
    unittest.main()
