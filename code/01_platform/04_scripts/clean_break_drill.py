#!/usr/bin/env python3
"""SCH-25 clean-break drill — approval-gated reset + replay procedure.

The clean break is the pre-production drill that proves a target table can be
reset and rebuilt to a known-good state from an IMMUTABLE source log. The
CONVERGENCE SEMANTICS are pinned by the pure-JVM CleanBreakSimulation (common
module, unit-tested: full replay reconverges; partial replay and a mutated
source diverge and fail closed). This runner enforces the two hard gates —
an explicit plan file AND --approve, never implied — and records the evidence.

Usage:
  python3 clean_break_drill.py --plan-file plan.json [--approve] [--dry-run]
                               [--out DIR]

Plan schema (plan.json):
  {
    "drill": "clean-break-2026-08-15",
    "tables": ["Signal_Candidates", "Positions"],
    "replay_from_offset": 0,
    "source_logs": {"Signal_Candidates": "Signal_Candidates", "Positions": "Fills"}
  }

Exit codes:
  0  PASS (approved + evidence written, or --dry-run)
  2  usage / plan errors
  3  destructive action without --approve (refused)
  4  NEVER emitted by this runner — the convergence verdict lives in the
     unit-tested simulation; the live post-replay parity check (runbook)
     is the operator-side fail-closed gate

Env: FLUSS_BOOTSTRAP (default localhost:9123) — recorded in the evidence
preamble only; this runner performs no cluster I/O.
"""
import argparse
import datetime as _dt
import json
import os
import sys


def load_plan(path):
    try:
        with open(path, encoding="utf-8") as fh:
            plan = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: cannot read plan {path}: {exc}", file=sys.stderr)
        return None
    if not isinstance(plan, dict) or "tables" not in plan:
        print("ERROR: plan must be an object with a non-empty 'tables' list", file=sys.stderr)
        return None
    tables = plan.get("tables", [])
    # isinstance(..., list) is load-bearing: a bare string is iterable, so a
    # plan written as "tables": "A,B" would pass the per-element check and the
    # runner would record single-character table names; a non-iterable scalar
    # (5, true) would raise TypeError out of all() and die on an undocumented
    # exit code instead of the declared plan-error path (exit 2).
    if not isinstance(tables, list) or not tables or not all(isinstance(t, str) and t for t in tables):
        print("ERROR: 'tables' must be a non-empty list of table names", file=sys.stderr)
        return None
    plan.setdefault("replay_from_offset", 0)
    plan.setdefault("source_logs", {t: t for t in tables})
    return plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plan-file", required=True)
    parser.add_argument("--approve", action="store_true",
                        help="acknowledge the destructive reset (drop + replay) — REQUIRED")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate + print the plan only; touch nothing")
    parser.add_argument("--out", default=os.path.join(os.getcwd(), "logs", "clean-break"))
    args = parser.parse_args(argv)

    plan = load_plan(args.plan_file)
    if plan is None:
        return 2

    print(f"drill: {plan.get('drill', '(unnamed)')}")
    print(f"  tables: {', '.join(plan['tables'])}")
    print(f"  replay_from_offset: {plan['replay_from_offset']}")
    print(f"  source_logs: {plan.get('source_logs')}")
    print(f"  bootstrap: {os.environ.get('FLUSS_BOOTSTRAP', 'localhost:9123')}")

    # Convergence semantics: CleanBreakSimulation (common, unit-tested) —
    # full replay reconverges; partial replay and a mutated source diverge.
    # The live post-replay parity check (01-runbooks.md clean-break drill) is
    # the operator-side fail-closed gate; it is never implied by this runner.
    print("  semantics: CleanBreakSimulation (unit-tested) — full replay reconverges;")
    print("             partial replay / mutated source diverge and fail closed")

    if args.dry_run:
        print("dry-run: plan valid — no destructive action taken")
        return 0

    if not args.approve:
        print("ERROR: destructive reset refused — pass --approve to acknowledge "
              "the drop + replay (exit 3)", file=sys.stderr)
        return 3

    run_id = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.makedirs(args.out, exist_ok=True)
    evidence = {
        "drill": plan.get("drill"),
        "run_id": run_id,
        "plan": plan,
        "status": "APPROVED",
        "note": "operator steps per 01-runbooks.md clean-break drill: (1) capture "
                "pre-reset projections as the reference, (2) stop the Signal/Executor "
                "jobs, (3) drop the target tables via `make ddl APPLY=1`, (4) restart "
                "the jobs for full replay from offset 0, (5) verify post-replay parity "
                "against the reference — fail closed on any divergence",
    }
    path = os.path.join(args.out, f"{run_id}-clean-break-drill.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"drill: RESULT=APPROVED EXIT=0 evidence={path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
