#!/usr/bin/env python3
"""cp_phase_capture.py — per-checkpoint, per-subtask checkpoint phases (Flink REST).

Why this exists: the `/jobs/:jid/checkpoints` summary endpoint reports
`alignment_duration`, `start_delay`, `sync_dur` and `async_dur` as null — those
fields are not part of the summary model. The ~200 ms checkpoint pause that
breaks p99 at the 10 s cadence must be attributed per subtask, and in Flink
2.2.1 the only place that carries it is the drill-down endpoint:

    GET /jobs/:jobid/checkpoints/details/:checkpointid/subtasks/:vertexid

(measured 2026-09-29: the checkpoint-details endpoint above it returns
task-level `TaskCheckpointStatistics` only — no `subtasks` key; the per-subtask
model `TaskCheckpointStatisticsWithSubtaskDetails` is served by the subtasks
sub-resource, one vertex per request).

The capture runs while the job is alive (the checkpoint history is rolling; a
job cancel loses it — measured 2026-09-29). The shared capture script
(`stage-capture.sh`) is deliberately not modified (stage-profiler reuse-by-
invocation contract); `stage-profile.sh` runs this tool for the capture window
and fails closed if completed checkpoints have no phase record.

Output: `<out-dir>/cp-phases-detail.jsonl`, one line per checkpoint:

    {"id": 7, "type": "UNALIGNED_CHECKPOINT", "status": "COMPLETED",
     "e2e": 653, "state_size": 123456,
     "tasks": [{"v": "<vertexId>", "n": "<vertex name or null>",
                "subtasks": [{"i": 0, "st": "completed", "e2e": 206,
                              "sy": 4, "as": 200, "al": null, "ab": null,
                              "sd": 2, "ua": true}]}]}

`al`/`ab` are null when the subtask ran an unaligned checkpoint (no alignment
step exists); `sy`/`as`/`sd` are the decisive fields and must be present —
the parser fails loudly if the payload shape lacks them.

Usage:
    python3 cp_phase_capture.py --rest-url URL --job-id JID --out-dir DIR
        [--duration SEC] [--interval SEC] [--stop-file PATH]

`--duration 0` (default) = single pass. `--stop-file` = exit after the next
pass once the file exists (used by the stage profiler to end the sampler at
capture completion). `--duration` is an optional hard cap.

Exit codes: 0 = every completed checkpoint captured (or none existed);
1 = completed checkpoints remain uncaptured after retries (fail-closed);
2 = the checkpoint list endpoint stayed unreachable.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

PHASE_FILE = "cp-phases-detail.jsonl"
COMPLETED_STATUS = "COMPLETED"


def _get_json(url: str, timeout: float = 10.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _int_or_none(value) -> int | None:
    return None if value is None else int(value)


def completed_ids_from_summary(payload: dict) -> list[int]:
    """Completed checkpoint ids from the summary endpoint, ascending.

    Failed checkpoints are deliberately excluded: their subtask stats can be
    partial (unacked subtasks) and the p99 pause investigation only concerns
    completed checkpoints. Failures stay visible in flink-checkpoints.jsonl.
    """
    ids = []
    for cp in payload.get("history") or []:
        if cp.get("status") == COMPLETED_STATUS:
            ids.append(int(cp["id"]))
    return sorted(ids)


def load_done_ids(out_path: Path) -> set[int]:
    """Ids already present in the output file (empty for a missing file)."""
    out_path = Path(out_path)
    if not out_path.exists():
        return set()
    done: set[int] = set()
    for line in out_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            done.add(int(json.loads(line)["id"]))
        except (ValueError, KeyError, json.JSONDecodeError):
            continue
    return done


def append_records(out_path: Path, records: list[dict]) -> int:
    """Append only records whose id is not already in the file; return the count."""
    out_path = Path(out_path)
    done = load_done_ids(out_path)
    fresh = [r for r in records if int(r["id"]) not in done]
    if not fresh:
        return 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as fh:
        for record in fresh:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
    return len(fresh)


def parse_subtask_details(payload: dict) -> list[dict]:
    """Flatten one vertex's subtask-details payload to compact subtask records.

    Fails loudly (ValueError) on a shape that would otherwise be read as
    "no phases" — silently-empty evidence is the failure mode this tool
    exists to remove.
    """
    subtasks = payload.get("subtasks")
    if not isinstance(subtasks, list) or not subtasks:
        raise ValueError("subtask details payload has no non-empty 'subtasks' list")
    parsed = []
    for sub in subtasks:
        if not isinstance(sub, dict):
            raise ValueError("subtask entry is not an object")
        cp_dur = sub.get("checkpoint")
        if not isinstance(cp_dur, dict) or "sync" not in cp_dur or "async" not in cp_dur:
            raise ValueError(f"subtask {sub.get('index')}: missing checkpoint.sync/.async")
        align = sub.get("alignment")
        if align is not None and not isinstance(align, dict):
            raise ValueError(f"subtask {sub.get('index')}: 'alignment' is not an object")
        parsed.append({
            "i": int(sub["index"]),
            "st": sub.get("status"),
            "e2e": _int_or_none(sub.get("end_to_end_duration")),
            "sy": int(cp_dur["sync"]),
            "as": int(cp_dur["async"]),
            "al": _int_or_none(align.get("duration")) if align else None,
            "ab": _int_or_none(align.get("buffered")) if align else None,
            "sd": _int_or_none(sub.get("start_delay")),
            "ua": bool(sub.get("unaligned_checkpoint")),
        })
    return parsed


def assemble_record(details: dict, per_vertex: dict[str, dict],
                    vertex_names: dict | None) -> dict:
    """One checkpoint's compact record from its details + per-vertex payloads."""
    if not isinstance(details, dict) or "id" not in details:
        raise ValueError("checkpoint details payload is not an object with 'id'")
    if not per_vertex:
        raise ValueError(f"checkpoint {details.get('id')}: no vertex subtask payloads")
    tasks = []
    for vertex_id in sorted(per_vertex):
        tasks.append({
            "v": vertex_id,
            "n": (vertex_names or {}).get(vertex_id),
            "subtasks": parse_subtask_details(per_vertex[vertex_id]),
        })
    return {
        "id": int(details["id"]),
        "type": details.get("checkpoint_type"),
        "status": details.get("status"),
        "e2e": _int_or_none(details.get("end_to_end_duration")),
        "state_size": _int_or_none(details.get("state_size")),
        "tasks": tasks,
    }


def _vertex_names(rest_url: str, job_id: str) -> dict:
    """Best-effort vertex-id -> operator-name map (job details, then plan)."""
    try:
        info = _get_json(f"{rest_url}/jobs/{job_id}")
        names = {v["id"]: v["name"] for v in (info.get("vertices") or [])
                 if v.get("id") and v.get("name")}
        if names:
            return names
    except Exception:
        pass
    try:
        plan = _get_json(f"{rest_url}/jobs/{job_id}/plan")
    except Exception:
        return {}
    names = {}
    for node in (plan.get("plan") or {}).get("nodes") or []:
        if node.get("id"):
            desc = (node.get("description") or "").split("<br/>")[0].strip()
            if desc:
                names[node["id"]] = desc
    return names


def _fetch_checkpoint(rest_url: str, job_id: str, cp_id: int,
                      vertex_ids: list[str]) -> dict:
    """Details + per-vertex subtasks for one checkpoint, assembled."""
    base = f"{rest_url}/jobs/{job_id}/checkpoints/details/{cp_id}"
    details = _get_json(base)
    vids = vertex_ids or list((details.get("tasks") or {}).keys())
    if not vids:
        raise ValueError(f"checkpoint {cp_id}: cannot enumerate vertices")
    per_vertex = {vid: _get_json(f"{base}/subtasks/{vid}") for vid in vids}
    return assemble_record(details, per_vertex, None)


def run(rest_url: str, job_id: str, out_dir: Path, duration: float,
        interval: float, stop_file: Path | None) -> int:
    rest_url = rest_url.rstrip("/")
    out_path = Path(out_dir) / PHASE_FILE
    names = _vertex_names(rest_url, job_id)
    vertex_ids = list(names)
    single_pass = duration <= 0 and stop_file is None
    deadline = time.monotonic() + duration if duration > 0 else None
    list_failures = 0
    errors = 0

    while True:
        try:
            summary = _get_json(f"{rest_url}/jobs/{job_id}/checkpoints")
            list_failures = 0
        except Exception as exc:  # noqa: BLE001 — any transport/parse failure is the same signal
            list_failures += 1
            print(f"cp-phase-capture: checkpoint list fetch failed {list_failures}x: {exc}",
                  file=sys.stderr)
            if list_failures >= 5:
                return 2
            time.sleep(interval)
            continue

        pending = [cp for cp in completed_ids_from_summary(summary) if cp not in load_done_ids(out_path)]
        for cp_id in pending:
            record = None
            for attempt in (1, 2, 3):
                try:
                    record = _fetch_checkpoint(rest_url, job_id, cp_id, vertex_ids)
                    if names:
                        for task in record["tasks"]:
                            task["n"] = names.get(task["v"], task["n"])
                    break
                except Exception as exc:  # noqa: BLE001 — retried, then counted
                    if attempt == 3:
                        print(f"cp-phase-capture: details {cp_id} failed 3x: {exc}",
                              file=sys.stderr)
                        errors += 1
                    else:
                        time.sleep(2)
            if record is not None:
                append_records(out_path, [record])
                print(f"cp-phase-capture: checkpoint {cp_id} ({record['status']}) captured",
                      flush=True)

        stopping = stop_file is not None and Path(stop_file).exists()
        if single_pass or stopping or (deadline is not None and time.monotonic() >= deadline):
            missing = [cp for cp in completed_ids_from_summary(summary)
                       if cp not in load_done_ids(out_path)]
            if missing or errors:
                print(f"cp-phase-capture: {len(missing)} completed checkpoint(s) without phases "
                      f"({missing}); fetch errors={errors}", file=sys.stderr)
                return 1
            return 0
        time.sleep(interval)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rest-url", required=True)
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--duration", type=float, default=0.0,
                    help="hard cap in seconds; 0 = single pass")
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--stop-file", type=Path, default=None)
    args = ap.parse_args(argv)
    return run(args.rest_url, args.job_id, args.out_dir, args.duration,
               args.interval, args.stop_file)


if __name__ == "__main__":
    raise SystemExit(main())
