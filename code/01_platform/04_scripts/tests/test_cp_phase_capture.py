"""Unit tests for cp_phase_capture.py (per-checkpoint, per-subtask phases).

Fixture-based: the payloads mirror the Flink 2.2.1 REST models and were
verified against a live endpoint on 2026-09-29:

  GET /jobs/:jid/checkpoints/details/:cpId
      -> task-level TaskCheckpointStatistics only (no `subtasks` key)
  GET /jobs/:jid/checkpoints/details/:cpId/subtasks/:vertexId
      -> TaskCheckpointStatisticsWithSubtaskDetails:
         subtasks[].index / .status / .end_to_end_duration
         subtasks[].checkpoint.sync | .async
         subtasks[].alignment.buffered | .duration   (absent when unaligned)
         subtasks[].start_delay
         subtasks[].unaligned_checkpoint

No live Flink dependency.
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cp_phase_capture import (  # noqa: E402
    append_records,
    assemble_record,
    completed_ids_from_summary,
    load_done_ids,
    parse_subtask_details,
)


def _subtask(index, sync, async_, start_delay, alignment=None, status="completed"):
    st = {
        "index": index,
        "status": status,
        "ack_timestamp": 111,
        "end_to_end_duration": sync + async_ + (start_delay or 0),
        "state_size": 5,
        "checkpointed_size": 5,
        "checkpoint": {"sync": sync, "async": async_},
        "start_delay": start_delay,
        "unaligned_checkpoint": True,
        "aborted": False,
    }
    if alignment is not None:
        st["alignment"] = alignment
    return st


def _vertex_payload():
    """One vertex's subtask-details payload, two subtasks."""
    return {
        "id": 7,
        "status": "COMPLETED",
        "num_subtasks": 2,
        "subtasks": [
            _subtask(0, 4, 200, 2),
            _subtask(
                1,
                5,
                198,
                3,
                alignment={
                    "buffered": 0,
                    "processed": 0,
                    "persisted": 0,
                    "duration": 1,
                },
            ),
        ],
    }


def _details():
    """The checkpoint-details payload: task-level stats only, no subtasks."""
    return {
        "id": 7,
        "status": "COMPLETED",
        "checkpoint_type": "UNALIGNED_CHECKPOINT",
        "end_to_end_duration": 653,
        "state_size": 123456,
        "tasks": {"vertex-abc": {"id": "vertex-abc"}},
    }


def test_assemble_extracts_per_subtask_phases():
    rec = assemble_record(
        _details(), {"vertex-abc": _vertex_payload()}, {"vertex-abc": "strategy-host"})
    assert rec["id"] == 7
    assert rec["type"] == "UNALIGNED_CHECKPOINT"
    assert rec["status"] == "COMPLETED"
    assert rec["e2e"] == 653
    (task,) = rec["tasks"]
    assert task["v"] == "vertex-abc"
    assert task["n"] == "strategy-host"
    s0, s1 = task["subtasks"]
    assert (s0["i"], s0["sy"], s0["as"], s0["sd"]) == (0, 4, 200, 2)
    # unaligned subtask: no alignment object recorded -> null, not a silent 0
    assert s0["al"] is None and s0["ab"] is None
    assert (s1["al"], s1["ab"]) == (1, 0)
    assert s0["st"] == "completed"
    assert s0["ua"] is True


def test_assemble_vertex_name_optional():
    rec = assemble_record(_details(), {"vertex-abc": _vertex_payload()}, {})
    assert rec["tasks"][0]["n"] is None


def test_completed_ids_skip_in_progress_and_failed():
    payload = {
        "history": [
            {"id": 1, "status": "COMPLETED"},
            {"id": 2, "status": "IN_PROGRESS"},
            {"id": 3, "status": "FAILED"},
        ]
    }
    assert completed_ids_from_summary(payload) == [1]


def test_append_records_dedupes(tmp_path):
    out = tmp_path / "cp-phases-detail.jsonl"
    assert append_records(out, [{"id": 1, "x": 1}, {"id": 2, "x": 2}]) == 2
    assert append_records(out, [{"id": 2, "x": 2}, {"id": 3, "x": 3}]) == 1
    assert load_done_ids(out) == {1, 2, 3}
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert json.loads(lines[2])["id"] == 3


def test_assemble_fails_loud_on_no_vertex_payloads():
    with pytest.raises(ValueError):
        assemble_record(_details(), {}, {})


def test_parse_fails_loud_on_missing_subtasks():
    with pytest.raises(ValueError):
        parse_subtask_details({"id": 1})


def test_parse_fails_loud_on_missing_sync_async():
    payload = _vertex_payload()
    del payload["subtasks"][0]["checkpoint"]
    with pytest.raises(ValueError):
        parse_subtask_details(payload)
