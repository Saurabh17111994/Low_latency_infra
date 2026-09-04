"""Unit tests for check_flink_properties.py (G22 validator).

Pins the validator's failure classes with synthetic compose texts so the
validator itself cannot silently rot:
  - a clean block passes
  - a comment line inside the block fails (entrypoint feeds '#' lines as
    config properties)
  - a prefix collision fails (state.backend vs state.backend.rocksdb.*)
  - a missing required key fails
  - the REAL docker-compose.yml in the repo passes
"""
import sys
import tempfile
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from check_flink_properties import check, COMPOSE  # noqa: E402


def _compose(tmp_path: Path, block_lines: str) -> Path:
    """Write a minimal compose file with the given FLINK_PROPERTIES block.

    Mirrors the real docker-compose.yml shape: anchor env keys at 4-space
    indent, FLINK_PROPERTIES block scalar lines at 6-space indent, and the
    next env key back at 4-space indent (what the validator's regex anchors
    on).
    """
    content = (
        "x-flink-common:\n"
        "  environment: &flink-common-env\n"
        '    SOME_ENV: "1"\n'
        "    FLINK_PROPERTIES: |\n"
        + "".join(f"      {line}\n" for line in block_lines.splitlines())
        + '    OTHER_ENV: "2"\n'
    )
    p = tmp_path / "compose-test.yml"
    p.write_text(content)
    return p


VALID_BLOCK = """\
jobmanager.rpc.address: flink-jobmanager
metrics.reporter.prom.port: 9249
metrics.latency.interval: 1000
taskmanager.numberOfTaskSlots: 16
taskmanager.memory.process.size: 7g
taskmanager.memory.managed.fraction: 0.6
state.backend.incremental: true
state.backend.rocksdb.localdir: /tmp/flink-rocksdb
"""


def test_valid_block_passes(tmp_path):
    assert check(_compose(tmp_path, VALID_BLOCK)) == []


def test_comment_inside_block_fails_with_reason(tmp_path):
    block = "# some comment: with colon\n" + VALID_BLOCK
    failures = check(_compose(tmp_path, block))
    assert len(failures) == 1
    assert "COMMENT LINE" in failures[0]
    assert "process_flink_properties" in failures[0]  # the WHY


def test_prefix_collision_fails_with_reason(tmp_path):
    block = "state.backend: rocksdb\n" + VALID_BLOCK
    failures = check(_compose(tmp_path, block))
    assert any("PREFIX COLLISION" in f for f in failures)
    assert any("state.backend" in f and "localdir" in f for f in failures)
    assert any("SILENTLY dropped" in f for f in failures)  # the WHY


def test_quoted_boolean_value_fails(tmp_path):
    # The 2026-09-04 regression: a YAML-quoted "true" survives the entrypoint
    # split verbatim -> config.yaml carries '"true"' -> Flink's boolean parse
    # dies at JobMaster init. The validator must fail this at edit time.
    block = VALID_BLOCK.replace(
        "state.backend.incremental: true",
        'state.backend.incremental: "true"')
    failures = check(_compose(tmp_path, block))
    assert any("QUOTED VALUE" in f and "incremental" in f for f in failures)
    assert any("VERBATIM" in f for f in failures)  # the WHY


def test_quoted_nonboolean_value_fails(tmp_path):
    # Quoting is wrong for ANY value here (paths, addresses, sizes): the
    # quotes reach config.yaml verbatim and corrupt typed parsing or the
    # literal value itself.
    block = VALID_BLOCK.replace(
        "state.backend.rocksdb.localdir: /tmp/flink-rocksdb",
        'state.backend.rocksdb.localdir: "/tmp/flink-rocksdb"')
    failures = check(_compose(tmp_path, block))
    assert any("QUOTED VALUE" in f and "localdir" in f for f in failures)


def test_single_quote_is_not_flagged(tmp_path):
    # Only literal DOUBLE quotes are the entrypoint-verbatim hazard; a value
    # that legitimately contains an apostrophe must not false-positive.
    block = VALID_BLOCK.replace(
        "state.backend.rocksdb.localdir: /tmp/flink-rocksdb",
        "state.backend.rocksdb.localdir: /tmp/flink-rocksdb-it's")
    failures = check(_compose(tmp_path, block))
    assert not any("QUOTED VALUE" in f for f in failures)


def test_forbidden_leaf_state_backend_fails(tmp_path):
    # no nested rocksdb key: still forbidden as a known-fragile leaf
    block = VALID_BLOCK.replace(
        "state.backend.rocksdb.localdir: /tmp/flink-rocksdb\n", "")
    block = block.replace('state.backend.incremental: "true"\n', "")
    block += "state.backend: rocksdb\n"
    failures = check(_compose(tmp_path, block))
    assert any("FORBIDDEN LEAF" in f and "state.backend" in f for f in failures)


def test_missing_required_key_fails(tmp_path):
    block = VALID_BLOCK.replace(
        "state.backend.rocksdb.localdir: /tmp/flink-rocksdb\n", "")
    failures = check(_compose(tmp_path, block))
    assert any("REQUIRED KEY MISSING" in f and "localdir" in f for f in failures)


def test_real_repo_compose_passes():
    """The actual docker-compose.yml in this repo must validate clean."""
    assert check(COMPOSE) == []
