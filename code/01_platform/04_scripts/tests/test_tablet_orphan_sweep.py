"""XC-22: tablet-orphan-sweep.py's zk_ls() carried a duplicated returncode
check after the "Node does not exist" return — CompletedProcess.returncode
cannot change, so the second raise was unreachable dead code."""
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "tablet-orphan-sweep.py"


def test_no_duplicate_unreachable_returncode_check():
    src = SCRIPT.read_text(encoding="utf-8")
    assert src.count("zkCli ls {path} failed") == 1, (
        "the first check already raises; the duplicate was unreachable")
