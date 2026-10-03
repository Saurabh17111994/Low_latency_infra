"""XC-17: split_manifest.py must refuse a chunk list that does not sum to the
input rows. Under-supply used to warn, write fewer slot files than requested,
and exit 0 (a header-only input wrote slot1.csv and stopped)."""
import csv
import subprocess
import sys
from pathlib import Path

SCRIPT = (Path(__file__).resolve().parents[2]
          / "05_instruments" / "split_manifest.py")


def _write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["Token", "Symbol"])
        for i in range(rows):
            w.writerow([1000 + i, f"S{i}"])


def _run(src, out, chunks):
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--input", str(src),
         "--out-dir", str(out), "--chunks", chunks],
        capture_output=True, text=True)


def test_under_supplied_chunks_fail_without_writing_partial_files(tmp_path):
    src = tmp_path / "in.csv"
    _write_csv(src, 5)
    out = tmp_path / "out"
    out.mkdir()
    r = _run(src, out, "3,3,3")
    assert r.returncode == 1, (r.returncode, r.stdout, r.stderr)
    assert "refusing to write partial" in r.stderr, r.stderr
    assert list(out.glob("*.csv")) == [], (
        "no partial slot files may be written when the chunks do not sum")


def test_over_supplied_chunks_still_fail(tmp_path):
    src = tmp_path / "in.csv"
    _write_csv(src, 7)
    out = tmp_path / "out"
    out.mkdir()
    r = _run(src, out, "3,3,3")
    assert r.returncode == 1, (r.returncode, r.stdout, r.stderr)


def test_exact_chunks_still_write(tmp_path):
    src = tmp_path / "in.csv"
    _write_csv(src, 6)
    out = tmp_path / "out"
    out.mkdir()
    r = _run(src, out, "3,3")
    assert r.returncode == 0, (r.returncode, r.stdout, r.stderr)
    assert sorted(p.name for p in out.glob("*.csv")) == ["slot1.csv", "slot2.csv"]
