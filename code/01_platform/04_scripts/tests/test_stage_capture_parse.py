"""Unit tests for stage_capture_parse.py (Stage A harness, plan:
docs/plans/2026-09-01-stage-throughput-latency-detection-plan.md).

Fixture-based — no live Flink dependency.
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from stage_capture_parse import (  # noqa: E402
    b2_consumer_read_report,
    b2_read_lag_report,
    compute_rates,
    divergence_report,
    operator_series,
    parse_stages_tsv,
    window_stats,
)

HEADER = ("epoch\tvertex_id\toperator\tnumRecordsIn\tnumRecordsOut\t"
          "busyMsSum\tbackpressuredMsSum\tidleMsSum")


def write_fixture(tmp: Path, rows: list[str]) -> Path:
    d = tmp / "capture"
    d.mkdir()
    (d / "stages.tsv").write_text("\n".join([HEADER] + rows) + "\n", encoding="utf-8")
    return d


def test_parse_and_rates():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        d = write_fixture(tmp, [
            "1000\tv1\tSource: raw-table-1\t1000\t1000\t500\t400\t100",
            "1005\tv1\tSource: raw-table-1\t6000\t6000\t500\t400\t100",
            "1010\tv1\tSource: raw-table-1\t11000\t10500\t500\t400\t100",
        ])
        rows = parse_stages_tsv(d / "stages.tsv")
        assert len(rows) == 3
        assert rows[0]["numRecordsIn"] == 1000
        series = operator_series(rows)
        rates = compute_rates(series["Source: raw-table-1"])
        # 1000 -> 6000 over 5s = 1000/s
        assert rates[0]["in_rate"] == 1000.0
        # 6000 -> 11000 over 5s = 1000/s in; 6000 -> 10500 => 900/s out
        assert rates[1]["in_rate"] == 1000.0
        assert rates[1]["out_rate"] == 900.0


def test_window_stats_and_throttle_flag():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        # fingerprint-dedup: in stays high, out collapses in the second window
        rows = []
        t = 2000
        for i in range(1, 13):  # 12 samples, 5s apart => 55s span
            cin = i * 5000
            cout = i * 5000 if i <= 6 else 6 * 5000 + (i - 6) * 500
            rows.append(f"{t}\tv2\tfingerprint-dedup\t{cin}\t{cout}\t400\t300\t300")
            t += 5
        d = write_fixture(tmp, rows)
        series = operator_series(parse_stages_tsv(d / "stages.tsv"))
        rates = compute_rates(series["fingerprint-dedup"])
        fresh = window_stats(rates, 2000, 2030)
        degraded = window_stats(rates, 2030, 2060)
        assert fresh["in_rate_avg"] == 1000.0
        assert fresh["out_rate_avg"] == 1000.0
        assert degraded["in_rate_avg"] == 1000.0
        assert degraded["out_rate_avg"] < 500  # OUT_THROTTLED territory

        report = divergence_report(d, [(2000, 2030), (2030, 2060)])
        assert "fingerprint-dedup" in report
        # first window: ratio 1.0 -> no OUT_THROTTLED; second: yes
        lines = report.splitlines()
        dedup_rows = [l for l in lines if l.startswith("fingerprint-dedup")]
        assert len(dedup_rows) == 2
        assert "OUT_THROTTLED" not in dedup_rows[0]
        assert "OUT_THROTTLED" in dedup_rows[1]


def test_source_flag_and_blank_counters():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        rows = [
            "3000\tv1\tSource: raw-table-1 -> raw-validation\t100\t100\t\t\t",
            "3005\tv1\tSource: raw-table-1 -> raw-validation\t5100\t5100\t0\t990\t10",
        ]
        d = write_fixture(tmp, rows)
        report = divergence_report(d, [(3000, 3010)])
        assert "SOURCE" in report
        # blank busy metrics must not crash the report
        assert "Source: raw-table-1 -> raw-validation" in report


def test_empty_and_malformed_lines_skipped():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        d = tmp / "capture"
        d.mkdir()
        (d / "stages.tsv").write_text(
            HEADER + "\nbad line without tabs\n", encoding="utf-8")
        rows = parse_stages_tsv(d / "stages.tsv")
        assert rows == []


def test_latency_report_quantile_names():
    from stage_capture_parse import latency_report
    prom = [{
        "epoch": 1000,
        "busy": {}, "backpressured": {}, "idle": {}, "watermarks": {},
        "latency": {("op_a", "0", "0"): {"0.5": 10.0, "0.98": 50.0, "0.999": 90.0}},
    }]
    out = latency_report(prom, [(1000, 1010)])
    lines = out.splitlines()
    assert lines[0].split("\t") == ["operator", "window",
                                     "p50_ms", "p98_ms", "p99.9_ms"], lines[0]
    assert "op_a" in lines[1]


# ---- B2 hook reports (2026-09-02, plan Stage B2) ---------------------------
def _write_b2_fixture(tmp: Path, lag_records: int = 200) -> Path:
    """stages.tsv source consuming 10k/s + read-lag.tsv log-end growing 200
    records per 5s sample FASTER than consumed (net +200/5s = 40/s) +
    consumer-read.tsv rows with a 300ms visibility staleness."""
    d = write_fixture(tmp, [
        "1000\tv1\tSource: raw-table-1 -> raw-validation\t0\t0\t0\t0\t0",
        "1005\tv1\tSource: raw-table-1 -> raw-validation\t10000\t10000\t0\t0\t0",
        "1010\tv1\tSource: raw-table-1 -> raw-validation\t20000\t20000\t0\t0\t0",
        "1015\tv1\tSource: raw-table-1 -> raw-validation\t30000\t30000\t0\t0\t0",
        "1020\tv1\tSource: raw-table-1 -> raw-validation\t40000\t40000\t0\t0\t0",
        "1025\tv1\tSource: raw-table-1 -> raw-validation\t50000\t50000\t0\t0\t0",
    ])
    # log-end: starts at lag_records (e.g. 2000) and grows 10000+lag_records
    # per 5s -> each 5s sample net-lags consumed by lag_records.
    base = lag_records * 5  # absolute offset; only deltas matter
    (d / "read-lag.tsv").write_text(
        "epoch_ms\ttable\tpartitions\tbuckets\tlog_end_sum\n" + "".join(
            f"{(1000 + i*5)*1000}\traw_table_1\t2\t16\t{base + 10000*i + lag_records*i}\n"
            for i in range(6)), encoding="utf-8")
    (d / "consumer-read.tsv").write_text(
        "epoch_ms\ttoken\twindow_start\toutput_ts\tlast_event_ts\tstaleness_ms\n"
        + "".join(f"{1010000+i}\t4\t5000\t{1010000+i-300}\t{1010000+i-305}\t305\n"
                  for i in range(10)), encoding="utf-8")
    return d


def test_b2_read_lag_report():
    with tempfile.TemporaryDirectory() as td:
        d = _write_b2_fixture(Path(td))
        out = b2_read_lag_report(d, [(1000, 1030)])
        lines = out.splitlines()
        assert lines[0].startswith("window\tlast_append_delta\tlast_consume_delta\t"
                                   "net_lag_p50_records\twindow_net_records")
        body = lines[1].split("\t")
        # Each 5s pair appends 10000+lag and consumes 10000 -> net lag_records.
        assert body[3] == f"{200}", body   # net_lag_p50 = injected per-pair lag
        assert body[4] == "1000", body     # window_net = 5 pairs x 200


def test_b2_consumer_read_report():
    with tempfile.TemporaryDirectory() as td:
        d = _write_b2_fixture(Path(td))
        out = b2_consumer_read_report(d, [(1000, 1030)])
        lines = out.splitlines()
        assert lines[0].startswith("window\tcp9cp10_p50_ms\tcp9cp10_p95_ms\tsamples")
        body = lines[1].split("\t")
        assert body[1] == "305", body   # injected visibility staleness
        assert body[3] == "10", body


def test_b2_reports_absent_files_degrade_with_note():
    with tempfile.TemporaryDirectory() as td:
        d = write_fixture(Path(td), [
            "1000\tv1\tSource: raw-table-1\t0\t0\t0\t0\t0",
        ])
        assert "absent" in b2_read_lag_report(d, [(1000, 1030)])
        assert "absent" in b2_consumer_read_report(d, [(1000, 1030)])
