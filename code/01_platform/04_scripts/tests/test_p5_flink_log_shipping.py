"""P5-027/028 — flink log4j rollover keeps the collector shipping contract.

The otel-collector filelog/flink receiver includes /data/flink/logs/*.log.
Two silent-breakage modes guarded here:
  - P5-027: an unresolved ${sys:log.file} lookup writes a literal file
    named "log.file" in the cwd (never ships) — the fileName needs a
    default matching the entrypoint's FLINK_LOG_DIR.
  - P5-028: rotated files must keep the .log suffix — "<role>.log.%i"
    fell out of the *.log glob at the first 10MB rollover and the rolled
    content silently stopped shipping.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
LOG4J = ROOT / "code/01_platform/01_docker/flink-log4j-console.properties"
COLLECTORS = [
    ROOT / "code/01_platform/01_docker/otel-collector-config.yaml",
    ROOT / "code/01_platform/01_docker/otel-collector-config.swarm.yaml",
]


class FlinkLogShippingTests(unittest.TestCase):
    def test_p5_027_file_name_has_default(self):
        m = re.search(r"appender\.rolling\.fileName\s*=\s*(.+)", LOG4J.read_text())
        self.assertIsNotNone(m, "rolling fileName missing")
        self.assertIn(":-", m.group(1),
                      "fileName must default the ${sys:log.file} lookup "
                      "(else a literal 'log.file' file appears in cwd)")
        self.assertIn("/opt/flink/log", m.group(1),
                      "default must match the entrypoint FLINK_LOG_DIR")

    def test_p5_028_rollover_keeps_log_suffix(self):
        m = re.search(r"appender\.rolling\.filePattern\s*=\s*(.+)", LOG4J.read_text())
        self.assertIsNotNone(m, "rolling filePattern missing")
        self.assertTrue(m.group(1).strip().endswith(".log"),
                        "rotated files must keep the .log suffix to stay in "
                        "the collector's *.log include glob")

    def test_collector_flink_glob_covers_rotated_names(self):
        """Contract pair: if either side changes, shipping breaks silently.
        filePattern ends .log <=> collector includes *.log for /data/flink."""
        for c in COLLECTORS:
            m = re.search(r"filelog/flink:\s*\n\s*include:\s*\[([^\]]*)\]", c.read_text())
            self.assertIsNotNone(m, f"{c.name}: filelog/flink receiver missing")
            self.assertIn("*.log", m.group(1), f"{c.name}: flink include must cover rotated *.log")


if __name__ == "__main__":
    unittest.main()
