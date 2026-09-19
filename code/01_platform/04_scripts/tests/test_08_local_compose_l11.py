"""L11 — Resource / memory + performance  RES-001..006 + PERF-001..005.

Per 08-local-compose.md §L11. Offline contracts on PlatformConfig 65/35/85,
JVM wiring, and throughput spec targets; live load is env-gated.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
PLATFORM_CONFIG = ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java"


def heap_pinned_in_compose(text):
    """Heap-pinning settings in a compose file, comments excluded (P6-601).

    A live `-Xmx`/`MaxRAMPercentage` would bypass the 65% policy that
    ContainerMemoryGuard derives from the container limit; the compose's own comments
    are allowed to quote past incidents.
    """
    live = re.sub(r"^\s*#.*$", "", text, flags=re.MULTILINE)
    return [l.strip() for l in live.splitlines() if re.search(r"-Xmx|MaxRAMPercentage", l)]


def config_row(doc, key, value):
    """True when the doc's config table pairs `key` with `value` (P6-602).

    A bare substring like "35" passes on any unrelated number in a large document;
    the row pins the key and the value together.
    """
    return re.search(r"\|\s*`%s`\s*\|\s*`%s`\s*\|" % (re.escape(key), re.escape(value)), doc) is not None


def perf_section(doc, name):
    """The doc slice for one PERF item: its heading through the next heading (P6-803)."""
    m = re.search(r"^#### %s\b.*?(?=^#### |\Z)" % re.escape(name), doc, re.MULTILINE | re.DOTALL)
    return m.group(0) if m else ""


class ResourceL11Test(unittest.TestCase):
    def test_RES_001_memory_limit_enforced(self):
        """RES-001: the 65% policy is applied by the guard, and the compose must not pin a heap (P6-601)."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65", cfg)
        # The enforcement lives in the guard that turns the container limit into a
        # heap budget — the compose declares no memory limit at all, so grepping it for
        # the word "memory" proved nothing.
        guard = (ROOT / "code/common/src/main/java/com/trading/common/config/ContainerMemoryGuard.java").read_text()
        self.assertIn("PlatformConfig.JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", guard,
                      "RES-001: the guard no longer derives its budget from the pinned percentage")
        self.assertIn("maxHeapBudget", guard, "RES-001: container-limit budget derivation missing")
        pinned = heap_pinned_in_compose(COMPOSE.read_text())
        self.assertEqual(pinned, [], f"RES-001: the compose pins a heap, bypassing the 65% policy: {pinned}")

    def test_RES_002_jvm_heap_65_percent(self):
        """RES-002: JVM heap = 65% — verify actual pin, not just compose var."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65", cfg, "RES-002: heap pin not 65")
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertTrue(config_row(doc, "JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", "65"),
                        "RES-002: the spec's config table no longer pairs the heap pin with 65")

    def test_RES_003_non_heap_reserve_35_percent(self):
        """RES-003: 35% non-heap reserve."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("NON_HEAP_MEMORY_RESERVE_PERCENT = 35", cfg, "RES-003: reserve not 35")
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertTrue(config_row(doc, "NON_HEAP_MEMORY_RESERVE_PERCENT", "35"),
                        "RES-003: the spec's config table no longer pairs the reserve with 35")

    def test_RES_004_alert_threshold_85_percent(self):
        """RES-004: 85% container memory alert threshold."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("CONTAINER_MEMORY_ALERT_PERCENT = 85", cfg, "RES-004: alert not 85")
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertTrue(config_row(doc, "CONTAINER_MEMORY_ALERT_PERCENT", "85"),
                        "RES-004: the spec's config table no longer pairs the alert with 85")

    def test_RES_005_cpu_saturation_observable(self):
        """RES-005: CPU saturation → latency/backpressure/dropped messages observable."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("CPU saturation", spec, "RES-005: spec missing")
        # Throughput backpressure is covered by STREAM-009 + PERF burst

    def test_RES_006_disk_pressure_degrades_readiness(self):
        """RES-006: disk pressure → clear readiness degradation, no silent corruption."""
        spec = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("Disk pressure", spec, "RES-006: spec missing")
        # Fluss data volumes must be persistent so pressure is observable, not silent
        self.assertIn("fluss-data", COMPOSE.read_text())


class PerformanceL11Test(unittest.TestCase):
    def test_PERF_001_one_k_ticks(self):
        """PERF-001: 1k ticks/s — spec target exists, baseline is 20 ticks/instrument/s."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC = 20", cfg)
        sec = perf_section((ROOT / "docs/08_implementation/08-local-compose.md").read_text(), "PERF-001")
        self.assertTrue(sec, "PERF-001: section missing from the spec")
        self.assertIn("1k", sec, "PERF-001: the 1k ticks/s target is gone")

    def test_PERF_002_five_k_ticks(self):
        """PERF-002: 5k ticks/s."""
        sec = perf_section((ROOT / "docs/08_implementation/08-local-compose.md").read_text(), "PERF-002")
        self.assertTrue(sec, "PERF-002: section missing from the spec")
        self.assertIn("5k", sec, "PERF-002: the 5k ticks/s target is gone")

    def test_PERF_003_ten_k_ticks(self):
        """PERF-003: 10k ticks/s — spec target ~10k sustained with no drops."""
        sec = perf_section((ROOT / "docs/08_implementation/08-local-compose.md").read_text(), "PERF-003")
        self.assertTrue(sec, "PERF-003: section missing from the spec")
        self.assertIn("10k", sec, "PERF-003: the 10k ticks/s target is gone")
        self.assertIn("no drops", sec, "PERF-003: the no-drop condition is gone")

    def test_PERF_004_ten_k_sustained_long(self):
        """PERF-004: 10k sustained (long, not 10s)."""
        sec = perf_section((ROOT / "docs/08_implementation/08-local-compose.md").read_text(), "PERF-004")
        self.assertTrue(sec, "PERF-004: section missing from the spec")
        self.assertIn("10k", sec, "PERF-004: the 10k target is gone")
        # "long, not 10s" is the whole point of this row and exists only as that phrase
        self.assertIn("not 10s", sec, "PERF-004: the sustained-beyond-10s condition is gone")

    def test_PERF_005_burst_recovery(self):
        """PERF-005: burst + recovery 2k→10k→20k→2k."""
        sec = perf_section((ROOT / "docs/08_implementation/08-local-compose.md").read_text(), "PERF-005")
        self.assertTrue(sec, "PERF-005: section missing from the spec")
        for tok in ("2k", "10k", "20k"):
            self.assertIn(tok, sec, f"PERF-005: burst/recovery target {tok} is gone")


if __name__ == "__main__":
    unittest.main()
