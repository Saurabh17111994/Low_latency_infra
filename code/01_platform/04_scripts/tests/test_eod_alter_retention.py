#!/usr/bin/env python3
"""EOD retention extension is one `table.log.ttl` ALTER (A2) — the shadow rewrite cannot return.

WHY THIS EXISTS
---------------
Through 0.9.1 `table.log.ttl` was create-time only (verified 2026-08-13), so the EOD
controller's block-delete-unverified guard extended retention with a shadow-table
rewrite: create `name__eod_ext_<date>` with the extended TTL, copy every row
(`COPY_WRITE_BATCH` upserts over 16 `COPY_THREADS`), verify count parity, then an
operator swap. Fluss 1.0.0 makes the option alterable AND enforced — A1 probe GREEN
2026-09-25 (accepted + in force), A2 probe GREEN 2026-09-25 (enforced expiry follows the
ALTER), A2b probe GREEN 2026-09-25 (enforced on existing partitions of a partitioned
table) — so `extend --apply` is a single `Admin.alterTable(TablePath, changes, false)` — one `TableChange.set`
for `table.log.ttl`, plus (2026-09-30, A2) a second SET of
`table.auto-partition.num-retention` when the table is partitioned, because a
partitioned table ages out by BOTH clocks. This test pins that contract so the
~250-line copy machinery (and its 180s/385k-row drill) cannot silently return.

HONEST LIMITATIONS (recorded, not hidden)
-----------------------------------------
* Static source assertions, not a live drill: the behavioral proof lives in the A1/A2/A2b
  probe evidence under `logs/soak/` (A2: `a2-alter-behavior-probe-20260925T072731Z`,
  A2b: `a2b-partitioned-probe-20260925T074250Z`). This test only guards that the code path
  stays an ALTER and that the rewrite stays gone.
* Anchors are file+literal pairs; a rename in the Java file fails here by design (same
  pattern as `test_compute_identifier_parity.py`).
"""

from pathlib import Path
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
REPO = SCRIPTS.parents[2]
EOD = REPO / "code/common/src/main/java/com/trading/common/schema/eod"
TOOL = EOD / "EodControllerTool.java"
POLICY = EOD / "EodRetentionPolicy.java"
OLD_COPY_TEST = (
    REPO / "code/common/src/test/java/com/trading/common/schema/eod/EodBucketCopyIntegrationTest.java"
)


class EodAlterRetentionTest(unittest.TestCase):
    def setUp(self):
        self.tool = TOOL.read_text(encoding="utf-8")
        self.policy = POLICY.read_text(encoding="utf-8")

    def test_extend_uses_one_alter_table(self):
        """The extension is an in-place ALTER of table.log.ttl, routed through alterTtl()."""
        self.assertIn('TableChange.set("table.log.ttl"', self.tool)
        self.assertIn("admin.alterTable(TablePath.of(", self.tool)
        self.assertIn("alterTtl(admin, opts.database, p.table(),", self.tool)

    def test_partitioned_tables_extend_the_partition_retention_too(self):
        """A2 (2026-09-30): the extension raises BOTH expiry mechanisms.

        A partitioned table ages out by the log TTL *and* auto-partition GC
        (num-retention); extending only the TTL would leave the partition to
        drop at its original count. The ALTER includes the retention change,
        gated on the table's partition keys.
        """
        self.assertIn('TableChange.set("table.auto-partition.num-retention"', self.tool)
        self.assertIn("getPartitionKeys()", self.tool)

    def test_shadow_rewrite_machinery_is_gone(self):
        """The 0.9.1 shadow create/copy/swap path must not come back."""
        for gone in ("performRewrite", "COPY_WRITE_BATCH", "COPY_THREADS", "__eod_ext_"):
            self.assertNotIn(gone, self.tool, f"shadow-rewrite machinery '{gone}' is back")
        for gone in (
            "import org.apache.fluss.client.table.scanner.batch.BatchScanner;",
            "import org.apache.fluss.client.table.writer.UpsertWriter;",
        ):
            self.assertNotIn(gone, self.tool, f"copy-path import '{gone}' is back")
        self.assertFalse(OLD_COPY_TEST.exists(), "EodBucketCopyIntegrationTest.java is back")

    def test_policy_records_the_1_0_alterable_rationale(self):
        """The create-only rationale is retired; the alterable+enforced rationale is recorded."""
        self.assertNotIn("create-only", self.policy)
        self.assertIn("alterable", self.policy)


if __name__ == "__main__":
    unittest.main(verbosity=2)
