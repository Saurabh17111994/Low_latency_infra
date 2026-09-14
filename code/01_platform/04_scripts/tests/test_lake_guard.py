"""Wave 12 — lake-guard.sh (P6-121, P6-122, P6-436..P6-440, P6-755, P6-756).

The guard is addressed through LAKE_GUARD_SCRIPT and always runs beside a stub
r2-list.sh in a scratch dir, so the suite can drive listing failures, control
object sizes/timestamps and record which prefixes were listed — without R2 — and
the same suite can be pointed at the pre-fix script for the recorded red run.
"""

import datetime
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "code" / "01_platform" / "04_scripts"
GUARD = pathlib.Path(os.environ.get("LAKE_GUARD_SCRIPT", SCRIPTS / "lake-guard.sh"))

TABLE = "lake/default/raw_table_1/"
MANIFEST = TABLE + "metadata/00001-abc.avro"

STUB = """#!/usr/bin/env bash
# stub r2-list.sh for the lake-guard suite: records the prefix it was asked for,
# can fail on demand, and prints the fixture TSV regardless of prefix so the
# guard's own prefix filtering is what the tests exercise.
R2_PREFIX="${STUB_R2_PREFIX:-lake}"
r2_load() { return "${STUB_R2_LOAD_RC:-0}"; }
r2_list_lake() {
    printf '%s\\n' "${1:-}" >> "$STUB_CALLS"
    if [ -n "${STUB_FAIL_PREFIX:-}" ] && [ "${1:-}" = "$STUB_FAIL_PREFIX" ]; then
        printf 'stub: ListObjectsV2 HTTP 403 AccessDenied\\n' >&2
        return 1
    fi
    cat "$STUB_TSV"
}
"""


def _iso(delta_hours=0):
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=delta_hours)
    return now.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def tsv(*objects):
    """objects: (key, size, stamp) triples -> the r2_list_lake TSV contract."""
    return "".join(f"{k}\t{s}\t{t}\n" for k, s, t in objects)


def day(count, day_, size=1024, ext=".parquet", stamp=None):
    return [(
        f"{TABLE}data/event_day={day_}/{i:02d}{ext}", size,
        _iso() if stamp is None else stamp,
    ) for i in range(count)]


class LakeGuardTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="lake-guard.")
        self.dir = pathlib.Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.script = self.dir / "lake-guard.sh"
        shutil.copy2(GUARD, self.script)
        (self.dir / "r2-list.sh").write_text(STUB, encoding="utf-8")
        self.calls = self.dir / "calls.log"
        self.calls.write_text("", encoding="utf-8")
        self.tsv = self.dir / "objects.tsv"

    def run_guard(self, objects, *, env=None, check_day="20260913", after="9999",
                  fail_prefix=None, r2_prefix=None, r2_load_rc=None):
        self.tsv.write_text(tsv(*objects), encoding="utf-8")
        full = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(self.dir),
            "TMPDIR": str(self.dir),
            "STUB_CALLS": str(self.calls),
            "STUB_TSV": str(self.tsv),
            "LAKE_GUARD_CHECK_DAY": check_day,
            "LAKE_GUARD_AFTER": after,
            "LC_ALL": "C",
        }
        for key, value in (
            ("STUB_FAIL_PREFIX", fail_prefix),
            ("STUB_R2_PREFIX", r2_prefix),
            ("STUB_R2_LOAD_RC", r2_load_rc),
        ):
            if value is not None:
                full[key] = str(value)
        full.update(env or {})
        return subprocess.run(["bash", str(self.script)], capture_output=True,
                              text=True, env=full, timeout=60)

    def listed(self):
        return [line for line in self.calls.read_text(encoding="utf-8").splitlines() if line]

    # ── pass ─────────────────────────────────────────────────────────────────
    def test_pass_on_a_real_parquet_and_a_fresh_manifest(self):
        out = self.run_guard(day(2, "20260913") + [(MANIFEST, 4096, _iso()),
                                                  (TABLE + "metadata/00002-def.avro", 2048, _iso())])
        self.assertEqual(out.returncode, 0, out.stderr + out.stdout)
        self.assertIn("lake-guard PASS", out.stdout)
        self.assertIn("yesterday data objects: 2", out.stdout)
        self.assertIn("iceberg manifests: 2", out.stdout)

    # ── P6-121 / P6-122: markers and sidecars are not data ───────────────────
    def test_success_marker_and_sidecars_do_not_satisfy_the_day_check(self):
        out = self.run_guard([
            (TABLE + "data/event_day=20260913/_SUCCESS", 0, _iso()),
            (TABLE + "data/event_day=20260913/.1.crc", 155, _iso()),
            (TABLE + "data/event_day=20260913/", 0, _iso()),
            (MANIFEST, 4096, _iso()),
        ])
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("yesterday data objects: 0", out.stdout)
        self.assertIn("!! lake-guard FAIL: no data for 20260913", out.stderr)

    def test_zero_byte_parquet_is_not_data(self):
        out = self.run_guard(day(1, "20260913", size=0) + [(MANIFEST, 4096, _iso())])
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("yesterday data objects: 0", out.stdout)

    def test_object_from_another_day_is_not_counted(self):
        out = self.run_guard(day(1, "20260901") + [(MANIFEST, 4096, _iso())])
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("yesterday data objects: 0", out.stdout)

    def test_today_uses_its_own_listing_and_marks_only_objects_still_fail(self):
        # P6-440: the today-check must see the current listing, not a snapshot
        # taken before it — and it must apply the same data-file rule (P6-122).
        out = self.run_guard(
            day(2, "20260913")
            + [(MANIFEST, 4096, _iso()), (TABLE + "metadata/00002-def.avro", 2048, _iso())]
            + [(TABLE + "data/event_day=20260914/_SUCCESS", 0, _iso())],
            after="0000",
        )
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("today data objects: 0", out.stdout)
        self.assertIn("no data for 20260914 after 00:00 IST", out.stderr)
        self.assertIn(f"{TABLE}data/event_day=20260914/", self.listed())

    # ── P6-437: one prefix per check, never the whole warehouse ──────────────
    def test_each_check_lists_only_its_own_prefix(self):
        out = self.run_guard(day(1, "20260913") + [(MANIFEST, 4096, _iso()), (MANIFEST, 4096, _iso())])
        self.assertEqual(out.returncode, 0, out.stderr + out.stdout)
        self.assertEqual(self.listed(),
                         [f"{TABLE}data/event_day=20260913/", f"{TABLE}metadata/"],
                         "one prefix-filtered listing per check, no whole-warehouse listing")

    def test_after_the_deadline_today_is_listed_third(self):
        out = self.run_guard(day(1, "20260913") + day(1, "20260914")
                             + [(MANIFEST, 4096, _iso()), (MANIFEST, 4096, _iso())],
                             after="0000")
        self.assertEqual(out.returncode, 0, out.stderr + out.stdout)
        self.assertEqual(self.listed(), [
            f"{TABLE}data/event_day=20260913/",
            f"{TABLE}metadata/",
            f"{TABLE}data/event_day=20260914/",
        ])

    # ── P6-438: a listing failure is a FAIL with a reason ────────────────────
    def test_listing_failure_reports_a_fail_line(self):
        out = self.run_guard(day(1, "20260913"), fail_prefix=f"{TABLE}data/event_day=20260913/")
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("!! lake-guard FAIL: R2 listing failed", out.stderr)
        self.assertIn("HTTP 403 AccessDenied", out.stderr)

    def test_unreadable_config_reports_a_fail_line(self):
        out = self.run_guard(day(1, "20260913"), r2_load_rc=1)
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("!! lake-guard FAIL: cannot read the R2 config", out.stderr)

    # ── P6-436: the day must be a day, not a regex ───────────────────────────
    def test_unvalidated_check_day_is_rejected(self):
        for hostile in (".*", "2026-09-13", "2026091", "20260913|"):
            with self.subTest(hostile=hostile):
                out = self.run_guard(day(1, "20260913"), check_day=hostile)
                self.assertEqual(out.returncode, 2, out.stdout)
                self.assertIn("!! lake-guard INPUT: LAKE_GUARD_CHECK_DAY", out.stderr)
                self.assertEqual(self.listed(), [], "no listing happens before validation")
                self.assertNotIn("PASS", out.stdout)

    def test_bad_numeric_knobs_are_rejected(self):
        for key, value in (("LAKE_GUARD_MIN_MANIFESTS", "two"),
                           ("LAKE_GUARD_MAX_MANIFEST_AGE_H", "-1"),
                           ("LAKE_GUARD_AFTER", "18:30")):
            with self.subTest(key=key):
                out = self.run_guard(day(1, "20260913"), env={key: value})
                self.assertEqual(out.returncode, 2, out.stdout)
                self.assertIn(f"!! lake-guard INPUT: {key}", out.stderr)

    # ── P6-439: an old manifest set is not a live table ──────────────────────
    def test_stale_manifest_fails(self):
        out = self.run_guard(day(2, "20260913") + [
            (MANIFEST, 4096, _iso(-100)),
            (TABLE + "metadata/00002-def.avro", 2048, _iso(-100)),
        ])
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("newest manifest is 100h old", out.stderr)
        self.assertIn("the latest commit never landed", out.stderr)

    def test_zero_byte_manifests_are_orphans_not_manifests(self):
        out = self.run_guard(day(2, "20260913") + [
            (MANIFEST, 0, _iso()),
            (TABLE + "metadata/00002-def.avro", 0, _iso()),
        ])
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("iceberg manifests: 0", out.stdout)

    def test_manifest_without_a_timestamp_refuses_to_pass(self):
        out = self.run_guard(day(2, "20260913") + [
            (MANIFEST, 4096, ""),
            (TABLE + "metadata/00002-def.avro", 2048, ""),
        ])
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("carries a LastModified timestamp", out.stderr)

    # ── P6-755 / P6-756: knobs instead of magic numbers and hard-coded paths ─
    def test_min_manifest_floor_is_configurable(self):
        objects = day(2, "20260913") + [(MANIFEST, 4096, _iso())]
        default = self.run_guard(objects)
        self.assertEqual(default.returncode, 1, default.stdout)
        self.assertIn("only 1 non-empty manifest(s)", default.stderr)
        relaxed = self.run_guard(objects, env={"LAKE_GUARD_MIN_MANIFESTS": "1"})
        self.assertEqual(relaxed.returncode, 0, relaxed.stderr + relaxed.stdout)
        self.assertIn("iceberg manifests: 1 (floor 1)", relaxed.stdout)

    def test_table_prefix_follows_the_warehouse_prefix(self):
        # The fixture keys use the default "lake/..." table; with R2_PREFIX set to
        # something else the guard must target THAT warehouse, so the same objects
        # are no longer data for this table (P6-756: the prefix is derived, not
        # hard-coded).
        out = self.run_guard(day(2, "20260913") + [(MANIFEST, 4096, _iso())],
                             r2_prefix="other-bucket")
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("table=other-bucket/default/raw_table_1/", out.stdout)
        self.assertEqual(self.listed()[0], "other-bucket/default/raw_table_1/data/event_day=20260913/")
        self.assertIn("yesterday data objects: 0", out.stdout)

    def test_table_prefix_can_be_overridden(self):
        custom = "lake/default/raw_table_9/"
        objects = [(f"{custom}data/event_day=20260913/00.parquet", 10, _iso()),
                   (f"{custom}metadata/m1.avro", 10, _iso()),
                   (f"{custom}metadata/m2.avro", 10, _iso())]
        out = self.run_guard(objects, env={"LAKE_GUARD_TABLE_PREFIX": custom})
        self.assertEqual(out.returncode, 0, out.stderr + out.stdout)
        self.assertIn(f"table={custom}", out.stdout)
        self.assertEqual(self.listed(), [f"{custom}data/event_day=20260913/", f"{custom}metadata/"])


if __name__ == "__main__":
    unittest.main()
