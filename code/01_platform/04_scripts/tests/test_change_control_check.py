"""Unit tests for change_control_check.py — the change-control reconciliation
validator (01-foundation.md "Change control", orig L205).
"""

import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import change_control_check as ccc

GOOD_RECORD = """\
# CHG-001

## Context

The signal-job serializer changes.

```text
affected_artifacts: schema_manifest.json, 04-signal-job.md
compatibility_class: COMPATIBLE_WITH_LIMITATION
savepoint_impact: migration — new serializer, old savepoints unreadable
test_updates: SIG-UNIT-010, replay battery
rollback_behavior: restore prior jar + savepoint
plan_tasks: tracker-04 P10
```
"""


def record_without(field):
    """GOOD_RECORD minus one required field line."""
    lines = GOOD_RECORD.splitlines()
    return "\n".join(
        ln for ln in lines if not ln.startswith(f"{field}:")
    ) + "\n"


class ParseTests(unittest.TestCase):
    def test_parses_all_six_fields(self):
        fields = ccc.parse_record(GOOD_RECORD)
        self.assertEqual(
            set(fields),
            {
                "affected_artifacts",
                "compatibility_class",
                "savepoint_impact",
                "test_updates",
                "rollback_behavior",
                "plan_tasks",
            },
        )
        self.assertEqual(fields["compatibility_class"], "COMPATIBLE_WITH_LIMITATION")

    def test_fields_outside_fenced_block_ignored(self):
        text = "affected_artifacts: prose, not a record\n\n" + GOOD_RECORD
        fields = ccc.parse_record(text)
        self.assertEqual(
            fields["affected_artifacts"],
            "schema_manifest.json, 04-signal-job.md",
        )

    def test_duplicate_key_first_wins(self):
        text = GOOD_RECORD.replace(
            "compatibility_class: COMPATIBLE_WITH_LIMITATION\n",
            "compatibility_class: INCOMPATIBLE\n"
            "compatibility_class: COMPATIBLE_WITH_LIMITATION\n",
        )
        self.assertEqual(
            ccc.parse_record(text)["compatibility_class"], "INCOMPATIBLE"
        )

    def test_comments_ignored(self):
        text = GOOD_RECORD.replace(
            "affected_artifacts:",
            "# a comment line\naffected_artifacts:",
        )
        self.assertEqual(ccc.parse_record(text)["affected_artifacts"],
                         "schema_manifest.json, 04-signal-job.md")


class ValidateTests(unittest.TestCase):
    def test_complete_record_has_no_issues(self):
        self.assertEqual(ccc.validate_text(GOOD_RECORD), [])

    def test_each_missing_field_is_reported(self):
        for field in ccc.REQUIRED_FIELDS:
            issues = ccc.validate_text(record_without(field))
            self.assertIn(f"missing required field '{field}'", issues)
            self.assertEqual(len(issues), 1, issues)

    def test_empty_value_is_missing(self):
        text = GOOD_RECORD.replace(
            "plan_tasks: tracker-04 P10",
            "plan_tasks:",
        )
        issues = ccc.validate_text(text)
        self.assertIn("missing required field 'plan_tasks'", issues)

    def test_invalid_compatibility_class_rejected(self):
        text = GOOD_RECORD.replace(
            "COMPATIBLE_WITH_LIMITATION", "SORT_OF_COMPATIBLE"
        )
        issues = ccc.validate_text(text)
        self.assertTrue(
            any("compatibility_class 'SORT_OF_COMPATIBLE'" in i for i in issues)
        )

    def test_all_valid_compatibility_classes_accepted(self):
        for cls in ccc.COMPATIBILITY_CLASSES:
            text = GOOD_RECORD.replace("COMPATIBLE_WITH_LIMITATION", cls)
            self.assertEqual(ccc.validate_text(text), [], cls)

    def test_no_fenced_block_fails(self):
        self.assertIn(
            "no fenced ```text record block",
            ccc.validate_text("# CHG-002\nno block here"),
        )

    def test_multiple_missing_fields_reported(self):
        text = GOOD_RECORD.replace("plan_tasks: tracker-04 P10\n", "")
        text = text.replace("rollback_behavior: restore prior jar + savepoint\n", "")
        issues = ccc.validate_text(text)
        self.assertEqual(
            sorted(issues),
            [
                "missing required field 'plan_tasks'",
                "missing required field 'rollback_behavior'",
            ],
        )


class ScanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _write(self, name, body):
        with open(os.path.join(self.tmp.name, name), "w", encoding="utf-8") as fh:
            fh.write(body)

    def test_template_excluded_from_records(self):
        self._write("_template.md", GOOD_RECORD)
        files, issues, missing = ccc.scan_records(self.tmp.name)
        self.assertFalse(missing)
        self.assertEqual(files, [])
        self.assertEqual(issues, {})

    def test_good_and_bad_records_reported(self):
        self._write("CHG-001.md", GOOD_RECORD)
        self._write("CHG-002.md", record_without("test_updates"))
        files, issues, missing = ccc.scan_records(self.tmp.name)
        self.assertEqual(files, ["CHG-001.md", "CHG-002.md"])
        self.assertEqual(issues["CHG-001.md"], [])
        self.assertIn("missing required field 'test_updates'", issues["CHG-002.md"])

    def test_missing_directory_detected(self):
        files, issues, missing = ccc.scan_records(
            os.path.join(self.tmp.name, "nope")
        )
        self.assertTrue(missing)
        self.assertEqual(files, [])

    def test_validate_file_reports_unreadable(self):
        issues = ccc.validate_file(os.path.join(self.tmp.name, "absent.md"))
        self.assertTrue(issues)
        self.assertTrue(issues[0].startswith("unreadable"))


class PlanTasksReferenceTests(unittest.TestCase):
    """plan_tasks references must resolve to real trackers/dossiers."""

    def test_tracker_reference_resolves(self):
        self.assertEqual(ccc.plan_task_issues("tracker-04 P10", ccc.DEFAULT_RECORDS_DIR), [])

    def test_tracker_without_dossier_fails(self):
        issues = ccc.plan_task_issues("tracker-99", ccc.DEFAULT_RECORDS_DIR)
        self.assertTrue(any("tracker-99" in i and "99-*.md" in i for i in issues))

    def test_repo_relative_md_resolves(self):
        self.assertEqual(
            ccc.plan_task_issues("docs/08_implementation/03-ingestion.md", ccc.DEFAULT_RECORDS_DIR),
            [],
        )

    def test_bare_dossier_name_resolves(self):
        self.assertEqual(ccc.plan_task_issues("03-ingestion.md", ccc.DEFAULT_RECORDS_DIR), [])

    def test_anchor_stripped_before_resolution(self):
        self.assertEqual(
            ccc.plan_task_issues(
                "11-testing-and-release.md#performance-benchmark-procedure",
                ccc.DEFAULT_RECORDS_DIR,
            ),
            [],
        )

    def test_unknown_md_fails(self):
        issues = ccc.plan_task_issues("definitely-not-a-file.md", ccc.DEFAULT_RECORDS_DIR)
        self.assertTrue(any("unknown file" in i for i in issues))

    def test_none_value_needs_no_reference(self):
        for v in ("none", "N/A", "-", "none — no plan task"):
            self.assertEqual(ccc.plan_task_issues(v, ccc.DEFAULT_RECORDS_DIR), [], v)

    def test_records_dir_relative_md_resolves(self):
        self.assertEqual(
            ccc.plan_task_issues(
                "../../08_implementation/03-ingestion.md", ccc.DEFAULT_RECORDS_DIR
            ),
            [],
        )

    def test_validate_file_reports_phantom_plan_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "CHG-001.md")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(GOOD_RECORD.replace("tracker-04 P10", "tracker-99"))
            issues = ccc.validate_file(path)
            self.assertTrue(any("tracker-99" in i for i in issues))


class AffectedArtifactsReferenceTests(unittest.TestCase):
    """affected_artifacts path-shaped tokens must resolve to real files."""

    def test_repo_relative_artifact_resolves(self):
        self.assertEqual(
            ccc.artifact_issues(
                "code/common/src/main/java/com/trading/common/config/PlatformConfig.java",
                ccc.DEFAULT_RECORDS_DIR,
            ),
            [],
        )

    def test_bare_md_artifact_resolves(self):
        self.assertEqual(
            ccc.artifact_issues("04-signal-job.md", ccc.DEFAULT_RECORDS_DIR), []
        )

    def test_bare_json_resolves_via_basename_search(self):
        self.assertEqual(
            ccc.artifact_issues("schema_manifest.json", ccc.DEFAULT_RECORDS_DIR), []
        )

    def test_records_dir_relative_artifact_resolves(self):
        self.assertEqual(
            ccc.artifact_issues(
                "../../02_requirements/04-data.md", ccc.DEFAULT_RECORDS_DIR
            ),
            [],
        )

    def test_unknown_artifact_fails(self):
        issues = ccc.artifact_issues(
            "totally-made-up-file.sql", ccc.DEFAULT_RECORDS_DIR
        )
        self.assertTrue(any("unknown artifact" in i for i in issues))

    def test_none_value_needs_no_reference(self):
        for v in ("none", "N/A", "-"):
            self.assertEqual(ccc.artifact_issues(v, ccc.DEFAULT_RECORDS_DIR), [], v)

    def test_prose_without_path_shape_ignored(self):
        self.assertEqual(
            ccc.artifact_issues(
                "the signal-job serializer and the trade decisions table",
                ccc.DEFAULT_RECORDS_DIR,
            ),
            [],
        )

    def test_non_artifact_extension_ignored(self):
        self.assertEqual(
            ccc.artifact_issues("Flink 2.2.1 with v0.91.5 deps", ccc.DEFAULT_RECORDS_DIR),
            [],
        )

    def test_validate_file_reports_phantom_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "CHG-001.md")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(GOOD_RECORD.replace("schema_manifest.json", "no-such-manifest.json"))
            issues = ccc.validate_file(path)
            self.assertTrue(any("no-such-manifest.json" in i for i in issues))


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _write(self, name, body):
        with open(os.path.join(self.tmp.name, name), "w", encoding="utf-8") as fh:
            fh.write(body)

    def test_good_dir_exits_zero(self):
        self._write("CHG-001.md", GOOD_RECORD)
        self.assertEqual(ccc.main(["--dir", self.tmp.name]), 0)

    def test_bad_dir_exits_one(self):
        self._write("CHG-001.md", record_without("savepoint_impact"))
        self.assertEqual(ccc.main(["--dir", self.tmp.name]), 1)

    def test_missing_dir_exits_one(self):
        self.assertEqual(
            ccc.main(["--dir", os.path.join(self.tmp.name, "missing")]), 1
        )

    def test_empty_dir_exits_zero(self):
        self.assertEqual(ccc.main(["--dir", self.tmp.name]), 0)

    def test_dir_flag_without_value_exits_two(self):
        self.assertEqual(ccc.main(["--dir"]), 2)


class TrackerRegexTests(unittest.TestCase):
    """A tracker reference is a standalone token, never a word ending in
    "tracker" (P6-716)."""

    def test_words_ending_in_tracker_are_not_references(self):
        for text in ("tasktracker-5", "non-tracker-5.md", "backTracker_3"):
            self.assertEqual(ccc.TRACKER_RE.findall(text), [], text)

    def test_standalone_reference_still_matches(self):
        self.assertEqual(ccc.TRACKER_RE.findall("tracker-14"), ["14"])
        self.assertEqual(ccc.TRACKER_RE.findall("logs/tracker-14/ run"), ["14"])
        self.assertEqual(ccc.TRACKER_RE.findall("(tracker 7)"), ["7"])


class RepoIndexTests(unittest.TestCase):
    """The repo-wide fallback resolves bare names and path suffixes, and pays
    for the walk once per process (P6-325, P6-326)."""

    def setUp(self):
        ccc._REPO_INDEX = None
        self.addCleanup(setattr, ccc, "_REPO_INDEX", None)

    def test_path_suffix_resolves_to_the_real_file(self):
        hit = ccc.find_basename("04_scripts/docs_audit.py")
        self.assertTrue(hit and os.path.isfile(hit), hit)
        self.assertTrue(hit.endswith("04_scripts/docs_audit.py"), hit)

    def test_path_suffix_that_matches_nothing_stays_unresolved(self):
        # Deliberately stricter than "fall back to the basename": a path-shaped
        # token naming the wrong directory must not resolve to a same-named file
        # elsewhere — a gate must not pass on a near match.
        self.assertIsNone(ccc.find_basename("code/98_nope/docs_audit.py"))

    def test_bare_name_resolves(self):
        hit = ccc.find_basename("docs_audit.py")
        self.assertTrue(hit and os.path.isfile(hit), hit)

    def test_unresolved_token_is_none_not_a_guess(self):
        self.assertIsNone(ccc.find_basename("definitely-not-in-the-tree.py"))

    def test_one_walk_serves_every_lookup(self):
        with mock.patch.object(ccc.os, "walk", wraps=os.walk) as walk:
            ccc.find_basename("docs_audit.py")
            ccc.find_basename("04_scripts/docs_audit.py")
            ccc.find_basename("definitely-not-in-the-tree.py")
        self.assertEqual(walk.call_count, 1)


if __name__ == "__main__":
    unittest.main()
