"""Wave 12 — catalog-guard.sh (P6-034, P6-035, P6-317..P6-321, P6-714, P6-715).

The guard is addressed through CATALOG_GUARD_SCRIPT and runs against a stub
`docker` on PATH that replays a zookeeper listing, so every branch (healthy,
empty, partial, over-count, untrustworthy probe, apply failure, concurrency) is
reachable without a stack — and the same suite can be pointed at the pre-fix
script for the recorded red run.
"""

import fcntl
import os
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "code" / "01_platform" / "04_scripts"
GUARD = pathlib.Path(os.environ.get("CATALOG_GUARD_SCRIPT", SCRIPTS / "catalog-guard.sh"))

STUB = r'''#!/usr/bin/env bash
# stub docker for the catalog-guard suite
printf '%s\n' "$*" >> "$STUB_LOG"
case "$1" in
  info)
    if [ "${STUB_DOCKER_INFO_RC:-0}" = "0" ]; then echo "Server Version: stub"; exit 0; fi
    echo "Cannot connect to the Docker daemon" >&2; exit 1 ;;
  inspect)
    printf '%s\n' "${STUB_ZK_RUNNING:-true}"; exit 0 ;;
esac
if [ "$1" = "compose" ]; then
  case "$*" in
    *" ps -q zookeeper"*)
      [ "${STUB_ZK_MISSING:-0}" = "1" ] && exit 0
      printf '%s\n' "${STUB_ZK_CID:-stub-zk-1}"; exit 0 ;;
    *"run --rm"*)
      # the apply swaps the probe fixture so the post-apply probe sees a full catalog
      if [ -n "${STUB_PROBE_AFTER:-}" ]; then cp "$STUB_PROBE_AFTER" "$STUB_PROBE_FIXTURE"; fi
      exit "${STUB_APPLY_RC:-0}" ;;
  esac
  exit 0
fi
if [ "$1" = "exec" ]; then
  case "$*" in
    *"sh -c"*)
      [ "${STUB_NO_ZK_CLI:-0}" = "1" ] && exit 0
      printf '%s\n' "${STUB_ZK_CLI:-/apache-zookeeper-3.9.2-bin/bin/zkCli.sh}"; exit 0 ;;
  esac
  last="${!#}"
  if [ "$last" = "/" ]; then echo "Zookeeper - /"; exit "${STUB_ZK_ROOT_RC:-0}"; fi
  n=0
  [ -f "$STUB_PROBE_COUNT" ] && n="$(cat "$STUB_PROBE_COUNT")"
  n=$((n + 1))
  printf '%s' "$n" > "$STUB_PROBE_COUNT"
  cat "$STUB_PROBE_FIXTURE"
  exit "${STUB_PROBE_RC:-0}"
fi
exit 0
'''

LISTING = "Zookeeper - ls /fluss/metadata/databases/default/tables\n[{}]\n"
NODE_ABSENT = "KeeperErrorCode = Nonode for /fluss/metadata/databases/default/tables\n" \
              "Node does not exist: /fluss/metadata/databases/default/tables\n"
# A probe that cannot be parsed and does not say "the node is absent": the guard
# must not read this as an empty catalog (P6-034).
PROBE_ERROR = "Zookeeper - ls /fluss/metadata/databases/default/tables\n" \
              "KeeperErrorCode = ConnectionLoss for /fluss/metadata/databases/default/tables\n"


class CatalogGuardTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="catalog-guard.")
        self.dir = pathlib.Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        (self.bin / "docker").write_text(STUB, encoding="utf-8")
        (self.bin / "docker").chmod(0o755)
        self.compose_dir = self.dir / "compose"
        self.compose_dir.mkdir()
        (self.compose_dir / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
        (self.compose_dir / ".env").write_text("", encoding="utf-8")
        (self.compose_dir / "secrets.env").write_text("", encoding="utf-8")
        self.log = self.dir / "docker.log"
        self.log.write_text("", encoding="utf-8")
        self.fixture = self.dir / "probe.txt"
        self.fixture.write_text(LISTING.format("a, b, c"), encoding="utf-8")
        self.count = self.dir / "probe.count"
        self.lock = self.dir / "guard.lock"

    def run_guard(self, *, before="", after=None, env=None, expected="3", apply_rc="0",
                  probe_text=None):
        # probe_text bypasses the "[...]" helper for fixtures that are not a
        # table list at all (an error, an absent node).
        self.fixture.write_text(probe_text if probe_text is not None else LISTING.format(before),
                                encoding="utf-8")
        full = {
            "PATH": str(self.bin) + ":" + os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(self.dir),
            "TMPDIR": str(self.dir),
            "STUB_LOG": str(self.log),
            "STUB_PROBE_FIXTURE": str(self.fixture),
            "STUB_PROBE_COUNT": str(self.count),
            "COMPOSE_PROJECT_DIR": str(self.compose_dir),
            "CATALOG_GUARD_LOCK": str(self.lock),
            "CATALOG_GUARD_ZK_WAIT": "1",
            "CATALOG_GUARD_ANSWER_WAIT": "1",
            "EXPECTED_TABLES": expected,
            "LC_ALL": "C",
        }
        if after is not None:
            after_file = self.dir / "after.txt"
            after_file.write_text(LISTING.format(after), encoding="utf-8")
            full["STUB_PROBE_AFTER"] = str(after_file)
        full["STUB_APPLY_RC"] = apply_rc
        full.update(env or {})
        return subprocess.run(["bash", str(GUARD)], capture_output=True, text=True,
                              env=full, timeout=60)

    def calls(self):
        return self.log.read_text(encoding="utf-8")

    def applied(self):
        return "run --rm" in self.calls()

    # ── healthy / empty / partial / over-count ───────────────────────────────
    def test_healthy_catalog_exits_zero_without_applying(self):
        out = self.run_guard(before="a, b, c")
        self.assertEqual(out.returncode, 0, out.stderr + out.stdout)
        self.assertIn("catalog probe: 3/3 tables", out.stdout)
        self.assertIn("catalog healthy", out.stdout)
        self.assertFalse(self.applied(), "a healthy catalog must not run DDL")

    def test_empty_catalog_is_repaired_then_verified(self):
        out = self.run_guard(before="", after="a, b, c")
        self.assertEqual(out.returncode, 0, out.stderr + out.stdout)
        self.assertTrue(self.applied(), "an empty catalog must be repaired")
        self.assertIn("catalog repaired — 3 tables present", out.stdout)

    def test_partial_catalog_exits_three_without_applying(self):
        out = self.run_guard(before="a, b")
        self.assertEqual(out.returncode, 3, out.stdout)
        self.assertIn("catalog PARTIAL (2/3)", out.stdout)
        self.assertFalse(self.applied())

    def test_over_count_is_drift_not_health(self):
        # P6-320: the old `-ge EXPECTED -> healthy` called an over-count healthy.
        out = self.run_guard(before="a, b, c, d")
        self.assertEqual(out.returncode, 3, out.stdout)
        self.assertIn("MORE tables than the manifest describes (4/3)", out.stdout)
        self.assertFalse(self.applied())

    def test_repair_that_leaves_the_catalog_incomplete_fails(self):
        out = self.run_guard(before="", after="a, b")
        self.assertEqual(out.returncode, 3, out.stdout)
        self.assertIn("catalog still incomplete after apply (2/3)", out.stderr)
        self.assertTrue(self.applied())

    # ── P6-034: the probe is not allowed to guess ────────────────────────────
    def test_connection_error_is_not_an_empty_catalog(self):
        out = self.run_guard(probe_text=PROBE_ERROR)
        self.assertEqual(out.returncode, 5, out.stdout)
        self.assertIn("ERROR: zkCli could not list", out.stderr)
        self.assertFalse(self.applied(), "an unreadable catalog must not be repaired")
        self.assertNotIn("catalog EMPTY", out.stdout)

    def test_absent_node_is_an_empty_catalog(self):
        out = self.run_guard(probe_text=NODE_ABSENT, after="a, b, c")
        self.assertEqual(out.returncode, 0, out.stderr + out.stdout)
        self.assertIn("catalog EMPTY", out.stdout)
        self.assertTrue(self.applied())

    def test_whitespace_only_list_is_empty_not_one_table(self):
        # P6-715: "[ ]" used to count as one table (awk NF == 1) and turn the
        # repairable state into a "PARTIAL" verdict.
        out = self.run_guard(before=" ", env={"DRY_RUN": "1"})
        self.assertEqual(out.returncode, 3, out.stdout)
        self.assertIn("catalog EMPTY and DRY_RUN=1", out.stderr)
        self.assertNotIn("PARTIAL", out.stdout + out.stderr)

    # ── P6-035: the apply's verdict is evidence ──────────────────────────────
    def test_apply_failure_is_reported_even_when_the_catalog_looks_complete(self):
        out = self.run_guard(before="", after="a, b, c", apply_rc="1")
        self.assertEqual(out.returncode, 5, out.stdout)
        self.assertIn("catalog complete (3 tables) but the DDL apply exited 1", out.stderr)
        self.assertNotIn("catalog repaired", out.stdout)

    # ── P6-317: the expectation must be a number ─────────────────────────────
    def test_non_integer_expected_tables_is_rejected(self):
        for bad in ("abc", "3.5", " 3", "-1"):
            with self.subTest(bad=bad):
                out = self.run_guard(before="a, b, c", expected=bad)
                self.assertEqual(out.returncode, 5, out.stdout)
                self.assertIn("EXPECTED_TABLES must be a whole number", out.stderr)
                self.assertNotIn("integer expression", out.stderr)

    def test_zero_expected_tables_is_rejected(self):
        out = self.run_guard(before="a, b, c", expected="0")
        self.assertEqual(out.returncode, 5, out.stdout)
        self.assertIn("EXPECTED_TABLES=0", out.stderr)

    # ── P6-318: preconditions are checked before anything else ───────────────
    def test_missing_compose_env_file_is_a_precondition_failure(self):
        (self.compose_dir / "secrets.env").unlink()
        out = self.run_guard(before="a, b, c")
        self.assertEqual(out.returncode, 4, out.stdout)
        self.assertIn("compose input missing", out.stderr)
        self.assertEqual(self.calls(), "", "nothing may run compose before the preflight")

    def test_unreachable_daemon_is_a_precondition_failure(self):
        out = self.run_guard(before="a, b, c", env={"STUB_DOCKER_INFO_RC": "1"})
        self.assertEqual(out.returncode, 4, out.stdout)
        self.assertIn("docker daemon is not reachable", out.stderr)

    def test_container_not_running_is_a_precondition_failure(self):
        out = self.run_guard(before="a, b, c", env={"STUB_ZK_MISSING": "1"})
        self.assertEqual(out.returncode, 4, out.stdout)
        self.assertIn("zookeeper container not found or not running", out.stderr)

    def test_missing_zkcli_is_not_a_zero_count(self):
        out = self.run_guard(before="", env={"STUB_NO_ZK_CLI": "1"})
        self.assertEqual(out.returncode, 5, out.stdout)
        self.assertIn("cannot find zkCli.sh", out.stderr)
        self.assertFalse(self.applied())

    # ── P6-321: two guards must not repair at once ───────────────────────────
    def test_concurrent_run_is_refused(self):
        with open(self.lock, "w", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            out = self.run_guard(before="", after="a, b, c")
        self.assertEqual(out.returncode, 6, out.stdout)
        self.assertIn("another catalog-guard is already running", out.stderr)
        self.assertFalse(self.applied(), "the second guard must not touch the catalog")

    # ── P6-714: fail() must not leak its exit code into the message ──────────
    def test_fail_messages_do_not_end_with_the_exit_code(self):
        # P6-714: `fail "msg" 3` used to print "ERROR: msg 3" because printf
        # expanded "$*" instead of "$1".
        out = self.run_guard(before="", env={"DRY_RUN": "1"})
        self.assertEqual(out.returncode, 3, out.stdout)
        self.assertIn("ERROR: catalog EMPTY and DRY_RUN=1 — refusing to apply", out.stderr)
        for line in out.stderr.splitlines():
            with self.subTest(line=line):
                self.assertFalse(line.endswith(" 3"), line)
                self.assertFalse(line.endswith(" 5"), line)


if __name__ == "__main__":
    unittest.main()
