"""Wave-30 regression tests for chaos-03-tablet-kill.sh (P6-049..052, 334..339, 718, 719).

The drill is a shell script that drives maven and docker, so this suite runs the
REAL entry point (`bash chaos-03-tablet-kill.sh`) with two external commands
shimmed through a PATH prefix — `docker` (scriptable container inventory,
inspect and daemon state) and `mvn` (argv + environment recording, scriptable
output and exit code).  No container is killed and no Java test is compiled or
run: the state under test is this script's decisions.

`W30_TREE` points at a fake repo root holding `code/01_platform/04_scripts/
chaos/chaos-03-tablet-kill.sh`; it defaults to the real repository and exists so
the red leg can run the same suite against the pre-wave copy.

Red leg: every assertion marked `# disc` fails against the pre-wave script.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

def _default_tree() -> Path:
    """The repo root: nearest ancestor holding the chaos scripts."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "code" / "01_platform" / "04_scripts" / "chaos").is_dir():
            return parent
    return here.parent


TREE = Path(os.environ.get("W30_TREE", _default_tree()))
SCRIPT = TREE / "code" / "01_platform" / "04_scripts" / "chaos" / "chaos-03-tablet-kill.sh"

# Resolved from the outer environment: a test that sets PATH to nothing must
# still be able to start bash.
BASH = shutil.which("bash") or "/usr/bin/bash"

TABLET = "01_docker-fluss-tablet-1"
TABLET_ID = "aaa111bbb222"
SENTINEL = "TABLET-KILL-CHAOS-03: RESULT=PASS EXIT=0"
# The drill's own PASS line. Assertions use this rather than the bare substring
# "RESULT=PASS", which the FAIL message quotes when the sentinel is missing.
PASS_LINE = "TABLET-KILL-CHAOS-03: PASS —"

SHIMS = r"""#!/usr/bin/env bash
# PATH-prefix shims for `docker` and `mvn`, driven by the files named in the
# environment. One file serves both commands and dispatches on argv[0].
shim_docker() {
  printf 'docker %s\n' "$*" >> "${DOCKER_LOG}"
  local fmt="" a=""
  for a in "$@"; do
    case "${a}" in *'{{'*) fmt="${a}" ;; esac
  done
  local last=""
  if [[ "$#" -gt 0 ]]; then last="${!#}"; fi
  case "${1:-}" in
    info)
      if [[ -n "${DOCKER_INFO_ERR:-}" ]]; then printf '%s\n' "${DOCKER_INFO_ERR}" >&2; fi
      return "${DOCKER_INFO_RC:-0}"
      ;;
    ps)
      # Both the pre-wave (`{{.ID}}`, `{{.ID}} {{.Names}}`) and the fixed
      # (`{{.Names}}`) shapes are served, so one shim covers the red and green legs.
      if [[ "$*" == *--filter* ]]; then
        case "${fmt}" in
          *'{{.Names}}'*) command cat "${CAND_NAMES}" ;;
          *) command cat "${CAND_IDS}" ;;
        esac
      else
        command cat "${ALL_PS}"
      fi
      ;;
    inspect)
      local line
      line="$(command awk -v k="${last}" '$1 == k { $1 = ""; sub(/^ /, ""); print; exit }' "${INSPECT}" 2>/dev/null)"
      if [[ -z "${line}" ]]; then
        printf 'Error: No such object: %s\n' "${last}" >&2
        return 1
      fi
      printf '%s\n' "${line}"
      ;;
    *) return 0 ;;
  esac
}

shim_mvn() {
  printf 'ARGV %s\n' "$*" >> "${MVN_LOG}"
  case "$*" in
    *test-compile*)
      if [[ "$*" == *" -o "* ]]; then
        [[ -s "${COMPILE_OUT_OFFLINE}" ]] && command cat "${COMPILE_OUT_OFFLINE}"
        return "${COMPILE_RC_OFFLINE:-0}"
      fi
      [[ -s "${COMPILE_OUT_ONLINE}" ]] && command cat "${COMPILE_OUT_ONLINE}"
      return "${COMPILE_RC_ONLINE:-0}"
      ;;
  esac
  command env | command grep -E '^(TABLET_CONTAINER|TABLET_KILL_ROWS|CHAOS_REPLICATION_REQUIRED|CHAOS_REPLICATION_MIN|CHAOS_SCAN_LIMIT_ROWS|FLUSS_BOOTSTRAP|COMPUTE_INT_TEST_TABLET_KILL)=' \
    | command sed 's/^/ENV /' >> "${MVN_LOG}"
  [[ -s "${MVN_OUT}" ]] && command cat "${MVN_OUT}"
  return "${MVN_RC:-0}"
}

case "${0##*/}" in
  docker) shim_docker "$@" ;;
  mvn) shim_mvn "$@" ;;
esac
"""


class TabletKillTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="w30-tabletkill-")
        self.tmp = Path(self._tmp)
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        for name in ("docker", "mvn"):
            path = self.bin / name
            path.write_text(SHIMS)
            path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        self.log = self.tmp / "calls.log"
        self.log.write_text("")
        self.cand_names = self.write("cand-names.txt", TABLET + "\n")
        self.cand_ids = self.write("cand-ids.txt", TABLET_ID + "\n")
        self.all_ps = self.write("all-ps.txt", f"{TABLET_ID} {TABLET}\nccc333 01_docker-fluss-coordinator-1\n")
        self.inspect = self.write("inspect.txt", f"{TABLET} /{TABLET} true\n")
        self.mvn_out = self.write("mvn-out.txt", SENTINEL + "\n")
        self.write("empty.txt", "")

    def tearDown(self) -> None:
        subprocess.run(["rm", "-rf", self._tmp], check=False)

    def write(self, name: str, text: str) -> Path:
        path = self.tmp / name
        path.write_text(text)
        return path

    def path_without_docker(self) -> Path:
        """Every real /usr/bin and /bin entry EXCEPT docker (and mvn)."""
        narrowed = self.tmp / "nodocker"
        narrowed.mkdir(exist_ok=True)
        for source in (Path("/usr/bin"), Path("/bin")):
            if not source.is_dir():
                continue
            for entry in source.iterdir():
                if entry.name in ("docker", "mvn"):
                    continue
                # /bin is a symlink to /usr/bin on this host, so the same name
                # arrives twice; skip rather than fail on the second pass.
                if (narrowed / entry.name).is_symlink() or (narrowed / entry.name).exists():
                    continue
                (narrowed / entry.name).symlink_to(entry)
        return narrowed

    def env(self, **overrides) -> dict:
        base = {
            "SHIM_DIR": str(self.tmp),
            "DOCKER_LOG": str(self.log),
            "MVN_LOG": str(self.log),
            "CAND_NAMES": str(self.cand_names),
            "CAND_IDS": str(self.cand_ids),
            "ALL_PS": str(self.all_ps),
            "INSPECT": str(self.inspect),
            "MVN_OUT": str(self.mvn_out),
            "COMPILE_OUT_OFFLINE": str(self.tmp / "empty.txt"),
            "COMPILE_OUT_ONLINE": str(self.tmp / "empty.txt"),
            "PATH": f"{self.bin}:{os.environ['PATH']}",
        }
        base.update({k: str(v) for k, v in overrides.items()})
        return {**os.environ, **base}

    def run_script(self, args: list[str] | None = None, **env) -> subprocess.CompletedProcess:
        return subprocess.run(
            [BASH, str(SCRIPT), *(args or [])],
            capture_output=True, text=True, timeout=300, env=self.env(**env),
        )

    def output(self, r: subprocess.CompletedProcess) -> str:
        return r.stdout + r.stderr

    def mvn_log(self) -> str:
        return self.log.read_text()

    def docker_log(self) -> str:
        return self.log.read_text()


class TabletResolutionTest(TabletKillTestCase):
    """P6-050/051, P6-334/338: which container may be killed, and how it is chosen."""

    def test_docker_absent_is_a_named_skip(self) -> None:
        # A PATH that still holds the ordinary tools but no docker: that is what
        # "docker not found" means. An emptied PATH would fail earlier, in the
        # script's own SCRIPT_DIR resolution, which is a different scenario.
        r = self.run_script(PATH=str(self.path_without_docker()))
        self.assertEqual(3, r.returncode, self.output(r))
        self.assertIn("SKIP — docker not found", r.stderr)

    def test_no_tablet_is_a_named_skip(self) -> None:
        self.write("cand-names.txt", "")
        self.write("cand-ids.txt", "")
        r = self.run_script()
        self.assertEqual(3, r.returncode, self.output(r))
        self.assertIn("SKIP — no fluss-tablet container", r.stderr)

    def test_unreachable_daemon_is_not_a_stack_that_is_down(self) -> None:  # disc (P6-050)
        r = self.run_script(DOCKER_INFO_RC="1", DOCKER_INFO_ERR="Cannot connect to the Docker daemon")
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("Cannot connect to the Docker daemon", r.stderr)
        self.assertIn("broken drill host, not a stack that is down", r.stderr)

    def test_a_pinned_container_that_does_not_exist_is_a_config_error(self) -> None:  # disc (P6-050)
        r = self.run_script(TABLET_CONTAINER="ghost-tablet")
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("does not exist", r.stderr)
        self.assertNotIn("SKIP", self.output(r))

    def test_a_pinned_non_tablet_container_is_refused(self) -> None:  # disc (P6-050/P6-051)
        # The pre-wave check was `docker ps --format '{{.ID}} {{.Names}}' | grep -q <value>`,
        # so any running container's name passed validation and reached `docker kill`.
        self.write("all-ps.txt", f"{TABLET_ID} {TABLET}\nccc333 01_docker-postgres-1\n")
        self.write("inspect.txt", f"{TABLET} /{TABLET} true\npostgres /01_docker-postgres-1 true\n")
        r = self.run_script(TABLET_CONTAINER="postgres")
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("is not a fluss-tablet container", r.stderr)
        self.assertNotIn(PASS_LINE, self.output(r))

    def test_a_pinned_regex_is_not_a_container(self) -> None:  # disc (P6-051)
        # `grep -q '.*'` matched every line of `docker ps`; an exact-field check cannot.
        r = self.run_script(TABLET_CONTAINER=".*")
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("does not exist", r.stderr)

    def test_a_pinned_stopped_tablet_is_a_config_error(self) -> None:  # disc (P6-050)
        self.write("inspect.txt", f"{TABLET} /{TABLET} false\n")
        r = self.run_script(TABLET_CONTAINER=TABLET)
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("is not running", r.stderr)

    def test_several_tablets_without_a_pin_is_refused(self) -> None:  # disc (P6-334/P6-338)
        # `head -n 1` used to pick an arbitrary replica and hide which one died.
        other = "01_docker-fluss-tablet-2"
        self.write("cand-names.txt", f"{TABLET}\n{other}\n")
        self.write("cand-ids.txt", f"{TABLET_ID}\nddd444\n")
        self.write("inspect.txt", f"{TABLET} /{TABLET} true\n{other} /{other} true\n")
        r = self.run_script()
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("2 fluss-tablet containers are running — set TABLET_CONTAINER", r.stderr)
        self.assertNotIn(PASS_LINE, self.output(r))

    def test_several_tablets_are_logged_sorted(self) -> None:  # disc (P6-334)
        other = "01_docker-fluss-tablet-2"
        self.write("cand-names.txt", f"{other}\n{TABLET}\n")  # unsorted on purpose
        self.write("inspect.txt", f"{TABLET} /{TABLET} true\n{other} /{other} true\n")
        r = self.run_script()
        self.assertIn(f"candidates: {TABLET} {other}", self.output(r))

    def test_one_tablet_is_auto_selected_and_named(self) -> None:  # disc (P6-334)
        # Pre-fix the discovery passed `{{.ID}}` through, so the run never named
        # the container it was about to kill.
        r = self.run_script()
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn(f"tablet under test: {TABLET}", r.stdout)
        self.assertIn(f"ENV TABLET_CONTAINER={TABLET}", self.mvn_log())

    def test_a_single_tablet_runs_without_a_pin(self) -> None:
        r = self.run_script()
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("ENV TABLET_CONTAINER=", self.mvn_log())


class KnobValidationTest(TabletKillTestCase):
    """P6-336/P6-337: knobs that would make the drill vacuous or crash late."""

    def test_zero_acked_rows_is_refused(self) -> None:  # disc (P6-336/P6-337)
        # rows=0 acks nothing, so `ackedAfter.containsAll(acked)` held vacuously: PASS with no chaos.
        r = self.run_script(TABLET_KILL_ROWS="0")
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("TABLET_KILL_ROWS=0 is not a positive integer", r.stderr)
        self.assertNotIn(PASS_LINE, self.output(r))

    def test_a_non_numeric_row_count_is_refused(self) -> None:  # disc (P6-336)
        r = self.run_script(TABLET_KILL_ROWS="abc")
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("TABLET_KILL_ROWS=abc is not a positive integer", r.stderr)

    def test_a_scan_window_smaller_than_the_batch_is_refused(self) -> None:  # disc (P6-336)
        r = self.run_script(TABLET_KILL_ROWS="25", CHAOS_SCAN_LIMIT_ROWS="10")
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("CHAOS_SCAN_LIMIT_ROWS=10 < TABLET_KILL_ROWS=25", r.stderr)

    def test_a_full_scan_window_is_allowed(self) -> None:
        r = self.run_script(CHAOS_SCAN_LIMIT_ROWS="-1")
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("ENV CHAOS_SCAN_LIMIT_ROWS=-1", self.mvn_log())

    def test_a_non_boolean_replication_flag_is_refused(self) -> None:  # disc (P6-336)
        r = self.run_script(CHAOS_REPLICATION_REQUIRED="maybe")
        self.assertEqual(2, r.returncode, self.output(r))
        self.assertIn("must be true or false", r.stderr)

    def test_the_replication_flag_is_normalised(self) -> None:  # disc (P6-336)
        r = self.run_script(CHAOS_REPLICATION_REQUIRED="TRUE")
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("ENV CHAOS_REPLICATION_REQUIRED=true", self.mvn_log())


class CompileProbeTest(TabletKillTestCase):
    """P6-335/P6-339: the offline compile probe and its online retry."""

    def test_the_online_retry_is_announced_and_bounded(self) -> None:  # disc (P6-335/P6-339)
        self.write("offline-err.txt", "offline: missing artifact\n")
        r = self.run_script(COMPILE_OUT_OFFLINE=str(self.tmp / "offline-err.txt"), COMPILE_RC_OFFLINE="1")
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn("retrying online once, bounded by CHAOS_COMPILE_TIMEOUT_S=300s", r.stderr)
        argv = self.mvn_log()
        self.assertIn("ARGV -f", argv.replace("ARGV", "ARGV"))  # argv recorded at all
        self.assertIn("-o test-compile -q", argv)
        self.assertIn("test-compile\n", argv)

    def test_a_failed_compile_fails_the_drill(self) -> None:
        r = self.run_script(COMPILE_RC_OFFLINE="1", COMPILE_RC_ONLINE="1")
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("FAIL — compile TabletKillChaosIntegrationTest", r.stderr)

    def test_a_clean_offline_compile_is_not_retried(self) -> None:
        r = self.run_script()
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertEqual(1, self.mvn_log().count("test-compile"), self.mvn_log())


class LiveRunTest(TabletKillTestCase):
    """P6-049/P6-052: a green maven build is not a P, and an unmet assumption is named."""

    def test_a_green_maven_without_the_sentinel_is_not_a_pass(self) -> None:  # disc (P6-049/P6-052)
        # `-DfailIfNoTests=false` plus an env-gated IT: maven exits 0 on a skipped test.
        self.write("mvn-out.txt", "Tests run: 0, Failures: 0, Errors: 0, Skipped: 0\n")
        r = self.run_script()
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("no invariant was proven", r.stderr)
        self.assertNotIn(PASS_LINE, self.output(r))

    def test_a_skipped_it_is_a_named_skip_not_a_pass(self) -> None:  # disc (P6-049)
        self.write(
            "mvn-out.txt",
            "Tests run: 1, Failures: 0, Errors: 0, Skipped: 1, Time elapsed: 0.1 s\n"
            "[WARNING] Tests run: 1, Failures: 0, Errors: 0, Skipped: 1\n",
        )
        r = self.run_script()
        self.assertEqual(3, r.returncode, self.output(r))
        self.assertIn("SKIP — the IT did not run", r.stderr)
        self.assertIn("Skipped: 1", r.stderr)

    def test_the_sentinel_is_the_pass(self) -> None:
        r = self.run_script()
        self.assertEqual(0, r.returncode, self.output(r))
        self.assertIn(PASS_LINE, r.stdout)

    def test_a_red_maven_fails(self) -> None:
        r = self.run_script(MVN_RC="1")
        self.assertEqual(1, r.returncode, self.output(r))
        self.assertIn("FAIL — TabletKillChaosIntegrationTest", r.stderr)
        self.assertNotIn(PASS_LINE, self.output(r))

    def test_the_java_gate_is_set_on_the_live_run(self) -> None:
        self.run_script()
        self.assertIn("ENV COMPUTE_INT_TEST_TABLET_KILL=true", self.mvn_log())

    def test_the_invariant_message_does_not_overstate_rf1(self) -> None:  # disc (P6-049)
        # At RF1 an unclean kill DOES lose the just-acked tail; the old message
        # claimed "acked rows still readable" unconditionally.
        r = self.run_script()
        self.assertIn("RF1 dev tail loss of the just-acked rows is reported, not failed", r.stdout)
        self.assertNotIn("PASS — acked rows still readable, LOG count never shrank", self.output(r))


if __name__ == "__main__":
    unittest.main()
