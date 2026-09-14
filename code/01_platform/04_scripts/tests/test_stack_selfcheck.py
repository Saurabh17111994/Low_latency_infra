#!/usr/bin/env python3
"""Behavioural tests for stack_selfcheck.sh (P6-019, P6-206..209, P6-550/551).

Hermetic: a stub `docker` first on PATH records every invocation and answers
from STUB_* variables, so no daemon, swarm or stack is ever touched.
"""

import os
import pathlib
import subprocess
import tempfile
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS = TESTS_DIR.parent
SCRIPT = SCRIPTS / "stack_selfcheck.sh"

# Real-looking values for the 12 `:?` vars docker-stack.yml requires. DEPLOY=1
# refuses placeholders (P6-208), so every deploy test must supply these.
REAL_ENV = {
    "FLUSS_IMAGE": "fluss@sha256:" + "a" * 64,
    "FLINK_IMAGE": "flink@sha256:" + "b" * 64,
    "INGESTION_IMAGE": "ingestion@sha256:" + "c" * 64,
    "EXECUTION_BRIDGE_IMAGE": "bridge@sha256:" + "d" * 64,
    "EXECUTION_GATEWAY_IMAGE": "gateway@sha256:" + "e" * 64,
    "NAUTILUS_IMAGE": "nautilus@sha256:" + "f" * 64,
    "OPENOBSERVE_IMAGE": "openobserve@sha256:" + "0" * 64,
    "S3_WAREHOUSE_PATH": "s3://real/warehouse",
    "R2_ENDPOINT": "https://real.example",
    "ARROW_APP_ID": "123456",
    "ARROW_USER_ID": "12345678",
    "CHECKPOINT_DIR": "s3://real/checkpoints",
}

STUB = r"""#!/usr/bin/env bash
# Stub docker: record argv, answer from STUB_* env vars.
printf '%s\n' "$*" >> "$DOCKER_CALLS"
case "${1:-} ${2:-}" in
  "info --format")
    case "${3:-}" in
      *LocalNodeState*) printf '%s\n' "${STUB_SWARM_STATE:-inactive}" ;;
      *NodeID*)         printf '%s\n' "${STUB_NODE_ID:-stubnodeid}" ;;
      *)                printf '\n' ;;
    esac
    exit 0 ;;
  "info ") exit 0 ;;
  "node ls")
    if [ "${STUB_NODE_COUNT:-1}" = "1" ]; then echo stubnodeid; else printf 'nodeA\nnodeB\n'; fi
    exit 0 ;;
  "node update"*)  exit 0 ;;
  "node inspect"*) echo "role=worker observability=true"; exit 0 ;;
  "swarm init"*)   exit "${STUB_SWARM_INIT_RC:-0}" ;;
  "stack ls")
    [ "${STUB_STACK_PRESENT:-0}" = "1" ] && printf '%s\n' "${STUB_STACK_NAME:-prod}"
    exit 0 ;;
  "stack rm"*)     exit "${STUB_STACK_RM_RC:-0}" ;;
  "stack config"*) printf '%s\n' "${CHECKPOINT_DIR:-UNSET}" > "${STUB_CONFIG_MARK:-/dev/null}"; exit 0 ;;
  "stack deploy"*) exit 0 ;;
esac
exit 0
"""


class StackSelfcheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = pathlib.Path(self.tmp.name)
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        stub = self.bin / "docker"
        stub.write_text(STUB)
        stub.chmod(0o755)
        self.calls = self.dir / "docker.calls"
        self.calls.write_text("")
        self.stack = self.dir / "docker-stack.yml"
        self.stack.write_text("version: '3.8'\nservices: {}\n")
        self.config_mark = self.dir / "config-env.txt"

    def run_script(self, *args, **stub_env):
        env = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "LC_ALL": "C",
            "DOCKER_CALLS": str(self.calls),
            "STUB_CONFIG_MARK": str(self.config_mark),
        }
        env.update({k: str(v) for k, v in stub_env.items()})
        return subprocess.run(
            ["bash", str(SCRIPT), f"STACK={self.stack}", *args],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )

    def calls_made(self):
        return self.calls.read_text().splitlines()

    def test_validate_only_never_removes_a_stack(self):
        """P6-019: a validate-only run used to `docker stack rm prod`."""
        done = self.run_script(STUB_STACK_PRESENT="1")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertNotIn("stack rm", "\n".join(self.calls_made()))
        self.assertIn("stack config", "\n".join(self.calls_made()))

    def test_deploy_then_removes_the_stack_it_deployed(self):
        """P6-019/P6-209: DEPLOY=1 as an argument deploys, then tears down."""
        done = self.run_script("DEPLOY=1", **REAL_ENV, STUB_STACK_PRESENT="1")
        self.assertEqual(done.returncode, 0, done.stderr)
        calls = "\n".join(self.calls_made())
        self.assertIn("stack deploy", calls)
        self.assertIn("stack rm prod", calls)
        self.assertIn("requested removal of stack prod", done.stdout)

    def test_a_failed_removal_fails_the_run(self):
        """P6-019: `|| true` plus an unconditional echo claimed success."""
        done = self.run_script("DEPLOY=1", **REAL_ENV, STUB_STACK_PRESENT="1", STUB_STACK_RM_RC="1")
        self.assertNotEqual(done.returncode, 0)
        self.assertNotIn("requested removal of stack", done.stdout)

    def test_a_missing_stack_is_reported_without_a_removal_call(self):
        done = self.run_script("DEPLOY=1", **REAL_ENV, STUB_STACK_PRESENT="0")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("not present, nothing to remove", done.stdout)
        self.assertNotIn("stack rm", "\n".join(self.calls_made()))

    def test_down_zero_keeps_the_deployed_stack(self):
        """P6-209: DOWN=0 was documented but never read from the arguments."""
        done = self.run_script("DEPLOY=1", "DOWN=0", **REAL_ENV)
        self.assertEqual(done.returncode, 0, done.stderr)
        calls = "\n".join(self.calls_made())
        self.assertIn("stack deploy", calls)
        self.assertNotIn("stack rm", calls)
        self.assertIn("left in place (DOWN=0)", done.stdout)

    def test_an_unknown_argument_is_refused_before_any_docker_call(self):
        done = self.run_script("--nonsense")
        self.assertEqual(done.returncode, 2)
        self.assertIn("unknown argument", done.stderr)
        self.assertEqual(self.calls_made(), [])

    def test_an_active_swarm_is_not_re_initialised(self):
        """P6-206: `docker node ls` failing for permissions triggered a swarm init."""
        done = self.run_script(STUB_SWARM_STATE="active")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertNotIn("swarm init", "\n".join(self.calls_made()))
        self.assertIn("swarm already active", done.stdout)

    def test_an_inactive_swarm_is_initialised(self):
        done = self.run_script(STUB_SWARM_STATE="inactive")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("swarm init", "\n".join(self.calls_made()))

    def test_a_multi_node_swarm_is_refused(self):
        """P6-207: labelling `node ls -q | head -1` mutated an arbitrary node."""
        done = self.run_script(STUB_SWARM_STATE="active", STUB_NODE_COUNT="2")
        self.assertEqual(done.returncode, 1)
        self.assertIn("refusing to label", done.stderr)
        self.assertNotIn("node update", "\n".join(self.calls_made()))

    def test_the_label_uses_this_nodes_own_id(self):
        """P6-207: the id comes from `docker info`, not from a list ordering."""
        done = self.run_script(STUB_SWARM_STATE="active", STUB_NODE_ID="node-xyz")
        self.assertEqual(done.returncode, 0, done.stderr)
        calls = "\n".join(self.calls_made())
        self.assertIn("node update --label-add role=worker node-xyz", calls)
        self.assertIn("node update --label-add observability=true node-xyz", calls)

    def test_a_missing_stack_file_is_refused(self):
        """P6-550: a wrong repo root used to surface only as a docker error."""
        missing = self.dir / "no-such-stack.yml"
        done = subprocess.run(
            ["bash", str(SCRIPT), f"STACK={missing}"],
            capture_output=True,
            text=True,
            env={"PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}", "LC_ALL": "C",
                 "DOCKER_CALLS": str(self.calls)},
            timeout=60,
        )
        self.assertEqual(done.returncode, 1)
        self.assertIn("stack file not found", done.stderr)
        self.assertEqual(self.calls_made(), [])

    def test_deploy_without_real_values_is_refused(self):
        """P6-208: placeholder defaults let DEPLOY=1 deploy an unpullable stack."""
        done = self.run_script("DEPLOY=1")
        self.assertEqual(done.returncode, 2)
        self.assertIn("DEPLOY=1 needs real values for:", done.stderr)
        self.assertNotIn("stack deploy", "\n".join(self.calls_made()))

    def test_deploy_with_real_values_proceeds_to_deploy(self):
        done = self.run_script("DEPLOY=1", **REAL_ENV)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("stack deploy", "\n".join(self.calls_made()))
        self.assertNotIn("placeholder", done.stdout)

    def test_validate_only_compiles_with_placeholders(self):
        """P6-208: offline validation must still work — and say what it is.

        The mark file proves CHECKPOINT_DIR was exported for `stack config`;
        pre-fix it was never set at all, so this is the discriminating half.
        """
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("compile-only: placeholder images/paths in use", done.stdout)
        self.assertEqual(self.config_mark.read_text().strip(), "s3://placeholder/checkpoints")

    def test_the_header_states_the_real_label_pair(self):
        """P6-551: the header claimed one label key held two values."""
        text = SCRIPT.read_text()
        self.assertIn("`role=worker` + `observability=true`", text)
        self.assertNotIn("`role=worker` AND `role=observability`", text)


if __name__ == "__main__":
    unittest.main()
