"""Hermetic tests for digest-pin.sh (P6 wave 20: P6-062..067, P6-353..358).

Two layers, like the Fluss-probe tests (wave 13):

* hermetic (this file) — the real script against PATH stubs: `docker`/
  `skopeo`/`crane` print fixture digests (or fail per STUB_FAIL) and record
  argv; `mktemp` is the real one. No registry, daemon, or network is touched.
  These run everywhere and are the differential signal for every wave-20 fix.
* live — `TestDigestPinLive` at the bottom: the real script against the real
  registry. Skipped unless DIGEST_PIN_LIVE=1, so the helper suite stays green
  on a machine with no registry access. Live runs are advisory: they pin the
  observed digest for the record but never fail on drift (tags move; the
  fail-closed regex is what forbids pinning garbage, and the hermetic legs
  prove that). The only hard live assertions are shape (`repo:tag@sha256:` +
  64 hex) and that an unresolvable tag still exits non-zero.
"""
import os
import pathlib
import re
import subprocess
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "digest-pin.sh"
SRC = SCRIPT.read_text()
GOOD = "a" * 64


class DigestHarness(unittest.TestCase):
    def setUp(self):
        self.t = pathlib.Path(tempfile.mkdtemp())
        self.bin = self.t / "bin"
        self.bin.mkdir()
        # Each resolver stub: record argv, optionally fail, else print a digest.
        # STDOUT_DIGEST / STDERR_LINE control the docker stub (P6-062 probe).
        (self.bin / "docker").write_text(
            '#!/usr/bin/env bash\n'
            'echo "DOCKER ARGS=$*" >> "$RESOLVER_CALLS"\n'
            'if [ "${STUB_FAIL:-}" = "docker" ]; then echo "auth warning: using default platform" >&2; exit 1; fi\n'
            'if [ -n "${STDERR_LINE:-}" ]; then echo "$STDERR_LINE" >&2; fi\n'
            'printf "%s\\n" "${STDOUT_DIGEST:-sha256:}"\n')
        (self.bin / "skopeo").write_text(
            '#!/usr/bin/env bash\n'
            'echo "SKOPEO ARGS=$*" >> "$RESOLVER_CALLS"\n'
            'if [ "${STUB_FAIL:-}" = "skopeo" ]; then echo "skopeo: unauthorized" >&2; exit 1; fi\n'
            'printf "sha256:%s\\n" "${SKOPEO_DIGEST:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}"\n')
        (self.bin / "crane").write_text(
            '#!/usr/bin/env bash\n'
            'echo "CRANE ARGS=$*" >> "$RESOLVER_CALLS"\n'
            'if [ "${STUB_FAIL:-}" = "crane" ]; then echo "crane: not found" >&2; exit 1; fi\n'
            'printf "sha256:%s\\n" "${CRANE_DIGEST:-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc}"\n')
        for b in ("docker", "skopeo", "crane"):
            (self.bin / b).chmod(0o755)
        self.env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}",
                        RESOLVER_CALLS=str(self.t / "calls.log"),
                        STDOUT_DIGEST=f"sha256:{GOOD}")
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.t)], check=False))

    def run_pin(self, *args, extra=None, timeout=60):
        env = dict(self.env)
        if extra:
            env.update(extra)
        return subprocess.run(["bash", str(SCRIPT), *args], env=env,
                              capture_output=True, text=True, timeout=timeout)

    def calls(self):
        p = self.t / "calls.log"
        return p.read_text() if p.exists() else ""

    # P6-064/065: registry-port refs rejected; empty tags rejected; good refs pass.
    def test_registry_port_ref_rejected(self):
        r = self.run_pin("registry:5000/repo")
        self.assertNotEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("no tag", r.stderr)

    def test_ref_shapes(self):
        for bad in ("repo", "repo:", "img@sha256:" + GOOD):
            r = self.run_pin(bad)
            self.assertNotEqual(r.returncode, 0, f"{bad} must fail: {r.stdout}{r.stderr}")
        r = self.run_pin("registry:5000/repo:tag")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"registry:5000/repo:tag@sha256:{GOOD}", r.stdout)

    # P6-062/066: a stderr warning alongside a good digest must NOT fail.
    def test_stderr_warning_does_not_contaminate_digest(self):
        r = self.run_pin("repo:tag", extra={"STDERR_LINE": "auth warning: using default platform"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"repo:tag@sha256:{GOOD}", r.stdout)

    # P6-063/067: human-readable index output is NOT parsed — falls through.
    def test_human_readable_digest_line_falls_through(self):
        idx = f"Digest: sha256:{'d' * 64}"
        r = self.run_pin("repo:tag", extra={"STDOUT_DIGEST": idx})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        c = self.calls()
        self.assertIn("SKOPEO ARGS=", c, "docker's Digest: line must fall through to skopeo")
        self.assertNotIn(idx, r.stdout)

    # P6-353/356 (+P6-062): malformed docker stdout PLUS a stderr warning
    # retries skopeo, then crane. Pre-fix the `2>&1` merge turned this into a
    # hard failure (warning text inside digest); post-fix stderr is separate.
    def test_malformed_docker_output_falls_through_to_crane(self):
        r = self.run_pin("repo:tag", extra={"STDOUT_DIGEST": "garbage!!",
                                            "STDERR_LINE": "warning: using default platform",
                                            "STUB_FAIL": "skopeo"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        c = self.calls()
        self.assertIn("SKOPEO ARGS=", c)
        self.assertIn("CRANE ARGS=", c)
        self.assertIn("repo:tag@sha256:" + "c" * 64, r.stdout)

    def test_all_resolvers_fail_reports_last_error(self):
        # Post-fix pin (passes on both scripts — the old defensive regex also
        # rejected crane's malformed output): documents the fail-closed shape,
        # not a changed behaviour.
        r = self.run_pin("repo:tag", extra={"STDOUT_DIGEST": "garbage!!",
                                            "STUB_FAIL": "skopeo",
                                            "CRANE_DIGEST": "short"})
        # crane prints sha256:short (malformed) -> reset -> no digest left.
        self.assertNotEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("could not resolve digest", r.stderr)

    # P6-354/355/357/358: options-first + `--` on every resolver.
    def test_resolvers_use_options_first_and_double_dash(self):
        self.run_pin("repo:tag")
        c = self.calls()
        self.assertIn('--format {{printf "%s" .Manifest.Digest}} -- repo:tag', c, c)
        self.run_pin("repo:tag", extra={"STDOUT_DIGEST": "bad", "STUB_FAIL": "skopeo"})
        c = self.calls()
        self.assertIn("--format {{.Digest}} -- docker://repo:tag", c, c)
        self.assertIn("CRANE ARGS=digest -- repo:tag", c, c)

    def test_dash_ref_never_parses_as_flag(self):
        # P6-354/357/358: a dash-led ref must die in validate_ref (no tag) —
        # no resolver is ever invoked, so it can never parse as a flag.
        r = self.run_pin("--help", extra={"STDOUT_DIGEST": "bad"})
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.calls(), "")

    # Static pins: no resolver merges stderr into the digest (only the
    # P6-062 comment names `2>&1` now), no head -1 extraction (only the
    # P6-063 comment names it), no Digest:-line sed.
    def test_static_pins(self):
        code_lines = [ln for ln in SRC.splitlines()
                      if not ln.strip().startswith("#")]
        code = "\n".join(code_lines)
        self.assertNotIn("2>&1", code)
        self.assertNotIn("head -1", code)
        self.assertNotIn("Digest:", code)


LIVE_REF = "hello-world:latest"   # tiny, public, stable tag
LIVE_TIMEOUT_S = 120
_HEX64 = re.compile(r"^sha256:[0-9a-f]{64}$")


def _live_enabled() -> bool:
    return os.environ.get("DIGEST_PIN_LIVE") == "1"


@unittest.skipUnless(_live_enabled(), "live registry check: set DIGEST_PIN_LIVE=1 to run")
class TestDigestPinLive(unittest.TestCase):
    """Live coverage for the docker-branch format fix (buildx v0.23+ ignores a
    bare `{{.Manifest.Digest}}` and prints the human-readable dump instead).

    Advisory on digest drift, strict on shape and on fail-closed behaviour:
    a moved tag still passes (digest recorded in the failure message for the
    human to compare), but a non-digest output or a zero exit on a bogus tag
    fails — those mean the resolver chain itself is broken.
    """

    def _run_live(self, *refs: str) -> "subprocess.CompletedProcess[str]":
        env = {k: v for k, v in os.environ.items()
               if k not in ("RESOLVER_CALLS", "STDOUT_DIGEST", "STDERR_LINE",
                            "STUB_FAIL", "SKOPEO_DIGEST", "CRANE_DIGEST")}
        return subprocess.run(["bash", str(SCRIPT), *refs], env=env,
                              capture_output=True, text=True, timeout=LIVE_TIMEOUT_S)

    def test_live_known_tag_resolves_to_sha256_shape(self):
        r = self._run_live(LIVE_REF)
        self.assertEqual(r.returncode, 0,
                         f"live resolve failed; stderr tail: {r.stderr[-2000:]}")
        self.assertTrue(r.stdout.strip(), f"no stdout; stderr tail: {r.stderr[-2000:]}")
        line = r.stdout.strip().splitlines()[-1]
        ref, _, digest = line.partition("@")
        self.assertEqual(ref, LIVE_REF, line)
        self.assertTrue(_HEX64.match(digest),
                        f"resolver output is not a bare manifest digest: {line!r} "
                        f"(buildx human-dump regression? digest recorded: {digest!r})")

    def test_live_bogus_tag_still_fails_closed(self):
        r = self._run_live("hello-world:this-tag-does-not-exist-xyz")
        self.assertNotEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("could not resolve digest", r.stderr)


if __name__ == "__main__":
    unittest.main()
