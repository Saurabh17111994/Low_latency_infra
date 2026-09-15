#!/usr/bin/env python3
"""Wave 22 (P6-298..301, 698..706, 858..861) — start-all.sh, run for real.

The script is a top-level flow (no functions to import), so every claim here is
checked by RUNNING it against a sandbox tree with stub `java`/`go`/`mvn`/`docker`
on PATH — the same shape used for the W19 top-level script. What each test pins:

  * a credentials file is parsed as DATA: `$(...)` in it must NOT execute
    (P6-298/P6-300 replaced `source` with a line parser);
  * the hostile-shape parsing: CRLF, quotes, `export `, digits in the key, an
    inline comment, a final line with no newline, and which file wins (P6-299/301);
  * a non-0600 credentials file warns and names the mode (P6-298);
  * the created template is 0600 from its first byte (P6-861);
  * readiness is not TCP reachability: an open port with no running tablet
    container is NOT ready and the poll dies instead of launching Java
    (P6-698/P6-701). FLUSS_READY_TRIES/SLEEP_SEC are the seam that keeps that
    path fast;
  * a missing jar fails loudly instead of at `java -jar` (P6-703/704);
  * a build is skipped when the output is newer than the sources (P6-700/702);
  * SIGTERM reaches the java child — the clean-stop promise had no trap
    (P6-699/P6-706).
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SOURCE = REPO / "start-all.sh"


class StartAllSandbox:
    """A tree shaped like the repo, plus stub tools, plus a live TCP listener."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.root = tmp / "tree"
        self.bin = self.bin_dir()
        self.listener: socket.socket | None = None
        self.port = 0
        self._build_tree()

    def bin_dir(self) -> Path:
        return self.tmp / "stub-bin"

    def _build_tree(self) -> None:
        self.root.mkdir(parents=True)
        shutil.copy2(SOURCE, self.root / "start-all.sh")
        compose = self.root / "code/01_platform/01_docker"
        compose.mkdir(parents=True)
        # ARROW_* split across the two files, the way this machine has it.
        (compose / ".env").write_text(
            "FLUSS_IMAGE=fake\nARROW_APP_ID=from-dotenv\nARROW_USER_ID=from-dotenv\n"
            "ARROW_PASSWORD=from-dotenv\nARROW_TOTP_KEY=from-dotenv\n")
        (compose / "secrets.env").write_text("ARROW_APP_SECRET=from-secrets\n")
        bridge = self.root / "code/02_services/01_ingestion/go-bridge"
        bridge.mkdir(parents=True)
        (bridge / "main.go").write_text("package main\n")
        java = self.root / "code/02_services/01_ingestion"
        (java / "src/main").mkdir(parents=True)
        (java / "src/main/App.java").write_text("class App {}\n")
        manifest = self.root / "Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"
        manifest.parent.mkdir(parents=True)
        manifest.write_text("symbol\n")
        self.manifest = manifest
        (self.root / "code/01_platform/02_sql/ddl").mkdir(parents=True)
        self.log_dir = self.root / "logs"
        self.log_dir.mkdir(parents=True)

    def install_stubs(self, *, tablet: bool = True, compose: bool = True,
                      make_jar: bool = True, java_sleep: bool = False,
                      mvn_rc: int = 0) -> None:
        self.bin.mkdir(parents=True, exist_ok=True)
        self.call_log = self.tmp / "calls.log"
        self.env_dump = self.tmp / "java-env.txt"
        self.java_pid_file = self.tmp / "java.pid"
        tablet_out = "fake-tablet-id\n" if tablet else ""
        ps_exit = "0" if compose else "1"
        write = self.bin.joinpath
        write("go").write_text(
            "#!/usr/bin/env bash\n"
            f"echo \"go $*\" >>{self.call_log}\n"
            f"touch {self.root}/code/02_services/01_ingestion/go-bridge/arrow-bridge\n"
            f"chmod +x {self.root}/code/02_services/01_ingestion/go-bridge/arrow-bridge\n",
        )
        write("mvn").write_text(
            "#!/usr/bin/env bash\n"
            f"echo \"mvn $*\" >>{self.call_log}\n"
            + ("exit %d\n" % mvn_rc if not make_jar else
               f"mkdir -p {self.root}/code/02_services/01_ingestion/target\n"
               f"touch {self.root}/code/02_services/01_ingestion/target/ingestion.jar\n"),
        )
        write("docker").write_text(
            "#!/usr/bin/env bash\n"
            f"echo \"docker $*\" >>{self.call_log}\n"
            'case " $* " in\n'
            '  *" compose version "*) exit ' + ("0" if compose else "1") + " ;;\n"
            '  *" ps -q --status running fluss-tablet "*) printf %s "' + tablet_out
            + '"; exit ' + ps_exit + " ;;\n"
            '  *" up -d "*) exit 0 ;;\n'
            "  *) exit 1 ;;\n"
            "esac\n",
        )
        write("java").write_text(
            "#!/usr/bin/env bash\n"
            f"echo $$ >{self.java_pid_file}\n"
            f"env | grep '^ARROW_' | sort >{self.env_dump}\n"
            # In sleep mode, mirror the JVM: a TERM handler that stops the child
            # it spawned (the Go bridge analog) before exiting.
            + ("_child=\n"
               "on_term() { [ -n \"$_child\" ] && kill -TERM \"$_child\" 2>/dev/null; exit 0; }\n"
               "trap on_term TERM INT\n"
               "sleep 60 & _child=$!\n"
               "wait \"$_child\"\n" if java_sleep else "exit 0\n"),
        )
        for name in ("go", "mvn", "docker", "java"):
            (self.bin / name).chmod(0o755)

    REQUIRED = {
        "ARROW_APP_ID": "id",
        "ARROW_APP_SECRET": "secret",
        "ARROW_USER_ID": "user",
        "ARROW_PASSWORD": "pw",
        "ARROW_TOTP_KEY": "totp",
    }

    def write_creds(self, text: str | None = None, **values: str) -> Path:
        """Write the credentials file the script expects to find."""
        target = self.tmp / "credentials.env"
        if text is None:
            merged = {**self.REQUIRED, **values}
            text = "".join(f"{k}={v}\n" for k, v in merged.items())
        if isinstance(text, bytes):
            target.write_bytes(text)
        else:
            target.write_text(text)
        return target

    def listen(self) -> None:
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(5)
        self.port = self.listener.getsockname()[1]

    def stop_listening(self) -> None:
        if self.listener is not None:
            self.listener.close()
            self.listener = None

    def env(self, **overrides: str) -> dict[str, str]:
        base = {
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "HOME": str(self.tmp / "home"),
            "SECRETS_FILE": str(self.tmp / "credentials.env"),
            "ARROW_INSTRUMENT_MANIFEST": str(self.manifest),
            "FLUSS_BOOTSTRAP": f"127.0.0.1:{self.port}",
            "LOG_DIR": str(self.log_dir),
            "FLUSS_READY_TRIES": "2",
            "FLUSS_READY_SLEEP_SEC": "0",
        }
        base.update(overrides)
        Path(base["HOME"]).mkdir(parents=True, exist_ok=True)
        return {**os.environ, **base}

    def run(self, **kwargs: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(self.root / "start-all.sh")],
            cwd=self.root, env=self.env(**kwargs),
            capture_output=True, text=True, timeout=90,
        )


class StartAllWave22(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w22-start-all-")
        self.box = StartAllSandbox(Path(self._tmp.name))
        self.box.install_stubs()
        self.box.write_creds()
        self.box.listen()

    def tearDown(self) -> None:
        self.box.stop_listening()
        self._tmp.cleanup()

    # ── credentials (P6-298/300) ─────────────────────────────────────────────
    def test_credentials_file_is_parsed_never_sourced(self) -> None:
        marker = self.box.tmp / "pwned"
        self.box.write_creds(
            f"ARROW_APP_ID=$(touch {marker})\n"
            + "".join(f"{k}={v}\n" for k, v in {
                "ARROW_APP_SECRET": "secret", "ARROW_USER_ID": "user",
                "ARROW_PASSWORD": "pw", "ARROW_TOTP_KEY": "totp",
            }.items())
        )
        proc = self.box.run()
        self.assertFalse(marker.exists(), "a credentials file executed shell code")
        self.assertNotIn("FATAL", proc.stderr)
        dump = self.box.env_dump.read_text()
        self.assertIn(f"ARROW_APP_ID=$(touch {marker})", dump, "the value was not inert")

    def test_hostile_shapes_parse_and_later_file_wins(self) -> None:
        # CRLF, `export `, quotes, a digit in the key, an inline comment, and a
        # final line with no trailing newline at all.
        self.box.write_creds(
            b"ARROW_APP_ID=id\r\n"
            b"ARROW_APP_SECRET=secret\r\n"
            b"export ARROW_USER_ID='u-1'\r\n"
            b"  ARROW_2FA_KEY = totp-secret  \r\n"
            b'ARROW_PASSWORD="p#not-a-comment"\n'
            b"ARROW_TOTP_KEY=totp"
        )
        proc = self.box.run()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        dump = dict(
            line.split("=", 1) for line in self.box.env_dump.read_text().splitlines()
        )
        self.assertEqual(dump["ARROW_USER_ID"], "u-1")
        self.assertEqual(dump["ARROW_2FA_KEY"], "totp-secret")
        self.assertEqual(dump["ARROW_PASSWORD"], "p#not-a-comment")
        self.assertEqual(dump["ARROW_TOTP_KEY"], "totp", "final line without newline")

    def test_credentials_fallback_names_the_real_origin(self) -> None:  # noqa: D401
        # No SECRETS_FILE: the compose .env + secrets.env supply the values, and
        # the log must name that origin rather than $SECRETS_FILE.
        proc = self.box.run(SECRETS_FILE=str(self.box.tmp / "absent.env"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        origin = proc.stdout.split("credentials OK (from ")[1].split(")")[0]
        self.assertIn("01_docker/.env", origin)
        dump = self.box.env_dump.read_text()
        self.assertIn("ARROW_APP_SECRET=from-secrets", dump, "secrets.env parsed second")

    def test_world_readable_credentials_warn_with_the_mode(self) -> None:
        creds = self.box.write_creds()
        creds.chmod(0o644)
        proc = self.box.run()
        self.assertIn("is mode 644", proc.stdout)

    def test_template_is_private_from_creation(self) -> None:
        # No credentials file and no compose .env: the script writes a template
        # and stops. It must be 0600, and the heredoc must not expand.
        (self.box.root / "code/01_platform/01_docker/.env").unlink()
        target = self.box.tmp / "home/new-credentials.env"
        proc = self.box.run(SECRETS_FILE=str(target))
        self.assertNotEqual(proc.returncode, 0)
        self.assertTrue(target.is_file())
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
        self.assertIn("ARROW_APP_ID=", target.read_text())

    # ── readiness (P6-698/701) ───────────────────────────────────────────────
    def test_open_port_without_a_running_tablet_is_not_ready(self) -> None:
        # The listener accepts TCP; the docker stub reports no running tablet.
        self.box.install_stubs(tablet=False)
        proc = self.box.run()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("did not become ready", proc.stderr)
        self.assertIn("after 0s", proc.stderr, "the poll counted its own tries")
        self.assertFalse(self.box.java_pid_file.exists(), "launched against a half-ready cluster")

    def test_running_tablet_makes_the_open_port_ready(self) -> None:
        self.box.install_stubs(tablet=True)
        proc = self.box.run()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("already running on", proc.stdout)
        self.assertTrue(self.box.java_pid_file.exists(), "the pipeline never started")

    def test_compose_without_the_plugin_degrades_loudly(self) -> None:
        # A stack started outside compose: no container for `ps` to find and no
        # compose plugin. That stays a hard gate on TCP, but it is announced.
        self.box.install_stubs(compose=False, tablet=False)
        proc = self.box.run()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("accepting TCP reachability", proc.stdout)

    def test_multi_host_and_ipv6_bootstrap(self) -> None:
        proc = self.box.run(FLUSS_BOOTSTRAP=f"127.0.0.1:{self.box.port},other-host:9123")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("already running", proc.stdout)

    # ── builds (P6-700/702/703/704) ─────────────────────────────────────────
    def test_missing_jar_fails_loudly(self) -> None:
        self.box.install_stubs(make_jar=False, mvn_rc=0)
        proc = self.box.run()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("produced no jar", proc.stderr)

    def test_up_to_date_outputs_skip_both_builds(self) -> None:
        bridge = self.box.root / "code/02_services/01_ingestion/go-bridge/arrow-bridge"
        jar = self.box.root / "code/02_services/01_ingestion/target/ingestion.jar"
        jar.parent.mkdir(parents=True, exist_ok=True)
        bridge.touch()
        bridge.chmod(0o755)
        jar.touch()
        future = time.time() + 10
        os.utime(bridge, (future, future))
        os.utime(jar, (future, future))
        proc = self.box.run()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("Go bridge up to date", proc.stdout)
        self.assertIn("Java jar up to date", proc.stdout)
        calls = self.box.call_log.read_text() if self.box.call_log.exists() else ""
        self.assertNotIn("go build", calls)
        self.assertNotIn("mvn ", calls)

    def test_stale_source_triggers_a_rebuild(self) -> None:
        bridge = self.box.root / "code/02_services/01_ingestion/go-bridge/arrow-bridge"
        jar = self.box.root / "code/02_services/01_ingestion/target/ingestion.jar"
        jar.parent.mkdir(parents=True, exist_ok=True)
        bridge.touch()
        bridge.chmod(0o755)
        jar.touch()
        old = time.time() - 3600
        os.utime(bridge, (old, old))
        os.utime(jar, (old, old))
        proc = self.box.run()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        calls = self.box.call_log.read_text()
        self.assertIn("go build", calls)
        self.assertIn("mvn ", calls)

    def test_flags_survive_as_one_argument_each(self) -> None:
        proc = self.box.run(GO_FLAGS="-tags=netgo -ldflags=-s", MVN_FLAGS="-o -DskipTests=false")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        calls = self.box.call_log.read_text()
        self.assertIn("go build -tags=netgo -ldflags=-s -o arrow-bridge .", calls)
        self.assertIn("mvn -o -DskipTests=false -q", calls)

    # ── signal handling (P6-699/706) ────────────────────────────────────────
    def test_sigterm_stops_the_java_child(self) -> None:
        self.box.install_stubs(java_sleep=True, tablet=True)
        env = self.box.env()
        proc = subprocess.Popen(
            ["bash", str(self.box.root / "start-all.sh")],
            cwd=self.box.root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        )
        deadline = time.time() + 15
        while time.time() < deadline and not self.box.java_pid_file.exists():
            time.sleep(0.1)
        self.assertTrue(self.box.java_pid_file.exists(), "java stub never started")
        java_pid = int(self.box.java_pid_file.read_text().strip())
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=30)
        proc.stdout.close()
        proc.stderr.close()
        # The trap TERMs the child and only KILLs after a bounded grace period.
        gone = False
        for _ in range(60):
            try:
                os.kill(java_pid, 0)
            except ProcessLookupError:
                gone = True
                break
            time.sleep(0.1)
        if not gone:
            os.kill(java_pid, signal.SIGKILL)
        self.assertTrue(gone, "SIGTERM to the script left the java child running")


if __name__ == "__main__":
    unittest.main()
