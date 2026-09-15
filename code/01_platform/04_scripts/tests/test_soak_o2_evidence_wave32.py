"""Wave-32 regression tests for soak-o2-evidence.py (P6-200, P6-547, P6-788, P6-789).

The script queries OpenObserve over HTTP, so this suite stands up a real
loopback HTTP server that serves scripted Prometheus responses and records the
requests (path, query, Authorization header). No O2 instance and no network are
involved, and nothing in the repository is written.

`W32_O2_SCRIPT` points at the script under test; it defaults to the real
repository file and exists so the red leg can run against the pre-wave copy.

Red leg: every test marked `# disc` fails against the pre-wave script; the
ones marked `# pin` hold a contract that both revisions already satisfy.
The 2026-09-15 wave-32 red leg measured 8 failures, exactly the 8 `# disc`
tests.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "code" / "01_platform" / "04_scripts").is_dir():
            return parent
    return here.parent


ROOT = _repo_root()
SCRIPT = Path(os.environ.get(
    "W32_O2_SCRIPT", ROOT / "code" / "01_platform" / "04_scripts" / "soak-o2-evidence.py"
))


class _Handler(BaseHTTPRequestHandler):
    """Serves one scripted response per query name, in query order."""

    def log_message(self, *args):  # keep the test output clean
        pass

    def address_string(self) -> str:
        # Never do a reverse DNS lookup per request.
        return self.client_address[0]

    def do_GET(self):  # noqa: N802
        srv = self.server  # type: ignore[attr-defined]
        srv.requests.append({"path": self.path, "auth": self.headers.get("Authorization", "")})
        body, status, ctype = srv.queue.pop(0) if srv.queue else (b"{}", 200, "application/json")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def series(values, **labels) -> dict:
    return {"metric": labels, "values": [[1700000000, str(v)] for v in values]}


def payload(*series_list) -> bytes:
    return json.dumps({"status": "success", "data": {"result": list(series_list)}}).encode()


class _Server(ThreadingHTTPServer):
    """A loopback server that does no name resolution at all.

    socketserver sets server_name with socket.getfqdn(), which costs ~5.5s of
    reverse DNS on this host per server construction — i.e. per test — and the
    tests never read server_name.
    """

    def server_bind(self) -> None:
        import socketserver

        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = host
        self.server_port = port


class SoakO2EvidenceTest(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="w32-o2-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.server = _Server(("127.0.0.1", 0), _Handler)
        self.server.queue = []          # type: ignore[attr-defined]
        self.server.requests = []       # type: ignore[attr-defined]
        self.port = self.server.server_address[1]
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()

        def stop() -> None:
            # One cleanup, in the right order: addCleanup runs LIFO, so two
            # separate registrations would join a still-serving thread and
            # block for the whole join timeout on every test.
            self.server.shutdown()
            thread.join(5)

        self.addCleanup(stop)

    # ---- helpers ---------------------------------------------------------
    def run_script(
        self,
        *,
        queue: list[tuple[bytes, int, str]] | None = None,
        env: dict[str, str] | None = None,
        script: Path | None = None,
        args: tuple[str, ...] = ("--start", "2026-09-04T12:50:00Z", "--end", "2026-09-04T13:05:00Z",
                                 "--run", "soak-test"),
    ) -> subprocess.CompletedProcess:
        if queue is not None:
            self.server.queue = list(queue)  # type: ignore[attr-defined]
        run_env = dict(os.environ)
        run_env["O2_AUTH_BASIC"] = "user:secret"
        run_env["O2_URL"] = f"http://127.0.0.1:{self.port}"
        run_env.pop("O2_ALLOW_INSECURE_HTTP", None)
        if env:
            run_env.update(env)
        return subprocess.run(
            [sys.executable, str(script or SCRIPT), *args],
            capture_output=True, text=True, env=run_env, timeout=120,
        )

    def records(self, r: subprocess.CompletedProcess) -> list[dict]:
        return [json.loads(l) for l in r.stdout.splitlines() if l.strip()]

    @staticmethod
    def _ok(payload_bytes: bytes = None, n: int = 10) -> list[tuple[bytes, int, str]]:
        return [(payload_bytes or payload(series([1, 2, 3])), 200, "application/json")] * n

    # ---- P6-200: every series is kept ------------------------------------
    def test_all_series_are_recorded(self) -> None:  # disc
        """`sum by (strategy)` returns one series per strategy."""
        multi = payload(
            series([10, 20], strategy="A"),
            series([30], strategy="B"),
            series([40, 50, 60], strategy="C"),
        )
        r = self.run_script(queue=[(multi, 200, "application/json")] * 10)
        recs = self.records(r)
        by_series = {}
        for rec in recs:
            by_series.setdefault(rec["series"]["strategy"], []).append(rec)
        self.assertEqual(sorted(by_series), ["A", "B", "C"], r.stdout + r.stderr)
        self.assertEqual(by_series["B"][0]["n_points"], 1)
        self.assertEqual(by_series["C"][0]["n_points"], 3)
        self.assertEqual(by_series["A"][0]["min"], 10.0)

    def test_multi_series_single_line_record_keeps_every_point(self) -> None:  # disc
        """The dropped series used to under-report points and min/max."""
        multi = payload(series([1], strategy="A"), series([999], strategy="B"))
        r = self.run_script(queue=[(multi, 200, "application/json")] * 10)
        recs = self.records(r)
        self.assertEqual(len(recs), 20, "2 series x 10 queries")
        b_recs = [x for x in recs if x["series"].get("strategy") == "B"]
        self.assertEqual([x["max"] for x in b_recs], [999.0] * 10)

    def test_series_labels_are_rendered(self) -> None:  # disc
        multi = payload(series([5], strategy="A", host="h1"))
        r = self.run_script(queue=[(multi, 200, "application/json")] * 10)
        recs = self.records(r)
        self.assertIn("host=h1,strategy=A", [x["series_labels"] for x in recs])

    def test_empty_result_keeps_one_record(self) -> None:  # pin
        """Zero series is data: it must stay visible in the file.

        Not a discriminator: the pre-wave script also emitted an empty-valued
        record, so this passes on both revisions. It pins the shape."""
        r = self.run_script(queue=[(payload(), 200, "application/json")] * 10)
        recs = self.records(r)
        self.assertEqual(len(recs), 10)
        self.assertTrue(all(x["n_points"] == 0 and x["min"] is None for x in recs))

    # ---- P6-547: a non-JSON body is a query failure, not a crash ---------
    def test_html_error_page_does_not_abort_the_run(self) -> None:  # disc
        html = (b"<html><body>502 Bad Gateway</body></html>", 200, "text/html")
        r = self.run_script(queue=[html] + self._ok(n=9))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertNotIn("Traceback", r.stderr)
        self.assertIn("FAILED", r.stderr)
        # the remaining 9 queries still ran and produced records
        self.assertEqual(len(self.records(r)), 9)

    def test_non_utf8_body_does_not_abort_the_run(self) -> None:  # disc
        bad = (b"\xff\xfe\x00\x01not json", 200, "application/json")
        r = self.run_script(queue=[bad] + self._ok(n=9))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertNotIn("Traceback", r.stderr)
        self.assertEqual(len(self.records(r)), 9)

    def test_json_failure_is_reported_per_query(self) -> None:  # disc
        bad = (b"<html>", 200, "text/html")
        r = self.run_script(queue=[bad] + self._ok(n=9))
        failed = [l for l in r.stderr.splitlines() if "FAILED" in l]
        self.assertEqual(len(failed), 1, r.stderr)
        self.assertIn("query session_filtered_post_close FAILED", failed[0])

    # ---- P6-788: CRLF secrets file ---------------------------------------
    def _crlf_secrets(self) -> Path:
        path = self.tmp / "secrets.env"
        path.write_bytes(b"# comment\r\nO2_AUTH_BASIC=user:secret\r\nOTHER=x\r\n")
        return path

    def test_crlf_secret_has_no_carriage_return(self) -> None:  # pin
        """CRLF secrets.env still yields a clean credential.

        Not a discriminator: Python text mode translates CRLF (universal
        newlines), so the pre-wave `rstrip("\\n")` already produced a clean
        value and the reported 401 does not reproduce. The strip now in place
        is defensive, and this test holds the contract either way.
        """
        env = {"O2_AUTH_BASIC": "", "O2_SECRETS_FILE": str(self._crlf_secrets())}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        auths = {req["auth"] for req in self.server.requests}  # type: ignore[attr-defined]
        self.assertEqual(auths, {"Basic user:secret"}, auths)

    def test_lf_secret_still_works(self) -> None:
        path = self.tmp / "secrets-lf.env"
        path.write_bytes(b"O2_AUTH_BASIC=user:secret\n")
        env = {"O2_AUTH_BASIC": "", "O2_SECRETS_FILE": str(path)}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_env_auth_wins_over_the_file(self) -> None:
        env = {"O2_AUTH_BASIC": "env:auth", "O2_SECRETS_FILE": str(self._crlf_secrets())}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        auths = {req["auth"] for req in self.server.requests}  # type: ignore[attr-defined]
        self.assertEqual(auths, {"Basic env:auth"})

    def test_no_auth_is_refused(self) -> None:
        env = {"O2_AUTH_BASIC": "", "O2_SECRETS_FILE": str(self.tmp / "missing.env")}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("REFUSED", r.stderr)
        self.assertEqual(self.server.requests, [])  # type: ignore[attr-defined]

    # ---- P6-789: plaintext Basic auth to a remote host -------------------
    def test_remote_plain_http_is_refused(self) -> None:  # disc
        env = {"O2_URL": "http://o2.example.com:5080"}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("REFUSED", r.stderr)
        self.assertIn("https://", r.stderr)
        self.assertEqual(self.server.requests, [])  # type: ignore[attr-defined]

    def test_remote_plain_http_can_be_overridden(self) -> None:  # disc
        env = {"O2_URL": "http://o2.example.com:5080", "O2_ALLOW_INSECURE_HTTP": "1"}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertNotEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("WARN", r.stderr)

    def test_remote_https_is_allowed(self) -> None:
        """No request can succeed here (nothing listens on 443); the point is
        that the transport check does not refuse it."""
        env = {"O2_URL": "https://o2.example.invalid:5080"}
        r = self.run_script(queue=self._ok(), env=env, args=(
            "--start", "2026-09-04T12:50:00Z", "--end", "2026-09-04T13:05:00Z",
            "--run", "soak-test", "--step", "60s"))
        self.assertNotEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertNotIn("REFUSED", r.stderr)

    def test_localhost_plain_http_is_allowed(self) -> None:
        env = {"O2_URL": f"http://localhost:{self.port}"}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    # ---- unchanged contracts ---------------------------------------------
    def test_baseline_fields_survive(self) -> None:
        r = self.run_script(queue=self._ok())
        recs = self.records(r)
        self.assertEqual(len(recs), 10)
        rec = recs[0]
        for key in ("soak_run", "job_id", "t_start_utc", "t_end_utc",
                    "query_name", "query", "n_points", "first", "last", "min", "max"):
            self.assertIn(key, rec)
        self.assertEqual(rec["soak_run"], "soak-test")
        self.assertEqual(rec["n_points"], 3)
        self.assertEqual(rec["first"], "1")
        self.assertEqual(rec["max"], 3.0)

    def test_one_request_per_query(self) -> None:
        r = self.run_script(queue=self._ok())
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(len(self.server.requests), 10)  # type: ignore[attr-defined]

    def test_query_names_are_stable(self) -> None:
        r = self.run_script(queue=self._ok())
        names = [x["query_name"] for x in self.records(r)]
        self.assertEqual(names, [
            "session_filtered_post_close", "session_filtered_pre_open",
            "aggregator_in_per_s", "source_in_per_s", "new_closed_sink_per_s",
            "new_live_sink_per_s", "multitf_signal_emitted", "multitf_signal_suppressed",
            "multitf_duplicate_window", "new_signal_sink_per_s",
        ])

    def test_step_and_job_reach_the_request(self) -> None:
        r = self.run_script(queue=self._ok(), args=(
            "--start", "2026-09-04T12:50:00Z", "--end", "2026-09-04T13:05:00Z",
            "--run", "soak-test", "--step", "30s", "--job", "job-42"))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        path = self.server.requests[0]["path"]  # type: ignore[attr-defined]
        self.assertIn("step=30s", path)
        self.assertIn("start=2026-09-04T12%3A50%3A00Z", path)
        self.assertEqual(self.records(r)[0]["job_id"], "job-42")

    def test_query_range_path(self) -> None:
        r = self.run_script(queue=self._ok())
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertTrue(self.server.requests[0]["path"].startswith(  # type: ignore[attr-defined]
            "/api/default/prometheus/api/v1/query_range?"))

    def test_unreachable_o2_is_per_query_failure(self) -> None:
        env = {"O2_URL": f"http://127.0.0.1:{self.port + 1}"}
        r = self.run_script(queue=self._ok(), env=env)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertNotIn("Traceback", r.stderr)
        self.assertEqual(len(self.records(r)), 0)


if __name__ == "__main__":
    unittest.main()
