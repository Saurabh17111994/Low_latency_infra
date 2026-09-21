#!/usr/bin/env python3
"""test_19_pernode_attribution_check.py — the per-node attribution check (CHG-286).

This is the command behind CHG-283's acceptance ("one address per node"), so the
case that matters most is the shape that regressed: a Swarm task name where an
address belongs. It is asserted directly rather than inferred from a green run.

TestCase style on purpose: pytest-only files are invisible to `unittest
discover`, which is one of the two canonical runners here.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "code").is_dir():
            return parent
    raise RuntimeError("repo root not found")


_SCRIPT = _repo_root() / "code/01_platform/04_scripts/pernode_attribution_check.py"
# base64 of "test:test" — a fake credential, and asserted never to be printed.
_CRED = "dGVzdDp0ZXN0"


class _Handler(BaseHTTPRequestHandler):
    payload = b"{}"
    status = 200
    last_body = None
    last_path = None

    def log_message(self, *args):
        pass

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        type(self).last_body = json.loads(self.rfile.read(length) or b"{}")
        type(self).last_path = self.path
        body = type(self).payload
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _Server(ThreadingHTTPServer):
    daemon_threads = True


class PerNodeAttributionCheckTest(unittest.TestCase):
    def setUp(self):
        _Handler.last_body = None
        _Handler.last_path = None
        self.srv = _Server(("127.0.0.1", 0), _Handler)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.addCleanup(self.srv.shutdown)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"

    def answer_with(self, instances, status=200, column="instance"):
        """O2 answers with the selected column as the row key, not a fixed name."""
        _Handler.status = status
        rows = [{column: i} for i in instances]
        _Handler.payload = json.dumps({"hits": rows}).encode()

    def run_check(self, *args, url=None, env=None):
        base_env = {k: v for k, v in os.environ.items()
                    if k not in ("O2_AUTH_BASIC", "O2_ALLOW_INSECURE_HTTP",
                                 "O2_SECRETS_FILE")}
        base_env["O2_AUTH_BASIC"] = _CRED
        base_env.update(env or {})
        return subprocess.run(
            [sys.executable, str(_SCRIPT), "--url", url or self.url, *args],
            capture_output=True, text=True, env=base_env, timeout=60,
        )

    # --- the check itself -------------------------------------------------

    def test_two_addresses_pass(self):
        self.answer_with(["10.0.1.31:9100", "10.0.1.27:9100"])
        r = self.run_check("--expect", "2")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("OK", r.stdout)
        self.assertIn("10.0.1.27:9100", r.stdout)
        self.assertIn("10.0.1.31:9100", r.stdout)

    def test_a_swarm_task_name_fails_the_check(self):
        """The pre-CHG-283 shape: the name resolves to many tasks, one is kept."""
        self.answer_with(["tasks.node-exporter:9100"])
        r = self.run_check("--expect", "1")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("Swarm task names, not addresses", r.stderr)
        self.assertIn("tasks.node-exporter:9100", r.stderr)

    def test_one_address_is_not_enough_for_a_two_node_fleet(self):
        self.answer_with(["10.0.1.27:9100"])
        r = self.run_check("--expect", "2")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("at least 2 distinct instance(s)", r.stderr)
        self.assertIn("found 1", r.stderr)

    def test_no_samples_in_the_window_is_a_failed_check(self):
        self.answer_with([])
        r = self.run_check("--expect", "2")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("no recent samples", r.stderr)
        self.assertIn("not at the label", r.stderr)

    def test_a_malformed_instance_fails_even_when_the_count_is_right(self):
        self.answer_with(["10.0.1.27:9100", "node-exporter"])
        r = self.run_check("--expect", "2")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("do not look like host:port", r.stderr)
        self.assertIn("node-exporter", r.stderr)

    # --- wiring: the request it actually makes ----------------------------

    def test_the_query_is_a_metrics_search_for_distinct_instances(self):
        self.answer_with(["10.0.1.27:9100", "10.0.1.31:9100"])
        r = self.run_check("--expect", "2", "--minutes", "30")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(_Handler.last_path, "/api/default/_search?type=metrics")
        q = _Handler.last_body["query"]
        self.assertEqual(q["sql"],
                         'select distinct(instance) from "node_boot_time_seconds"')
        self.assertGreater(q["end_time"], q["start_time"])
        self.assertEqual(q["end_time"] - q["start_time"], 30 * 60 * 1_000_000)
        self.assertGreaterEqual(q["size"], 2)

    def test_a_custom_stream_and_column_reach_the_query(self):
        # The value is an address because the check asserts address-shaped
        # values whatever the column is called; the column name is the point here.
        self.answer_with(["10.0.1.27:9100"], column="nodename")
        r = self.run_check("--expect", "1", "--stream", "node_uname_info",
                           "--column", "nodename")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("10.0.1.27:9100", r.stdout)
        self.assertEqual(
            _Handler.last_body["query"]["sql"],
            'select distinct(nodename) from "node_uname_info"')

    # --- exit 2: the check never ran --------------------------------------

    def test_no_credential_exits_2(self):
        r = self.run_check("--expect", "2", env={
            "O2_AUTH_BASIC": "", "O2_SECRETS_FILE": "/nonexistent/secrets.env"})
        self.assertEqual(r.returncode, 2, r.stdout)
        self.assertIn("no O2_AUTH_BASIC", r.stderr)

    def test_an_o2_error_exits_2_and_says_o2_answered(self):
        self.answer_with([], status=400)
        r = self.run_check("--expect", "2")
        self.assertEqual(r.returncode, 2, r.stdout)
        self.assertIn("HTTP 400", r.stderr)
        self.assertIn("not the network", r.stderr)

    def test_an_unreachable_o2_exits_2(self):
        r = self.run_check("--expect", "2", url="http://127.0.0.1:1")
        self.assertEqual(r.returncode, 2, r.stdout)
        self.assertIn("unreachable", r.stderr)

    def test_plain_http_to_a_remote_host_is_refused_before_any_request(self):
        r = self.run_check("--expect", "2", url="http://10.255.255.1:5080")
        self.assertEqual(r.returncode, 2, r.stdout)
        self.assertIn("unencrypted", r.stderr)
        self.assertIsNone(_Handler.last_body)

    def test_expect_is_required(self):
        r = self.run_check()
        self.assertEqual(r.returncode, 2, r.stdout)
        self.assertIn("--expect", r.stderr)

    # --- the credential is never printed ----------------------------------

    def test_the_credential_never_reaches_the_output(self):
        for instances, expect in ((["10.0.1.27:9100"], "1"),
                                  (["tasks.node-exporter:9100"], "1"),
                                  ([], "2")):
            with self.subTest(instances=instances):
                self.answer_with(instances)
                r = self.run_check("--expect", expect)
                self.assertNotIn(_CRED, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
