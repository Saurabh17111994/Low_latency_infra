"""Guardrails of r2-delete-prefix.sh against a local mock R2 endpoint.

These tests never touch the network: the script is pointed at an in-process
HTTP server (`R2_ENDPOINT=http://127.0.0.1:<port>`, which it honours) and at a
fixture config through the R2_ENV_FILE / R2_SECRETS_FILE seams.

The ones that matter most:
  * the default is a dry run — the listing happens, no DELETE is ever sent;
  * --apply deletes and then fails closed if any object survived the delete;
  * an empty, duplicate, overlapping or live-metadata prefix is refused before
    any request leaves the process;
  * the credentials never appear in a process command line (P6-158) — argv is
    world-readable through /proc/<pid>/cmdline.
"""

import http.server
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
import urllib.parse
from pathlib import Path

# The override exists so a red-before-green run can point the suite at a
# sandboxed copy of a previous revision without touching repo fixtures.
SCRIPT = Path(os.environ.get(
    "R2_DELETE_SCRIPT", Path(__file__).resolve().parent.parent / "r2-delete-prefix.sh"))
BUCKET = "test-bucket"
AK_SENTINEL = "AK-SENTINEL-1234567890"
SK_SENTINEL = "SK-SENTINEL-abcdefghijklmnop"


def _proc_cmdlines():
    """/proc/<pid>/cmdline for every readable pid (best effort)."""
    out = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as fh:
                out.append(fh.read().decode("utf-8", "replace"))
        except OSError:
            continue
    return out


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *args):  # keep the test output clean
        pass

    def _key(self):
        return urllib.parse.unquote(urllib.parse.urlparse(self.path).path
                                    .removeprefix(f"/{BUCKET}/"))

    def _query(self):
        return urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

    def _send(self, status, body=b"", headers=None):
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _record(self):
        srv = self.server
        key = self._key()
        srv.log.append((self.command, key))
        if not srv.cmdlines:
            srv.cmdlines = _proc_cmdlines()
        fail = srv.fail_once.get((self.command, key))
        if fail:
            srv.fail_once.pop((self.command, key))
            self._send(fail, b"<Error><Code>SlowDown</Code></Error>")
            return key, True
        return key, False

    def do_GET(self):
        key, failed = self._record()
        if failed:
            return
        if "list-type" in self._query():
            prefix = self._query().get("prefix", [""])[0]
            keys = sorted(k for k in self.server.objects if k.startswith(prefix))
            xml = ["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                   "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
                   "<IsTruncated>false</IsTruncated>"]
            for k in keys:
                xml.append(f"<Contents><Key>{k}</Key>"
                           f"<Size>{len(self.server.objects[k])}</Size></Contents>")
            xml.append("</ListBucketResult>")
            self._send(200, "".join(xml).encode())
            return
        self._send(404, b"<Error><Code>NoSuchKey</Code></Error>")

    def do_DELETE(self):
        key, failed = self._record()
        if failed:
            return
        # The hook lets a test simulate a backend that answers 204 but does
        # not actually remove the object (the final verification must catch it).
        hook = self.server.delete_hook
        removed = hook(key) if hook else True
        existed = self.server.objects.pop(key, None) is not None if removed else False
        self._send(204 if (removed and existed) else 404, b"")


class MockR2:
    def __init__(self):
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.objects = {}
        self.server.log = []
        self.server.cmdlines = []
        self.server.fail_once = {}
        self.server.delete_hook = None
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    @property
    def log(self):
        return list(self.server.log)

    @property
    def objects(self):
        return dict(self.server.objects)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class DeletePrefixTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w7-del-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.r2 = MockR2()
        self.addCleanup(self.r2.close)
        self.env_file = Path(self.tmp) / "env"
        self.sec_file = Path(self.tmp) / "secrets"
        self.write_config(endpoint=self.r2.endpoint)
        self.r2.server.objects["src/a/one.bin"] = b"one-bytes"
        self.r2.server.objects["src/a/two.bin"] = b"two-bytes-longer"
        self.r2.server.objects["src/a_backup/other.bin"] = b"not-mine"

    def write_config(self, endpoint, ak=AK_SENTINEL, sk=SK_SENTINEL, bucket=BUCKET):
        self.env_file.write_text(f"R2_ENDPOINT={endpoint}\nR2_BUCKET={bucket}\n")
        self.sec_file.write_text(f"AWS_ACCESS_KEY_ID={ak}\nAWS_SECRET_ACCESS_KEY={sk}\n")

    def run_delete(self, *args, timeout=120):
        env = dict(os.environ)
        env["R2_ENV_FILE"] = str(self.env_file)
        env["R2_SECRETS_FILE"] = str(self.sec_file)
        return subprocess.run(["/usr/bin/env", "bash", str(SCRIPT), *args],
                              capture_output=True, text=True, timeout=timeout, env=env)

    def test_dry_run_lists_everything_and_deletes_nothing(self):
        result = self.run_delete("src/a/")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("src/a/one.bin", result.stdout)
        self.assertIn("src/a/two.bin", result.stdout)
        self.assertIn("DRY RUN", result.stdout)
        self.assertNotIn(("DELETE", "src/a/one.bin"), self.r2.log)
        self.assertNotIn(("DELETE", "src/a/two.bin"), self.r2.log)
        self.assertIn("src/a/one.bin", self.r2.objects)

    def test_apply_deletes_and_verifies(self):
        result = self.run_delete("--apply", "src/a/")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DELETE COMPLETE", result.stdout)
        self.assertNotIn("src/a/one.bin", self.r2.objects)
        self.assertNotIn("src/a/two.bin", self.r2.objects)
        # "src/a" must not also pick up the sibling "src/a_backup/".
        self.assertIn("src/a_backup/other.bin", self.r2.objects)

    def test_empty_prefix_is_refused_without_a_request(self):
        self.r2.server.log.clear()
        result = self.run_delete("--apply", "/")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(self.r2.log, [], "refusal must precede any request")

    def test_no_prefix_is_a_usage_error(self):
        result = self.run_delete()
        self.assertEqual(result.returncode, 2)

    def test_duplicate_and_overlapping_prefixes_are_refused_without_a_request(self):
        for prefixes in (("src/a/", "src/a/"), ("src/a/", "src/a/b/"),
                         ("src/a/b/", "src/a/")):
            with self.subTest(prefixes=prefixes):
                self.r2.server.log.clear()
                result = self.run_delete("--apply", *prefixes)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(self.r2.log, [], "refusal must precede any request")

    def test_live_iceberg_metadata_is_refused_without_a_request(self):
        for prefix in ("lake/default/raw_table_1/",
                       "lake/default/raw_table_1/metadata/",
                       "lake/default/"):
            with self.subTest(prefix=prefix):
                self.r2.server.log.clear()
                result = self.run_delete("--apply", prefix)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("live Iceberg metadata", result.stderr)
                self.assertEqual(self.r2.log, [], "refusal must precede any request")

    def test_prefix_without_a_trailing_slash_is_normalized(self):
        result = self.run_delete("--apply", "src/a")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("src/a/one.bin", self.r2.objects)
        self.assertIn("src/a_backup/other.bin", self.r2.objects)

    def test_transient_503_is_retried_instead_of_aborting_the_delete(self):
        self.r2.server.fail_once[("DELETE", "src/a/one.bin")] = 503
        result = self.run_delete("--apply", "src/a/")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("retry", result.stdout)
        self.assertNotIn("src/a/one.bin", self.r2.objects)
        self.assertGreaterEqual(self.r2.log.count(("DELETE", "src/a/one.bin")), 2)

    def test_surviving_object_fails_the_final_verification(self):
        # The backend answers 204 but removes nothing: the post-run listing must
        # catch it and fail the script instead of reporting success.
        self.r2.server.delete_hook = lambda key: False
        result = self.run_delete("--apply", "src/a/")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("DELETE INCOMPLETE", result.stderr)
        self.assertIn("src/a/one.bin", self.r2.objects)

    def test_credentials_never_appear_in_a_command_line(self):
        result = self.run_delete("--apply", "src/a/")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.r2.server.cmdlines, "no /proc cmdlines captured")
        for cmdline in self.r2.server.cmdlines:
            self.assertNotIn(AK_SENTINEL, cmdline)
            self.assertNotIn(SK_SENTINEL, cmdline)

    def test_missing_or_incomplete_config_fails_closed(self):
        self.sec_file.write_text("AWS_ACCESS_KEY_ID=only-half\n")
        result = self.run_delete("--apply", "src/a/")
        self.assertEqual(result.returncode, 2)
        self.assertIn("incomplete R2 config", result.stderr)
        self.assertEqual(self.r2.log, [])

    def test_missing_scheme_or_host_in_the_endpoint_fails_closed(self):
        self.write_config(endpoint="r2.example.com")
        result = self.run_delete("--apply", "src/a/")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("R2_ENDPOINT", result.stderr)
        self.assertEqual(self.r2.log, [])


if __name__ == "__main__":
    unittest.main()
