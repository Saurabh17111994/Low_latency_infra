"""Guardrails of r2-move-prefix.sh against a local mock R2 endpoint.

These tests never touch the network: the script is pointed at an in-process
HTTP server (`R2_ENDPOINT=http://127.0.0.1:<port>`, which it honours since
P6-491) and at a fixture config through the R2_ENV_FILE / R2_SECRETS_FILE seams.

The two that matter most:
  * the source is deleted only after the destination is verified (P6-013) —
    a mock that reports a short destination must leave BOTH copies in place;
  * the credentials never appear in a process command line (P6-158) — argv is
    world-readable through /proc/<pid>/cmdline.
"""

import hashlib
import http.server
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.parse
from pathlib import Path

# The override exists so the wave's red-before-green run can point the suite at
# a sandboxed copy of a previous revision without touching repo fixtures.
SCRIPT = Path(os.environ.get(
    "R2_MOVE_SCRIPT", Path(__file__).resolve().parent.parent / "r2-move-prefix.sh"))
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
        if body and self.command != "HEAD":
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
                xml.append(f"<Contents><Key>{k}</Key></Contents>")
            xml.append("</ListBucketResult>")
            self._send(200, "".join(xml).encode())
            return
        body = self.server.objects.get(key)
        if body is None:
            self._send(404, b"<Error><Code>NoSuchKey</Code></Error>")
            return
        self._send(200, body, {"Content-Type": self.server.content_types.get(
            key, "application/octet-stream")})

    def do_PUT(self):
        key, failed = self._record()
        if failed:
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.server.objects[key] = body
        self.server.content_types[key] = self.headers.get("Content-Type")
        self._send(200, b"")

    def do_HEAD(self):
        key, failed = self._record()
        if failed:
            return
        body = self.server.objects.get(key)
        if body is None:
            self._send(404, b"")
            return
        md5 = hashlib.md5(body).hexdigest()
        headers = {"Content-Length": str(len(body)), "ETag": f'"{md5}"'}
        status = 200
        if self.server.head_hook is not None:
            status, headers = self.server.head_hook(key, len(body), md5)
        self._send(status, b"", headers)

    def do_DELETE(self):
        key, failed = self._record()
        if failed:
            return
        existed = self.server.objects.pop(key, None) is not None
        self.server.content_types.pop(key, None)
        self._send(204 if existed else 404, b"")


class MockR2:
    def __init__(self):
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.objects = {}
        self.server.content_types = {}
        self.server.log = []
        self.server.cmdlines = []
        self.server.fail_once = {}
        self.server.head_hook = None
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


class MovePrefixTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w7-move-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.r2 = MockR2()
        self.addCleanup(self.r2.close)
        self.env_file = Path(self.tmp) / "env"
        self.sec_file = Path(self.tmp) / "secrets"
        self.write_config(endpoint=self.r2.endpoint)
        self.r2.server.objects["src/a/one.bin"] = b"one-bytes"
        self.r2.server.objects["src/a/two.bin"] = b"two-bytes-longer"
        self.r2.server.content_types["src/a/one.bin"] = "application/octet-stream"
        self.r2.server.content_types["src/a/two.bin"] = "application/x-parquet"

    def write_config(self, endpoint, ak=AK_SENTINEL, sk=SK_SENTINEL, bucket=BUCKET):
        self.env_file.write_text(f"R2_ENDPOINT={endpoint}\nR2_BUCKET={bucket}\n")
        self.sec_file.write_text(f"AWS_ACCESS_KEY_ID={ak}\nAWS_SECRET_ACCESS_KEY={sk}\n")

    def run_move(self, src, dst, timeout=120):
        env = dict(os.environ)
        env["R2_ENV_FILE"] = str(self.env_file)
        env["R2_SECRETS_FILE"] = str(self.sec_file)
        return subprocess.run(["/usr/bin/env", "bash", str(SCRIPT), src, dst],
                              capture_output=True, text=True, timeout=timeout, env=env)

    def test_move_verifies_destination_before_deleting_the_source(self):
        result = self.run_move("src/a/", "dst/a/")
        self.assertEqual(result.returncode, 0, result.stderr)

        moved = self.r2.objects
        self.assertNotIn("src/a/one.bin", moved)
        self.assertNotIn("src/a/two.bin", moved)
        self.assertEqual(moved["dst/a/one.bin"], b"one-bytes")
        self.assertEqual(moved["dst/a/two.bin"], b"two-bytes-longer")

        # Per object: GET, PUT, HEAD (verification), then DELETE — HEAD before
        # the delete is the whole point of P6-013.
        per_object = [op for op in self.r2.log
                      if op[1] in ("src/a/one.bin", "src/a/two.bin", "dst/a/one.bin",
                                   "dst/a/two.bin")]
        tail = [op[0] for op in per_object[-4:]]
        self.assertEqual(tail, ["GET", "PUT", "HEAD", "DELETE"])

        # P6-492: the object's content type survives the copy.
        self.assertEqual(self.r2.server.content_types["dst/a/two.bin"],
                         "application/x-parquet")

        # P6-158: the credentials are not in any process command line.
        self.assertTrue(self.r2.server.cmdlines, "no /proc cmdlines captured")
        for cmdline in self.r2.server.cmdlines:
            self.assertNotIn(AK_SENTINEL, cmdline)
            self.assertNotIn(SK_SENTINEL, cmdline)

    def test_short_destination_is_never_deleted(self):
        # The destination claims fewer bytes than were copied: keep both copies.
        self.r2.server.head_hook = (
            lambda key, size, md5: (200, {"Content-Length": str(size - 1),
                                          "ETag": f'"{md5}"'}))
        result = self.run_move("src/a/", "dst/a/")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("REFUSING TO DELETE", result.stderr)
        self.assertIn("dst/a/one.bin", self.r2.objects)
        self.assertIn("src/a/one.bin", self.r2.objects, "source lost without a verified copy")
        self.assertNotIn(("DELETE", "src/a/one.bin"), self.r2.log)

    def test_destination_with_a_different_md5_is_never_deleted(self):
        self.r2.server.head_hook = (
            lambda key, size, md5: (200, {"Content-Length": str(size),
                                          "ETag": '"' + "0" * 32 + '"'}))
        result = self.run_move("src/a/", "dst/a/")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("REFUSING TO DELETE", result.stderr)
        self.assertNotIn(("DELETE", "src/a/one.bin"), self.r2.log)
        self.assertIn("src/a/one.bin", self.r2.objects)

    def test_overlapping_or_equal_prefixes_are_refused_without_a_request(self):
        for src, dst in (("a/", "a/b/"), ("a/b/", "a/"), ("a/", "a/")):
            with self.subTest(src=src, dst=dst):
                self.r2.server.log.clear()
                result = self.run_move(src, dst)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(self.r2.log, [], "refusal must precede any request")

    def test_prefix_without_a_trailing_slash_is_normalized(self):
        # "src/a" must not also pick up the sibling "src/a_backup/".
        self.r2.server.objects["src/a_backup/other.bin"] = b"not-mine"
        result = self.run_move("src/a", "dst/a")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("src/a_backup/other.bin", self.r2.objects)
        self.assertNotIn("dst/a/other.bin", self.r2.objects)
        self.assertEqual(self.r2.objects["dst/a/one.bin"], b"one-bytes")

    def test_transient_503_is_retried_instead_of_aborting_the_move(self):
        self.r2.server.fail_once[("GET", "src/a/one.bin")] = 503
        result = self.run_move("src/a/", "dst/a/")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("retry", result.stdout)
        self.assertEqual(self.r2.objects["dst/a/one.bin"], b"one-bytes")
        self.assertNotIn("src/a/one.bin", self.r2.objects)
        self.assertGreaterEqual(self.r2.log.count(("GET", "src/a/one.bin")), 2)

    def test_missing_or_incomplete_config_fails_closed(self):
        self.sec_file.write_text("AWS_ACCESS_KEY_ID=only-half\n")
        result = self.run_move("src/a/", "dst/a/")
        self.assertEqual(result.returncode, 2)
        self.assertIn("incomplete R2 config", result.stderr)
        self.assertEqual(self.r2.log, [])

    def test_missing_scheme_or_host_in_the_endpoint_fails_closed(self):
        self.write_config(endpoint="r2.example.com")
        result = self.run_move("src/a/", "dst/a/")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("R2_ENDPOINT", result.stderr)
        self.assertEqual(self.r2.log, [])


if __name__ == "__main__":
    unittest.main()
