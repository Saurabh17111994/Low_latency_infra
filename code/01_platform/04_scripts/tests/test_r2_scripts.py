"""Guardrails for the R2 storage tools (P6 wave 10).

Every test here is hermetic: the R2 endpoint is either a fixture string that is
never dialled or a TLS stub server on 127.0.0.1 that speaks ListObjectsV2 back at
the script, and `duckdb` is a stub on PATH. No test reads 01_docker/.env, and no
test can reach Cloudflare.

The two scripts are addressed through R2_LIST_SCRIPT / R2_QUERY_SCRIPT so the
same suite can be pointed at the pre-fix versions (recorded red run in CHG-147).
"""

import os
import shlex
import shutil
import ssl
import subprocess
import stat
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "code" / "01_platform" / "04_scripts"
LIST_SCRIPT = Path(os.environ.get("R2_LIST_SCRIPT", SCRIPTS / "r2-list.sh"))
QUERY_SCRIPT = Path(os.environ.get("R2_QUERY_SCRIPT", SCRIPTS / "r2-query.sh"))

NS = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'
AK = "AKIAFIXTUREACCESSKEY"
SK = "fixture-secret-access-key"

DUCKDB_FAILS = """
#!/usr/bin/env bash
# stub duckdb: fails unless the script it was fed tries INSTALL first
input="$(cat)"
{
  printf 'argv: %s\n' "$*"
  printf '%s\n' "$input"
} >> "$R2_TEST_RECORD"
if [ "${R2_TEST_DUCKDB_MODE:-ok}" = "ok" ]; then
  printf 'n\n42\n'; exit 0
fi
if [ "${R2_TEST_DUCKDB_MODE:-ok}" = "need-install" ]; then
  case "$input" in
    *INSTALL*httpfs*) printf 'n\n42\n'; exit 0 ;;
    *) echo "stub duckdb: extension not installed" >&2; exit 1 ;;
  esac
fi
echo "stub duckdb: always refuses" >&2; exit 1
"""

PYTHON_RECORDER = """
#!/usr/bin/env bash
{
  printf 'argv: %s\n' "$*"
  printf 'env: R2_AK=%s R2_SK=%s\n' "${R2_AK:-<unset>}" "${R2_SK:-<unset>}"
} >> "$R2_TEST_RECORD"
"""


def list_xml(keys, truncated=False, token=None, ns=True, sizes=None, stamps=None):
    attr = f' {NS}' if ns else ""
    parts = [f'<?xml version="1.0" encoding="UTF-8"?>', f"<ListBucketResult{attr}>"]
    for i, key in enumerate(keys):
        size = (sizes or {}).get(key, i + 1)
        # LastModified is part of the listing and is carried through to the TSV
        # (P6-439): a caller that must age-check an object cannot do it without it.
        stamp = (stamps or {}).get(key, "2026-09-14T00:00:00.000Z")
        parts.append(
            f"<Contents><Key>{key}</Key><Size>{size}</Size>"
            f"<LastModified>{stamp}</LastModified></Contents>"
        )
    parts.append(f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>")
    if token is not None:
        parts.append(f"<NextContinuationToken>{token}</NextContinuationToken>")
    parts.append("</ListBucketResult>")
    return "\n".join(parts).encode()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):  # noqa: N802 (http.server API)
        self.server.requests.append(self.path)
        status, body = self.server.responder(len(self.server.requests))
        self.send_response(status)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _StubServer:
    def __init__(self, responder, cert):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.httpd.responder = responder
        self.httpd.requests = []
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, cert)
        self.httpd.socket = ctx.wrap_socket(self.httpd.socket, server_side=True)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self):
        return self.httpd.server_address[1]

    @property
    def requests(self):
        return self.httpd.requests

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class Base(unittest.TestCase):
    """Fixture env files + a run() that never inherits the developer's config."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(prefix="r2-tests.")
        cls.tmp = Path(cls._tmp.name)
        cls.bin = cls.tmp / "bin"
        cls.bin.mkdir()
        # $HOME/bin/duckdb is the ONLY place the pre-fix r2-query.sh looks for the
        # binary, so the red run points it at the same stub through this seam.
        cls.home = cls.tmp / "home"
        (cls.home / "bin").mkdir(parents=True)
        shim = os.environ.get("R2_TEST_HOME_DUCKDB")
        if shim:
            shutil.copy2(shim, cls.home / "bin" / "duckdb")
            (cls.home / "bin" / "duckdb").chmod(0o755)
        cls.record = cls.tmp / "record.txt"
        cls.cert = cls._self_signed()

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    @classmethod
    def _self_signed(cls):
        cert = cls.tmp / "stub-cert.pem"
        if cert.exists():
            return cert
        if not shutil.which("openssl"):
            return None
        rc = subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
             "-keyout", str(cert), "-out", str(cert), "-subj", "/CN=127.0.0.1",
             "-addext", "subjectAltName=IP:127.0.0.1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return cert if rc.returncode == 0 else None

    def write_config(self, endpoint="https://127.0.0.1:9", bucket="lake-bucket",
                     warehouse="s3://lake-bucket/lake", env_extra="", secrets_extra=""):
        env_file = self.tmp / "fixture.env"
        sec_file = self.tmp / "fixture.secrets"
        env_file.write_text(
            "R2_ENDPOINT=" + endpoint + "\n"
            "R2_BUCKET=" + bucket + "\n"
            "S3_WAREHOUSE_PATH=" + warehouse + "\n" + env_extra)
        sec_file.write_text(
            "AWS_ACCESS_KEY_ID=" + AK + "\n"
            "AWS_SECRET_ACCESS_KEY=" + SK + "\n" + secrets_extra)
        self.record.unlink(missing_ok=True)
        return env_file, sec_file

    def run_script(self, script, args=(), config=None, extra_env=None, with_bin=True,
                   home=None):
        env_file, sec_file = config or self.write_config()
        env = {
            "PATH": (str(self.bin) + ":" if with_bin else "") + os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(home if home is not None else self.home),
            "TMPDIR": str(self.tmp),
            "R2_ENV_FILE": str(env_file),
            "R2_SECRETS_FILE": str(sec_file),
            "R2_TEST_RECORD": str(self.record),
            "LC_ALL": "C",
        }
        env.update(extra_env or {})
        return subprocess.run([str(script), *args], capture_output=True, text=True, env=env,
                              timeout=60)

    def records(self):
        return self.record.read_text() if self.record.exists() else ""

    def stub_python(self):
        path = self.bin / "python3"
        path.write_text(PYTHON_RECORDER)
        path.chmod(0o755)

    def stub_duckdb(self):
        path = self.bin / "duckdb"
        path.write_text(DUCKDB_FAILS)
        path.chmod(0o755)
        return path


class R2VarTests(Base):
    """r2_var: the four parsing defects P6-485/P6-162 describe."""

    def value(self, text, key="R2_ENDPOINT"):
        probe = self.tmp / "probe.env"
        probe.write_text(text)
        out = subprocess.run(
            ["bash", "-c", 'source "$1"; r2_var "$2" "$3"', "_",
             str(SCRIPTS / "r2-env.sh"), str(probe), key],
            capture_output=True, text=True, timeout=30)
        return out

    def test_quotes_and_crlf_and_export_are_stripped(self):
        cases = {
            'R2_ENDPOINT="https://quoted.example"\n': "https://quoted.example",
            "R2_ENDPOINT='https://single.example'\n": "https://single.example",
            "R2_ENDPOINT=https://bare.example\r\n": "https://bare.example",
            "export R2_ENDPOINT=https://exported.example\n": "https://exported.example",
            "  R2_ENDPOINT =  https://spaced.example  \n": "https://spaced.example",
            "R2_ENDPOINT=https://commented.example # prod\n": "https://commented.example",
            # a "#" that is not preceded by whitespace is part of the secret
            "R2_ENDPOINT=https://hash#value.example\n": "https://hash#value.example",
        }
        for text, expected in cases.items():
            with self.subTest(text=text.strip()):
                out = self.value(text)
                self.assertEqual(out.returncode, 0, out.stderr)
                self.assertEqual(out.stdout, expected)

    def test_last_definition_wins(self):
        out = self.value("R2_ENDPOINT=https://first.example\nR2_ENDPOINT=https://second.example\n")
        self.assertEqual(out.stdout, "https://second.example")

    def test_missing_key_names_the_file(self):
        out = self.value("OTHER=1\n")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("missing R2_ENDPOINT", out.stderr)
        self.assertIn("probe.env", out.stderr)

    def test_missing_file_and_empty_value_are_errors(self):
        out = subprocess.run(
            ["bash", "-c", 'source "$1"; r2_var "$2" K', "_",
             str(SCRIPTS / "r2-env.sh"), str(self.tmp / "nope.env")],
            capture_output=True, text=True, timeout=30)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("cannot read", out.stderr)

        empty = self.value("R2_ENDPOINT=\n")
        self.assertNotEqual(empty.returncode, 0)
        self.assertIn("empty R2_ENDPOINT", empty.stderr)

    def test_reading_a_value_never_touches_the_callers_shell_options(self):
        out = subprocess.run(
            ["bash", "-c",
             'source "$1"; case $- in *e*) echo "pipefail-ish flags leaked"; exit 9;; esac; '
             'r2_var "$2" R2_ENDPOINT >/dev/null; echo survived', "_",
             str(SCRIPTS / "r2-env.sh"), str(self.tmp / "nope-too.env")],
            capture_output=True, text=True, timeout=30)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("survived", out.stdout)
        self.assertNotIn("leaked", out.stdout)


class R2ListConfigTests(Base):
    """Config defects that used to reach R2 as a SignatureDoesNotMatch."""

    def test_secrets_go_to_the_environment_not_argv(self):
        # P6-154: argv is world-readable; the stub python is the only process
        # that ever sees the credentials.
        self.stub_python()
        out = self.run_script(LIST_SCRIPT, ["lake"])
        self.assertEqual(out.returncode, 0, out.stderr)
        record = self.records()
        self.assertNotIn(AK, record.split("env:")[0], "access key id reached argv")
        self.assertNotIn(SK, record.split("env:")[0], "secret reached argv")
        self.assertIn(f"env: R2_AK={AK} R2_SK={SK}", record)

    def test_warehouse_bucket_mismatch_is_refused(self):
        config = self.write_config(warehouse="s3://some-other-bucket/lake")
        out = self.run_script(LIST_SCRIPT, ["lake"], config=config)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("bucket is not R2_BUCKET", out.stderr)

    def test_bucket_only_warehouse_is_refused(self):
        for warehouse in ("s3://lake-bucket", "s3://lake-bucket/", "not-an-s3-uri"):
            with self.subTest(warehouse=warehouse):
                config = self.write_config(warehouse=warehouse)
                out = self.run_script(LIST_SCRIPT, ["lake"], config=config)
                self.assertNotEqual(out.returncode, 0)
                self.assertIn("S3_WAREHOUSE_PATH", out.stderr)

    def test_non_https_endpoint_is_refused(self):
        for endpoint in ("http://127.0.0.1:9", "127.0.0.1:9", ""):
            with self.subTest(endpoint=endpoint):
                config = self.write_config(endpoint=endpoint or "x")
                out = self.run_script(LIST_SCRIPT, ["lake"], config=config)
                self.assertNotEqual(out.returncode, 0)
                self.assertIn("R2_ENDPOINT", out.stderr)

    def test_missing_key_aborts_with_a_named_error(self):
        env_file = self.tmp / "no-bucket.env"
        env_file.write_text("R2_ENDPOINT=https://127.0.0.1:9\nS3_WAREHOUSE_PATH=s3://lake-bucket/lake\n")
        sec = self.tmp / "fixture.secrets"
        out = self.run_script(LIST_SCRIPT, ["lake"], config=(env_file, sec))
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("missing R2_BUCKET", out.stderr)

    def test_executed_without_a_command_prints_usage(self):
        out = self.run_script(LIST_SCRIPT, [])
        self.assertEqual(out.returncode, 2)
        self.assertIn("usage: r2-list.sh", out.stderr)
        bad = self.run_script(LIST_SCRIPT, ["lak"])
        self.assertEqual(bad.returncode, 2)
        self.assertIn("unknown command", bad.stderr)

    def test_sourcing_the_script_does_not_abort_a_clean_shell(self):
        script = (
            "set -euo pipefail\n"
            f'source "{LIST_SCRIPT}"\n'
            'case $- in *e*) : ;; *) echo "lost -e"; exit 9;; esac\n'
            "echo sourced-ok\n"
        )
        env = dict(os.environ, R2_ENV_FILE=str(self.tmp / "absent.env"),
                   R2_SECRETS_FILE=str(self.tmp / "absent.env"))
        out = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                             env=env, timeout=30)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("sourced-ok", out.stdout)


class R2ListHttpTests(Base):
    """ListObjectsV2 handling: signing, XML, errors, pagination (P6-155..157, 487, 488)."""

    def setUp(self):
        if self.cert is None:
            self.skipTest("openssl unavailable — cannot bring up the TLS stub endpoint")
        self.servers = []

    def tearDown(self):
        for server in self.servers:
            server.stop()

    def serve(self, responder):
        server = _StubServer(responder, self.cert)
        self.servers.append(server)
        return server

    def run_list(self, endpoint, *, warehouse="s3://lake-bucket/lake"):
        # no python stub here: these tests must exercise the real SigV4 client
        config = self.write_config(endpoint=endpoint, warehouse=warehouse)
        env = {"SSL_CERT_FILE": str(self.cert), "R2_TEST_RECORD": str(self.record)}
        return self.run_script(LIST_SCRIPT, ["lake"], config=config, extra_env=env)

    def test_success_prints_tsv_and_asks_for_the_lake_prefix(self):
        server = self.serve(lambda n: (200, list_xml(["lake/default/raw_table_1/x.parquet"])))
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(out.stdout.strip(),
                         "lake/default/raw_table_1/x.parquet\t1\t2026-09-14T00:00:00.000Z")
        request = server.requests[0]
        self.assertTrue(request.startswith("/lake-bucket/?"), request)
        self.assertIn("prefix=lake%2F", request, "the warehouse prefix must be percent-encoded")
        self.assertNotIn("prefix=lake/", request, "a raw '/' in the query value breaks SigV4")
        self.assertIn("list-type=2", request)

    def test_r2_list_lake_takes_a_sub_prefix(self):
        # P6-437: lake-guard lists ONE partition instead of paginating the whole
        # warehouse and grepping the buffer.
        suffix = "lake/default/raw_table_1/data/event_day=20260913/a.parquet"
        server = self.serve(lambda n: (200, list_xml([suffix])))
        helper = self.tmp / "call-r2-list-lake-with-prefix.sh"
        helper.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            'source "$1"\n'
            'r2_list_lake "$2"\n'
        )
        helper.chmod(0o755)
        config = self.write_config(endpoint=f"https://127.0.0.1:{server.port}")
        out = self.run_script(
            helper, [str(LIST_SCRIPT), "lake/default/raw_table_1/data/"],
            config=config, extra_env={"SSL_CERT_FILE": str(self.cert)})
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertTrue(out.stdout.startswith(suffix + "\t1\t"), out.stdout)
        self.assertIn("prefix=lake%2Fdefault%2Fraw_table_1%2Fdata%2F", server.requests[0],
                      "the sub-prefix must reach the LIST request")

    def test_namespaceless_response_still_lists_keys(self):
        # P6-488: findall('s3:Contents') read a namespace-less page as "empty".
        server = self.serve(lambda n: (200, list_xml(["lake/a.parquet"], ns=False)))
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(out.stdout.strip(),
                         "lake/a.parquet\t1\t2026-09-14T00:00:00.000Z")

    def test_http_error_reports_the_s3_code(self):
        body = (b'<?xml version="1.0"?><Error><Code>AccessDenied</Code>'
                b"<Message>no</Message></Error>")
        server = self.serve(lambda n: (403, body))
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("HTTP 403", out.stderr)
        self.assertIn("Code=AccessDenied", out.stderr)

    def test_error_document_with_http_200_is_not_read_as_empty(self):
        body = (b'<?xml version="1.0"?><Error><Code>NoSuchBucket</Code>'
                b"<Message>gone</Message></Error>")
        server = self.serve(lambda n: (200, body))
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("error document", out.stderr)
        self.assertIn("NoSuchBucket", out.stderr)

    def test_non_xml_body_is_reported(self):
        server = self.serve(lambda n: (200, b"<html>not xml"))
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("not XML", out.stderr)

    def test_missing_is_truncated_is_refused(self):
        body = (b'<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/'
                b'doc/2006-03-01/"><Contents><Key>lake/a</Key><Size>1</Size></Contents>'
                b"</ListBucketResult>")
        server = self.serve(lambda n: (200, body))
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("no IsTruncated", out.stderr)

    def test_truncated_page_without_a_token_fails_instead_of_looping(self):
        server = self.serve(lambda n: (200, list_xml(["lake/a"], truncated=True)))
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("no usable", out.stderr)
        self.assertEqual(len(server.requests), 1, "it re-requested the same page")

    def test_pagination_follows_the_continuation_token(self):
        def responder(n):
            if n == 1:
                return 200, list_xml(["lake/one"], truncated=True, token="tok-2")
            return 200, list_xml(["lake/two"])

        server = self.serve(responder)
        out = self.run_list(f"https://127.0.0.1:{server.port}")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(
            out.stdout.split(),
            ["lake/one", "1", "2026-09-14T00:00:00.000Z",
             "lake/two", "1", "2026-09-14T00:00:00.000Z"],
            "TSV is key<TAB>size<TAB>LastModified")
        self.assertEqual(len(out.stdout.strip().splitlines()), 2, "one object per line")
        self.assertEqual(len(server.requests), 2)
        self.assertIn("continuation-token=tok-2", server.requests[1])

    def test_endpoint_with_a_trailing_slash_and_port_still_signs_the_right_host(self):
        # P6-487: 'host/' produced a double slash and a bad Host header.
        server = self.serve(lambda n: (200, list_xml(["lake/a"])))
        out = self.run_list(f"https://127.0.0.1:{server.port}/")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(len(server.requests), 1)
        self.assertFalse(server.requests[0].startswith("//"), server.requests[0])

    def test_network_failure_is_reported_not_traced(self):
        # nothing listening: port 9 (discard) refuses instantly
        out = self.run_list("https://127.0.0.1:9")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("r2-list:", out.stderr)
        self.assertNotIn("Traceback", out.stderr)


class R2QueryTests(Base):
    """The DuckDB wrapper: no argv secrets, private SQL, LOAD-before-INSTALL."""

    def test_sql_and_secrets_never_reach_argv_and_the_sql_file_is_private(self):
        duck = self.stub_duckdb()
        env_file, sec_file = self.write_config(endpoint="https://mini.example.test/",
                                               secrets_extra="")
        out = self.run_script(QUERY_SCRIPT, ["SELECT count(*) AS n FROM t;"],
                              config=(env_file, sec_file),
                              extra_env={"DUCKDB_BIN": str(duck)})
        self.assertEqual(out.returncode, 0, out.stderr)
        record = self.records()
        argv_line = next(l for l in record.splitlines() if l.startswith("argv:"))
        self.assertNotIn(SK, argv_line, "secret reached argv")
        self.assertNotIn("SELECT", argv_line, "SQL reached argv")
        self.assertIn(":memory:", argv_line)
        self.assertIn("SET s3_access_key_id='" + AK + "'", record)
        self.assertIn("SET s3_endpoint='mini.example.test'", record)
        self.assertIn("SELECT count(*) AS n FROM t;", record)
        self.assertIn("SET s3_endpoint='mini.example.test'", record)
        self.assertNotIn("mini.example.test/", record)

    def test_sql_file_mode_is_0600_and_the_temp_files_are_removed(self):
        duck = self.stub_duckdb()
        mode_probe = (
            "#!/usr/bin/env bash\n"
            'input="$(cat)"\n'
            'stat -L -c %a /proc/self/fd/0 >> "$R2_TEST_RECORD" 2>/dev/null || true\n'
            "printf 'n\\n42\\n'\n"
        )
        duck.write_text(mode_probe)
        out = self.run_script(QUERY_SCRIPT, ["SELECT 1;"], extra_env={"DUCKDB_BIN": str(duck)})
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(self.records().strip(), "600")
        leftovers = [p.name for p in self.tmp.glob("r2-query.*")]
        self.assertEqual(leftovers, [], f"temp files left behind: {leftovers}")

    def test_load_first_then_install_retry(self):
        duck = self.stub_duckdb()
        out = self.run_script(QUERY_SCRIPT, ["SELECT 1;"],
                              extra_env={"DUCKDB_BIN": str(duck),
                                         "R2_TEST_DUCKDB_MODE": "need-install"})
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(out.stdout.split(), ["n", "42"])
        argv_lines = [l for l in self.records().splitlines() if l.startswith("argv:")]
        self.assertEqual(len(argv_lines), 2, "expected the LOAD attempt and the INSTALL retry")

    def test_both_failures_are_reported(self):
        duck = self.stub_duckdb()
        out = self.run_script(QUERY_SCRIPT, ["SELECT 1;"],
                              extra_env={"DUCKDB_BIN": str(duck),
                                         "R2_TEST_DUCKDB_MODE": "fail"})
        self.assertEqual(out.returncode, 1)
        self.assertIn("INSTALL retry failed", out.stderr)
        self.assertIn("always refuses", out.stderr)

    def test_missing_duckdb_is_named(self):
        out = self.run_script(QUERY_SCRIPT, ["SELECT 1;"],
                              extra_env={"DUCKDB_BIN": str(self.tmp / "no-such-duckdb")})
        self.assertEqual(out.returncode, 2)
        self.assertIn("duckdb not found", out.stderr)

    def test_usage_and_config_errors_exit_2(self):
        duck = self.stub_duckdb()
        for args, config, needle in (
            ([], None, "usage: r2-query.sh"),
            (["SELECT 1;", "SELECT 2;"], None, "usage: r2-query.sh"),
        ):
            with self.subTest(args=args):
                out = self.run_script(QUERY_SCRIPT, args, config=config,
                                      extra_env={"DUCKDB_BIN": str(duck)})
                self.assertEqual(out.returncode, 2)
                self.assertIn(needle, out.stderr)
        bad = self.write_config(endpoint="http://127.0.0.1:9")
        out = self.run_script(QUERY_SCRIPT, ["SELECT 1;"], config=bad,
                              extra_env={"DUCKDB_BIN": str(duck)})
        self.assertEqual(out.returncode, 2)
        self.assertIn("must be an https:// URL", out.stderr)

    def test_missing_config_key_names_the_file(self):
        duck = self.stub_duckdb()
        env_file = self.tmp / "query-missing.env"
        env_file.write_text("R2_BUCKET=x\n")
        out = self.run_script(QUERY_SCRIPT, ["SELECT 1;"], config=(env_file, self.tmp / "fixture.secrets"),
                              extra_env={"DUCKDB_BIN": str(duck)})
        self.assertEqual(out.returncode, 2)
        self.assertIn("missing R2_ENDPOINT", out.stderr)

    def test_single_quotes_in_secrets_are_escaped(self):
        duck = self.stub_duckdb()
        env_file, sec_file = self.write_config()
        sec_file.write_text(
            "AWS_ACCESS_KEY_ID=" + AK + "\n"
            "AWS_SECRET_ACCESS_KEY=abc'def\n")
        out = self.run_script(QUERY_SCRIPT, ["SELECT 1;"], config=(env_file, sec_file),
                              extra_env={"DUCKDB_BIN": str(duck)})
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("SET s3_secret_access_key='abc''def'", self.records())


if __name__ == "__main__":
    unittest.main()
