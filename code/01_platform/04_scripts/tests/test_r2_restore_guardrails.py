"""Guardrails of r2-restore.sh, with DuckDB replaced by a stub.

The stub (DUCKDB_BIN seam) captures the SQL it receives on stdin, so the tests
can check the two CRITICAL items without a database or a network:
  * P6-014 — the SQL (secrets included) arrives on stdin, from a 0600 file, and
    no secret appears in the process command line;
  * P6-165/P6-166/P6-498 — Iceberg delete files and zero rows abort the restore
    instead of silently writing a wrong or empty parquet, and a refused or
    failed run leaves an existing output file untouched.
"""

import os
import re
import shutil
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

# The override exists so the wave's red-before-green run can point the suite at
# a sandboxed copy of a previous revision (with fixture config next to it)
# instead of running an old revision against the real secrets file.
SCRIPT = Path(os.environ.get(
    "R2_RESTORE_SCRIPT", Path(__file__).resolve().parent.parent / "r2-restore.sh"))
AK_SENTINEL = "AK-SENTINEL-1234567890"
SK_SENTINEL = "SK-SENT'INEL-with$quote`and-backtick"

STUB = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import os, re, sys
    sql = sys.stdin.read()
    with open(os.environ["STUB_CAPTURE"], "w") as fh:
        fh.write(sql)
    with open(os.environ["STUB_ARGV"], "w") as fh:
        fh.write("\\n".join(sys.argv))
    # what a real duckdb process would expose through /proc/<pid>/cmdline
    with open("/proc/self/cmdline", "rb") as fh:
        cmdline = fh.read()
    with open(os.environ["STUB_CMDLINE"], "wb") as fh:
        fh.write(cmdline)
    # the SQL temp file's mode, observed from the process that reads it
    fd_path = os.readlink("/proc/self/fd/0")
    with open(os.environ["STUB_SQL_MODE"], "w") as fh:
        fh.write(oct(os.stat(fd_path).st_mode & 0o777))
    if os.environ.get("STUB_CREATE_TMP") == "1":
        m = re.search(r"TO '([^']*)' \\(FORMAT PARQUET\\)", sql)
        with open(m.group(1), "wb") as fh:
            fh.write(b"stub-parquet-bytes")
    if os.environ.get("STUB_SILENT") != "1":
        print("delete_files=" + os.environ.get("STUB_DELETE_FILES", "0"))
        print("restored_rows=" + os.environ.get("STUB_ROWS", "7"))
    """
)


class RestoreGuardrailsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="w7-restore-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.stub = self.tmp / "duckdb-stub"
        self.stub.write_text(STUB)
        self.stub.chmod(self.stub.stat().st_mode | stat.S_IXUSR)
        self.capture = self.tmp / "sql-captured"
        self.cmdline = self.tmp / "stub-cmdline"
        self.sql_mode = self.tmp / "sql-mode"
        self.env_file = self.tmp / "env"
        self.sec_file = self.tmp / "secrets"
        self.write_config()
        self.out = self.tmp / "day.parquet"

    def write_config(self, endpoint="https://account.r2.cloudflarestorage.com/",
                     bucket="test-bucket", ak=AK_SENTINEL, sk=SK_SENTINEL):
        self.env_file.write_text(f"R2_ENDPOINT={endpoint}\nR2_BUCKET={bucket}\n")
        self.sec_file.write_text(f"AWS_ACCESS_KEY_ID={ak}\nAWS_SECRET_ACCESS_KEY={sk}\n")

    def run_restore(self, day="20260831", out=None, **stub_env):
        env = dict(os.environ)
        env.update({
            "DUCKDB_BIN": str(self.stub),
            "R2_ENV_FILE": str(self.env_file),
            "R2_SECRETS_FILE": str(self.sec_file),
            "STUB_CAPTURE": str(self.capture),
            "STUB_ARGV": str(self.tmp / "stub-argv"),
            "STUB_CMDLINE": str(self.cmdline),
            "STUB_SQL_MODE": str(self.sql_mode),
        })
        env.update({k: str(v) for k, v in stub_env.items()})
        argv = ["/usr/bin/env", "bash", str(SCRIPT), day]
        if out is not None:
            argv.append(str(out))
        return subprocess.run(argv, capture_output=True, text=True, timeout=120, env=env)

    def captured_sql(self):
        self.assertTrue(self.capture.exists(), "the stub never received the SQL")
        return self.capture.read_text()

    # ---- P6-014: secrets never on a command line, always escaped -----------

    def test_sql_arrives_on_stdin_from_a_0600_file_with_no_secret_in_argv(self):
        result = self.run_restore(out=self.out, STUB_CREATE_TMP=1, STUB_ROWS=7)
        self.assertEqual(result.returncode, 0, result.stderr)

        sql = self.captured_sql()
        # The single quote in the secret is doubled (SQL escaping) and the $
        # and backtick survived verbatim: bash never re-expanded them, because
        # the value went through a file, not a double-quoted -c string.
        escaped = SK_SENTINEL.replace("'", "''")
        self.assertIn(f"SET s3_secret_access_key='{escaped}';", sql)
        self.assertIn("$quote`and-backtick", sql)

        self.assertEqual(stat.S_IMODE(int(self.sql_mode.read_text(), 8)), 0o600)
        cmdline = self.cmdline.read_text(errors="replace")
        self.assertNotIn(SK_SENTINEL, cmdline)
        self.assertNotIn(AK_SENTINEL, cmdline)

    def test_endpoint_scheme_and_trailing_slash_are_stripped(self):
        self.write_config(endpoint="http://account.r2/")   # http, not https
        result = self.run_restore(out=self.out, STUB_CREATE_TMP=1)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("SET s3_endpoint='account.r2';", self.captured_sql())

    def test_incomplete_config_is_refused_before_duckdb_runs(self):
        self.sec_file.write_text("AWS_ACCESS_KEY_ID=only-half\n")
        result = self.run_restore(out=self.out)
        self.assertEqual(result.returncode, 2)
        self.assertIn("incomplete R2 config", result.stderr)
        self.assertFalse(self.capture.exists())

    # ---- P6-164/P6-166: input validation happens before anything else ------

    def test_bad_day_is_refused_before_duckdb_runs(self):
        for day in ("2026-08-31", "2026083", "20261332", "20260831'; DROP TABLE x; --"):
            with self.subTest(day=day):
                result = self.run_restore(day=day, out=self.out)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(self.capture.exists(), "duckdb ran on a bad day")

    def test_bad_output_path_is_refused_before_duckdb_runs(self):
        for out in ("x.parquet'; DROP TABLE x; --", "../escape.parquet",
                    "sub/../../escape.parquet", "x;y.parquet"):
            with self.subTest(out=out):
                result = self.run_restore(out=self.tmp / out)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(self.capture.exists(), "duckdb ran on a bad OUT")

    # ---- P6-165/P6-498: never write a wrong or empty parquet ---------------

    def test_delete_files_abort_the_restore(self):
        result = self.run_restore(out=self.out, STUB_CREATE_TMP=1,
                                  STUB_DELETE_FILES=2, STUB_ROWS=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("delete file", result.stderr)
        self.assertFalse(self.out.exists())
        self.assertEqual(list(self.tmp.glob("day.parquet.tmp.*")), [])

    def test_zero_rows_do_not_clobber_an_existing_restore(self):
        self.out.write_bytes(b"previous-good-restore")
        result = self.run_restore(out=self.out, STUB_CREATE_TMP=1, STUB_ROWS=0)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("zero rows", result.stderr)
        self.assertEqual(self.out.read_bytes(), b"previous-good-restore")
        self.assertEqual(list(self.tmp.glob("day.parquet.tmp.*")), [])

    def test_a_successful_restore_is_renamed_into_place(self):
        result = self.run_restore(out=self.out, STUB_CREATE_TMP=1, STUB_ROWS=11)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.out.read_bytes(), b"stub-parquet-bytes")
        self.assertIn("11 rows", result.stdout)
        self.assertEqual(list(self.tmp.glob("day.parquet.tmp.*")), [])
        self.assertNotIn("union_by_name", self.captured_sql())
        self.assertIn("glob('s3://test-bucket/lake/default/raw_table_1/data/"
                      "event_day=20260831/**/*delete*.parquet')", self.captured_sql())

    def test_stub_that_reports_no_counts_is_a_failure_not_a_silent_move(self):
        result = self.run_restore(out=self.out, STUB_SILENT=1, STUB_CREATE_TMP=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("did not report both counts", result.stderr)
        self.assertFalse(self.out.exists())


if __name__ == "__main__":
    unittest.main()
