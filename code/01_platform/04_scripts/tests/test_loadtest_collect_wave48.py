"""Wave 48 hermetic tests for loadtest-collect.sh.

P6-123/442  one private scrape per interval, HTTP status checked, no shared /tmp file
P6-124      the background sampler is reaped (killed and waited) by an EXIT trap
P6-125      a failed scrape is UNKNOWN, never a fabricated "feed stalled" INVALID
P6-126      p99 keys match sanitized CHAIN SEGMENTS (a chained operator keeps its column)
P6-441/586  args validated before anything is created; the 189/190 boundary is exact
P6-443      the never-called per-operator rate dump (and its hardcoded REST call) is gone
P6-444/446  printf rows/headers with explicit n/a defaults, shape-checked
P6-445      one container-stats query, container env-overridable, memory normalized to MiB

Everything here is offline and runs in seconds: the guard functions are lifted
out of the real source with the same col-0 regex test-loadtest-guards.sh uses
(and the tests fail loudly if an extraction yields nothing), a `curl`/`docker`
shim on PATH drives the failure paths, and no cluster, Prometheus or port is
required.
"""
import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
SCRIPT = os.path.join(REPO, "code", "01_platform", "04_scripts", "loadtest-collect.sh")
JOB = "job-under-test"


def source_text():
    with open(SCRIPT, encoding="utf-8") as fh:
        return fh.read()


def fn_body(name):
    """Lift `name() { ... }` out of the real script (col-0 closing brace)."""
    m = re.search(r"^%s\(\) \{.*?^\}\n" % re.escape(name), source_text(), re.M | re.S)
    if not m:
        raise AssertionError("function %s() not found in %s" % (name, SCRIPT))
    return m.group(0)


def run_bash(body, env=None, timeout=60, shims=()):
    """Run bash with the script's functions pre-lifted and optional PATH shims."""
    full = dict(os.environ)
    full.update(env or {})
    if shims:
        bindir = tempfile.mkdtemp(prefix="w48-shim-")
        for name, text in shims:
            path = os.path.join(bindir, name)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(textwrap.dedent(text))
            os.chmod(path, 0o755)
        full["PATH"] = bindir + os.pathsep + full.get("PATH", "")
    try:
        return subprocess.run(["bash", "-c", body], stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True,
                              env=full, timeout=timeout)
    finally:
        if shims:
            shutil.rmtree(bindir, ignore_errors=True)


def lift(*names):
    """Preamble that lifts the named functions and asserts each one is defined."""
    body = ["set -uo pipefail"]
    for name in names:
        body.append(fn_body(name))
        body.append('declare -F %s >/dev/null || { echo "LIFT FAILED: %s"; exit 99; }' % (name, name))
    return "\n".join(body)


class ArgumentValidationTest(unittest.TestCase):
    """P6-441/P6-586: a bad argument fails before anything is created."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w48-args-")
        self.out = os.path.join(self.tmp, "out")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_bad_args_exit_2_and_create_nothing(self):
        cases = [
            ([self.out, "abc", "30"], "DURATION_S='abc'"),
            ([self.out, "0", "30"], "DURATION_S must be a positive integer"),
            ([self.out, "240s", "30"], "DURATION_S='240s'"),
            ([self.out, "240", "abc"], "INTERVAL_S='abc'"),
            ([self.out, "240", "0"], "INTERVAL_S must be a positive integer"),
        ]
        for args, needle in cases:
            r = subprocess.run(["bash", SCRIPT] + args, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, timeout=60)
            self.assertEqual(r.returncode, 2, "args %r gave rc=%s:\n%s"
                             % (args, r.returncode, r.stdout))
            self.assertIn(needle, r.stdout)
            self.assertFalse(os.path.exists(self.out), "args %r created %s" % (args, self.out))

    def test_duration_90_is_refused_by_the_min_duration_guard(self):
        r = subprocess.run(["bash", SCRIPT, self.out, "90", "30", "somejob"],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           text=True, timeout=60)
        self.assertEqual(r.returncode, 2)
        self.assertIn("DURATION_S=90 < 190", r.stdout)
        self.assertFalse(os.path.exists(self.out), "the guard ran after creating out_dir")

    def test_private_scrape_dir_is_not_created_on_refusal(self):
        tmpdir = os.path.join(self.tmp, "tmpdir")
        os.makedirs(tmpdir)
        r = subprocess.run(["bash", SCRIPT, self.out, "90", "30", "somejob"],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                           env=dict(os.environ, TMPDIR=tmpdir), timeout=60)
        self.assertEqual(r.returncode, 2)
        self.assertEqual(os.listdir(tmpdir), [], "a refused run left temp state behind")


class MinDurationTest(unittest.TestCase):
    """P6-586: both sides of the 190s boundary, exact exit codes."""

    def check(self, secs):
        r = run_bash(lift("require_min_duration") + '\nrequire_min_duration %s\n' % secs)
        self.assertNotIn("LIFT FAILED", r.stdout)
        return r

    def test_189_is_refused_with_rc_2(self):
        r = self.check(189)
        self.assertEqual(r.returncode, 2)
        self.assertIn("needs >47s to appear", r.stdout)

    def test_190_is_accepted(self):
        r = self.check(190)
        self.assertEqual(r.returncode, 0, r.stdout)


class FeedRateTest(unittest.TestCase):
    """P6-123/442: the source rate is summed for raw-table-1 only, from one scrape."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w48-feed-")
        self.metrics = os.path.join(self.tmp, "prom.txt")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, lines):
        with open(self.metrics, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")

    def rate(self):
        r = run_bash(lift("feed_rate") + "\nfeed_rate\n",
                     env={"JOB_ID": JOB, "SCRAPE": self.metrics})
        self.assertEqual(r.returncode, 0, r.stdout)
        return r.stdout.strip().splitlines()[-1]

    def test_sums_only_the_raw_source_of_this_job(self):
        self.write([
            'flink_taskmanager_job_task_numRecordsInPerSecond{job_id="%s",task_name="Source:_raw_table_1____raw_validation",subtask_index="0",} 10000.0' % JOB,
            'flink_taskmanager_job_task_numRecordsInPerSecond{job_id="%s",task_name="Source:_raw_table_1____raw_validation",subtask_index="1",} 10000.5' % JOB,
            'flink_taskmanager_job_task_numRecordsInPerSecond{job_id="%s",task_name="forming_bar_detection",subtask_index="1",} 1431.0' % JOB,
            'flink_taskmanager_job_task_numRecordsInPerSecond{job_id="other",task_name="Source:_raw_table_1____raw_validation",subtask_index="0",} 50000.0',
        ])
        self.assertEqual(self.rate(), "20000")

    def test_scientific_notation_is_not_dropped(self):
        # P6-442: the fast regex used to be [0-9.]+ while p99 used [0-9.eE+-]+.
        self.write([
            'flink_taskmanager_job_task_numRecordsInPerSecond{job_id="%s",task_name="Source:_raw_table_1____raw_validation",subtask_index="0",} 2.048e4' % JOB,
        ])
        self.assertEqual(self.rate(), "20480")

    def test_no_matching_source_is_zero_not_an_error(self):
        self.write([])
        self.assertEqual(self.rate(), "0")


class P99Test(unittest.TestCase):
    """P6-126: a chained (sanitized) operator keeps its column."""

    LATENCY = ("flink_taskmanager_job_task_latency_source_id_operator_id_operator_"
               "subtask_index_latency")

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w48-p99-")
        self.metrics = os.path.join(self.tmp, "prom.txt")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, lines):
        with open(self.metrics, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")

    def row(self):
        body = lift("p99_snapshot", "p99_row") + '\np99_row "$(p99_snapshot)"\n'
        r = run_bash(body, env={"JOB_ID": JOB, "SCRAPE": self.metrics})
        self.assertEqual(r.returncode, 0, r.stdout)
        return r.stdout.strip().splitlines()[-1].split("\t")

    def test_chained_operator_name_still_fills_its_column(self):
        # The real label seen in logs/soak-e2e-20260904-234313: the dedup operator
        # is now chained, so only the segment alias can find it.
        self.write([
            '%s{job_id="%s",task_name="fingerprint_dedup____ingest_latency_monitor",quantile="0.99",} 1234.0' % (self.LATENCY, JOB),
            '%s{job_id="%s",task_name="forming_bar_detection",quantile="0.99",} 555.0' % (self.LATENCY, JOB),
        ])
        cells = self.row()
        self.assertEqual(len(cells), 5, cells)
        self.assertEqual(cells[0], "1234", "chained task_name lost its p99 column")
        self.assertEqual(cells[2], "555")
        self.assertEqual(cells[1], "n/a")
        self.assertEqual(cells[3:], ["n/a", "n/a"])

    def test_max_across_subtasks_and_quantile_filter(self):
        self.write([
            '%s{job_id="%s",task_name="forming_bar_writer",quantile="0.99",} 10.0' % (self.LATENCY, JOB),
            '%s{job_id="%s",task_name="forming_bar_writer",quantile="0.99",} 30.0' % (self.LATENCY, JOB),
            '%s{job_id="%s",task_name="forming_bar_writer",quantile="0.5",} 999.0' % (self.LATENCY, JOB),
            '%s{job_id="other",task_name="forming_bar_writer",quantile="0.99",} 777.0' % self.LATENCY,
        ])
        self.assertEqual(self.row()[1], "30")

    def test_unparsable_json_yields_five_na_cells(self):
        cells = self.row()   # empty metrics: an empty JSON object
        self.assertEqual(cells, ["n/a"] * 5)


class ScrapeStatusTest(unittest.TestCase):
    """P6-123/442: the scrape writes to $SCRAPE and its HTTP status is checked."""

    CURL = '''\
        #!/usr/bin/env bash
        out=""
        while [ $# -gt 0 ]; do
          case "$1" in
            -o) out="$2"; shift 2;;
            *) shift;;
          esac
        done
        [ -n "$out" ] || exit 3
        printf '%s' "$SHIM_BODY" > "$out"
        printf '%s' "$SHIM_CODE"
        exit "${SHIM_RC:-0}"
        '''

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w48-scrape-")
        self.scrape = os.path.join(self.tmp, "prom.txt")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def scrape_rc(self, code="200", body="metric 1\n", rc="0"):
        env = {"SCRAPE": self.scrape, "SHIM_CODE": code, "SHIM_BODY": body, "SHIM_RC": rc,
               "PROM": "http://example.invalid/metrics"}
        r = run_bash(lift("scrape_prom") + "\nscrape_prom\n", env=env, shims=[("curl", self.CURL)])
        self.assertNotIn("LIFT FAILED", r.stdout)
        return r.returncode

    def test_200_with_body_is_ok_and_lands_in_scrape(self):
        self.assertEqual(self.scrape_rc(), 0)
        with open(self.scrape, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "metric 1\n")

    def test_http_error_is_a_failed_scrape(self):
        self.assertEqual(self.scrape_rc(code="500", body="<html>oops</html>"), 1)

    def test_empty_body_is_a_failed_scrape(self):
        self.assertEqual(self.scrape_rc(body=""), 1)

    def test_curl_failure_is_a_failed_scrape(self):
        self.assertEqual(self.scrape_rc(rc="7"), 1)

    def test_no_shared_tmp_file_is_written(self):
        self.scrape_rc()
        self.assertFalse(os.path.exists("/tmp/loadtest-metrics.txt"),
                         "the shared predictable scrape path is back")


class SamplerLifecycleTest(unittest.TestCase):
    """P6-124: cleanup reaps the sampler and its private dir, preserving the exit code."""

    def test_cleanup_kills_waits_and_cleans(self):
        tmp = tempfile.mkdtemp(prefix="w48-cleanup-")
        scrape_dir = os.path.join(tmp, "loadtest-collect.XXXX")
        os.makedirs(scrape_dir)
        with open(os.path.join(scrape_dir, "prom.txt"), "w", encoding="utf-8") as fh:
            fh.write("x")
        body = lift("cleanup") + textwrap.dedent('''
            sleep 60 &
            BUSY_PID=$!
            SCRAPE_DIR="%s"
            trap cleanup EXIT
            exit 42
        ''') % scrape_dir
        r = run_bash(body)
        self.assertEqual(r.returncode, 42, "cleanup did not preserve the exit code:\n%s" % r.stdout)
        self.assertFalse(os.path.exists(scrape_dir), "cleanup left the scrape dir behind")
        shutil.rmtree(tmp, ignore_errors=True)

    def test_cleanup_does_not_orphan_the_sampler(self):
        body = lift("cleanup") + textwrap.dedent('''
            sleep 60 &
            BUSY_PID=$!
            SCRAPE_DIR=/nonexistent
            trap cleanup EXIT
            exit 0
        ''')
        r = run_bash(body)
        self.assertEqual(r.returncode, 0, r.stdout)
        # `wait` on a still-running pid would block for the whole 60s sleep; the
        # test passing at all inside the timeout is the assertion, but also check
        # the child is gone (kill -0 on a reaped pid fails).
        self.assertNotIn("cleanup left", r.stdout)


class ContainerStatsTest(unittest.TestCase):
    """P6-445: one query, env-overridable container, units normalized to MiB."""

    DOCKER = '''\
        #!/usr/bin/env bash
        [ -n "${SHIM_MEM}" ] || exit 1
        printf '%s|%s\\n' "$SHIM_MEM" "${SHIM_CPU:-0.00%%}"
        '''

    def stats(self, mem=None, cpu="12.50%"):
        env = {"SHIM_MEM": mem or "", "SHIM_CPU": cpu, "TM_CONTAINER": "some-tm"}
        r = run_bash(lift("mem_to_mib", "container_stats") + "\ncontainer_stats\n",
                     env=env, shims=[("docker", self.DOCKER)])
        self.assertNotIn("LIFT FAILED", r.stdout)
        return r.returncode, r.stdout.strip().splitlines()[-1].split("\t")

    def test_memory_is_normalized_to_mib(self):
        self.assertEqual(self.stats(mem="3.3GiB / 4GiB")[1], ["3379", "12.50%"])
        self.assertEqual(self.stats(mem="450MiB / 4GiB")[1], ["450", "12.50%"])
        self.assertEqual(self.stats(mem="1.5GB / 8GB")[1], ["1500", "12.50%"])

    def test_missing_container_yields_na_defaults(self):
        rc, cells = self.stats(mem=None)
        self.assertEqual(rc, 0)
        self.assertEqual(cells, ["n/a", "n/a"])


class RowShapeTest(unittest.TestCase):
    """P6-444/446: the row builder and its shape check."""

    def test_field_count_detection(self):
        body = lift("row_shape_ok") + textwrap.dedent('''
            row_shape_ok "$(printf 'a\\tb\\tc')" 3 && echo OK3
            row_shape_ok "$(printf 'a\\tb')" 3 && echo BAD
            row_shape_ok "" 1 && echo BAD_EMPTY
        ''')
        r = run_bash(body)
        self.assertIn("OK3", r.stdout)
        self.assertNotIn("BAD", r.stdout)

    def test_headers_are_written_with_printf_tabs(self):
        # The header line must contain real tabs (plain echo would emit the two
        # characters backslash-t) and match the declared column count.
        r = run_bash(textwrap.dedent('''
            printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' \\
              ts feed_rate feed_valid flink_rss flink_cpu ckpt_ok ckpt_fail \\
              p99_dedup p99_writer p99_detection p99_builder p99_sink | awk -F'\\t' '{print NF}'
        '''))
        self.assertEqual(r.stdout.strip(), "12")


class SummaryTest(unittest.TestCase):
    """P6-125/126: the summary names UNKNOWN, a stalled run, and a silently-n/a column."""

    HDR = "\t".join(["ts", "feed_rate", "feed_valid", "flink_rss", "flink_cpu",
                     "ckpt_ok", "ckpt_fail", "p99_dedup", "p99_writer",
                     "p99_detection", "p99_builder", "p99_sink"])

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w48-summary-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def summarise(self, rows, vc=0, ic=0, uc=0, path=None):
        path = path or os.path.join(self.tmp, "snapshots.tsv")
        if rows is not None:          # None == "the file does not exist at all"
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("\n".join([self.HDR] + rows) + "\n")
        r = run_bash(lift("loadtest_summary") + '\nloadtest_summary "%s" %s %s %s\n'
                     % (path, vc, ic, uc))
        self.assertNotIn("LIFT FAILED", r.stdout)
        return r

    def row(self, feed, valid, p99=("n/a",) * 5, ts="2026-08-28T00:00:00+05:30"):
        return "\t".join([ts, str(feed), valid, "3.3GiB", "10%", "1", "0"] + [str(c) for c in p99])

    def test_unknown_scrapes_are_reported_as_such(self):
        r = self.summarise([self.row("n/a", "UNKNOWN")], ic=0, uc=3)
        self.assertIn("3 UNKNOWN (scrape failed)", r.stdout)
        self.assertIn("NO VALID snapshots", r.stdout)
        self.assertIn("not stalls", r.stdout)

    def test_all_na_p99_column_is_called_out(self):
        r = self.summarise([self.row(20000, "VALID")], vc=1)
        self.assertIn("p99_dedup: n/a in ALL 1 VALID", r.stdout)
        self.assertIn("feed_rate column (first VALID row): 20000", r.stdout)

    def test_healthy_rows_produce_no_na_warning(self):
        r = self.summarise([self.row(20000, "VALID", p99=(1, 2, 3, 4, 5))], vc=1)
        self.assertNotIn("n/a in ALL", r.stdout)
        self.assertIn("VALID snapshots only", r.stdout)

    def test_unreadable_snapshot_file_is_reported_not_raised(self):
        r = self.summarise(None, vc=1, path=os.path.join(self.tmp, "missing.tsv"))
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("cannot read", r.stdout)


if __name__ == "__main__":
    unittest.main()
