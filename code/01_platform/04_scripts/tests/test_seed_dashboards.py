"""Validate the OpenObserve dashboard corpus + seed script contracts."""

import base64
import http.server
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
DASH_DIR = ROOT / "code/01_platform/01_docker/openobserve/dashboards"
SEED = ROOT / "code/01_platform/04_scripts/seed_dashboards.py"

REQUIRED_MANIFEST_FIELDS = [
    "title", "file", "folder", "dashboard_version", "query_version",
    "measurement_boundary", "workload", "duration", "utc_clock",
    "sample_count", "failures_or_restarts_included", "streams", "software_versions",
]
PANEL_TYPES = {"bar", "line", "table", "area", "gauge"}


def _load_all():
    manifest = json.loads((DASH_DIR / "manifest.json").read_text())
    docs = {}
    for rec in manifest["dashboards"]:
        docs[rec["title"]] = (rec, json.loads((DASH_DIR / rec["file"]).read_text()))
    return manifest, docs


def test_manifest_records_carry_evidence_fields():
    manifest, _ = _load_all()
    assert manifest["schema_version"] == 1
    for rec in manifest["dashboards"]:
        for field in REQUIRED_MANIFEST_FIELDS:
            assert rec.get(field), f"{rec['title']} missing manifest field {field}"
        assert set(rec["streams"]) <= {"metrics", "logs", "traces"}


def test_dashboard_files_are_valid_v8_corpus():
    manifest, docs = _load_all()
    assert len(docs) >= 3, "at least the core dashboards must exist"
    for title, (rec, doc) in docs.items():
        assert doc["version"] == 8, f"{title}: v8 required"
        assert doc["title"] == rec["title"]
        assert doc.get("folder_id") == rec["folder"]
        assert doc.get("tabs"), f"{title}: at least one tab"
        seen = set()
        for tab in doc["tabs"]:
            assert tab.get("tabId") and tab.get("name")
            for panel in tab.get("panels", []):
                assert panel.get("id") not in seen, f"{title}: duplicate panel id"
                seen.add(panel.get("id"))
                assert panel["type"] in PANEL_TYPES, f"{title}: bad type {panel['type']}"
                assert panel["queryType"] == "sql"
                assert panel.get("layout"), f"{title}: panel {panel['id']} missing layout"
                lay = panel["layout"]
                assert lay["w"] > 0 and lay["x"] + lay["w"] <= 192, f"{title}: panel off 192-grid"
                assert panel.get("queries"), f"{title}: panel {panel['id']} has no queries"
                for q in panel["queries"]:
                    assert q.get("fields", {}).get("stream"), f"{title}: query stream required"
                    assert q.get("query"), f"{title}: query sql required"


def test_no_secrets_in_corpus():
    for f in ["manifest.json", "safe-to-trade.json", "order-execution.json",
              "data-ingestion.json", "storage-eod.json"]:
        doc = json.loads((DASH_DIR / f).read_text())

        def walk(node):
            if isinstance(node, dict):
                for k, v in node.items():
                    if any(s in k.lower() for s in ("password", "secret", "token")):
                        raise AssertionError(f"{f}: suspicious key {k}")
                    walk(v)
            elif isinstance(node, list):
                for it in node:
                    walk(it)

        walk(doc)


def test_seed_refuses_without_password():
    env = {k: v for k, v in os.environ.items() if k != "O2_PASSWORD"}
    proc = subprocess.run([sys.executable, str(SEED), "--dry-run"], env=env,
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 2, "blank O2_PASSWORD must exit 2"
    assert "O2_PASSWORD" in proc.stderr


class _DashboardsStub:
    """A stand-in for OpenObserve's read-only `GET /api/{org}/dashboards`.

    P6-539 made `--dry-run` fetch the list before printing, because a plan that
    says "create" for a dashboard that already exists is a wrong plan. That
    makes the dry-run path need a reachable endpoint; this answers the one GET
    it makes, so the test needs neither the stack nor the real admin password.
    """

    def __init__(self, dashboards=(), *, expected_auth=None):
        self.dashboards = list(dashboards)
        self.expected_auth = expected_auth
        self.seen_path = None
        self.seen_auth = None
        stub = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 — BaseHTTPRequestHandler's spelling
                stub.seen_path = self.path
                stub.seen_auth = self.headers.get("Authorization")
                if stub.expected_auth is not None and stub.seen_auth != stub.expected_auth:
                    # A real 401, so a wrong-credential run exercises the same
                    # branch the live server does.
                    self.send_error(401, "Unauthorized Access")
                    return
                body = json.dumps({"dashboards": [
                    {"title": t, "dashboard_id": t.lower().replace(" ", "-")}
                    for t in stub.dashboards]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass  # keep pytest output clean

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        return False


def _seed_env(stub_url: str, password: str) -> dict:
    # no_proxy: a host-level HTTP proxy must not intercept the loopback stub.
    return {**os.environ, "O2_PASSWORD": password, "O2_API_URL": stub_url,
            "no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"}


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def test_seed_dry_run_plans_known_titles():
    """--dry-run plans against a stubbed list endpoint, fully offline.

    The stub keeps the "the plan reflects what is already deployed" invariant
    that P6-539 introduced (a dashboard reported present must print `keep`, not
    `create`) while restoring the offline property the test had before it. It
    also asserts the Basic credential actually travels — which a fake password
    against a dead endpoint never exercised.
    """
    with _DashboardsStub(["Safe to Trade"],
                         expected_auth=_basic("admin@example.com", "Dry-Run!2026")) as stub:
        proc = subprocess.run([sys.executable, str(SEED), "--dry-run"],
                              env=_seed_env(stub.url, "Dry-Run!2026"),
                              capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "[keep  ] Safe to Trade" in proc.stdout, (
        "the stub reported this dashboard as already present, so the plan must "
        f"say keep — got:\n{proc.stdout}"
    )
    assert "RESULT:" in proc.stdout
    assert stub.seen_path == "/api/default/dashboards"
    assert stub.seen_auth == _basic("admin@example.com", "Dry-Run!2026")


def test_seed_reports_401_as_exit_1():
    """A refused credential is exit 1 with the status named, never a plan.

    This is the shape that broke the previous test on 2026-09-15: its fake
    password reached a live server for the first time and came back 401.
    """
    with _DashboardsStub(expected_auth=_basic("admin@example.com", "the-real-one")) as stub:
        proc = subprocess.run([sys.executable, str(SEED), "--dry-run"],
                              env=_seed_env(stub.url, "not-the-real-one"),
                              capture_output=True, text=True, timeout=60)
    assert proc.returncode == 1, f"401 must exit 1, got {proc.returncode}"
    assert "401" in proc.stderr, proc.stderr
    assert "RESULT:" not in proc.stdout, "a refused list must not print a plan"
