#!/usr/bin/env python3
"""Persistent alert consumer for OpenObserve (G6 single-webhook routing).

Replaces the dev-only stdout webhook-receiver (CHG-093 proof receiver):
deliveries are now DURABLE — appended to a JSONL file on a named volume,
queryable over HTTP, classified by severity and source class. This is the
"who gets told, how fast" answer for dev: the routing endpoint keeps a
complete record instead of docker-log ephemerality.

Runs in OpenObserve's network namespace (compose: network_mode:
service:openobserve) so the O2 alert destination URL
http://localhost:9999/noop is reachable and passes the loopback allowance
of the SSRF guard (ZO_SSRF_ALLOW_LOOPBACK=true, dev only). Production
replacement: point the O2 destination at a real pager (or keep this file
and front it with auth) — the record format is the contract.

Endpoints:
  POST *           O2 alert delivery (any path; /noop from the provisioned
                   destination). Body: {"alert": {"name": ...}} (v0.91.5
                   prebuilt_webhook template — name only). Malformed body
                   -> 400 (delivery failure is visible in O2), never a crash.
  GET  /alerts     recent records: ?limit=N (default 50, max 1000),
                   ?severity=crit|error|warn|info, ?class=ing|signal|infra|...
  GET  /stats      totals by severity/class + last delivery age
  GET  /healthz    liveness

Storage: ALERT_STORE (default /data/alerts/alerts.jsonl), one JSON object
per line: {"received_at", "name", "severity", "class", "body"}.
Stdlib only.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

STORE_PATH = os.environ.get("ALERT_STORE", "/data/alerts/alerts.jsonl")
PORT = int(os.environ.get("ALERT_CONSUMER_PORT", "9999"))
# Dev (netns-shared with O2): loopback only. Swarm (separate netns, O2
# reaches the consumer via service DNS): set ALERT_BIND=0.0.0.0.
BIND = os.environ.get("ALERT_BIND", "127.0.0.1")
# Tail-read cap: the /alerts endpoint reads at most this many bytes from the
# end of the file, so a large history cannot make the endpoint slow.
TAIL_BYTES = 512 * 1024

_write_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def classify(name: str) -> tuple[str, str]:
    """Return (severity, class) from an O2 alert name.

    Convention (docs/06_operations/02-ingestion-alerting.md + o2-provision.py):
    names are PREFIX-SEVERITY-description, e.g. SIGNAL-crit-checkpoint-failed,
    INFRA-warn-host-cpu-80, ING-crit-bridge-disconnected. pos-state-* alerts
    (position-state-alerts.json) carry no severity infix -> warn (they are
    investigation prompts, not pages).
    """
    m = re.match(r"^([A-Za-z]+)-([a-z]+)-", name or "")
    if m and m.group(2) in ("crit", "error", "warn"):
        severity = m.group(2)
    else:
        severity = "warn" if (name or "").lower().startswith("pos-state") else "info"
    prefix = (m.group(1) if m else (name or "").split("-", 1)[0]).upper()
    known = {"ING": "ing", "SIGNAL": "signal", "INFRA": "infra"}
    cls = known.get(
        prefix, "pos-state" if (name or "").lower().startswith("pos-state") else "other"
    )
    return severity, cls


def build_record(raw_body: str) -> dict:
    """Build the persisted record from an O2 delivery body."""
    body = json.loads(raw_body) if raw_body else {}
    alert = body.get("alert") if isinstance(body, dict) else None
    name = ""
    if isinstance(alert, dict):
        name = str(alert.get("name") or "")
    if not name:  # tolerate template variants: top-level AlertName / name
        if isinstance(body, dict):
            name = str(body.get("AlertName") or body.get("name") or "")
    severity, cls = classify(name)
    return {
        "received_at": _now_iso(),
        "name": name,
        "severity": severity,
        "class": cls,
        "body": body,
    }


def append_record(record: dict) -> None:
    os.makedirs(os.path.dirname(STORE_PATH) or ".", exist_ok=True)
    line = json.dumps(record, separators=(",", ":")) + "\n"
    with _write_lock:
        with open(STORE_PATH, "a", encoding="utf-8") as f:
            f.write(line)


def read_records(limit: int, severity: str | None = None, cls: str | None = None) -> list[dict]:
    """Read the most recent matching records, newest last."""
    try:
        with open(STORE_PATH, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - TAIL_BYTES))
            data = f.read().decode("utf-8", "replace")
    except FileNotFoundError:
        return []
    # Drop the first (possibly truncated) line.
    lines = [ln for ln in data.split("\n") if ln.strip()]
    if size > TAIL_BYTES and lines:
        lines = lines[1:]
    records = []
    for ln in lines:
        try:
            records.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    if severity:
        records = [r for r in records if r.get("severity") == severity]
    if cls:
        records = [r for r in records if r.get("class") == cls]
    return records[-limit:]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload: dict) -> None:
        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode("utf-8", "replace") if length else ""
        try:
            record = build_record(raw)
        except (json.JSONDecodeError, ValueError):
            # Delivery failure is VISIBLE in O2 (retry/dedup) — do not
            # silently accept malformed payloads.
            print(f"POST {self.path} malformed body: {raw[:500]!r}", flush=True)
            self._send(400, {"ok": False, "error": "malformed JSON body"})
            return
        append_record(record)
        print(
            f"POST {self.path} {record['severity']} {record['name']}",
            flush=True,
        )
        self._send(200, {"ok": True})

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/healthz":
            self._send(200, {"ok": True})
            return
        if url.path == "/stats":
            records = read_records(limit=10_000)
            by_sev: dict[str, int] = {}
            by_cls: dict[str, int] = {}
            for r in records:
                by_sev[r["severity"]] = by_sev.get(r["severity"], 0) + 1
                by_cls[r["class"]] = by_cls.get(r["class"], 0) + 1
            last = records[-1]["received_at"] if records else None
            payload = {
                "total": len(records),
                "by_severity": by_sev,
                "by_class": by_cls,
                "last_delivery_at": last,
                "note": f"counts computed over the last {TAIL_BYTES // 1024} KB of history",
            }
            self._send(200, payload)
            return
        if url.path == "/alerts":
            q = parse_qs(url.query)
            limit = min(int(q.get("limit", ["50"])[0]), 1000)
            severity = q.get("severity", [None])[0]
            cls = q.get("class", [None])[0]
            self._send(200, {"alerts": read_records(limit, severity, cls)})
            return
        self._send(404, {"ok": False, "error": "use /alerts, /stats or /healthz"})

    def log_message(self, fmt: str, *args) -> None:  # silence per-request noise
        pass


if __name__ == "__main__":
    print(f"alert-consumer: store={STORE_PATH} bind={BIND} port={PORT}", flush=True)
    ThreadingHTTPServer((BIND, PORT), Handler).serve_forever()
