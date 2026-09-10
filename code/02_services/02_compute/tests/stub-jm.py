"""Stub Flink JobManager for test-submit-jobs.sh (P2-080/081/083/085/201 + 7.2).

Serves adversarial-but-valid JSON (pretty-printed, nested data.filename,
camelCase jobId, spaced colons) plus scripted transient/permanent failures
and state sequences. Scenario via STUB_SCENARIO:
  happy | flaky-polls | dead-upload | dead-run
  | cancel-then-running | job-fails
"""
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

SCENARIO = os.environ.get("STUB_SCENARIO", "happy")
# Per-path transient-failure budgets (requests failing before success).
BUDGETS = {
    "happy": {},
    "flaky-polls": {"/jobs/abc123": 2, "/jobs/abc123/checkpoints": 1},
}
HITS: dict = {}          # failure-budget counter
STATE_HITS: dict = {}    # state-sequence counter (cancel-then-running)


class H(BaseHTTPRequestHandler):
    server_version = "StubJM/1"

    def _send(self, code, obj=None):
        body = json.dumps(obj, indent=2).encode() if obj is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _should_fail(self, path):
        n = HITS.get(path, 0)
        HITS[path] = n + 1
        return n < BUDGETS.get(SCENARIO, {}).get(path, 0)

    def _state_for(self, path):
        if SCENARIO == "cancel-then-running":
            n = STATE_HITS.get(path, 0)
            STATE_HITS[path] = n + 1
            return "CANCELING" if n < 2 else "RUNNING"
        if SCENARIO == "job-fails":
            return "FAILED"
        return "RUNNING"

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/v1/config":
            return self._send(200, {})
        if path == "/jobs/overview":
            return self._send(200, {"jobs": []})
        if path == "/jobs/abc123":
            if self._should_fail(path):
                return self._send(500, {"errors": ["flaky"]})
            # spaced colons + extra fields + scenario-driven state
            return self._send(200, {"extra": 1, "state": self._state_for(path), "jid": "abc123"})
        if path == "/jobs/abc123/checkpoints":
            if self._should_fail(path):
                return self._send(500, {"errors": ["flaky"]})
            return self._send(200, {"counts": {"restored": 0, "completed": 1}})
        return self._send(404, {})

    def do_POST(self):
        ln = int(self.headers.get("Content-Length", 0))
        self.rfile.read(ln)  # drain (upload multipart / run body)
        if self.path == "/jars/upload":
            if SCENARIO == "dead-upload":
                return self._send(500, {"errors": ["dead"]})
            # nested data.filename, pretty-printed: jq must handle nesting
            return self._send(200, {"status": "success", "data": {"filename": "/tmp/flink-web-upload/abc.jar"}})
        if self.path.endswith("/run"):
            if SCENARIO == "dead-run":
                return self._send(500, {"errors": ["dead"]})
            return self._send(200, {"jobId": "abc123"})
        return self._send(404, {})

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("STUB_PORT", "8081"))
    HTTPServer(("127.0.0.1", port), H).serve_forever()
