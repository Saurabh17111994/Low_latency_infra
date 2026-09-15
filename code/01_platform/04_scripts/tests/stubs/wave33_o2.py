#!/usr/bin/env python3
"""Stub OpenObserve metrics API for the wave-33 bench-throughput tests.

The bench talks to O2 over HTTP (curl with a -K config file), so serving real
HTTP here means the credential file, the request payload and the JSON
extraction all go through the script's own code path instead of a replaced
shell function.

Values are driven by a per-metric call counter, because the bench reads each
counter twice per window — once as the opening sample (`CNT_A`) and once as the
closing one (`CNT_B`). The odd call is the opening read, the even call the
closing one, so a window always shows a delta of `rate * window_s` and the
counter stays monotonic across windows.

Scenario control (env):
  W33_O2_MODE       "ok" | "no-baseline" | "no-latency"
  W33_O2_RATE       rows attributed to each simulated window
  W33_O2_WINDOW_S   the simulated window width those rows are spread over
  W33_O2_STATE      file holding the call counts (shared across processes)
"""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

MODE = os.environ.get("W33_O2_MODE", "ok")
RATE = int(os.environ.get("W33_O2_RATE", "20480"))
WINDOW_S = float(os.environ.get("W33_O2_WINDOW_S", "60"))


# The handler can run either in-process (the tests call serve_forever from the
# test process, where os.environ is the TEST's — not the sandbox's) or as a
# standalone script. STATE is the in-process override; without it every server
# in a run would share one counter file and the deltas would be nonsense.
STATE = None


def _state_path() -> str:
    return STATE or os.environ.get("W33_O2_STATE", "/tmp/w33-o2-state.json")


def bump(key: str) -> int:
    """Increment and return the call count for this metric."""
    path = _state_path()
    try:
        with open(path) as f:
            d = json.load(f)
    except (OSError, ValueError):
        d = {}
    d[key] = d.get(key, 0) + 1
    with open(path, "w") as f:
        json.dump(d, f)
    return d[key]


def value_for(sql: str):
    """The metric value for this SQL, or None for "no hits"."""
    if "append_latency_ms_count" in sql:
        if MODE == "no-baseline":
            return None
        n = bump("count")
        # (n-1)//2, not n//2. The call sequence is 1=baseline, 2=CNT_A (window
        # opens), 3=CNT_B (window closes), 4=CNT_A, 5=CNT_B ... so the value
        # must step between the OPEN and the CLOSE call, not on every other
        # call: k = 0,0,1,1,2,2 gives every window a delta of RATE*WINDOW_S.
        return 1000 + ((n - 1) // 2) * RATE * WINDOW_S
    if "decode_errors" in sql:
        return 0
    if "append_latency_ms_sum" in sql:
        # No-latency: the histogram is missing entirely, so neither the gauges
        # nor the mean fallback can produce a number (the -1 shape).
        if MODE == "no-latency":
            return None
        n = bump("sum")
        return 5 * (1000 + ((n - 1) // 2) * RATE * WINDOW_S)
    if "_p50" in sql or "_p99" in sql:
        return None if MODE == "no-latency" else 5
    return 1


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        sql = (body.get("query") or {}).get("sql", "")
        v = value_for(sql)
        payload = ({"hits": []} if v is None else
                   {"hits": [{"_timestamp": 1789000000000000, "value": v}]})
        out = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1]) if len(sys.argv) > 1 else 5080),
               Handler).serve_forever()
