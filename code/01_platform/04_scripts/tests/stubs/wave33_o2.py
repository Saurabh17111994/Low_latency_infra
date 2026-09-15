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
  W33_O2_MODE       "ok" | "no-baseline" | "no-latency" | "stale-once"
                    | "frozen" | "decode-backwards"
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


def clock() -> int:
    """How many append-counter reads the bench has made — the shared clock.

    Real O2 serves one "newest sample" per query, and the emitter ships
    append_latency_ms_count and decode_errors in ONE OTLP payload on one 10s
    tick. So every read is a draw from a single timeline: counting the append
    reads is enough to know which process either counter is answering from,
    and it keeps the two counters coupled the way the real payload couples
    them.
    """
    path = _state_path()
    try:
        with open(path) as f:
            return int(json.load(f).get("count", 0))
    except (OSError, ValueError):
        return 0


def value_for(sql: str):
    """The metric value for this SQL, or None for "no hits"."""
    if "append_latency_ms_count" in sql:
        if MODE == "no-baseline":
            return None
        n = bump("count")
        if MODE == "frozen":
            # A process whose emitter has stopped: O2 keeps serving the last
            # point, so the value is present but never advances.
            return 1000
        if MODE == "stale-once":
            # The 2026-09-15 incident, whose O2 series is quoted in the test:
            # the DEAD process's tail (0 appends) stays the newest sample for
            # the first two reads, exactly as it did live — long enough to be
            # the baseline AND window 1's opening read. The freshness poll must
            # reject both zeros and wait for a real advance.
            #
            # Stepping on every read (not every other) keeps a window's delta
            # at RATE*WINDOW_S whatever parity the variable-length poll leaves.
            if n <= 2:
                return 0
            return 1000 + (n - 2) * RATE * WINDOW_S
        # (n-1)//2, not n//2. The call sequence is 1=baseline, 2=CNT_A (window
        # opens), 3=CNT_B (window closes), 4=CNT_A, 5=CNT_B ... so the value
        # must step between the OPEN and the CLOSE call, not on every other
        # call: k = 0,0,1,1,2,2 gives every window a delta of RATE*WINDOW_S.
        # The freshness poll consumes an odd number of reads in this mode (3 —
        # call 1 has no predecessor to compare against, call 2 has not stepped
        # yet, call 3 steps), so the open/close calls stay (even, odd) and the
        # step still lands between them.
        return 1000 + ((n - 1) // 2) * RATE * WINDOW_S
    if "decode_errors" in sql:
        if MODE == "decode-backwards":
            # A restart that lands BETWEEN a window's opening and closing read,
            # so the two samples straddle two processes. Guards the per-window
            # backwards check in isolation from the baseline poll.
            return 7 if bump("err") <= 2 else 3
        if MODE == "stale-once":
            # Coupled to the append clock, the way the real emitter's single
            # OTLP payload couples them: while the dead process's sample is
            # newest, decode_errors answers with its tail (7) too. The live
            # process counts 3, so a straddling window reads 7 -> 3.
            return 7 if clock() <= 2 else 3
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
