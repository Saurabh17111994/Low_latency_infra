#!/usr/bin/env python3
"""`curl` stub for the wave-38 stage-capture teardown tests.

Serves the three endpoints stage-capture.sh's preflight touches (`/jobs/<jid>`
for both the state check and the vertex map, and `/metrics` for the TM prom
liveness gate) and then, in W38_CURL_MODE=fail-after-preflight, reports the job
as FAILED on every call after the preflight — which is what drives the real
script into its "job left RUNNING" fail-fast exit (the P6-562 path).

Call counts are kept per-URL in W38_CURL_STATE so the two preflight reads of
`/jobs/<jid>` (state, then vertices) can be told apart from the in-loop one.
"""
import json
import os
import pathlib
import sys

url = next((a for a in sys.argv[1:] if a.startswith("http")), "")
mode = os.environ.get("W38_CURL_MODE", "stay-running")
counts_path = pathlib.Path(os.environ["W38_CURL_STATE"])
counts = json.loads(counts_path.read_text()) if counts_path.exists() else {}
counts[url] = counts.get(url, 0) + 1
counts_path.write_text(json.dumps(counts))

if url.endswith("/metrics"):
    # Non-empty scrape: the preflight only checks it answers, sample_tick greps it.
    print("# TYPE busyTimeMsPerSecond gauge")
    print("busyTimeMsPerSecond 1.0")
    sys.exit(0)

if "/jobs/" in url and "overview" not in url:
    if counts[url] <= 2:
        # 1st = preflight job_state, 2nd = vertex map: both must succeed.
        print(json.dumps({"state": "RUNNING",
                          "vertices": [{"id": "v1", "name": "Source: raw-table-1"}]}))
    elif mode == "fail-after-preflight":
        print(json.dumps({"state": "FAILED"}))
    else:
        print(json.dumps({"state": "RUNNING",
                          "vertices": [{"id": "v1", "name": "Source: raw-table-1"}]}))
    sys.exit(0)

print(json.dumps({"jobs": [{"jid": "test-jid", "state": "RUNNING",
                            "name": "signal-job"}]}))
