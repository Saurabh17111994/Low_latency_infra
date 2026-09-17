#!/usr/bin/env python3
"""`curl` stub for the wave-38 stage-capture teardown tests.

Serves the three endpoints stage-capture.sh's preflight touches (`/jobs/<jid>`
for both the state check and the vertex map, and `/metrics` for the TM prom
liveness gate) and then, in W38_CURL_MODE=fail-after-preflight, reports the job
as FAILED on every call after the preflight — which is what drives the real
script into its "job left RUNNING" fail-fast exit (the P6-562 path).

Call counts are kept per-URL in W38_CURL_STATE so the two preflight reads of
`/jobs/<jid>` (state, then vertices) can be told apart from the in-loop one.

Wave-38 commit-4 modes (W38_CURL_MODE):
  scrape-empty: /metrics answers HTTP 200 with a body matching nothing
    (P6-214: alive-but-empty warns, keeps the raw scrape, never dies).
  scrape-dead: /metrics answers once (preflight) then curl fails every tick
    (P6-215: the tick warns-and-continues instead of exiting the script).
Post-preflight job states (any mode) can be scripted per-poll with
W38_CURL_QUEUE="FAILED,RUNNING,..." — the Nth exact-URL /jobs/<jid> poll
after the 2 preflight reads serves the Nth entry (P6-216: "FAILED" alone =
one blip the debounce must survive; exhausted queue falls back to the mode
default). Queued responses keep the vertices shape the diagnostics need.
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

args = sys.argv[1:]
out_file = args[args.index("-o") + 1] if "-o" in args else None


def _emit(text):
    # Real curl -o writes the body to the file and stays silent; the P6-214
    # split relies on that (an empty tmp file must mean a dead endpoint).
    if out_file is not None:
        pathlib.Path(out_file).write_text(text)
    else:
        sys.stdout.write(text)


if url.endswith("/metrics"):
    max_alive = os.environ.get("W38_CURL_MAX_ALIVE")
    if max_alive is not None and counts[url] > int(max_alive):
        sys.exit(1)  # scrape dies mid-run (P6-565 freshness pin)
    if mode == "scrape-empty":
        _emit("# alive but no matching series\nup 1\n")
        sys.exit(0)
    if mode == "scrape-dead" and counts[url] > 1:
        # Preflight (1st call) must succeed; every in-loop tick fails.
        sys.exit(1)
    # Non-empty scrape: the preflight only checks it answers, sample_tick greps it.
    _emit("# TYPE busyTimeMsPerSecond gauge\nbusyTimeMsPerSecond 1.0\n")
    sys.exit(0)

if url.endswith("/checkpoints"):
    cp_mode = os.environ.get("W38_CURL_CP_MODE", "static")
    if cp_mode == "dead":
        sys.exit(1)
    if cp_mode == "grow":
        # Cumulative history with one new id per fetch, like a live job.
        hist = [{"id": i, "status": "COMPLETED"} for i in range(1, counts[url] + 1)]
    else:
        hist = [{"id": 1, "status": "COMPLETED"}]
    _emit(json.dumps({"history": hist}) + "\n")
    sys.exit(0)

if "/jobs/" in url and "overview" not in url:
    if counts[url] <= 2:
        # 1st = preflight job_state, 2nd = vertex map: both must succeed.
        _emit(json.dumps({"state": "RUNNING",
                          "vertices": [{"id": "v1", "name": "Source: raw-table-1"}]}) + "\n")
    elif os.environ.get("W38_CURL_QUEUE", "") and counts[url] - 2 <= len(
            [s for s in os.environ["W38_CURL_QUEUE"].split(",") if s]):
        queue = [s for s in os.environ["W38_CURL_QUEUE"].split(",") if s]
        _emit(json.dumps({"state": queue[counts[url] - 3],
                          "vertices": [{"id": "v1", "name": "Source: raw-table-1"}]}) + "\n")
    elif mode == "fail-after-preflight":
        _emit(json.dumps({"state": "FAILED"}) + "\n")
    else:
        _emit(json.dumps({"state": "RUNNING",
                          "vertices": [{"id": "v1", "name": "Source: raw-table-1"}]}) + "\n")
    sys.exit(0)

_emit(json.dumps({"jobs": [{"jid": "test-jid", "state": "RUNNING",
                            "name": "signal-job"}]}) + "\n")
