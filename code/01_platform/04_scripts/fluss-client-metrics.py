#!/usr/bin/env python3
"""Extract Fluss client/source fetch+writer metrics from OpenObserve for a
run window into a TSV next to the fused timeline.

O2 promql quirks honored: no regex label matchers (enumerate exact series
names from the label index; Flink folds the operator name into the metric
name). Read-only diagnosis tooling.
"""
import argparse
import datetime
import json
import os
import urllib.error
import urllib.parse
import urllib.request


def o2_get(path, params, auth):
    url = "http://localhost:5080" + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": "Basic " + auth})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def prom_range(query, start, end, step, auth):
    d = o2_get("/api/default/prometheus/api/v1/query_range",
               {"query": query, "start": start, "end": end, "step": step}, auth)
    if d.get("status") != "success":
        raise RuntimeError(d)
    return d["data"]["result"]


WANT = [
    # (label-in-output, exact metric-name substring)
    ("req_lat_avg", "fluss_client_client_id_requestlatencyms_avg"),
    ("req_lat_max", "fluss_client_client_id_requestlatencyms_max"),
    ("req_per_sec", "fluss_client_client_id_requestspersecond_avg"),
    ("req_inflight", "fluss_client_client_id_requestsinflight_total"),
    ("fetch_lat", "scanner_client_id_database_table_fetchlatencyms"),
    ("fetch_req_per_sec", "scanner_client_id_database_table_fetchrequestspersecond"),
    ("remote_fetch_rps", "scanner_client_id_database_table_remotefetchrequestspersecond"),
    ("remote_fetch_bps", "scanner_client_id_database_table_remotefetchbytespersecond"),
    ("send_lat", "writer_client_id_sendlatencyms"),
    ("batch_queue_ms", "writer_client_id_batchqueuetimems"),
    ("rec_per_batch", "writer_client_id_recordsperbatch"),
    ("rec_send_ps", "writer_client_id_recordsendpersecond"),
    ("fetch_lag", "operator_currentfetcheventtimelag"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="epoch seconds UTC")
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    start, end = int(args.start), int(args.end)

    secrets = {}
    for line in open(os.path.join(os.path.dirname(__file__),
                                  "../01_docker/secrets.env")):
        if "=" in line:
            k, v = line.split("=", 1)
            secrets[k.strip()] = v.strip()
    auth = secrets["O2_AUTH_BASIC"]

    names = o2_get("/api/default/prometheus/api/v1/label/__name__/values",
                   {}, auth)["data"]
    grid = list(range(start - 15, end + 30, 15))
    series = {}  # col -> {t: v} (max across operators/subtasks)
    used = {}
    for col, frag in WANT:
        cands = [n for n in names if frag in n]
        if not cands:
            print(f"WARN: no series containing {frag}")
            continue
        by_t = {}
        ops = set()
        for n in cands:
            try:
                res = prom_range(n, start - 15, end + 30, 15, auth)
            except (RuntimeError, urllib.error.URLError, OSError) as e:
                print(f"WARN: {n[-50:]}: {e}")
                continue
            for s in res:
                ops.add(s["metric"].get("operator_name") or
                        n.split("_operator_")[-1].split("_fluss")[0][:30])
                for t, v in s["values"]:
                    by_t[t] = max(by_t.get(t, float(v)), float(v))
        if by_t:
            series[col] = by_t
            used[col] = sorted(ops)[:6]
    cols = [c for c, _, in WANT if c in series]
    with open(args.out, "w") as f:
        f.write("epoch_s\tiso_time\t" + "\t".join(cols) + "\n")
        for g in grid:
            row = [str(g), datetime.datetime.fromtimestamp(
                g, datetime.timezone.utc).isoformat()]
            for c in cols:
                v = series[c].get(g)
                row.append("" if v is None else f"{v:.2f}")
            f.write("\t".join(row) + "\n")
    print(f"wrote {args.out}: {len(grid)} grid points x {len(cols)} cols")
    for c in cols:
        print(f"  {c}: operators={used[c]}")


if __name__ == "__main__":
    main()
