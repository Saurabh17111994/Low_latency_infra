#!/usr/bin/env bash
# io-latency-probe.sh — per-op disk I/O latency + queue depth for a capture run.
#
# Why (2026-09-02 decline hunt): node_exporter's 15s cadence is too coarse for
# a 120s run (~8 samples) and exposes counters, not per-op latency. A machine
# can push 30 MB/s aggregate while fsync/queue latency starves a stateful
# streaming job. This probe:
#   1. streams host `iostat -x 2 <samples>` for the capture window, tagging
#      each 2s sample with its wall-clock epoch (streaming parse — timestamps
#      reflect when iostat PRINTED the sample, not when parsing finished),
#   2. writes TSV evidence to $OUT_DIR/stages/io-latency.tsv (raw truth),
#   3. pushes samples to OpenObserve stream host_io_latency (single-pane
#      policy) via o2_ingest.py — best-effort, WARN on failure.
#
# Usage: io-latency-probe.sh OUT_DIR DURATION_S [DEVICE...]
#   DEVICE default: nvme0n1. If no requested device matches, the probe keeps
#   ALL devices and warns (single-NVMe host: usually means a rename).
# Exit codes: 0 = wrote samples (O2 push may have WARNed); 3 = iostat absent;
# 4 = iostat failed / zero samples parsed (degraded evidence, loud).
set -uo pipefail

OUT_DIR="${1:?OUT_DIR required}"
DURATION_S="${2:?DURATION_S required}"
shift 2 || true
DEVICES="${*:-nvme0n1}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

command -v iostat >/dev/null 2>&1 || {
  echo "io-latency-probe: FATAL — iostat not installed (sysstat); this probe is the per-op latency evidence source" >&2
  exit 3
}

# Output dir contract: probe writes <stages-dir>/io-latency.{tsv,jsonl}.
# Default stages-dir = $OUT_DIR/stages (standalone: OUT_DIR is a capture
# root). stage-capture.sh sets IO_PROBE_STAGES_DIR=$OUT_DIR because ITS
# OUT_DIR is ALREADY the stages dir (2026-09-02: nested stages/stages + a
# redirect-to-nonexistent-dir silently killed the probe background fork).
STAGES_DIR="${IO_PROBE_STAGES_DIR:-$OUT_DIR/stages}"
mkdir -p "$STAGES_DIR"
SAMPLES=$(( DURATION_S / 2 + 2 ))   # 2s interval; +2 covers capture spin-up
TSV="$STAGES_DIR/io-latency.tsv"
JSONL="$STAGES_DIR/io-latency.jsonl"
# Host pressure columns ride the same 2s samples (one common timeline):
# PSI (io/memory/cpu avg10), cpu MHz, MemAvailable, and the avg-cpu block
# iostat prints each interval (user/system/iowait/idle %).
echo -e "epoch_s\tdevice\tr_await_ms\tw_await_ms\taqu_sz\tutil_pct\tr_iops\tw_iops\tr_kb_s\tw_kb_s\tpsi_io_some_avg10\tpsi_mem_some_avg10\tpsi_cpu_some_avg10\tcpu_mhz_avg\tmem_avail_mb\tcpu_user_pct\tcpu_system_pct\tcpu_iowait_pct\tcpu_idle_pct" > "$TSV"
: > "$JSONL"

python3 - "$TSV" "$JSONL" "$SAMPLES" "$DEVICES" <<'PYEOF'
import json, subprocess, sys, time

tsv_path, jsonl_path = sys.argv[1], sys.argv[2]
samples, devices = int(sys.argv[3]), set(sys.argv[4].split())

proc = subprocess.Popen(
    ["iostat", "-x", "2", str(samples)],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

n_tsv = 0
seen = set()
first_block = set()   # devices whose next row is the since-boot average
colmap = {}           # column-name -> index, rebuilt from each header row
await_cpu = False     # next numeric line is the avg-cpu block
pending_cpu = {}      # avg-cpu percentages for the NEXT device row


def _host_stats():
    out = {}
    try:
        # file is /proc/pressure/memory but the column contract is psi_mem_*
        for name, key in (("io", "io"), ("memory", "mem"), ("cpu", "cpu")):
            with open(f"/proc/pressure/{name}", encoding="utf-8") as fh:
                parts = dict(p.split("=") for p in fh.readline().split()
                             if "=" in p)
                out[f"psi_{key}_some_avg10"] = float(parts.get("avg10", 0))
    except (OSError, ValueError):
        pass
    try:
        freqs = []
        with open("/proc/cpuinfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("cpu MHz"):
                    freqs.append(float(line.split(":")[1]))
        if freqs:
            out["cpu_mhz_avg"] = sum(freqs) / len(freqs)
    except (OSError, ValueError):
        pass
    try:
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    out["mem_avail_mb"] = float(line.split()[1]) / 1024
    except (OSError, ValueError):
        pass
    return out
tsv = open(tsv_path, "a", buffering=1)
jsonl = open(jsonl_path, "a", buffering=1)
try:
    for raw in proc.stdout:
        cells = [c.strip() for c in raw.split() if c.strip()]
        if not cells:
            continue
        if cells[0].startswith("Linux"):
            continue  # banner line
        if await_cpu:
            # avg-cpu percentages line: user nice system iowait steal idle
            try:
                pending_cpu = {"cpu_user_pct": float(cells[0]),
                               "cpu_nice_pct": float(cells[1]),
                               "cpu_system_pct": float(cells[2]),
                               "cpu_iowait_pct": float(cells[3]),
                               "cpu_idle_pct": float(cells[5])}
            except (IndexError, ValueError):
                pending_cpu = {}
            await_cpu = False
            continue
        if cells[0] == "avg-cpu:":
            await_cpu = True
            continue
        if cells[0] == "Device":
            # Header row — rebuild the column map BY NAME. Fixed indices are
            # WRONG across sysstat versions (12.6.1 here: r_await is col 5,
            # the assumed "col 9" is wrqm/s — a 2026-09-02 false 2121ms
            # r_await taught us to parse by header, not by position).
            colmap = {name: i for i, name in enumerate(cells)}
            continue
        dev = cells[0]
        seen.add(dev)
        if devices and dev not in devices:
            continue
        if dev not in first_block:
            first_block.add(dev)   # sample 0 = since-boot average: skip
            continue
        try:
            rec = {
                "epoch_s": int(time.time()),
                "device": dev,
                "r_await_ms": float(cells[colmap["r_await"]]),
                "w_await_ms": float(cells[colmap["w_await"]]),
                "aqu_sz": float(cells[colmap["aqu-sz"]]),
                "util_pct": float(cells[colmap["%util"]]),
                "r_iops": float(cells[colmap["r/s"]]),
                "w_iops": float(cells[colmap["w/s"]]),
                "r_kb_s": float(cells[colmap["rkB/s"]]),
                "w_kb_s": float(cells[colmap["wkB/s"]]),
            }
        except (KeyError, IndexError, ValueError):
            continue  # malformed row — drop it loudly? keep evidence clean
        rec.update(_host_stats())
        rec.update(pending_cpu)
        tsv.write("\t".join(str(rec.get(k, "")) for k in
                   ("epoch_s", "device", "r_await_ms", "w_await_ms",
                    "aqu_sz", "util_pct", "r_iops", "w_iops",
                    "r_kb_s", "w_kb_s", "psi_io_some_avg10",
                    "psi_mem_some_avg10", "psi_cpu_some_avg10",
                    "cpu_mhz_avg", "mem_avail_mb", "cpu_user_pct",
                    "cpu_system_pct", "cpu_iowait_pct",
                    "cpu_idle_pct")) + "\n")
        jsonl.write(json.dumps(rec) + "\n")
        n_tsv += 1
finally:
    tsv.close()
    jsonl.close()
    proc.stdout.close()
    rc = proc.wait(timeout=10)

if devices and not (devices & seen):
    print(f"io-latency-probe: WARN — none of {sorted(devices)} in iostat "
          f"output (saw {sorted(seen)}); rerun with the right device",
          file=sys.stderr)
if rc != 0 or n_tsv == 0:
    print(f"io-latency-probe: FAILED — iostat rc={rc}, samples={n_tsv} "
          "(io-latency.tsv is empty; disk-latency evidence DEGRADED)",
          file=sys.stderr)
    sys.exit(4)
print(f"io-latency-probe: {n_tsv} sample rows -> {tsv_path}", file=sys.stderr)
PYEOF
PROBE_RC=$?

if [ "$PROBE_RC" -ne 0 ]; then
  exit "$PROBE_RC"
fi

# Single-pane push (best-effort; TSV/JSONL remain raw truth).
if python3 "$HERE/o2_ingest.py" host_io_latency < "$JSONL"; then
  echo "io-latency-probe: O2 push OK" >&2
else
  echo "io-latency-probe: WARN — O2 push failed (raw truth intact: $TSV)" >&2
fi
exit 0
