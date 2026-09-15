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
# Exit codes: 0 = wrote samples (O2 push may have WARNed); 2 = bad argument
#   (DURATION_S is not a positive integer); 3 = iostat absent; 4 = iostat
#   failed / the stages dir is not writable / zero samples parsed (degraded
#   evidence, loud). SIGINT/SIGTERM exit 130/143 (128+signal) after stopping
#   the capture and flushing whatever samples it had already written.
#
# Seams (tests and slow hosts; each value is normalised, so a typo cannot turn
# into a python traceback):
#   IO_PROBE_STAGES_DIR   stages dir; stage-capture.sh sets it to $OUT_DIR (see
#                         the note below — its OUT_DIR is already the stages dir)
#   IOSTAT_WAIT_SEC       grace for iostat to exit after the capture (default 10)
#   O2_PUSH_TIMEOUT_SEC   bound on the best-effort O2 push (default 60)
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

# Type check BEFORE any side effect: bash arithmetic reads a non-numeric
# DURATION_S as 0, so "120s" or an empty var used to produce a 2-sample
# "successful" capture instead of a loud usage error.
if [[ ! "$DURATION_S" =~ ^[0-9]+$ ]] || (( DURATION_S < 1 )); then
  echo "io-latency-probe: FATAL — DURATION_S must be a positive integer (got '$DURATION_S')" >&2
  exit 2
fi
IOSTAT_WAIT_SEC="${IOSTAT_WAIT_SEC:-10}"
[[ "$IOSTAT_WAIT_SEC" =~ ^[0-9]+$ ]] || IOSTAT_WAIT_SEC=10
O2_PUSH_TIMEOUT_SEC="${O2_PUSH_TIMEOUT_SEC:-60}"
[[ "$O2_PUSH_TIMEOUT_SEC" =~ ^[0-9]+$ ]] || O2_PUSH_TIMEOUT_SEC=60

# Output dir contract: probe writes <stages-dir>/io-latency.{tsv,jsonl}.
# Default stages-dir = $OUT_DIR/stages (standalone: OUT_DIR is a capture
# root). stage-capture.sh sets IO_PROBE_STAGES_DIR=$OUT_DIR because ITS
# OUT_DIR is ALREADY the stages dir (2026-09-02: nested stages/stages + a
# redirect-to-nonexistent-dir silently killed the probe background fork).
STAGES_DIR="${IO_PROBE_STAGES_DIR:-$OUT_DIR/stages}"
mkdir -p "$STAGES_DIR" || {
  echo "io-latency-probe: FATAL — cannot create $STAGES_DIR" >&2
  exit 4
}
SAMPLES=$(( DURATION_S / 2 + 2 ))   # 2s interval; +2 covers capture spin-up
TSV="$STAGES_DIR/io-latency.tsv"
JSONL="$STAGES_DIR/io-latency.jsonl"
# Host pressure columns ride the same 2s samples (one common timeline):
# PSI (io/memory/cpu avg10), cpu MHz, MemAvailable, and the avg-cpu block
# iostat prints each interval (user/nice/system/iowait/idle %). The header and
# the python row tuple below must list the same columns in the same order.
if ! echo -e "epoch_s\tdevice\tr_await_ms\tw_await_ms\taqu_sz\tutil_pct\tr_iops\tw_iops\tr_kb_s\tw_kb_s\tpsi_io_some_avg10\tpsi_mem_some_avg10\tpsi_cpu_some_avg10\tcpu_mhz_avg\tmem_avail_mb\tcpu_user_pct\tcpu_nice_pct\tcpu_system_pct\tcpu_iowait_pct\tcpu_idle_pct" > "$TSV" \
   || ! : > "$JSONL"; then
  # Without this guard an unwritable stages dir killed only the redirects (no
  # `set -e`), and python then died with a traceback and exit 1 — not the
  # documented "degraded evidence" exit 4.
  echo "io-latency-probe: FATAL — cannot write $TSV / $JSONL" >&2
  exit 4
fi

# The reader is backgrounded so a trap has a pid to signal, and the trap is
# what makes SIGTERM/SIGINT here stop the capture: without it, signalling this
# script killed bash alone and left the reader (and its iostat) reparented to
# init, writing into $TSV after the caller had gone. The reader's own handler
# is what reaps iostat once signalled — the leaking child is a GRANDCHILD, so
# this trap alone would not be enough.
CAPTURE_PID=""
cleanup() {
  local rc=$?
  if [ -n "$CAPTURE_PID" ] && kill -0 "$CAPTURE_PID" 2>/dev/null; then
    echo "io-latency-probe: interrupted — stopping the capture (pid $CAPTURE_PID)" >&2
    kill -TERM "$CAPTURE_PID" 2>/dev/null || true
    local _i
    for _i in $(seq 1 50); do
      kill -0 "$CAPTURE_PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$CAPTURE_PID" 2>/dev/null || true
  fi
  exit "$rc"
}
trap cleanup INT TERM EXIT

python3 - "$TSV" "$JSONL" "$SAMPLES" "$DEVICES" "$IOSTAT_WAIT_SEC" <<'PYEOF' &
import json, signal, subprocess, sys, time

tsv_path, jsonl_path = sys.argv[1], sys.argv[2]
samples, devices = int(sys.argv[3]), set(sys.argv[4].split())
wait_sec = int(sys.argv[5])

_capture = {}          # {"proc": Popen} — the handler has to reach the child


def _on_signal(signum, _frame):
    # Why a handler at all: python's DEFAULT disposition for SIGTERM/SIGINT is
    # to die immediately, so the `finally` below never ran and the iostat this
    # process spawned was reparented to init and kept streaming (measured: it
    # outlives its reader indefinitely). The kill has to happen HERE rather
    # than in the finally: in the half-exit case the reader is parked inside
    # proc.wait() when the signal lands, so a finally-only kill is never
    # reached and the grandchild survives. Raising SystemExit then unwinds
    # through the finally, flushing the partial evidence.
    proc = _capture.get("proc")
    if proc is not None:
        try:
            proc.kill()
        except Exception:
            pass
    raise SystemExit(128 + signum)


signal.signal(signal.SIGTERM, _on_signal)
signal.signal(signal.SIGINT, _on_signal)

proc = subprocess.Popen(
    ["iostat", "-x", "2", str(samples)],
    # stderr to DEVNULL, never PIPE: nothing drains the pipe, so one verbose
    # iostat warning past 64KiB would block the child forever.
    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
_capture["proc"] = proc

n_tsv = 0
n_malformed = 0
seen = set()
first_block = set()   # devices whose next row is the since-boot average
colmap = {}           # column-name -> index, rebuilt from each header row
await_cpu = False     # next numeric line is the avg-cpu block
pending_cpu = {}      # avg-cpu percentages of the CURRENT interval block
staged_cpu = {}       # avg-cpu block; it precedes the interval's Device header
interval_epoch = int(time.time())   # one epoch per 2s interval block
interval_host = {}                  # one /proc snapshot per interval block
keep = None           # device filter; None until the first interval is done
pending_rows = []     # rows held while the filter is undecided


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


def emit(rec):
    """Write one sample to both sinks: TSV (raw truth) + JSONL (O2 source)."""
    global n_tsv
    tsv.write("\t".join(str(rec.get(k, "")) for k in
               ("epoch_s", "device", "r_await_ms", "w_await_ms",
                "aqu_sz", "util_pct", "r_iops", "w_iops",
                "r_kb_s", "w_kb_s", "psi_io_some_avg10",
                "psi_mem_some_avg10", "psi_cpu_some_avg10",
                "cpu_mhz_avg", "mem_avail_mb", "cpu_user_pct",
                "cpu_nice_pct", "cpu_system_pct", "cpu_iowait_pct",
                "cpu_idle_pct")) + "\n")
    jsonl.write(json.dumps(rec) + "\n")
    n_tsv += 1


def settle():
    """Pick the device filter once the first interval's rows are all seen.

    The documented contract is that a DEVICE argument matching nothing keeps
    ALL devices and warns. That cannot be decided row by row — at the first row
    we do not yet know that `nvme1` never appears — so the decision waits for an
    interval that has already reported devices (`seen` non-empty; the first
    `Device` header of a capture arrives before any row, and deciding there
    would always mean "keep everything"). An empty `keep` means the same.
    """
    global keep, pending_rows
    if keep is not None or not seen:
        return
    keep = devices & seen
    held, pending_rows = pending_rows, []
    for rec in held:
        if not keep or rec["device"] in keep:
            emit(rec)


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
                staged_cpu = {"cpu_user_pct": float(cells[0]),
                              "cpu_nice_pct": float(cells[1]),
                              "cpu_system_pct": float(cells[2]),
                              "cpu_iowait_pct": float(cells[3]),
                              "cpu_idle_pct": float(cells[5])}
            except (IndexError, ValueError):
                staged_cpu = {}
            await_cpu = False
            continue
        if cells[0] == "avg-cpu:":
            await_cpu = True
            # This block precedes the interval's Device header, so whatever is
            # staged now belongs to the coming interval — never to the previous
            # one. Clearing here means a malformed numeric line stages nothing.
            staged_cpu = {}
            continue
        if cells[0] == "Device":
            settle()   # the previous interval is complete: decide the filter
            # Header row — rebuild the column map BY NAME. Fixed indices are
            # WRONG across sysstat versions (12.6.1 here: r_await is col 5,
            # the assumed "col 9" is wrqm/s — a 2026-09-02 false 2121ms
            # r_await taught us to parse by header, not by position).
            colmap = {name: i for i, name in enumerate(cells)}
            # One epoch + one /proc snapshot per interval, NOT per device row:
            # every device of a 2s sample then shares a timestamp and the host
            # stats it was read with.
            interval_epoch = int(time.time())
            interval_host = _host_stats()
            # Consume-once: the avg-cpu block that preceded this header belongs
            # to THIS interval's rows. If interval N+1 prints no avg-cpu block
            # at all, `staged_cpu` is empty and its rows carry no cpu field —
            # they must never inherit the percentages from two seconds ago.
            pending_cpu, staged_cpu = staged_cpu, {}
            continue
        dev = cells[0]
        seen.add(dev)
        if dev not in first_block:
            first_block.add(dev)   # sample 0 = since-boot average: skip
            continue
        if keep and dev not in keep:
            continue
        try:
            rec = {
                "epoch_s": interval_epoch,
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
        except (KeyError, IndexError, ValueError) as exc:
            # A sysstat header rename or a locale decimal comma lands here. The
            # row is dropped (evidence stays parseable) but never silently: a
            # sparse TSV used to be indistinguishable from an idle disk.
            n_malformed += 1
            if n_malformed <= 3:
                print(f"io-latency-probe: WARN — dropping malformed row "
                      f"{dev!r}: {exc} (columns: {sorted(colmap)})",
                      file=sys.stderr)
            continue
        rec.update(interval_host)
        rec.update(pending_cpu)
        if keep is None:
            pending_rows.append(rec)   # filter undecided: hold (see settle)
        else:
            emit(rec)
finally:
    settle()          # flush rows held while the filter was undecided
    tsv.close()
    jsonl.close()
    try:
        proc.stdout.close()
    except Exception:
        pass
    try:
        rc = proc.wait(timeout=wait_sec)
    except subprocess.TimeoutExpired:
        # A hung iostat must not skip the verdict below. Before this, the
        # exception escaped the finally, so bash saw exit 1 and the caller lost
        # the "io-latency.tsv is empty; evidence DEGRADED" line.
        print(f"io-latency-probe: WARN — iostat did not exit in {wait_sec}s, "
              "killing it", file=sys.stderr)
        proc.kill()
        try:
            rc = proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            rc = -9   # unkillable: report as a failed capture, not a clean one

if n_malformed:
    print(f"io-latency-probe: WARN — dropped {n_malformed} malformed row(s) "
          f"(columns: {sorted(colmap)})", file=sys.stderr)
if devices and not (devices & seen):
    print(f"io-latency-probe: WARN — none of {sorted(devices)} in iostat "
          f"output (saw {sorted(seen)}); kept ALL devices (documented "
          f"fallback) — rerun with the right device", file=sys.stderr)
if rc != 0 or n_tsv == 0:
    print(f"io-latency-probe: FAILED — iostat rc={rc}, samples={n_tsv} "
          "(io-latency.tsv is empty; disk-latency evidence DEGRADED)",
          file=sys.stderr)
    sys.exit(4)
print(f"io-latency-probe: {n_tsv} sample rows -> {tsv_path}", file=sys.stderr)
PYEOF
CAPTURE_PID=$!
wait "$CAPTURE_PID"
PROBE_RC=$?
# Reaped: a recycled pid must not be signalled by the EXIT trap.
CAPTURE_PID=""

if [ "$PROBE_RC" -ne 0 ]; then
  exit "$PROBE_RC"
fi

# Single-pane push (best-effort; TSV/JSONL remain raw truth). Bounded: a hung
# o2_ingest.py (OpenObserve outage, dead TCP) used to block teardown forever
# after the evidence was already on disk.
if timeout "$O2_PUSH_TIMEOUT_SEC" python3 "$HERE/o2_ingest.py" host_io_latency < "$JSONL"; then
  echo "io-latency-probe: O2 push OK" >&2
else
  echo "io-latency-probe: WARN — O2 push failed or timed out after ${O2_PUSH_TIMEOUT_SEC}s (raw truth intact: $TSV)" >&2
fi
exit 0
