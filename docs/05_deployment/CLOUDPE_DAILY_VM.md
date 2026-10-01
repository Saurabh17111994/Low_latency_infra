# CloudPe daily VM — golden image, fresh boot, EOD to R2

**Model (operator decision 2026-09-28):** the platform VM is rented for the
trading day and **destroyed at the end**; every morning starts from an **empty
disk**. So the morning cost must be *boot + start*, never installs or builds:
the machine is built **once** into a CloudPe image, and each day is launched
from it. The dev PC runs the same stack while powered on (its disk is kept).

CloudPe capabilities used (verified in their knowledge base 2026-09-28):
volume snapshots, "Create Image" from a snapshot/volume, launching a VM from an
image, and the OpenStack API for scripted launches.

Related: `docs/06_operations/01-runbooks.md` (daily flow, F1/F8 notes),
`docs/06_operations/07-lake-archive-ops.md` (lake/tiering), `.env.vm.example`
(the VM profile), `code/01_platform/04_scripts/vm-golden-build.sh` (the on-VM
build).

## 1. Sizing

| | Minimum | Recommended |
|---|---|---|
| Flavor | 4 vCPU / 16 GB RAM | 8 vCPU / 32 GB RAM |
| Boot volume | 60 GB | 100 GB |
| OS | Ubuntu 22.04 LTS | Ubuntu 24.04 LTS |
| Network | public (SSH + broker + R2) | same |

The disk only ever holds one day of Fluss/Flink state; the lake data lives in R2
(`tradingticks-aug-2026`, APAC — keep the VM in an Indian region).

## 2. One-time: build the golden image

### 2.1 Create the build VM
CloudPe dashboard → Compute → create VM (Ubuntu, recommended flavor, 100 GB,
public network, your SSH key).

### 2.2 Put the repo on it (one of)
- **git clone** (after pushing the commit you want baked):
  `git clone git@github.com:Saurabh17111994/Low_latency_infra.git /opt/trading/streaming_project`
- **from the dev PC** (no push needed):
  `rsync -a --delete --exclude logs --exclude code/02_services/04_executor/target ./ saurabh@<vm>:/opt/trading/streaming_project/`

### 2.3 Run the build script (on the VM)

```bash
sudo bash /opt/trading/streaming_project/code/01_platform/04_scripts/vm-golden-build.sh \
  --repo /opt/trading/streaming_project \
  --r2-endpoint "https://${R2_ACCOUNT_ID:?set R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
  --r2-bucket tradingticks-aug-2026
```

What it does — and does not do:

| Does | Does not |
|---|---|
| installs Docker + compose plugin (if missing), python3 + tzdata for the host scheduler, and enables Docker | start the stack |
| writes `.env` from `.env.example` with the real R2 endpoint/bucket/warehouse | write any secret |
| writes `.env.vm` from `.env.vm.example` (fresh start, dev+full universe (2433 tokens, 3 sockets), multi-TF candles + strategy host (`n7-range-breakout-v1`) + execution intents (full dev parity, CHG-492), 15:45 EOD, compose runner, stop gate, live DataStream channel via `ARROW_FEED=token`) | run the EOD |
| installs + enables the `trading-eod` systemd unit | change the code |

The script warns when the project image set is missing; that is what §2.4 loads. `--check`
additionally proves the in-image toolchain before you snapshot — the ingestion JDK and
`/app/probe/FlussReadLagProbe.class`, the EOD image's java + controller + m2 repo, and the
`.env.vm` runner/fresh-start/stop-gate keys — so a snapshot that boots a VM unable to run the day
fails here instead of at 09:15.

The daily VM intentionally runs the **dev + full** universe (`DEPLOYMENT_ENV=dev`,
`UNIVERSE=full` from `.env.vm`): the real 2433-instrument NSE cash manifest over the three
approved dev sockets, multi-connection approved. This is a recorded decision (H4-2), not an
accidental default — `day_run.resolve_universe` still refuses `UNIVERSE=full` under
`DEPLOYMENT_ENV=prod|production` (CHG-320), and that refusal stays.

### 2.4 Load the images (from the dev PC — no builds, no registry)

List exactly what the stack needs (run this on the dev PC):

```bash
: "${REPO:?set REPO to the repo checkout}"
cd "$REPO"
COMPOSE_PROFILES=execution-t3 docker compose \
  --env-file code/01_platform/01_docker/.env \
  --env-file code/01_platform/01_docker/secrets.env \
  -f code/01_platform/01_docker/docker-compose.yml config --images | sort -u
```

Stream them to the VM (one command; nothing is written to disk on the PC):

```bash
: "${REPO:?set REPO to the repo checkout}" "${VM_HOST:?set VM_HOST to the VM hostname}"
cd "$REPO"
docker save $(COMPOSE_PROFILES=execution-t3 docker compose \
    --env-file code/01_platform/01_docker/.env \
    --env-file code/01_platform/01_docker/secrets.env \
    -f code/01_platform/01_docker/docker-compose.yml config --images | sort -u) | \
  zstd -T0 -1 | ssh "saurabh@$VM_HOST" 'zstd -dc | sudo docker load'
```

(The VM needs `zstd`; Docker's install step in §2.3 already added it.)

### 2.5 Verify, then make the image

```bash
sudo bash /opt/trading/streaming_project/code/01_platform/04_scripts/vm-golden-build.sh --check
```

`--check` must print OK: Docker present, all 7 project images present, `.env`
and `.env.vm` present. Then Dashboard → Volumes → boot volume → Snapshots →
create snapshot → **Create Image** (KB: *Creating a New Virtual Machine Using
a Snapshot of a Volume*). Name it by date, e.g. `trading-golden-20260928`.

### 2.6 Destroy the build VM
The image is the artifact. Rebuild it when code or images change (repeat
2.2–2.5 — each image shares one stable commit + image set).

## 3. Every morning (daily VM)

| Step | Command / action |
|---|---|
| 1 | Create a VM **from the image** (dashboard; §5 has the API note) |
| 2 | Inject secrets (never baked into the image): `scp code/01_platform/01_docker/secrets.env root@<vm>:/opt/trading/streaming_project/code/01_platform/01_docker/secrets.env` |
| 3 | Start: `cd /opt/trading/streaming_project && make day ARGS="start"` — fresh start (`ALLOW_FRESH=1` from `.env.vm`), ready in ~2–4 min (87 s measured software path) |
| 4 | Provision observability — a fresh OpenObserve starts **empty**: `bash code/01_platform/04_scripts/provision-observability.sh` (destination + dashboards + 47 rules + storage/disk alerts + retention; refuses without `secrets.env` and derives `O2_AUTH_BASIC` from `O2_PASSWORD`). Off-session, a few metric-stream rules are deferred (they need streams the signal job only emits when signals flow) — the disk/ING/INFRA safety rules are already in; re-run when the feed is live. Without this the day runs blind — measured 2026-09-28: zero alerts loaded while the data disk reached 85.13% |
| 5 | Start lake tiering: `bash code/01_platform/04_scripts/tiering-start.sh` — without it the day's parquet never reaches R2 (idempotent; check `--status`) |
| 6 | Let the day run. At **15:45 IST** the `trading-eod` unit runs the EOD controller **in the `eod-controller` compose service** (`EOD_RUNNER=compose` from `.env.vm` — the VM has no host JDK/m2) with `EOD_OFFLOAD=lake`, and records the success to `/var/lib/trading/eod-last-run`. M2-2: the per-slot outcome also lands in `/var/lib/trading/eod-state.json`, so a slot missed while the VM was down (or a failed fire) is **caught up the same day** — the loop fires it at start, retries every `EOD_RETRY_DELAY_SEC` (900 s) until it succeeds, and logs `CATCH-UP` loudly. The controller's lease/state keeps the extra fire idempotent |
| 7 | After the archive: `make day ARGS="stop"` — the gate refuses until today's record exists ("EOD archive confirmed …"), then the stack goes down |
| 8 | Destroy the VM. The day's data is in R2 — verify: `bash code/01_platform/04_scripts/r2-list.sh all` |

## 4. What lives where

| Item | Location | Survives the VM? |
|---|---|---|
| Market data (parquet) | R2 `lake/...` (Fluss tiering) | **yes** |
| EOD manifest/state | Fluss during the day; lake evidence in R2 | state dies with Fluss; the archive is the evidence | 
| EOD last-run record | `/var/lib/trading/eod-last-run` | no (re-read by the stop gate the same day) |
| EOD slot state (M2-2) | `/var/lib/trading/eod-state.json` | no (durable across scheduler restarts within the day) |
| Flink checkpoints | local volume | no — every morning is a fresh start by design |
| Secrets | `secrets.env`, injected at boot | no |
| Golden image | CloudPe Images | **yes** |

## 5. Optional: create the daily VM via the API
CloudPe exposes OpenStack APIs (KB: *Create and Access a VM Using the API*):
application credential → token → create a server booting from the golden
**image** (or a volume created from it), `block_device_mapping_v2` with
`source_type: image`. The dashboard path is the first-class flow until the
launch is boring enough to script.

## 6. Failure modes

| Symptom | Meaning | Fix |
|---|---|---|
| `stop` RED: "today's EOD->R2 archive is not confirmed" | the 15:45 run failed or never fired | `systemctl status trading-eod`; `python3 code/01_platform/04_scripts/eod_schedule.py --check-heartbeat --last-run /var/lib/trading/eod-last-run` says whether the scheduler is dead or the day is merely unarchived (M2-2); a live scheduler retries on its own (`/var/lib/trading/eod-state.json` has the slot outcome); re-run the unit's path once: `set -a; . code/01_platform/01_docker/.env.vm; set +a; EOD_RUNNER=compose python3 code/01_platform/04_scripts/eod_schedule.py --once`; re-run `stop`; `DAY_STOP_FORCE=1` only when the loss is deliberate |
| `start` refused: "no savepoint/checkpoint" | `.env.vm` missing (no `ALLOW_FRESH=1`) | `cp .env.vm.example .env.vm` |
| "no RUNNING tiering job" | the day's parquet will not reach R2 | `bash code/01_platform/04_scripts/tiering-start.sh`; if it dies, `docs/06_operations/07-lake-archive-ops.md` §Recovery |
| compose starts **building** an image | the image set was not loaded | repeat §2.4, then rebuild the golden image |
| No alert fires all day | the fresh OpenObserve was never provisioned | §3 step 4 (`provision-observability.sh`) |
