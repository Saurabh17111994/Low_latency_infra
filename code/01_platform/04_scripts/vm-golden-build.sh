#!/usr/bin/env bash
# vm-golden-build.sh — turn a CloudPe VM into the daily trading golden image
# (CHG-362, 2026-09-28).
#
# Run ON the build VM, as root, with the repo already present (git clone or
# rsync — docs/05_deployment/CLOUDPE_DAILY_VM.md §2). It installs Docker if
# missing, the host toolchain (python3 + tzdata), writes `.env` / `.env.vm`
# from their committed templates, and installs + enables the trading-eod
# unit (fires at EOD_AT from `.env.vm`; records its last clean run for `stop`).
#
# It NEVER writes secrets and NEVER starts the stack.
#
#   sudo bash code/01_platform/04_scripts/vm-golden-build.sh \
#       --repo /opt/trading/streaming_project \
#       --r2-endpoint https://<account-id>.r2.cloudflarestorage.com \
#       --r2-bucket <bucket>
#
#   --check   inventory only: fail unless Docker, the project images + their
#             toolchain and env files are present; change nothing. Run again
#             after loading images, before snapshotting.
set -euo pipefail

REPO="/opt/trading/streaming_project"
R2_ENDPOINT=""
R2_BUCKET=""
CHECK=0

say() { printf 'vm-golden-build: %s\n' "$*"; }
fail() { printf 'vm-golden-build: ERROR %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --repo) REPO="${2:?--repo needs a path}"; shift 2 ;;
    --r2-endpoint) R2_ENDPOINT="${2:?--r2-endpoint needs a URL}"; shift 2 ;;
    --r2-bucket) R2_BUCKET="${2:?--r2-bucket needs a name}"; shift 2 ;;
    --check) CHECK=1; shift ;;
    -h | --help) usage 0 ;;
    *) fail "unknown argument: $1 (see --help)" ;;
  esac
done

ENV_DIR="$REPO/code/01_platform/01_docker"
UNIT=/etc/systemd/system/trading-eod.service
PROJECT_IMAGES="01_docker-ingestion 01_docker-compute 01_docker-nautilus
01_docker-execution-bridge 01_docker-execution-gateway 01_docker-ddl-apply
01_docker-eod-controller"

missing_images() {
  command -v docker >/dev/null 2>&1 || {
    printf '%s\n' "$PROJECT_IMAGES" | tr ' ' '\n' | sed '/^$/d'
    return
  }
  local image
  for image in $PROJECT_IMAGES; do
    docker image inspect "${image}:latest" >/dev/null 2>&1 || echo "${image}:latest"
  done
}

[ -d "$REPO" ] || fail "repo not found at $REPO — clone or rsync it first (docs/05_deployment/CLOUDPE_DAILY_VM.md §2)"
[ -f "$ENV_DIR/docker-compose.yml" ] || fail "$ENV_DIR/docker-compose.yml missing — is $REPO the repo root?"
[ -f "$ENV_DIR/.env.example" ] || fail "$ENV_DIR/.env.example missing"
[ -f "$ENV_DIR/.env.vm.example" ] || fail "$ENV_DIR/.env.vm.example missing"

# ---- inventory -------------------------------------------------------------
have_docker=0
command -v docker >/dev/null 2>&1 && have_docker=1
have_compose=0
[ "$have_docker" = 1 ] && docker compose version >/dev/null 2>&1 && have_compose=1

say "repo:    $REPO"
say "docker:  $([ "$have_docker" = 1 ] && echo present || echo 'missing (will install)')"
say "compose: $([ "$have_compose" = 1 ] && echo present || echo 'missing (installed with docker)')"
say ".env:    $([ -f "$ENV_DIR/.env" ] && echo present || echo 'will write from .env.example')"
say ".env.vm: $([ -f "$ENV_DIR/.env.vm" ] && echo present || echo 'will write from .env.vm.example')"
say "unit:    $UNIT"
missing="$(missing_images | tr '\n' ' ')"
if [ -n "$missing" ]; then
  say "images:  MISSING: $missing"
else
  say "images:  all 7 project images present"
fi

if [ "$CHECK" = 1 ]; then
  [ "$have_docker" = 1 ] || fail "--check: docker missing"
  [ -f "$ENV_DIR/.env" ] || fail "--check: .env missing (run without --check first)"
  [ -f "$ENV_DIR/.env.vm" ] || fail "--check: .env.vm missing (run without --check first)"
  [ -z "$missing" ] || fail "--check: image set incomplete: $missing (load it, then re-run)"
  # C3-5: the snapshot must own the whole daily toolchain. Prove it inside the
  # images before the volume is snapshotted — a snapshot missing the in-image
  # JDK, the probe class or the EOD m2 repo cannot be repaired later without
  # rebuilding and reloading the image set.
  docker run --rm --entrypoint java 01_docker-ingestion -version >/dev/null 2>&1 \
    || fail "--check: java missing in the ingestion image"
  docker run --rm --entrypoint sh 01_docker-ingestion \
    -c 'test -f /app/probe/FlussReadLagProbe.class' >/dev/null 2>&1 \
    || fail "--check: /app/probe/FlussReadLagProbe.class missing in the ingestion image"
  docker run --rm --entrypoint sh 01_docker-eod-controller \
    -c 'java -version >/dev/null 2>&1 && test -f /app/code/01_platform/04_scripts/eod_controller.py && test -d /opt/ddl-apply/m2/repository' \
    >/dev/null 2>&1 \
    || fail "--check: the EOD image is missing java, the controller script or the m2 repo"
  docker run --rm --entrypoint python3 01_docker-eod-controller \
    /app/code/01_platform/04_scripts/eod_controller.py --help >/dev/null 2>&1 \
    || fail "--check: eod_controller.py --help fails inside the EOD image"
  grep -q '^EOD_RUNNER=compose$' "$ENV_DIR/.env.vm" \
    || fail "--check: .env.vm must set EOD_RUNNER=compose (no host JDK/m2 on the VM)"
  grep -q '^ALLOW_FRESH=1$' "$ENV_DIR/.env.vm" \
    || fail "--check: .env.vm must set ALLOW_FRESH=1 (fresh disk every morning)"
  grep -q '^DAY_STOP_REQUIRE_EOD=1$' "$ENV_DIR/.env.vm" \
    || fail "--check: .env.vm must set DAY_STOP_REQUIRE_EOD=1 (archive before the disk dies)"
  say "--check: OK — nothing was changed"
  exit 0
fi

[ "$(id -u)" = 0 ] || fail "run as root: sudo bash $0 ..."

# ---- docker ----------------------------------------------------------------
if [ "$have_docker" != 1 ]; then
  say "installing docker engine + compose plugin (official apt repo)"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl gnupg zstd >/dev/null
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  # shellcheck disable=SC1091
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin >/dev/null
else
  if ! command -v zstd >/dev/null 2>&1; then
    apt-get update -qq
    apt-get install -y -qq zstd >/dev/null
  fi
fi
systemctl enable --now docker >/dev/null

# ---- host toolchain ---------------------------------------------------------
# The daily flow keeps timing, the heartbeat and the last-run stamp on the host
# (trading-eod unit) and shells every JVM/data step into containers, so the host
# needs python3 (tzdata for zoneinfo) and nothing else. A recipe-built fresh VM
# has neither guaranteed.
if ! command -v python3 >/dev/null 2>&1 || [ ! -d /usr/share/zoneinfo ]; then
  say "installing python3 + tzdata (the host scheduler needs both)"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq python3 tzdata >/dev/null
fi

# ---- env files -------------------------------------------------------------
if [ ! -f "$ENV_DIR/.env" ]; then
  cp "$ENV_DIR/.env.example" "$ENV_DIR/.env"
  say "wrote $ENV_DIR/.env from .env.example"
else
  say "$ENV_DIR/.env already present — left untouched"
fi

if [ -n "$R2_ENDPOINT" ] || [ -n "$R2_BUCKET" ]; then
  [ -n "$R2_ENDPOINT" ] || fail "--r2-bucket given without --r2-endpoint"
  [ -n "$R2_BUCKET" ] || fail "--r2-endpoint given without --r2-bucket"
  R2_ENDPOINT="$R2_ENDPOINT" R2_BUCKET="$R2_BUCKET" python3 - "$ENV_DIR/.env" <<'PY'
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
keys = {
    "R2_ENDPOINT": os.environ["R2_ENDPOINT"],
    "R2_BUCKET": os.environ["R2_BUCKET"],
    "S3_WAREHOUSE_PATH": f"s3://{os.environ['R2_BUCKET']}/lake",
}
seen = set()
out = []
for line in path.read_text().splitlines():
    key = line.split("=", 1)[0].strip() if "=" in line else None
    if key in keys:
        out.append(f"{key}={keys[key]}")
        seen.add(key)
    else:
        out.append(line)
for key, value in keys.items():
    if key not in seen:
        out.append(f"{key}={value}")
path.write_text("\n".join(out) + "\n")
PY
  say "patched R2_ENDPOINT / R2_BUCKET / S3_WAREHOUSE_PATH in .env"
fi

if [ ! -f "$ENV_DIR/.env.vm" ]; then
  cp "$ENV_DIR/.env.vm.example" "$ENV_DIR/.env.vm"
  say "wrote $ENV_DIR/.env.vm from .env.vm.example"
else
  say "$ENV_DIR/.env.vm already present — left untouched"
fi
install -d -m 0755 /var/lib/trading

# ---- EOD scheduler unit ----------------------------------------------------
# No clock in the unit: EOD_AT / EOD_ZONE / EOD_LAST_RUN_FILE come from
# .env.vm (EnvironmentFile). It starts on every boot of the daily VM.
cat > "$UNIT" <<EOF
[Unit]
Description=Trading EOD scheduler (fires at EOD_AT) - CHG-362
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$REPO
EnvironmentFile=$ENV_DIR/.env.vm
ExecStart=/usr/bin/python3 $REPO/code/01_platform/04_scripts/eod_schedule.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable trading-eod.service >/dev/null
say "installed + enabled $UNIT (starts at boot, fires at EOD_AT from .env.vm)"

# ---- what remains ----------------------------------------------------------
missing="$(missing_images | tr '\n' ' ')"
if [ -n "$missing" ]; then
  say "WARNING: project images still missing: $missing"
  say "         load the image set from the dev PC (CLOUDPE_DAILY_VM.md §2.4),"
  say "         re-run with --check, and only then snapshot the volume."
else
  say "DONE — snapshot the boot volume and create the image (CLOUDPE_DAILY_VM.md §2.6)."
  say "secrets are NOT on this machine by design: inject secrets.env on each daily VM."
fi
