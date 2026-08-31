# Alert Routing Ops (G6)

> Single-webhook alert routing: OpenObserve alert rules → `dev-webhook`
> destination → `alert-consumer` (durable JSONL). This is the dev form of
> "who gets told, how fast" — production replaces the destination URL with
> a real pager; nothing else changes.

## Chain (what talks to what)

```
O2 alert rules (47: ING-*/SIGNAL-*/INFRA-*/pos-state-*)
  └─ destination "dev-webhook" → http://localhost:9999/noop
       (localhost = O2's own netns; consumer shares it via
        network_mode: service:openobserve — O2 v0.91.5's SSRF guard
        blocks all private ranges, only 127/8 is exempt, dev only)
       └─ alert-consumer (compose service, python:3.11-slim)
            └─ /data/alerts/alerts.jsonl  (alert-store volume, durable)
```

## Alert consumer endpoints

Port 9999 is **not published to the host** (netns-shared with O2). Query it
from inside the container:

```bash
# recent deliveries (newest last)
docker exec 01_docker-alert-consumer-1 python3 -c \
  "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9999/alerts?limit=20').read().decode())"
# with filters: ?severity=crit|error|warn|info  ?class=ing|signal|infra|pos-state
# totals + last delivery
docker exec 01_docker-alert-consumer-1 python3 -c \
  "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9999/stats').read().decode())"
```

Record format (one JSON per line — this is the contract a production pager
replacement must accept/produce):

```json
{"received_at": "2026-08-31T…", "name": "SIGNAL-crit-checkpoint-failed",
 "severity": "crit", "class": "signal", "body": {"alert": {"name": "…"}}}
```

Classification: `severity` from the name infix (`crit`/`error`/`warn`;
pos-state alerts → `warn`, anything unrecognized → `info`), `class` from
the prefix (`ING`/`SIGNAL`/`INFRA`/`pos-state`/other).

## Selftest (the mechanical guard — run after ANY routing change)

```bash
O2_PASSWORD=$(grep ^O2_PASSWORD= code/01_platform/01_docker/secrets.env | cut -d= -f2) \
  make alert-routing-test
```

Creates a temporary always-firing probe alert, waits for the durable record
(O2 scheduled granularity ~1 min; 4-minute budget), asserts classification,
deletes the probe. Negative proof included: malformed delivery → HTTP 400
and the consumer stays alive. Offline unit tests:
`code/01_platform/04_scripts/tests/test_alert_consumer.py`.

## Alert rule inventory

- Provisioned by `o2-provision.py` (ING/SIGNAL/INFRA, incl. the G1
  `SIGNAL-warn-source-volume-drop` — source rate < 50% of design for 5 min)
  and `seed_alerts.py` (pos-state). Full rule table with thresholds:
  `./02-ingestion-alerting.md` + the catalogs in the two scripts.
- Destination provisioning: `o2-provision.py provision_destination()`.

## Known behaviors (not defects)

- Quiesced dev feeds false-fire the rate-based rules
  (`SIGNAL-error-source-stalled`, `SIGNAL-warn-source-volume-drop`,
  `SIGNAL-warn-source-lag`) — feed-state-driven by design; they become
  meaningful on a live feed.
- O2 retries a delivery on non-2xx and re-delivers after each silence
  window; duplicate records in the JSONL are honest delivery history, not
  consumer bugs.
- `INFRA-warn-net-80` / `INFRA-warn-disk-io-20` fire regularly on the dev
  box (single-host NIC + NVMe) — expected on dev hardware.

## Production migration (when the 4-VM stack lands)

1. Swarm already ships the consumer (`docker-stack.yml` service
   `alert-consumer`, script via Swarm config, `alert-store` volume; the O2
   destination URL must be `http://alert-consumer:9999/noop` — service DNS,
   and `ALERT_BIND=0.0.0.0` is set for the overlay network).
2. Create the destination in prod O2 with the real pager URL (Slack/email
   webhook) — keep `dev-webhook` pointing at the consumer for the audit
   trail, or repoint entirely; the record format is the contract.
3. Re-run `make alert-routing-test` against prod O2.
