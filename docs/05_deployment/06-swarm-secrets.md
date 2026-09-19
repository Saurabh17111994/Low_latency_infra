# Docker Swarm Secret Sequence (Ingestion)

Applies to: `02_services/01_ingestion` deployment via Docker Swarm.

## Why secrets, not env

Secrets must never appear in stack files, source, images, command lines,
environment dumps, or telemetry (see `04-secrets-rotation.md` §Policy). The
ingestion service reads the following credentials at runtime:

| Secret | Used by | Ingested as |
|--------|---------|-------------|
| `arrow_app_id` | Go bridge (SDK auth) | `ARROW_APP_ID` |
| `arrow_app_secret` | Go bridge (autologin) | `ARROW_APP_SECRET` |
| `arrow_user_id` | Go bridge (autologin) | `ARROW_USER_ID` |
| `arrow_password` | Go bridge (autologin) | `ARROW_PASSWORD` |
| `arrow_totp_key` | Go bridge (autologin TOTP) | `ARROW_TOTP_KEY` |
| `fluss_bootstrap` | Java writers (coordinator) | `FLUSS_BOOTSTRAP` (not a secret, but pinned) |

Autologin (`ARROW_USER_ID` + `ARROW_PASSWORD` + `ARROW_TOTP_KEY`) is the only
supported path. Since 2026-08-24 `IngestionConfig` rejects a non-blank
`ARROW_TOKEN` outright, so do not create that secret or export that variable —
ingestion fails config validation before it connects.

## Sequence

### 1. Create the secrets (one-time)

```
docker secret create arrow_app_id     <(printf '%s' "<app-id>")
docker secret create arrow_app_secret <(printf '%s' "<app-secret>")
docker secret create arrow_user_id    <(printf '%s' "<user-id>")
docker secret create arrow_password   <(printf '%s' "<password>")
docker secret create arrow_totp_key   <(printf '%s' "<totp-key>")
```

Never pass secrets on a shell command line visible to `ps` — use the
`<(printf …)` process substitution or an encrypted file.

### 2. Reference secrets in the stack file

```yaml
services:
  ingestion:
    image: trading-platform/ingestion:<version>
    secrets:
      - arrow_app_id
      - arrow_app_secret
      - arrow_user_id
      - arrow_password
      - arrow_totp_key
    environment:
      # non-secret config only
      FLUSS_BOOTSTRAP: "fluss-coordinator:9123"
      ARROW_INSTRUMENT_MANIFEST: "/instruments/NSE_CM_EQUITY (1024).csv"
    configs:
      - source: instruments_1024
        target: /instruments/NSE_CM_EQUITY (1024).csv

secrets:
  arrow_app_id:     { external: true }
  arrow_app_secret: { external: true }
  arrow_user_id:    { external: true }
  arrow_password:   { external: true }
  arrow_totp_key:   { external: true }

configs:
  instruments_1024:
    file: ../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv
```

### 3. Map Swarm secrets → env in the entrypoint

Docker Swarm mounts secrets under `/run/secrets/<name>`. The ingestion
`docker-entrypoint.sh` must export them before `exec java`:

```bash
export ARROW_APP_ID="$(cat /run/secrets/arrow_app_id)"
export ARROW_APP_SECRET="$(cat /run/secrets/arrow_app_secret)"
export ARROW_USER_ID="${ARROW_USER_ID:-$(cat /run/secrets/arrow_user_id)}"
export ARROW_PASSWORD="${ARROW_PASSWORD:-$(cat /run/secrets/arrow_password)}"
export ARROW_TOTP_KEY="${ARROW_TOTP_KEY:-$(cat /run/secrets/arrow_totp_key)}"
```

### 4. Deploy

```
docker stack deploy -c docker-stack.yml trading
```

### 5. Rotate (per `04-secrets-rotation.md`)

1. Open a change record (owner, expiry, rollback credential).
2. Create the new secret under a versioned identity.
3. Update the service to reference the new secret version.
4. Validate auth, readiness, telemetry, and **no secret leakage**.
5. Remove the old secret after the service is stable.

## Verification (no secret leakage)

| Check | Command |
|-------|---------|
| No secrets in image | `docker history <image>` / `docker run --entrypoint env <image>` |
| No secrets in logs | `docker service logs trading_ingestion \| grep -iE 'app_secret\|token='` (expect empty) |
| No secrets in telemetry | OpenObserve search for the secret prefix (expect empty) |
| Secrets mounted | `docker exec <container> ls /run/secrets/` |
