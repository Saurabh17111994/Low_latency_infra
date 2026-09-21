# Docker Swarm secrets — creation, storage, rotation

Applies to the production stack (`code/01_platform/01_docker/docker-stack.yml`, deployed as `prod`).
Supersedes the ingestion-era sequence, which created five names — two of them (`arrow_app_id`,
`arrow_user_id`) not secrets at all, and two more absent from the stack entirely. §What is not a
secret records what changed and why.

## Why secrets, not env

Secrets must never appear in stack files, source, images, command lines, environment dumps, or
telemetry (`docs/05_deployment/04-secrets-rotation.md` §Storage rules). The stack declares exactly
nine names `external: true`, and `code/01_platform/04_scripts/secrets-bootstrap.sh --self-check`
fails when its own list drifts from that block — the drift that fails a deploy with "secret not
found". `--check` is the runtime form: it asks the cluster whether all nine exist.

| Secret | Mounted by | How the consumer reads it |
| --- | --- | --- |
| `aws_access_key_id` · `aws_secret_access_key` | `fluss-coordinator`, `fluss-tablet-1/2/3`, `flink-jobmanager`, `flink-taskmanager` | `/run/secrets/…` via `AWS_ACCESS_KEY_ID_FILE` / `AWS_SECRET_ACCESS_KEY_FILE` |
| `execution_bridge_auth_token` | `execution-bridge` | `EXECUTION_BRIDGE_AUTH_TOKEN_FILE` |
| `arrow_app_secret` · `arrow_password` · `arrow_totp_key` | `ingestion` | `/run/secrets/arrow_*` |
| `o2_auth_basic` | `otel-collector` | `/run/secrets/o2_auth_basic`, expanded by `${file:…}` in its own config |
| `o2_password` | *mounted nowhere* | OpenObserve v0.91.5 has no `_FILE` support; it takes the same value as the deploy variable `ZO_ROOT_USER_PASSWORD` |
| `gateway_shared_secret` | `execution-gateway`, `nautilus` | `/run/secrets/gateway_shared_secret` |

## Creation — one command, no secret on disk

`code/01_platform/04_scripts/secrets-bootstrap.sh` creates all nine: six values from the operator,
three generated on the host (the two internal tokens plus `o2_auth_basic`, derived from the O2 user
and password). The procedure and the values-file format are in
`PROD_VM_PROVISIONING.md` §9 `S6`.

```bash
# the outer double quotes expand here; the inner single quotes keep the remote command intact
ssh "${SSH_USER:?set the SSH user}@${VM1_IP:?set the VM1 public IPv4}" \
  "cd ~/arrow-infra && code/01_platform/04_scripts/secrets-bootstrap.sh \
  --values-file /dev/stdin --o2-user '${O2_ADMIN:?set the OpenObserve admin email}'" < ~/vm-secrets.env
code/01_platform/04_scripts/secrets-bootstrap.sh --check   # [PASS] all 9 secrets exist
```

### The idiom that looks safe and is not

`docker secret create app_secret <(printf '%s' "<value>")` — the form this document recommended
before CHG-248 — runs `printf` in a subshell with the **secret as an argument**, which is exactly
what `ps` exposes. `printf` with the value already in a variable is a shell builtin
(`printf '%s' "$value" | docker secret create <name> -`): no child process, no argument list, no
disk. The script uses that form, and `tests/test_15_secrets_bootstrap.py` asserts no value ever
appears in an argument.

`o2_auth_basic` is **bare base64** — `base64("<o2-user>:<password>")` with no `Basic ` prefix —
because the collector configs (`otel-collector-config.swarm.yaml` and, since CHG-273, the file-reading
half `otel-collector-logs.swarm.yaml`) write the scheme themselves
(`Authorization: "Basic ${file:/run/secrets/o2_auth_basic}"`). A prefixed value authenticates as
`Basic Basic …` and answers 401.

## What is not a secret

- `ARROW_APP_ID`, `ARROW_USER_ID` — plain `${…:?}` deploy values on the stack's `ingestion` service.
- `FLUSS_BOOTSTRAP` — a plain address, not a credential.
- `arrow_token` — removed 2026-08-24; the live bridge rejects a non-blank `ARROW_TOKEN` outright.
  A Swarm secret of that name is a leftover: no service in the stack declares or mounts it. One
  exists on the development node today, so this is a live example rather than a hypothetical.
  `ConfigKeys.ARROW_TOKEN` still exists in `code/common` and is matched by the log-redaction
  pattern; do not create the secret.

## Rotation

1. Open a change record (owner, expiry, rollback credential).
2. Create the new secret under a versioned identity (`docker secret create <name>-v2 -`).
3. Repoint the service and force it to re-read: `docker service update --force <service>`.
4. Validate auth, readiness, telemetry, and no leakage (§Verification).
5. Remove the old secret once the service is stable (`docker secret rm <name>`).

Swarm secret values are immutable — there is no in-place update, so rotation is always
create-new → repoint → remove-old. `secrets-bootstrap.sh` refuses when a name already exists instead
of silently skipping it, because a skipped rotation looks deployed and keeps serving the old value.

## Verification (no secret leakage)

| Check | Command |
| --- | --- |
| No secrets in image | `docker history <image>` / `docker run --entrypoint env <image>` |
| No secrets in logs | `docker service logs prod_ingestion \| grep -iE 'app_secret\|token='` (expect empty) |
| No secrets in telemetry | OpenObserve search for the secret prefix (expect empty) |
| Secrets mounted | `docker exec <container> ls /run/secrets/` |
| No value in an argument | `ps -ef \| grep <secret-name>` during creation (expect the command, never the value) |
