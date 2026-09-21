# Secrets Rotation and Revocation

## Scope

This runbook covers broker/Arrow, Fluss, S3, OpenObserve, TLS, Swarm, and operator credentials. Exact secret names, providers, and rotation APIs are deployment-specific and must be verified against the pinned release.

## Storage rules

### Local development

- Use an ignored `.env` file only with sandbox/test credentials.
- Never commit `.env`, print it in logs, embed secrets in images, or use production credentials locally.
- Local credentials must not grant live-money access.
- **Known exception, dated 2026-09-21 — the live Arrow app is on this laptop.** `~/.env.arrow` (mode 600,
  unchanged since 2026-08-21) holds `ARROW_APP_ID`, `ARROW_APP_SECRET`, `ARROW_PASSWORD` and
  `ARROW_TOTP_KEY` for the live app, because the local ingestion pipeline needs them (TOTP autologin is the
  only supported auth). The operator classified the app as live on 2026-09-21, so all three rules above are
  knowingly broken until the rotation runs. Value-equality against the four real values found them in 48
  files outside that file: 15 `logs/soak/monday-gates-*/compose-config.log` files (git-ignored; no commit
  ever touched `logs/soak`), about 20 agent transcripts outside the repository, and the tracked document
  `08_implementation/05-execution-core.md`, whose app id is therefore published. **The remedy is rotation,
  not deletion** — a copied secret cannot be un-copied and a published app id cannot be unpublished — so
  the two Arrow rows below rotate the app secret, the password and the TOTP key, and the app id rotates
  with its pair. Timing is the operator's: rotating now ends local ingestion until VM day, when Fluss is
  running on the VMs; rotating at VM day leaves a live trading login on an unencrypted laptop meanwhile.
  Values are recorded here as names only, never printed.

### Production

- Use Docker Swarm secrets or an approved equivalent secret manager.
- Do not place secrets in stack files, source, images, command lines, environment dumps, or telemetry.
- Separate identities and permissions for Ingestion, Flink, Action Capture, Executor, S3, Fluss, OpenObserve, and operators.
- Encrypt secret storage and restrict access by service identity and operator role.

## Rotation procedure

1. Open a change record with secret owner, affected services, expiry, dependencies, rollback credential, and validation plan.
2. Confirm the order gate is `HALTED` for any rotation affecting the order path, broker, Arrow REST, Executor, Fluss, S3, or operator authorization.
3. Create the replacement credential with the minimum required scope.
4. Store it in the production secret mechanism under a versioned identity.
5. Roll or reload only services that can safely reload credentials; use controlled restart where required.
6. Validate authentication, authorization, readiness, telemetry, and no secret leakage.
7. Revoke the old credential after the overlap window and verify rejection.
8. Record the secret version, timestamps, service instances, test evidence, and audit event.
9. Reconcile the order path and require the single-operator (Saurabh, DEC-044) approval before returning the gate to `ENABLED`.

## Credential classes

| Credential | Consumers | Required checks |
| --- | --- | --- |
| Arrow/broker market-data | Ingestion | Subscription, decode, reconnect, readiness |
| Arrow/broker postback | Action Capture | Intake, status parsing, correlation |
| Arrow REST/broker execution | Executor/Arrow REST | Request authorization, response, reconciliation |
| Fluss client/admin | Ingestion, Flink, Action Capture, Executor, operators | Least privilege, table scope, revocation |
| S3 checkpoint/lake | Flink, offload, recovery operators | Read/write scope, encryption, manifest operations |
| OpenObserve | Services/operators | Telemetry only; cannot authorize orders |
| TLS/mTLS material | Cross-host/service paths | Certificate chain, expiry, rotation, transport health |
| Operator identities | Reconciliation/gate control | MFA/authentication, role, distinct approvals, audit |
| Internal service auth | Execution bridge, Execution gateway | Both halves replaced on the same day; the gate halts, both services redeploy, and each resumes with a fresh epoch |

Every Swarm secret created by `code/01_platform/04_scripts/secrets-bootstrap.sh` must map to a class
above: `arrow_app_secret`, `arrow_password`, `arrow_totp_key` → the Arrow rows; `aws_access_key_id`,
`aws_secret_access_key` → S3 checkpoint/lake; `o2_password`, `o2_auth_basic` → OpenObserve;
`execution_bridge_auth_token`, `gateway_shared_secret` → internal service auth. (Added 2026-09-21: those
last two had no class, so no procedure would ever have rotated them.)

## Failure behavior

- Expired or revoked credential: affected service becomes not ready, emits a critical alert, and stops affected processing according to its contract.
- Executor/Arrow REST credential uncertainty: gate `HALTED`; no blind retry.
- S3 credential failure: checkpoint/offload readiness fails; retain source data and do not claim recovery or EOD verification.
- Fluss credential failure: stop unsafe writes/reads, preserve uncertainty, and reconcile before resuming.
- Observability credential failure: buffer durable audit where supported; telemetry readiness fails and the live-money gate remains blocked if acceptance evidence is unavailable.
- Operator credential compromise: revoke, preserve evidence, halt affected order flow, and require fresh authenticated single-operator (Saurabh, DEC-044) approval.

## Secret safety checks

CI and deployment checks must detect:

- Secrets in source, stack files, images, command lines, logs, traces, support bundles, and generated manifests
- Excessive service permissions
- Expired, duplicate, or unowned credentials
- Missing rotation/revocation evidence
- Insecure transport or certificate expiry
- Audit access without access logging

Original packet bytes, postback payloads, tokens, and credentials are never copied into ordinary logs.

## Rotation acceptance

Test planned rotation, expired credentials, immediate revocation, failed refresh, service restart, overlapping credential validity, compromised credentials, unauthorized gate operations, and recovery after rotation. Prove that rotation cannot automatically enable order placement and that all money-moving audit remains reconstructable.

## Rotation log

The log is the evidence that a rotation happened, and the reminder that the next one is due. It lives
outside the repository, with the password and recovery material: a log committed to a public repo would
publish which classes were rotated and when — a map of the credentials worth attacking.

One row per rotation, newest first. `Proof` is the check that showed the new value working; "deployed
successfully" is not proof, because a deploy that left the old credential alive looks identical from the
outside. No cadence is fixed yet: the first rotation sets it, and the `Next date` column is what makes it
enforceable instead of remembered.

```text
| Date       | Class                 | Secrets replaced                                   | Proof                                                    | Next date  |
|------------|-----------------------|----------------------------------------------------|----------------------------------------------------------|------------|
| YYYY-MM-DD | Arrow trading login   | arrow_password, arrow_totp_key                     | broker login works with the new password, fails with the old | YYYY-MM-DD |
| YYYY-MM-DD | Arrow market data     | arrow_app_secret                                   | ingestion reconnects and its ING-* alerts clear          | YYYY-MM-DD |
| YYYY-MM-DD | R2 checkpoint/lake    | aws_access_key_id, aws_secret_access_key           | a CHECKPOINT_DIR write and read both succeed             | YYYY-MM-DD |
| YYYY-MM-DD | OpenObserve           | o2_password, o2_auth_basic                         | dashboard login works, routing selftest passes           | YYYY-MM-DD |
| YYYY-MM-DD | Internal service auth | execution_bridge_auth_token, gateway_shared_secret | bridge and gateway reconnect, gate resumes cleanly       | YYYY-MM-DD |
| YYYY-MM-DD | Operator identities   | (broker portal)                                    | login plus 2FA, audit trail present                      | YYYY-MM-DD |
```

TLS/mTLS material has no row of its own: Docker Swarm manages that PKI, and the only action attached to it
is keeping `--autolock` on (guide §9 row 3).

## References

- Security requirements: `../02_requirements/03-non-functional.md` §3.6
- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md` §§REQ-PF-004 and REQ-PF-010
- Security architecture: `../03_architecture/04-security-model.md`
- Operational requirements: `../02_requirements/06-operational.md` §§6.7–6.10
