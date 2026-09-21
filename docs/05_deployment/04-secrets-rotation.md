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
  **Correction, same day, measured once T2's value-equality mode existed:** that count was an
  undercount. The same mode over the repository read 5,131 files and found four hits in two more files —
  `code/01_platform/01_docker/secrets.env` (mode 600) and `code/01_platform/01_docker/.env.bak-20260828`,
  both holding the password and the TOTP key. Both are git-ignored (`.gitignore:31`, `:32`, `:89`) and no
  commit has ever touched either, so neither was published. A names-and-shapes scan cannot see those
  files at all: the values sit in them without their names. They must be emptied or deleted as part of
  the rotation, not before it — `secrets.env` is what a local live run reads, and a backup of values
  being rotated away has no further use.
  **Update 2026-09-22:** `.env.bak-20260828` is deleted — a redundant snapshot of values being rotated
  away, mode 664 (the widest of the three copies), and nothing read it: only `.gitignore:32` and these
  records named it. `secrets.env` stays until the rotation itself, for the reason above and one measured
  the same day: compose delivers secrets to the local stack through that file, and `SecretGuard` names it
  as the sanctioned local store, so emptying it early takes the local live path away before its
  replacement exists.
  **Same day, measured with the O2 values in the values file:** `o2_password` and `o2_auth_basic`
  are in `secrets.env` (git-ignored, mode 600, never committed) and in three session transcripts —
  2026-08-23, 2026-08-27 and 2026-09-17, the last two carrying the file's contents as pasted text.
  No tracked file carries either value; the scan's only other hits were the seven published digests in
  `images.published.env`, which is that file's purpose. So those two names are exposed locally, not
  published — and whether the rotation's trigger reaches OpenObserve depends on one fact only the
  operator has: is the value in `secrets.env` the production OpenObserve password or a local-stack one?

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

## What the home-wide scan measured — 2026-09-22

T2's criterion had never been measured over the whole home directory; the figures in
circulation came from three narrower roots. Measured now in value mode, four values (the two
secrets, the user id and the app id from `~/.env.arrow`), root `/home/saurabh`: **229 hits in
51 files** — 2 inside this repository, both in the git-ignored
`code/01_platform/01_docker/secrets.env`, and 227 outside.

| Location | Hits | What it is |
| --- | --- | --- |
| `~/.omp/agent/sessions` | 111 | transcripts of a second agent tool |
| `~/.pi/agent/sessions` | 74 | transcripts of this tool |
| `~/.config/manicode` | 20 | transcripts of a third tool |
| `~/Jupyter_notebook` | 8 | sibling trees: `Arrow_broker/.env`, two older project copies |
| `~/.commandcode` | 6 | file-history copies |
| `~/.env.arrow` | 5 | the live local store |
| `~/.local/share/Trash` | 3 | a deleted session file |
| `~/.pi-lens` | 2 | a gitleaks cache |

Coverage, so the number is read for what it is: 524,267 files read; `skipped_big: 0` (value
mode has no size cap); 27,595 skipped by the extension policy; 583 special files; 2,290
unreadable (0.4%) — that last figure is the honest residual, since a value could sit in one of
them. No tracked or published file carries a value: the two in-repo hits are in a git-ignored
file, and the repository's own value scan found only the seven published image digests in
`images.published.env`, which is that file's purpose.

**It is a floor, not a total.** Value mode searches the values it is given, and this run was
given four. A fifth string carries the same name and a different value:

| Name | File | Length | Shape |
| --- | --- | --- | --- |
| `ARROW_APP_SECRET` | `~/.env.arrow` | 64 | lowercase hex, 16 distinct characters |
| `ARROW_APP_SECRET` | `code/01_platform/01_docker/secrets.env` | 64 | lowercase hex, 16 distinct characters, no shared prefix with the one above |

Neither string is one repeated character, a word, or a recognised placeholder, so neither can
be dismissed as a dummy. The disk cannot say whether two applications exist or whether the
secret was reissued in the portal and only one file was updated. Either way both strings stay
live until the application is retired, which is why the order of work below retires the
application instead of rotating the string in place.

### The app id is published, and cannot be recalled

`ARROW_APP_ID` left the tree of this repository on 2026-09-21 (`5f9db548`), but removing a
string from a tree does not unpublish it. Measured 2026-09-22: it appears twice in the
published history of that document, and two sibling trees under `Jupyter_notebook` commit it
in their own documentation — `streaming_project/docs/08_implementation/20-close-execution-service-gaps-plan.md`,
a worktree of the public `github.com/Saurabh17111994/streaming_project.git`, and
`streaming_project_p6/docs/08_implementation/05-execution-core.md`. Both trees are outside
this repository and were not modified. In both, the value stores beside them
(`Arrow_broker/.env`, `code/01_platform/01_docker/secrets.env`) are untracked, measured with
`git ls-files`; a value scan of those trees has not been run, so this section does not claim
they are secret-free.

An identifier is not a secret, and the files that carry it are documentation. The measurement
adds one thing: a published app id paired with any leaked secret is a working credential pair,
and only broker-side retirement answers that.

## References

- Security requirements: `../02_requirements/03-non-functional.md` §3.6
- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md` §§REQ-PF-004 and REQ-PF-010
- Security architecture: `../03_architecture/04-security-model.md`
- Operational requirements: `../02_requirements/06-operational.md` §§6.7–6.10
