# Runbook — Policy-Controlled Audit Store (Cloudflare R2 bucket locks)

```text
runbook_id: OPS-AUDIT-STORE-001
scope: policy-controlled money-moving audit retention store (Cloudflare R2; one-year minimum target)
severity: setup/validation procedure; no live-money impact
owner: Platform Team
```

This runbook provisions and validates the policy-controlled audit store on Cloudflare R2
and records the WORM control (NFR 3.4.1 / AC-NFR-005). It is the

## Background (the reality, 2026-08-14)

- The object store is **Cloudflare R2**, used as the S3-compatible endpoint for
  the Iceberg lake tier and checkpoints.
- R2 **does not implement the S3 Object Lock API** (`Get/PutObjectLockConfiguration`
  are not supported) and **does not implement the S3 versioning APIs**
  (`PutBucketVersioning` → `NotImplemented`, `ListObjectVersions` → `NotImplemented`).
- R2's WORM-equivalent is **bucket locks** — prefix retention rules (duration /
  until-date / **indefinite**) configured via the **Cloudflare dashboard,
  Wrangler, or Cloudflare API** (not the S3-compat keys).
- The WORM control = an **indefinite bucket-lock rule on the audit prefix**
  (tooling default `audit/`). Bucket-lock rules take precedence over lifecycle
  rules and apply to new and existing objects.

## Preconditions

1. R2 credentials in `code/01_platform/01_docker/.env` (or the environment):
   `R2_ENDPOINT`, `R2_BUCKET`, `AWS_REGION` (usually `auto`),
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
2. To read or set **bucket locks** (the WORM control), additionally:
   `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID`. Without these, the tool
   records `bucket_lock: NOT_CHECKED` and cannot set the lock.
3. Python 3 with stdlib only (no third-party dependencies).
4. A bucket that already exists (provisioning does not create buckets with
   bucket locks; `provision` creates the bucket only if it is absent — no lock
   is applied to a newly created bucket without `--set-lock`).

## Token scoping (Cloudflare API)

The S3-compat Access Key/Secret **cannot** read or set bucket locks. Create an
Account-scoped API token (dashboard → My Profile → API Tokens → Create Token)
with:

- Permission: **R2** → **Edit** (bucket configuration), scoped to the account
  and restricted to the audit bucket where possible.
- Store it as `CLOUDFLARE_API_TOKEN` in `.env`; keep it out of logs, evidence
  files, and commit history (the tool masks the R2 key id; never paste secrets
  into evidence).
- `CLOUDFLARE_ACCOUNT_ID` is the account id from the R2 dashboard (also the
  prefix of the R2 endpoint hostname).

## Offline check (no R2 access needed)

```bash
make test-audit-r2        # or:
python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
```

## Provisioning (idempotent, non-destructive)

```bash
python3 code/01_platform/04_scripts/audit_r2.py provision
```

What it does (never modifies your data):

| Step | Behavior |
| --- | --- |
| bucket | `EXISTS` or `CREATED` (create only if absent) |
| versioning | `ENABLED` (S3 API) or `NOT_SET` — R2 does not implement `PutBucketVersioning`; enable via dashboard → Settings → Versioning, Wrangler, or the Cloudflare API if desired |
| lifecycle | `EXISTS` (existing rules **preserved**, scratch rule NOT added) or `APPLIED` (probe-scratch cleanup only, `_audit_probe/` prefix) — the audit prefix is never expired by this tool |
| bucket_lock | reads current rules via the Cloudflare API when the token is present; `NOT_SET` note otherwise |

## Setting the WORM lock (explicit flag required)

```bash
python3 code/01_platform/04_scripts/audit_r2.py provision --set-lock [--audit-prefix audit/]
```

- Requires `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID`; exits 2 otherwise.
- Adds rule id `audit-worm-indefinite`, condition `Indefinite`, on the given
  prefix (default `audit/`).
- **Merges** existing rules (the Cloudflare PUT replaces the whole
  configuration — existing rules are always preserved) and is idempotent:
  re-running with the rule present reports `already present — not modified`.
- Alternative channels: Cloudflare dashboard (Settings → Bucket lock rules →
  Add rule → prefix, retention *indefinite*) or `wrangler r2 bucket lock add`.

## Validation and evidence

```bash
python3 code/01_platform/04_scripts/audit_r2.py validate [--out DIR]
```

Checks recorded (probe objects live under `_audit_probe/` and are purged):

| Check | PASS means | Notes |
| --- | --- | --- |
| connectivity / bucket_exists | S3 API reachable, bucket present | required for PASS |
| object_io_probe | put → get (content verified) → delete round-trip | required for PASS |
| lifecycle | lifecycle configuration exists | — |
| versioning | `ENABLED` via S3 API | usually `NOT_ENABLED_VIA_S3` (R2 manual setting) — limitation, not failure |
| object_lock (S3 API) | S3 lock config set | on R2: `NOT_SET` — expected; the WORM control is bucket locks |
| bucket_lock (Cloudflare API) | ≥ 1 bucket-lock rule read | `PASS`/`NONE`/`NOT_CHECKED`/`ERROR`; needs the CF token |

Result is `PASS` only when bucket_exists + object_io_probe pass; every caveat is
recorded in `limitations`.

The process exit follows the verdict (H4-4): `validate` exits 0 on `PASS` and 1 on `FAIL` — the
evidence JSON is written either way, so a FAIL is a result a caller can gate on rather than an
abort that loses the record. `provision` exits 1 when an attempted bucket-lock read/set fails;
`NOT_SET` (no Cloudflare config) and `UNSUPPORTED` remain recorded limitations.

## Evidence file convention

- Location: `logs/audit-r2/<UTC-run-id>-audit-r2-evidence.json`
  (run id format `YYYYMMDDTHHMMSSZ`; `--out DIR` overrides).
- Shape follows the foundation EvidenceRecord (01-foundation.md L159):
  `work_item_id`, `requirement_ids`, `artifact`, `version`, `environment`,
  `workload`, `clock`, `result`, `owner`, `date`, `checks`, `limitations`.
- Store the evidence file path and `result` in the change/evidence record for
  NFR 3.4.1. Never commit the Cloudflare token or R2 secret into the

## Verification and closure evidence

1. `provision` runs clean (bucket, lifecycle preserved).
2. `provision --set-lock` reports `SET audit-worm-indefinite (prefix=…, condition=Indefinite)`.
3. `validate` reports `result: PASS` with `bucket_lock: PASS` and the rule id listed.
4. Evidence JSON saved under `logs/audit-r2/`; path recorded.

## Rollback / abort criteria

- **`--set-lock` never runs without the flag** — a plain `provision` is
  read-only for locks.
- **Versioning** (if enabled via dashboard): suspendable, not disableable.
- **Bucket lock removal**: exclude the rule from the configuration (dashboard
  delete, or PUT the config without it via the Cloudflare API). A bucket
  **cannot be emptied** while any bucket-lock rule exists, and lock rules take
  precedence over lifecycle expiration — confirm the prefix is audit-only
  before applying.
- Abort a run at any point; provisioning and validation are idempotent and
  leave no residue beyond `_audit_probe/` objects, which are purged by
  `validate` (cleanup result recorded as `probe_cleanup`).
