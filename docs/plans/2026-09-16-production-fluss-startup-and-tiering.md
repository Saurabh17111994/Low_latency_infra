# Production Fluss — make the four servers actually start, and tier to R2

## Overview

**Requested outcome.** CHG-179 gave the production Swarm stack a working Flink
image and closed five blockers. Planning the follow-up — moving the production
`remote.data.dir` off a node-local volume, as the dev stack already did in
W35x/CHG-180 — turned up a **larger, earlier blocker**: the four production Fluss
services never start at all. This plan fixes that first, then completes the
storage move, so that a production deploy has working Fluss servers writing
tiered data to R2.

**Problem 1 — the four Fluss services run nothing (verified live).**
`apache/fluss:0.9.1-incubating` declares `Entrypoint=["/docker-entrypoint.sh"]`
and `CMD=["help"]`. With no `command:` in the stack, each container prints usage
and exits **0**:

```text
$ docker run --rm apache/fluss:0.9.1-incubating
Usage: docker-entrypoint.sh (coordinatorServer|tabletServer)
    Or docker-entrypoint.sh help
$ echo $?
0
```

`docker stack config -c docker-stack.yml` renders `command=None` for
`fluss-coordinator` and `fluss-tablet-1/2/3`, while the Flink services — fixed by
CHG-179 — render `command: [jobmanager]` / `[taskmanager]`. This is the same
defect class CHG-179 fixed for Flink, and it means production has no Fluss
coordinator and no tablet servers. The dev compose has the correct
`command: ["coordinatorServer"]` / `["tabletServer"]`.

**Problem 2 — the healthchecks can never pass (verified live).**
The four Fluss healthchecks use `CMD-SHELL` with a `/dev/tcp` probe. In this
image `/bin/sh` is **dash**, and `/dev/tcp` is a bash-only feature. Against a
confirmed-open port on the same Docker network:

```text
$ docker run --rm --network tcptest --entrypoint bash <image> \
    -c '(echo > /dev/tcp/listener2/9123) >/dev/null 2>&1 && echo CONNECTED || echo FAILED'
CONNECTED
$ docker run --rm --network tcptest --entrypoint sh <image> -c '<same probe>'
FAILED
```

So the probe reports **unhealthy for a healthy server**. What Swarm then does
with that status is **not settled by the sources, and was not observable here** —
this host is a Swarm worker, not a manager, so no service could be created to
watch (see *Required External or Manual Verification*). The evidence conflicts:

- Docker's "how swarm mode works" page: *"If the container fails health checks or
  terminates, the task terminates"*, and the orchestrator creates a replacement.
- The `docker service update` docs define update failure as a task that *"doesn't
  start up, or if it stops running within the monitoring period"* — health is not
  named as an input to `--update-monitor`.
- moby#23962 has a maintainer stating *"I'm not sure that swarm is taking
  healthchecks into account at all atm"*, and a commenter describing SwarmKit as
  keying on container status (`running`/`exited`) rather than `health_status`.

Either way the health signal is worthless: it is permanently negative for a
working server, so nothing that reads it — an operator, a rollback policy, or
Swarms own rescheduling — can distinguish healthy from broken. `docs/09`
documented these checks as a real readiness contract, which makes the false
signal actively misleading. The fix is justified regardless of which Swarm
behaviour is true; Task 2 has the executor pin the actual behaviour on a manager
host if one is available.

**Problem 3 — the servers cannot authenticate to R2.**
The four services receive the R2 credentials as Swarm **file** secrets at
`/run/secrets/aws_access_key_id` and `/run/secrets/aws_secret_access_key`, but
their `FLUSS_PROPERTIES` carry **no** `s3.*` keys and no credential environment
variables. The upstream entrypoint has no `*_FILE` handling. The Fluss S3 plugin
therefore falls through to the delegation-token path, which cannot work on R2
(no STS — established by wave-35x commit `21a78cbe`). Static keys are required.

**Problem 4 — `remote.data.dir` is node-local and driver-less.**
All four services point `remote.data.dir` at `/tmp/fluss/remote-data`, backed by
the `fluss-remote-data` volume — declared with no driver, so it is node-local,
and the services carry **no placement pin** to keep a given tablet on a given
node. Fluss tiers by default (`ConfigOptions.java:795`,
`defaultValue(Duration.ofMinutes(1))`), so tiered segments land on whichever node
happened to run the tablet, and a later read that resolves to another node finds
nothing. Dev already moved this to `s3://${R2_BUCKET}/remote-data` (W35x).

**Acceptance criteria.**
1. `docker stack config -c docker-stack.yml` renders a `command:` for all four
   Fluss services.
2. Each Fluss healthcheck probe exits 0 against a listening port in that image,
   and non-zero when the port is closed — i.e. it is a working signal, not a
   permanently-failing one.
3. `remote.data.dir` on all four services is an `s3://` URI; the four services
   carry the `s3.*` key set and the credentials reach the Fluss process.
4. A container started from the changed spec, with the file secrets mounted,
   starts a real server (`Starting Coordinator Server`) and its process
   environment contains the R2 credentials.
5. `fluss-remote-data` is no longer declared or mounted anywhere in the stack.
6. `make test-09`, `make stack-selfcheck`, `change_control_check.py` and
   `docs_audit.py` are green; no new lint or banned-substring failures.

## Context

- **`code/01_platform/01_docker/docker-stack.yml`** — the production stack. Four
  Fluss services: `fluss-coordinator` (L236), `fluss-tablet-1` (L299),
  `fluss-tablet-2` (L350), `fluss-tablet-3` (L401). Each holds
  `remote.data.dir: /tmp/fluss/remote-data`, a `fluss-remote-data:` mount, and
  `secrets: [aws_access_key_id, aws_secret_access_key]`. `configs:` (L148) is the
  in-repo pattern for shipping a file to a service (`otel-collector-config` L821,
  `alert-consumer-src` L868). `x-deploy-workload` (L66) carries the restart and
  update policy. Volumes are declared at L132.
- **`code/01_platform/01_docker/docker-compose.yml`** (dev) — the working
  reference. `command: ["coordinatorServer"]` (L321) / `["tabletServer"]` (L437);
  the full `s3.*` block at L388-394 (coordinator) and L494-500 (tablet);
  credentials as `${env.AWS_ACCESS_KEY_ID}` / `${env.AWS_SECRET_ACCESS_KEY}`;
  the plugin-mount comment at L326-343.
- **`code/01_platform/01_docker/flink-runtime/`** — CHG-179's precedent and the
  template for this fix: `20-r2-secrets-from-file.sh` bridges `*_FILE` secrets
  into the environment, and its `Dockerfile` replaces `ENTRYPOINT` with that
  bridge and re-declares `CMD ["help"]`.
- **`code/01_platform/04_scripts/tests/test_flink_runtime_image.py`** —
  `ProductionStackTests` (L363) already pins "Flink services declare what to run"
  (L386) via a `_service_block()` helper that strips comments. `BridgeTests`
  (L322) is the template for behavioural tests of a shell bridge.
- **`code/01_platform/04_scripts/tests/test_09_stack.py`** — `STACK` /
  `_load()` read the stack at L24/L35. `TestVolumes` (L139) with
  `test_durable_volumes_declared` (L140-149) asserts `fluss-remote-data` is
  declared; `TestSecrets` (L127) asserts the secrets are external and that
  `fluss-coordinator` consumes the AWS ones; `TestTier1ProductionConfig` (L188)
  reads `FLUSS_PROPERTIES` from the stack text.
- **`code/01_platform/04_scripts/stack_selfcheck.sh`** — the deploy harness.
  `required_vars` (L107-111) lists the 12 `:?` variables; `DEPLOY=1` refuses a
  missing one and exits 2 (L112-121); compile-only defaults them (L123 onward).
  Credentials are external Swarm secrets with no env fallback (the comment block
  ending near L104).
- **Image facts (verified 2026-09-16).** Ubuntu 24.04, `/bin/sh` to dash, `bash`
  present at `/usr/bin/bash`, `envsubst` present. Baked plugins:
  `plugins/s3/fluss-fs-s3-0.9.1-incubating.jar` and
  `plugins/hdfs/fluss-fs-hdfs-0.9.1-incubating.jar` — the same artifacts dev
  bind-mounts into `plugins/iceberg/`. `plugins/iceberg/` contains only
  `fluss-lake-iceberg-0.9.1-incubating.jar`; the dev-only
  `fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar` is absent everywhere.
- **Mechanics verified for this plan (2026-09-16).**
  - `docker stack config` **interpolates `$` inside `entrypoint:`**. An inline
    `["/bin/sh","-c","... \"$0\" ..."]` fails with *"invalid interpolation
    format ... you may need to escape any $ with another $"*, and `$$`-escaping
    it mangles the script (`eval "f=\{_FILE:-}"`). A bridge script delivered as a
    **Swarm config** renders cleanly (`configs:` plus
    `entrypoint: ["/bin/sh", "/opt/fluss/bin/r2-secrets-from-file.sh"]`,
    `command: ["coordinatorServer"]`).
  - With the base entrypoint replaced by `/bin/sh`, `command:` arrives as `$0`:
    `docker run --entrypoint /bin/sh <image> -c 'echo "[$0]"' coordinatorServer`
    prints `[coordinatorServer]`.
  - `envsubst` leaves `${env.NAME}` untouched (dotted names are not valid shell
    variables), whether or not the variable is set; the live dev tablet shows the
    literal placeholder at `conf/server.yaml:105`. Hadoop expands it at read time.
  - A bash bridge with `set -eu`, an explicit `[ -r "$f" ]` check and an abort on
    an empty value works: `FATAL: AWS_ACCESS_KEY_ID: unreadable secret file ...`
    for a missing file, `FATAL: ... empty after read` for an empty one, and
    `Starting Coordinator Server` on the happy path with `$@` dispatch intact.
## Review Handoff

- **Selected approach.** Fix everything through the stack file plus one small
  config-delivered shell script; **no new image**. The stock
  `apache/fluss:0.9.1-incubating` image already contains the s3 plugin and
  `bash`, so a purpose-built image would add a build/push/digest-pin pipeline for
  no capability. Rejected alternatives:
  - *Inline `entrypoint:` shell string* — impossible: compose interpolates `$`
    inside `entrypoint:` (evidence above).
  - *A `flink-runtime`-style custom Fluss image* — the only thing it would add is
    the bridge script, which a Swarm config delivers without a build step.
  - *Read the secret files from Java config* — Fluss reads `${env.*}` through
    Hadoop's `EnvironmentVariableCredentialsProvider`; there is no file-based
    provider without a credential store and another secret for its password.
  - *Keep the local volume and pin tablets by node label* — the volume is
    driver-less, so it is not portable and does not survive node replacement;
    this was the rejected dev option too.
- **Non-goals.** (a) `datalake.*` lake-tiering wiring for production — see
  *Post-Completion*; it is a separate gap with its own plugin-jar question.
  (b) Re-pinning `FLINK_IMAGE` (blocked on `docker push` plus `digest-pin.sh`).
  (c) Changing dev compose — it already works.
- **Assumptions.** Static R2 keys are the intended production mechanism (dev uses
  them; the delegation-token path is documented broken on R2). `R2_BUCKET` is a
  legitimate deploy-time variable — it exists in `.env.example`, and prod's
  `${S3_WAREHOUSE_PATH}` already points at that bucket.
- **Unresolved decision — healthcheck shape.** The probe must run under an
  interpreter that supports it. Two options: **(A)** switch the four Fluss
  services to `test: ["CMD", "bash", "-c", "<probe>"]`, leaving the command
  untouched; **(B)** leave `CMD-SHELL` and replace the probe with a non-`/dev/tcp`
  check (for example, having the bridge script export a readiness file, as
  `ingestion` does at `docker-compose.yml:799`). Option A is the smaller diff and
  keeps the probe's meaning. Task 2 selects A unless the executor finds a reason
  to prefer B; record the choice in the CHG record either way, because the
  healthcheck change is itself a behaviour change.
- **Authorization.** Implementation is authorized as a single lane ending in
  gated commits (standing authorization: verified, lint-clean). Needs the user's
  explicit word: any `docker swarm init` / `docker stack deploy`, any Swarm
  secret creation, `docker push`, and anything that writes outside the repo.

## Implementation Steps

### Task 1: Make the four Fluss services start the server they are meant to run
**Why:** Without this nothing else matters — production has no Fluss cluster.
**Files:** `code/01_platform/01_docker/docker-stack.yml` (the `environment:`
block of `fluss-coordinator`, `fluss-tablet-1`, `fluss-tablet-2`,
`fluss-tablet-3`)
**Depends on:** none

- [x] Add `command: ["coordinatorServer"]` to `fluss-coordinator` and
      `command: ["tabletServer"]` to each of `fluss-tablet-1/2/3`, placed
      alongside the `image:` / `networks:` keys the way the dev compose and the
      Flink services do. Mirror the dev comment explaining that the image's `CMD`
      is only the usage fallback. Done in `de88cac2`.
- [x] Add a `ProductionStackTests`-style regression test in `test_09_stack.py`
      asserting all four services declare their `command`, using a
      comment-stripping service-block helper (same technique as
      `test_flink_runtime_image.py:370`). Verify it **fails** against the
      pre-change stack file and passes after. Done in `dfa95636` as
      `test_every_service_declares_what_to_run` (broad contract over all 16
      services, allowlist `COMMAND_ALLOWED_BY_IMAGE_DEFAULT`) plus
      `test_fluss_services_run_the_server_they_are_named_for` (exact four).
      Simpler than the planned comment-stripping helper: asserts on the parsed
      YAML value, so prose cannot satisfy it. Verified failing-first (3 failed
      against the pre-fix stack, naming fluss-coordinator, then tablet-1 once
      the coordinator was fixed).
- [x] Verify the render and the dispatch:
      `docker stack config -c docker-stack.yml` (with the 12 placeholder vars)
      shows `command:` for all four (render shows `- coordinatorServer` once,
      `- tabletServer` three times), and
      `docker run --rm --entrypoint /bin/sh <image> -c 'echo "[$0]"' coordinatorServer`
      prints `[coordinatorServer]` (verified in the planning session).

### Task 2: Give the healthchecks an interpreter that can run them
**Why:** The probe is permanently negative for a healthy server, so no consumer
of the health signal — operator, rollback policy, or Swarm's own rescheduling —
can tell a working server from a broken one. `docs/09` treats these checks as a
readiness contract, so a permanently-failing probe is a false contract.
**Files:** `code/01_platform/01_docker/docker-stack.yml` (the `healthcheck.test`
of the four Fluss services); `code/01_platform/04_scripts/tests/test_09_stack.py`
**Depends on:** none (independent of Task 1, but commit together)

- [x] Change the four Fluss `healthcheck.test` entries to
      `["CMD", "bash", "-c", "(echo > /dev/tcp/127.0.0.1/<port>) >/dev/null 2>&1 || exit 1"]`
      (option A: 9123 for the coordinator, 9124 for each tablet), or apply
      option B from *Review Handoff* and record why. Done in `de88cac2`, but
      the plan's shape was **wrong in two ways found during execution**:
      (1) the interpreter was only half the defect - Fluss binds the container
      hostname, not loopback, so `127.0.0.1:9124` is refused even under bash
      (`rc=1` vs `$HOSTNAME:9124 -> rc=0`, live tablet). The fixed probe is
      `(echo > /dev/tcp/$$HOSTNAME/<port> || echo > /dev/tcp/127.0.0.1/<port>)`
      (`$$` because compose interpolation rejects a bare `$(...)`; render
      verified to deliver `$HOSTNAME`). (2) Scope was 6 probes, not 4:
      flink-jobmanager `:8081` had the same dash defect (fixed the same way;
      Flink listens on loopback too, so the fallback covers it), and
      openobserve `:5080` is a static binary with no shell/curl/wget at all -
      its probe reported exit=-1 and flipped the status to unhealthy, so it
      was REMOVED with an `x-healthcheck` exception marker (otel-collector
      precedent), not repaired.
- [x] Add a test asserting the four services' healthcheck does not rely on
      `CMD-SHELL` with a `/dev/tcp` probe — i.e. that the probe's interpreter is
      `bash`. State in the test docstring that `/bin/sh` is dash in this image.
      Done in `dfa95636` as `test_healthchecks_can_actually_run_where_declared`,
      which pins both the interpreter AND the `$HOSTNAME` target, plus asserts
      shell-less images carry no probe. Verified failing-first against the
      pre-fix stack.
- [x] Verify behaviourally, not just by reading YAML: with a listener on a
      Docker network, the `bash` probe against a **closed** port exits non-zero
      and against the **open** port exits 0. Record both commands and their exit
      codes as the evidence. Done on the live dev containers: fixed probe
      `rc=0` on real ports 9123/9124/8081, `rc=1` on closed 9999, all three
      servers.
- [ ] If a Swarm **manager** is reachable, settle what Swarm does with an
      unhealthy task (create a throwaway service whose probe always fails and
      watch the task states). If no manager is reachable, record the behaviour as
      unverified in the CHG record rather than asserting either documented
      possibility; the fix stands on the false-signal argument alone. Still
      open: this host is a Swarm worker, unchanged.

### Task 3: Deliver the R2 credentials to the Fluss process, fail-closed
**Why:** Static keys are the only working R2 mechanism and the servers currently
receive them as files nothing reads.
**Files:** new `code/01_platform/01_docker/fluss-r2-secrets-from-file.sh`;
`code/01_platform/01_docker/docker-stack.yml` (`configs:` at L148 plus each Fluss
service's `entrypoint:`); new tests in
`code/01_platform/04_scripts/tests/test_fluss_runtime.py`
**Location note:** every existing `configs.file:` source lives flat in
`code/01_platform/01_docker/` (`otel-collector-config.swarm.yaml`,
`alert-consumer.py`), and `configs.file:` paths are relative to the compose
file — so a flat path is the pattern to follow. `flink-runtime/` is the
**script's** design precedent (a `*_FILE` bridge that execs the base entrypoint),
not its location precedent; a subdirectory would also need a matching relative
path in `configs.file:`.
**Depends on:** none (independent of Tasks 1-2, but the `entrypoint:` edit lands
in the same blocks)

- [x] Done in `cedb4405` (fix) + `d508aa51` (tests, 18 in three classes).
      Verified live against the real image in four runs: both variables reach the
      server process with the newline stripped, a missing or empty secret aborts
      `FATAL` with rc=1, and an unset `*_FILE` is tolerated. Write the bridge
      script: for each of `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, read
      `${VAR}_FILE`; if set, require the file to be
      readable and non-empty (after stripping CR/LF), `export` the value, and on
      failure print a `FATAL: ...` line and `exit 1`. Then
      `exec /docker-entrypoint.sh "$@"`. An **unset** `*_FILE` must be tolerated
      (dev passes the values directly).
- [x] Ensure the script is **fail-closed**: a `*_FILE` that is set and unusable
      must abort. Note in the script header that dash does not abort on a failed
      command substitution inside a prefix assignment even under `set -e` — which
      is why the value is read into a variable and checked explicitly rather than
      exported inline.
- [x] Done in `cedb4405`: `fluss-r2-secrets-bridge: file: ./fluss-r2-secrets-from-file.sh`,
      flat in `code/01_platform/01_docker/` as this plan's location note advised.
      Register the script under `configs:` and set each Fluss service's
      `entrypoint: ["/bin/sh", "/opt/fluss/bin/r2-secrets-from-file.sh"]`, with
      `AWS_ACCESS_KEY_ID_FILE` / `AWS_SECRET_ACCESS_KEY_FILE` pointing at
      `/run/secrets/...`. Do **not** put the credentials in `environment:` —
      `/run/secrets` stays the only source.
- [x] Decided: **leave B7 alone.** The bridge is delivered through
      `configs.file:`, and `pipeline_validate_compose_bind_sources` guards
      dev-compose bind sources only — `configs.file:` sources have never been in
      that list. No claim is made that B7 covers config-delivered files; the pin
      lives in `test_fluss_runtime.py::StackWiringTests` instead. Decide whether
      the new file belongs in `pipeline-lib.sh`'s
      `pipeline_validate_compose_bind_sources` list (L147-165). Note what that
      list actually guards: **dev-compose bind-mount sources**, `$LIB_COMPOSE_DIR`
      relative, checked with `-f`. The stack delivers this script through
      `configs.file:`, a different mechanism (`configs.file` sources are not in
      that list today — `alert-consumer.py` appears there as a bind source, not
      as the `configs:` source). Add it only if the file also becomes a dev
      bind-mount source; otherwise leave the guard alone and say so in the CHG
      record. Do **not** claim the B7 guard covers config-delivered files.
- [x] Add tests in `test_fluss_runtime.py` (class names `*Tests`, no `test_`
      helper prefixes) covering: both variables bridged; the `*_FILE` name
      derived from the target variable; trailing newline stripped; unreadable
      file aborts; empty file aborts; unset `*_FILE` is not an error; the `exec`
      line preserves `"$@"`; the header explains the dash caveat. Follow
      `BridgeTests` (`test_flink_runtime_image.py:322`) — assert on
      `code_lines()` output, never raw text, so a comment cannot satisfy a pin.
      Run each test against a deliberately broken variant of the script and
      confirm it names the specific break.
- [x] Verify end-to-end with real files: start a container using the changed
      spec (base image plus a fake secret pair under `/run/secrets`) and confirm
      it prints `Starting Coordinator Server`, then confirm the credentials are
      actually in the process environment. Never print real credential values.

### Task 4: Move `remote.data.dir` to R2 and drop the local volume
**Why:** A node-local, driver-less, unpinned tier directory is not a valid
location for data a cluster must read back.
**Files:** `code/01_platform/01_docker/docker-stack.yml` (four `FLUSS_PROPERTIES`
blocks, four `volumes:` lists, the `volumes:` declaration at L132);
`code/01_platform/04_scripts/stack_selfcheck.sh` (the `required_vars` list at
L107 and the compile-only defaults from L123); `code/01_platform/04_scripts/tests/test_09_stack.py`
**Depends on:** Task 3 (credentials must be in the environment, or these `s3.*`
keys resolve to nothing)

- [x] Done in `4f338631` (fix) + `4ec56985` (tests). Red leg: 2 failed
      pre-change. Replace `remote.data.dir: /tmp/fluss/remote-data` with
      `remote.data.dir: s3://${R2_BUCKET:?set R2_BUCKET for tiering}/remote-data`
      on all four services, and add the dev key set — `s3.endpoint`,
      `s3.endpoint.region`, `s3.access-key: $${env.AWS_ACCESS_KEY_ID}`,
      `s3.secret-key: $${env.AWS_SECRET_ACCESS_KEY}`, `s3.path.style.access`,
      `s3.connection.ssl.enabled` — matching `docker-compose.yml:388-394`. Note
      the escaping contract: single `$` for values compose interpolates at render
      time, double `$$` for the `${env....}` placeholders that must survive to the
      container.
- [x] Delete the `fluss-remote-data` mounts from all four services and its
      declaration at L132. Confirm nothing else references it
      (`grep -rn fluss-remote-data`).
- [x] Add `R2_BUCKET` to `stack_selfcheck.sh`'s `required_vars` (L107), the
      message that lists what is missing, and the compile-only placeholder
      defaults (from L123). It joins the other `:?` variables, so DEPLOY=1
      refuses to run with it unset. Update `REAL_ENV` in
      `test_stack_selfcheck.py:20-34` to match.
- [x] Update `test_09_stack.py`: drop `fluss-remote-data` from
      `TestVolumes.test_durable_volumes_declared` (L140-149) and add an assertion
      that no service carries a local `remote.data.dir` — asserting on the
      `FLUSS_PROPERTIES` value, not on prose. Verify both the new test and the
      volume test fail against the pre-change file and pass after.
- [x] Confirm the fail-closed behaviour. **This checkbox's premise was wrong,
      found during execution:** `docker stack config` with `R2_BUCKET` unset does
      **not** error — the `:?` guard does not fire inside the `FLUSS_PROPERTIES`
      block scalar. Measured: rc=0 and it renders `s3:///remote-data`. The same
      holds in the dev twin, so this is a compose behaviour, not a stack typo.
      The fail-closed behaviour therefore moved to `stack_selfcheck.sh`'s
      `DEPLOY=1` gate, which refuses an unset or empty `R2_BUCKET` and names it
      before any deploy; `test_deploy_without_r2_bucket_names_it` pins it and
      the render with a bucket set leaves `s3.access-key: ${env.AWS_ACCESS_KEY_ID}`
      **unexpanded** in a real run (verified).

### Task 5: Update the production documentation
**Why:** `09-production-swarm.md` recounts the M2 deployment and the CHG-179
Flink fix; leaving the Fluss half undocumented repeats the gap that made the
first blocker invisible.
**Files:** `docs/08_implementation/09-production-swarm.md`
**Depends on:** Tasks 1-4

- [x] Done: new section *The Fluss startup fix (CHG-182 fixes CHG-181 — Tasks
      1-4)* before *Storage and recovery*, with the four-defect table, the four
      bridge runs, the live tiering write, and the not-verified list; the storage
      bullet now reads hot-segments-local / tiered-segments-on-R2. Document the
      two defects and their fixes in the section that already
      narrates the Flink startup fix (around L255, which discusses `command:` and
      `AWS_REGION`), with the verified evidence: the usage-plus-exit-0 render, the
      `command=None` render, and the dash-versus-bash probe result.
- [x] Document the tiering location change and the credential path (file secrets
      to bridge to `${env.*}` to Hadoop at read time).
- [x] Note the residual risk that the plugin jars for lake tiering (`datalake.*`)
      are still unwired in production, cross-referencing *Post-Completion*.

### Task 6: Record the change and close the lane
**Why:** The repo's change control is a gate, not paperwork; every wave here has
landed with a CHG record.
**Files:** new `docs/05_deployment/change-records/CHG-181.md` (confirm the next
free number by listing the directory)
**Depends on:** Tasks 1-5

- [x] Done: `docs/05_deployment/change-records/CHG-182.md` (**not** CHG-181 as
      this plan assumed — 181 was already filed as the record-of-defect, so 182
      is the record of the fix). Write the CHG record in the established format
      (see CHG-180): header with
      status/filed/closed, the `text` metadata block (`change_record_id`, `scope`,
      `affected_artifacts`, `compatibility_class`, `savepoint_impact`,
      `test_updates`, `rollback_behavior`, `plan_tasks`), then what was wrong and
      what changed. Be explicit about the healthcheck change and about what was
      **not** verified.
- [x] State the red-leg evidence for each new test (which test failed against
      which pre-change file and named which break) — the standard the repo holds
      test claims to.
- [x] Run the gates and record them:
      `python3 code/01_platform/04_scripts/change_control_check.py` (all records
      complete), `docs_audit.py`, the banned-substring set, `pytest` on the
      touched suites, and `shellcheck -S warning` on the new script.

### Task 7: Verify acceptance criteria
**Depends on:** Tasks 1-6

- [x] `make test-09` green, including the new command, healthcheck and tiering
      assertions.
- [x] `make stack-selfcheck` — see *Required External or Manual Verification* for
      the environment caveat: this host is a Swarm **worker**
      (`ControlAvailable=false`), so the script exits 1 at "cannot determine this
      node's swarm NodeID" **before** reaching the stack. On the worker host the
      equivalent compile check is the manual `docker stack config` invocation with
      the placeholder values (13 required vars after Task 4 added `R2_BUCKET`, not
      the 12 this plan assumed); record which was actually run. **Run:** the manual
      `docker stack config` with all 13 placeholders, rc=0 — renders
      `coordinatorServer` once, `tabletServer` three times, `jobmanager` and
      `taskmanager`, 4x `remote.data.dir: s3://placeholder-bucket/remote-data`,
      and 0 references to `fluss-remote-data`. `make stack-selfcheck` was also run
      and exits 1 at "cannot determine this node's swarm NodeID", as this section
      predicted.
- [x] `pytest` on the full touched suite list, reporting the pass count and any
      skip.
- [x] Confirm no credential value appears in any committed file. Grep the changed
      files for AWS access-key prefixes and for a long literal value on any
      `*-key:` / `*-secret:` line — the committed forms must be `${env....}`
      placeholders or `/run/secrets/...` paths only. Write the pattern so it does
      not match its own text in this plan.
- [x] Landed across the lane's commit pairs: `de88cac2`+`dfa95636` (Tasks 1-2),
      `cedb4405`+`d508aa51` (Task 3), `4f338631`+`4ec56985` (Task 4), then this
      docs/CHG commit. Each fix preceded its test. Commit in the repo's order —
      fix commit, test commit, docs/CHG commit —
      showing the diffstat for approval first. Never `git add -A` / `git add .`;
      never amend, rebase, force-push or push.
## Technical Details

**Escaping contract in `docker-stack.yml`.** `docker stack config` interpolates
every `$` in the file, including inside `entrypoint:` strings. Use a single `$`
for anything compose should resolve at render time (`${R2_BUCKET:?...}`,
`${R2_ENDPOINT:?...}`) and `$$` for anything the container must receive literally
(`$${env.AWS_ACCESS_KEY_ID}`). Verified renderings: `$${env....}` becomes
`${env....}`; `$${R2_BUCKET:?...}` becomes `${R2_BUCKET:?...}` — so the `:?` guard
is **not** evaluated with a double `$`, and the guard only works with a single
`$`. A single `$` with the variable unset fails the render.

**Why the bridge is a config, not an inline string.** Compose's interpolation
pass rejects a `$`-bearing inline `entrypoint:` command, and `$$` escaping
corrupts the script text. Delivering the script as a Swarm config sidesteps the
interpolation entirely and matches the existing `otel-collector-config` /
`alert-consumer-src` pattern.

**Invoke the config script through an interpreter.** The entrypoint is
`["/bin/sh", "/opt/fluss/bin/<script>"]` rather than the script path alone, so it
does not depend on the config's file mode carrying an executable bit. The
script's shebang and `+x` bit are then irrelevant at runtime.

**`configs.file:` is resolved relative to the compose file.** Verified: running
`docker stack config -c <abs path>/s.yml` from an unrelated working directory
resolved a `./bridge.sh` source to that compose file's own directory. A
subdirectory source needs the matching relative path.

**Swarm configs are immutable, and untested here.** A `docker config` cannot be
replaced in place; a redeploy that changes this script needs the config (or the
stack) removed and recreated. Every step of that procedure requires a Swarm
**manager**, which this host is not — so the config-delivery path in this plan is
verified only up to `docker stack config` rendering. The first real deploy must
confirm the file lands where the entrypoint expects it and that `docker stack
deploy` accepts the config. Treat it as an unproven leg in the CHG record until
then.

**`${env.NAME}` is expanded late, by Hadoop.** `envsubst` in the entrypoint
cannot expand it (the dotted name is not a valid shell variable), so the literal
placeholder survives into `conf/server.yaml` and Hadoop's
`EnvironmentVariableCredentialsProvider` resolves it from the process
environment at read time. This is why the bridge must export real environment
variables rather than writing them into the config.

**Ordering inside the entrypoint chain.** The bridge `exec`s the base
`/docker-entrypoint.sh`, which still performs its own `sed`, `FLUSS_PROPERTIES`
append and `envsubst` before launching the server. Replacing `ENTRYPOINT` must
therefore preserve the `"$@"` hand-off, or the server type never arrives.

**Healthcheck interpreter.** `CMD-SHELL` in this image is dash; dash has no
`/dev/tcp`. The failure is silent (the probe is wrapped in `>/dev/null 2>&1`),
which is why it survived review. Any replacement must be verified against both an
open and a closed port — an always-failing probe and a never-failing probe are
both wrong.

## Required External or Manual Verification

| Check | Acceptance criterion | Procedure | Owner / environment | Authorization | Evidence required |
|---|---|---|---|---|---|
| Compile/render of the stack | All four Fluss services render `command:`, an `s3://` `remote.data.dir`, and no `fluss-remote-data` mount | `docker stack config -c docker-stack.yml` with the 12 placeholder values, parsed as YAML | Anyone with the docker CLI; **no swarm manager needed** | none | The parsed per-service `command` / entrypoint / properties lines |
| Real deploy of the production stack | The four Fluss services run, report healthy, and tier to R2 | `DEPLOY=1 bash stack_selfcheck.sh` after creating the Swarm secrets, or a real `docker stack deploy` on the target cluster | Requires a Swarm **manager**; this host is a worker and cannot run it | **user's explicit word** — it creates swarm resources and secrets | `docker service ps` showing running+healthy tasks and the tiered-object listing |
| Secret bridge against real credentials | The Fluss process sees the R2 credentials and starts | Start a container from the changed spec with the real secret files mounted | Any Docker host with the credentials in a gitignored env file | none (never print values) | `Starting Coordinator Server` plus a boolean check that the variable is set |
| Tiered read from a second client | A segment written by one tablet is readable after tiering | Not achievable today: the dev run produced 0 remote-only segments, and forcing one needs a destructive delete of local segments | Deferred | **user's explicit word** (destructive) | Deliberately left open; record as unverified |

## Post-Completion

- **How the tiering write was proven, and what is still open.** The trial proved
  the copy, the manifest commit and the bucket objects (833 objects / 66 MB /
  208 `.log` segments, counted after the trial was stopped so the figure could
  not drift). It did **not** prove a read served from R2: every row a `SELECT`
  returned came off local disk, and a tablet started against a fresh empty local
  directory did **not** re-read from R2 (`Reset remote end offset to local end
  offset`, then `COUNT(*)` = 0). Production gives each tablet its own data volume,
  so R2 is a tiering target and not a recovery source for a lost local log —
  state that in operations docs rather than implying R2 restores a wiped node.
  Two measurement traps are recorded in CHG-182: a small write proves nothing
  about tiering (only *closed* segments are copied), and wiping local disk proves
  nothing about the write path.
- **Trial resources were cleaned up**: the two trial containers, the
  `/trial-chroot` ZooKeeper subtree (the dev `/fluss` root was left untouched),
  the 931 objects under the three trial R2 prefixes, and the `/tmp` scripts. The
  dev five-container cluster was left running and unchanged.
- **Lake tiering (`datalake.*`) in production is still unwired.** The four
  services carry `datalake.iceberg.*` keys pointing at `${S3_WAREHOUSE_PATH}`, but
  production mounts **no** plugin jars, while dev bind-mounts `fluss-fs-s3`,
  `fluss-fs-hdfs` and `fluss-fs-hadoop-shaded` into `plugins/iceberg/` — dev's own
  comment says the iceberg classloader needs the first two. The stock image bakes
  `plugins/s3/` and `plugins/hdfs/` but not the shuffled-namespace
  `hadoop-shaded` jar, which is absent from the image entirely. Whether lake
  tiering works in production without those mounts is **unverified** and needs its
  own investigation; it is out of scope here because it does not affect log/KV
  tiering to R2.
- **`FLINK_IMAGE` is still un-pinned** — the locally built image must be pushed
  and digest-pinned (`digest-pin.sh`) before `runtime.lock` can reference it.
- **Production deploy verification** needs a 4-VM Swarm; the single-node harness
  on this worker cannot substitute for it.
