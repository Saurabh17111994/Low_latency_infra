# Trading_project — common dev commands (MVP scaffold)
# Run from the repo root: make <target>

# S2 (2026-08-29): secrets live in secrets.env (git-ignored); both env files are
# loaded at parse time so ${VAR} interpolation sees the secrets.
COMPOSE := docker compose --env-file code/01_platform/01_docker/.env --env-file code/01_platform/01_docker/secrets.env -f code/01_platform/01_docker/docker-compose.yml

# Every target that mutates the shared stack takes the gate's lock first:
# one stack, two writers — see code/01_platform/04_scripts/stack-lock.sh.
STACK_LOCK := bash code/01_platform/04_scripts/stack-lock.sh
# R-143: default to ONLINE maven (a fresh checkout has an empty ~/.m2 and -o
# fails obscurely). Set MVN_FLAGS=-o when the local cache is warm.
MVN := mvn $(MVN_FLAGS)

.PHONY: help env ddl up down logs build clean cep-check cep-check-module test test-ingestion test-audit-r2 drill-live execution-network-check gate gate-order static-check docs-audit stale-tables full-audit pin-check ddl-apply-smoke ddl-image evidence-ownership-check test-09 stack-selfcheck stack-config seed-dashboards rollout-savepoint chaos-suite gate-fast check-image-stale check-image-stale-fast images branch-check proto flink-image fluss-image

# P6-302: these recipes create no file of their own name, so a stray file in the
# repo root would make make treat the target as up to date and skip the recipe.
.PHONY: test-local test-network test-08-phaseA test-08-phaseB test-08-phaseC test-08-phaseD test-execution test-failure test-observability test-performance test-25-smoke test-prod-hardening test-all test-all-plus-prod alert-routing-test check-loadtest-env check-ingestion-clean test-loadtest-guards loadtest-20k-regression disaster-drills eod-controller holistic holistic-quick

# Branch guard: work must happen on `main` — the single working branch per
# AGENTS.md 'Branch Context'. Any agent (human or AI) MUST run this before
# editing code. Fails (exit 1) with a clear message if the branch is wrong.
branch-check:
	@branch=$$(git branch --show-current); \
	if [ "$$branch" != "main" ]; then \
		echo "ERROR: on branch '$$branch' — work is ONLY allowed on"; \
		echo "       'main'. Switch first or ask the operator."; \
		echo "       (AGENTS.md 'Branch Context'; contract §header)."; \
		exit 1; \
	fi; \
	echo "OK: on main"

# Regenerate protobuf code (T1). Requires protoc + protoc-gen-go on PATH.
# Outputs: go-bridge/marketdata/market_data.pb.go + Java transport classes.
proto:
	@echo "== regenerating proto code from proto/market_data.proto =="
	@protoc --version >/dev/null 2>&1 || { echo "ERROR: protoc not found"; exit 1; }
	protoc --proto_path=proto \
	  --go_out=code/02_services/01_ingestion/go-bridge/marketdata --go_opt=paths=source_relative \
	  --java_out=code/02_services/01_ingestion/src/main/java \
	  proto/market_data.proto
	@echo "== done =="

help:
	@echo "Targets:"
	@echo "  env    copy code/01_platform/01_docker/.env.example to code/01_platform/01_docker/.env, then add secrets"
	@echo "  ddl    validate + emit schema manifest; APPLY=1 EVIDENCE=<file> runs the full 9-step"
	@echo "         application contract (empty catalog, apply, parity, smoke, evidence record; PASS"
	@echo "         only when every table smoke passes — composite-PK limitations need"
	@echo "         DDL_APPLY_ACK_LIMITATIONS=<tables>); FLUSS_BOOTSTRAP=<host:port> picks the"
	@echo "         cluster; DDL_APPLY_TABLE_PREFIX=<p> for dev-scratch verification only"
	@echo "         APPLY=1 cannot complete FROM THE HOST on a multi-tablet cluster: the client follows"
	@echo "         each tablet's advertised address (compose DNS), so run it in-network:"
	@echo "         docker compose run --rm --no-deps ddl-apply apply  (see ddl-image below)"
	@echo "  ddl-apply-smoke  live regression smoke for the exit-code contract (0/6/1 + sentinels):"
	@echo "         runs the orchestrator 3x against scratch-prefixed catalogs (PASS / refused /"
	@echo "         acknowledged-auto) + a containerized bad-ownership drill (pre-seeded 644 record"
	@echo "         -> apply exit 1 + EVIDENCE OWNERSHIP CHECK FAILED) when docker + the ddl-apply"
	@echo "         image are available; env-gated on FLUSS_BOOTSTRAP, wired into make gate"
	@echo "  ddl-image  build the ddl-apply contract image (one-shot container on trading-net;"
	@echo "         run: docker compose -f code/01_platform/01_docker/docker-compose.yml run"
	@echo "         --rm ddl-apply {validate|apply|smoke|self-test}) — FLUSS_BOOTSTRAP resolves"
	@echo "         via compose DNS, no host /etc/hosts aliases"
	@echo "  evidence-ownership-check  non-root ownership contract gate: evidence root setgid 2775,"
	@echo "         every container-written record group-writable with the engine GID, none root-owned;"
	@echo "         wired into docs-audit C15 + the Monday gate DDL step"
	@echo "  up     docker compose up -d, full stack"
	@echo "  down   docker compose down"
	@echo "  logs   tail compose logs"
	@echo "  build  build all service images"
	@echo "  clean  stop stack + remove volumes"
	@echo "  cep-check   fail if Flink CEP is referenced (project policy)"
	@echo "  cep-check-module  CI-style module check: cep_guard.sh scoped to the compute module + the"
	@echo "         SIG-UNIT-007 CepDependencyGuardTest, with an agreement/scope-parity assertion"
	@echo "  test        run unit tests (common + ingestion)"
	@echo "  test-ingestion  run only the ingestion module tests"
	@echo "  test-audit-r2   run audit_r2.py unit tests (stdlib unittest, no R2 access needed)"
	@echo "  drill-live  LIVE Fluss drills (common + gateway): the classes gated on"
	@echo "         FLUSS_BOOTSTRAP that make test and the Monday Java step record as 0"
	@echo "         tests. SKIPPED when FLUSS_BOOTSTRAP is unset; reports land in"
	@echo "         target/surefire-reports-drills (docs-audit C6 counts the plain suite)"
	@echo "  execution-network-check  verify resolved Compose execution-net/Arrow-egress isolation"
	@echo "  gate        run the full Monday verification gate (static + compose + go + java + schema/perf)"
	@echo "  gate-order  mandatory implementation order gate (01-foundation.md): 7 tasks in sequence,"
	@echo "              stops if any upstream task's acceptance checks are red or missing"
	@echo "  static-check  bash -n + shellcheck every repo shell script"
	@echo "  docs-audit  doc-vs-code truth gate (foundation L388): manifest, ownership matrix,"
	@echo "              schema-state diagram, compat vocabulary, stale phrases, test counts, version pins"
	@echo "  stale-tables  doc truth scan (dossiers + upstream layers: decisions,"
	@echo "              requirements, architecture, contracts, deployment minus change-records):"
	@echo "              fail when a line reads feature_candles_15s as LOG,"
	@echo "              Signal_Candidates as KV, or feature_candles_15s_current as live"
	@echo "              without a historical/superseded annotation (2026-08-13 re-scope),"
	@echo "              a stale phase-status claim, or a drifted count (21 tables vs 24,"
	@echo "              151 acceptance IDs vs 152, common/ingestion/compute test counts"
	@echo "              and docs-audit C6 line N/N/N citations vs the truth line in 01-foundation.md)"
	@echo "              (forming-bar postponed, ranking/reservation postponed,"
	@echo "              Trade_Decisions active) without a status marker"
	@echo "  full-audit  run the whole doc audit in one command: the three gates (stale-tables,"
	@echo "              docs-audit, --ddl parity) + the beyond-scanner sweeps (live ranking/"
	@echo "              reservation claims, stale 'pending implementation' prose) + the"
	@echo "              dossier-trio coherence checks (04-signal-job / 13 / 14 agree on the"
	@echo "              re-scope, DEC-038 landing, and P11 status) — exit 0 only when all green"
	@echo "  gate-fast   fast pre-gate subset: static-check + the python suites + the ddl-apply"
	@echo "              image + an optional MODULE=<module> suite (~20 s). NOT a release"
	@echo "              certificate — releases, market sessions and soak runs need \`make gate\`"
	@echo "  flink-image build the production Flink runtime image (Fluss jars + R2 config +"
	@echo "              secret bridge) from SHA256-pinned Maven Central artifacts"
	@echo "  fluss-image build the production Fluss server image (stock apache/fluss + the two"
	@echo "              lake plugins in plugins/iceberg/) from SHA256-pinned artifacts"
	@echo "  images      rebuild every build: image WITH its content stamp (sha256 of its"
	@echo "              inputs, baked in as build.labels) and verify with check-image-stale"
	@echo "  pin-check   pin discipline (foundation L548/553/554): matrix shape, corpus integrity,"
	@echo "              external-SNAPSHOT ban, platform version pins"
	@echo "  test-09     offline static validation of docker-stack.yml (no swarm/VM needed):"
	@echo "              label-only placement, no build/depends_on/ports, encrypted overlays,"
	@echo "              external secrets, durable volumes, replicas scale"
	@echo "  stack-selfcheck  one-host Swarm mimic: docker swarm init (single node), label it"
	@echo "              role=worker+observability, then docker stack config. DEPLOY=1 also runs"
	@echo "              docker stack deploy -c docker-stack.yml prod; DOWN=0 leaves it up (M2 gate)"
	@echo "  stack-config  compile-only via docker stack config (needs docker; catches schema errors)"
	@echo "  seed-dashboards  idempotent OpenObserve dashboard provisioning (D7); needs O2_PASSWORD"
	@echo "  rollout-savepoint  G5/T12: savepoint -> stop -> redeploy SignalJob with"
	@echo "         STATE_RECOVERY_PATH=<fresh savepoint> + ALLOW_FULL_REPLAY=false, verify restore"
	@echo "         (dedup continuity). Env: JAR, JOB_ID/JOB_NAME, RECOVERY_PATH, JM_URL,"
	@echo "         SAVEPOINT_DIR, COMPOSE_FILE, DRY_RUN=1; required pinned job env (DEDUP_TTL_MS,"
	@echo "         CANDLE_WINDOW_MS, CHECKPOINT_INTERVAL_MS, CHECKPOINT_TIMEOUT_MS,"
	@echo "         MAX_CONCURRENT_CHECKPOINTS) must be exported. See docs/08_implementation/21-savepoint-rollout.md"
	@echo "  holistic    full end-to-end measurement (holistic-measure.sh): raw<->candle zero-loss"
	@echo "              parity over candle_closed, dedup/late inject gates, latency legs. Writes"
	@echo "              logs/tracker-14/holistic-measure-<ts>/. Needs the stack up (make up) and a"
	@echo "              QUIET cluster — the time-sensitive probes are invalid while it is deleting"
	@echo "              or re-electing. Result: exit 0 = every measurable gate passed; the legs the"
	@echo "              live schema cannot measure are printed as UNAVAILABLE, not as failures"
	@echo "  holistic-quick   same harness, short phases (SMOKE_S=60 MAIN_S=300)"

env:
	@if [ ! -f code/01_platform/01_docker/.env ]; then \
		cp code/01_platform/01_docker/.env.example code/01_platform/01_docker/.env; \
	fi
	@echo "Created code/01_platform/01_docker/.env — edit it with real secrets."

ddl:
	@python3 code/01_platform/04_scripts/ddl_apply.py \
		$(if $(APPLY),--apply-verified,) $(if $(EVIDENCE),--matrix-evidence $(EVIDENCE),)
	@echo "(Plain 'make ddl' only validates; run 'make ddl APPLY=1 EVIDENCE=<file>' to execute the contract.)"

up:
	$(STACK_LOCK) $(COMPOSE) up -d
	@bash code/01_platform/04_scripts/catalog-guard.sh \
		|| echo "!!! catalog-guard: catalog NOT healthy — see messages above. Fix before trading."

down:
	$(STACK_LOCK) $(COMPOSE) down

logs:
	$(COMPOSE) logs -f

build:
	cd code && $(MVN) -q package -pl 02_services/01_ingestion -am -DskipTests

# Fail the build if Apache Flink CEP (Complex Event Processing) is referenced.
# Project rule: no CEP dependency in the MVP order path.
cep-check:
	@bash code/01_platform/04_scripts/cep_guard.sh .

# CI-style module-scoped check: the shell guard and the SIG-UNIT-007 JUnit test
# must both pass and agree on the scanned file set (cep_module_check.sh).
cep-check-module:
	@bash code/01_platform/04_scripts/cep_module_check.sh

# Run all unit tests (common + ingestion modules).
test:
	cd code && $(MVN) -q test -pl common,02_services/01_ingestion

# Run only the ingestion module tests.
test-ingestion:
	cd code && $(MVN) -q test -pl 02_services/01_ingestion -am

# Live Fluss drills — every test class gated on FLUSS_BOOTSTRAP. `make test` and
# the Monday gate's Java step record these as 0 tests (the gate exports only the
# INGESTION_INT_TEST_* flags), and the gateway module is outside that step's scope
# (-pl 02_services/01_ingestion -am, which rides on common only as a dependency),
# so these are live paths nothing else runs. The gate arms them now (it defaults
# FLUSS_BOOTSTRAP for its DDL smoke), which is why this target exists: it is the
# cheap loop for what the gate only checks once every 16 min (measured 2026-09-13:
# 387 s total, 88 s of it compute's B4 leg). Compute's harness
# drills (tablet-kill, chaos, object-store checkpoint) still need their own flags
# and belong to the compute/chaos targets; its B4 signal-to-intent E2E gates on
# FLUSS_BOOTSTRAP alone, so it runs here too -- from compute's own pom (R-272
# keeps 02_compute out of the code/pom.xml reactor).
# The drill-reports profile (code/pom.xml) sends the reports to
# target/surefire-reports-drills on purpose: docs-audit C6 sums
# target/surefire-reports against the documented plain-suite triple, and a live
# run rewrites those same class XMLs with real (non-zero) test counts.
# Env-gated: SKIPPED (exit 0) when FLUSS_BOOTSTRAP is unset.
drill-live:
	@if [ -z "$$FLUSS_BOOTSTRAP" ]; then \
		echo "SKIP: drill-live (no FLUSS_BOOTSTRAP) — export FLUSS_BOOTSTRAP=<host:port>"; \
	else \
		cd code && $(MVN) test -Pdrill-reports -pl common,02_services/06_execution_gateway \
			-Dtest='EodBucketCopyIntegrationTest,FlussGateAttemptStoresIntegrationTest,FlussPositionsStateStoreIntegrationTest,FlussPostbackQuarantineStoreIntegrationTest,CompatFlussIntegrationTest,CompatFlussCompositeKeyIntegrationTest,CompatFlussDdlParityIntegrationTest,DdlSmokeTwinSweepTest,GateMergeEngineDrillIntegrationTest,GatewayFlussIntegrationTest,GatewayFlussDurableReplayIntegrationTest,B4HaltedIntentConsumeDeferE2ETest,GatewayStartupPrewarmTest,FlussProjectionWriterIntegrationTest' \
			-Dsurefire.failIfNoSpecifiedTests=false \
			&& cd 02_services/02_compute && $(MVN) test -Pdrill-reports \
				-Dtest='B4SignalIntentE2ETest' \
				-Dsurefire.failIfNoSpecifiedTests=false; \
	fi

# audit_r2.py unit tests (stdlib unittest — SigV4 golden vector, config
# parsing, provisioning/validation against an in-memory fake client).
test-audit-r2:
	python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
	python3 -m pytest code/01_platform/04_scripts/tests -q -p no:cacheprovider -p no:unittest

# Live regression smoke for the DDL apply exit-code contract (0 full PASS / 6
# acknowledged PASS_WITH_LIMITATION / 1 refused) + the machine-readable
# sentinels. Env-gated: SKIPPED when FLUSS_BOOTSTRAP is unset; wired into the
# Monday verification gate (run-monday-gates.sh) after the Java full gate.
# T8: no public execution route, HALTED defaults, no Arrow on compute — the
# offline compose contract (CHG-047). Wired into the Monday gate.
execution-network-check:
	@python3 code/01_platform/04_scripts/execution_network_check.py --compose code/01_platform/01_docker/docker-compose.yml

# CHG-101: no compose `build:` image older than the last change to the source
# it packages. The 2026-08-24 gateway/bridge incident (08-20 images vs 08-24
# source) was found only by chance — this makes it a machine gate. Run before
# any enable (T8/T9) or Monday-gate step.
check-image-stale:
	@python3 code/01_platform/04_scripts/image_staleness_check.py --git-root . --compose code/01_platform/01_docker/docker-compose.yml

# Fast pre-gate confidence (2026-09-12): the subset that catches most of what an
# ordinary edit breaks, in ~20 s measured (warm ~/.m2) instead of the gate's ~13 min. It is NOT a
# certificate -- the live Fluss drills, the DDL apply smoke, the full doc audit (scanners/
# sweeps/trio), the Go bridge suite and every module suite except MODULE=<module> do not run
# here; the docs_audit.py script (C1-C16 doc<->code truth, ~1 s) does run. Image staleness
# covers ddl-apply only. Releases, market sessions and soak runs require `make gate`; that
# is the only thing that certifies a tree.
# Static DDL-manifest check first: a DDL byte change (even a comment-only edit) with no
# manifest refresh fails here in seconds instead of at gate step 9 an hour later.
gate-fast:
	@$(MAKE) --no-print-directory ddl
	@echo "GATE-FAST: NOT A RELEASE CERTIFICATE — no live drills, no DDL apply smoke, no full doc audit (scanners/sweeps/trio), no Go suite; images beyond ddl-apply unchecked."
	@$(MAKE) --no-print-directory static-check
	@$(MAKE) --no-print-directory test-audit-r2
	@echo "GATE-FAST: pin discipline (pin-check.sh: matrix, corpus digests, SNAPSHOT ban, image/toolchain pins)"
	@$(MAKE) --no-print-directory pin-check
	@$(MAKE) --no-print-directory check-image-stale-fast
	@if [ -n "$(MODULE)" ]; then cd code && $(MVN) test -pl $(MODULE); else echo "GATE-FAST: no MODULE= given — module suite skipped (e.g. make gate-fast MODULE=02_services/06_execution_gateway)"; fi
	@echo "GATE-FAST: doc<->code truth audit (docs_audit.py, C1-C16)"
	@python3 code/01_platform/04_scripts/docs_audit.py
	@echo "GATE-FAST RESULT: subset green (run 'make gate' before a release)"

# Twin of Monday-gate step [8/14]: the one service image that gate actually
# starts (ddl-apply, in step 11). Keep the scope in step with run-monday-gates.sh.
check-image-stale-fast:
	@python3 code/01_platform/04_scripts/image_staleness_check.py --git-root . --compose code/01_platform/01_docker/docker-compose.yml --service ddl-apply

# Rebuild every build: image WITH its content stamp (CHG-124). The stamp is
# sha256 over the image's inputs; compose bakes it into build.labels, so
# check-image-stale can verify freshness by content instead of by timestamp
# (which is unsound: an image built from committed content just before the
# commit looks STALE forever, and a cache-hit rebuild never clears it).
# Fail-closed: no stamps computed => build nothing.
# Content-addressed rebuild (CHG-124). The stamps must ride on the build
# command's own environment (`env VAR=... compose build`): `eval`ing them would
# set shell-only variables that the compose child never sees, every image would
# stay unlabelled and the checker would pass by clock — the exact no-op caught
# on 2026-09-12. --require-stamps keeps that failure loud instead of silent.
images:
	@stamps="$$(python3 code/01_platform/04_scripts/image_staleness_check.py --git-root . --print-stamps-env)" || exit 1; \
	[ -n "$$stamps" ] || { echo "images: no build stamps computed — refusing to build unstamped images"; exit 1; }; \
	$(STACK_LOCK) env $$stamps $(COMPOSE) build $$(python3 code/01_platform/04_scripts/image_staleness_check.py --git-root . --print-services) && \
	python3 code/01_platform/04_scripts/image_staleness_check.py --git-root . --compose code/01_platform/01_docker/docker-compose.yml --require-stamps

# 08 Local Compose Phase A — L0-L4 (offline + gated container probes)
test-local:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l0.py -v

test-network:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l2.py -v

test-08-phaseA:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l0.py code/01_platform/04_scripts/tests/test_08_local_compose_l2.py code/01_platform/04_scripts/tests/test_08_local_compose_l1_l3.py code/01_platform/04_scripts/tests/test_08_local_compose_l4.py -v

test-execution:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l6_l7.py code/01_platform/04_scripts/tests/test_08_local_compose_l10.py -v
	@python3 code/01_platform/04_scripts/local_int_004_smoke.py --offline

test-08-phaseB:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l6_l7.py code/01_platform/04_scripts/tests/test_08_local_compose_l10.py code/01_platform/04_scripts/tests/test_08_local_compose_l0.py code/01_platform/04_scripts/tests/test_08_local_compose_l2.py -v

test-failure:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l8.py -v

test-08-phaseC:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l8.py code/01_platform/04_scripts/tests/test_08_local_compose_l6_l7.py code/01_platform/04_scripts/tests/test_08_local_compose_l10.py -v

test-observability:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l9.py -v

test-performance:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l11.py -v

test-08-phaseD:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l9.py code/01_platform/04_scripts/tests/test_08_local_compose_l11.py -v

test-25-smoke:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_25.py -v
	@python3 code/01_platform/04_scripts/local_int_004_smoke.py --offline --instruments 25

test-prod-hardening:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_prod.py -v

test-all:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l0.py code/01_platform/04_scripts/tests/test_08_local_compose_l1_l3.py code/01_platform/04_scripts/tests/test_08_local_compose_l2.py code/01_platform/04_scripts/tests/test_08_local_compose_l4.py code/01_platform/04_scripts/tests/test_08_local_compose_l6_l7.py code/01_platform/04_scripts/tests/test_08_local_compose_l8.py code/01_platform/04_scripts/tests/test_08_local_compose_l9.py code/01_platform/04_scripts/tests/test_08_local_compose_l10.py code/01_platform/04_scripts/tests/test_08_local_compose_l11.py -v
	@python3 code/01_platform/04_scripts/local_int_004_smoke.py --offline

test-all-plus-prod:
	@pytest code/01_platform/04_scripts/tests/test_08_local_compose_l0.py code/01_platform/04_scripts/tests/test_08_local_compose_l1_l3.py code/01_platform/04_scripts/tests/test_08_local_compose_l2.py code/01_platform/04_scripts/tests/test_08_local_compose_l4.py code/01_platform/04_scripts/tests/test_08_local_compose_l6_l7.py code/01_platform/04_scripts/tests/test_08_local_compose_l8.py code/01_platform/04_scripts/tests/test_08_local_compose_l9.py code/01_platform/04_scripts/tests/test_08_local_compose_l10.py code/01_platform/04_scripts/tests/test_08_local_compose_l11.py code/01_platform/04_scripts/tests/test_08_local_compose_prod.py code/01_platform/04_scripts/tests/test_08_local_compose_25.py -v
	@python3 code/01_platform/04_scripts/local_int_004_smoke.py --offline --instruments 10
	@python3 code/01_platform/04_scripts/local_int_004_smoke.py --offline --instruments 25

# 09 Production Swarm — OFFLINE static validation of docker-stack.yml. No
# swarm, no VMs, no docker needed. Gates M2 (offline prep): every service has a
# pinned image + deploy block; placement is by node LABEL (never hostname, so
# v1 v→2 needs no rewrite); no Swarm-ignored keys; encrypted overlays; external
# secrets; durable volumes; replicas scale to v2 workers. Live SWARM-MGR-* quorum
# tests are M3 (multi-VM) and intentionally NOT here.
test-09:
	@pytest code/01_platform/04_scripts/tests/test_09_stack.py -v

# One-host Swarm mimic (M2): init a single-node swarm, label it role=worker +
# role=observability, then `docker stack config` to prove the stack compiles.
# DEPLOY=1 additionally runs `docker stack deploy -c docker-stack.yml prod`;
# DOWN=0 leaves the stack up. Skips cleanly if docker/daemon is absent (offline
# prep then relies on `make test-09` for static coverage).
stack-selfcheck:
	@bash code/01_platform/04_scripts/stack_selfcheck.sh

# Compile-only via `docker stack config` (needs the docker CLI + a swarm node);
# catches deploy-schema/YAML errors that the offline test can't. Add DEPLOY=1
# to actually deploy the prod stack from the same command.
stack-config:
	@bash code/01_platform/04_scripts/stack_selfcheck.sh $(if $(DEPLOY),DEPLOY=1,) $(if $(DOWN),DOWN=$(DOWN),)

# Idempotent OpenObserve dashboard provisioning (D7): ensures every dashboard
# in code/01_platform/01_docker/openobserve/dashboards/ exists in the local O2
# org. O2_PASSWORD must be in the environment (from .env — never defaulted by
# the tool; exit 2 otherwise). O2_API_URL defaults to http://localhost:5080.
# Example: O2_PASSWORD=$(grep ^O2_PASSWORD= code/01_platform/01_docker/.env | cut -d= -f2) make seed-dashboards
seed-dashboards:
	@python3 code/01_platform/04_scripts/seed_dashboards.py $(ARGS)

# G6 alert routing (2026-08-31): end-to-end proof of O2 alert rule ->
# dev-webhook destination -> alert-consumer JSONL persistence. Creates a
# temporary always-firing probe alert, waits for the durable record, asserts
# classification, deletes the probe. Also the negative proof: malformed
# delivery -> 400, consumer survives. O2_PASSWORD must be in the environment.
# Example: O2_PASSWORD=$(grep ^O2_PASSWORD= code/01_platform/01_docker/secrets.env | cut -d= -f2) make alert-routing-test
alert-routing-test:
	@python3 code/01_platform/04_scripts/alert-routing-selftest.py $(ARGS)

# G5 Ops T12 (streaming-3000 hardening): rolling update of the SignalJob that
# keeps the fingerprint-dedup state. Triggers a savepoint, stops the job,
# copies the new compute jar into the flink-jobmanager container and submits
# it via the flink CLI with STATE_RECOVERY_PATH=<savepoint> and
# ALLOW_FULL_REPLAY=false (restore mode — the SignalJobConfig F005 gate
# rejects anything else), then verifies RUNNING + a completed checkpoint +
# dedup-state continuity (Prometheus, degradable). See docs/08_implementation/
# 21-savepoint-rollout.md.
rollout-savepoint:
	@bash code/01_platform/04_scripts/rollout-savepoint.sh $(ARGS)

# G5 Ops T13 (streaming-3000 hardening): failure chaos suite — the 4 L11
# gates (slot kill / TM kill / tablet kill / VM loss) run in order by
# code/01_platform/04_scripts/chaos/chaos-run.sh. Tests 01+02 run offline
# (Go tests + MiniCluster IT); 03+04 require the live stack / M3 swarm and
# SKIP cleanly otherwise (deployment runs are executed later, serially).
# Pass-through env: FLUSS_BOOTSTRAP TABLET_CONTAINER CHAOS_ORDER_PROBE_TCP
# CHAOS_WORKLOAD_NODE CHAOS_VM_OFF_MODE ... See docs/08_implementation/
# 22-failure-chaos-suite.md. Example: make chaos-suite
chaos-suite:
	@bash code/01_platform/04_scripts/chaos/chaos-run.sh $(ARGS)

ddl-apply-smoke:
	@python3 code/01_platform/04_scripts/ddl_apply_smoke.py

# Run the Item F disaster drills against the live local compose stack
# (coordinator/tablet/ZK quorum/O2/gateway/network-partition faults, each with
# recovery assertions + dated evidence under logs/disaster-drills/).
# Fault injection required --approve; --dry-run prints the plan without
# touching the stack. Example: make disaster-drills ARGS="--dry-run"
disaster-drills:
	@python3 code/01_platform/04_scripts/disaster_drills.py $(ARGS)

# Run the SCH-23 EOD controller CLI on the host (status/run/extend/reconcile/
# reset) against the live Fluss cluster. Env: FLUSS_BOOTSTRAP + EOD_* (see
# eod_controller.py). Example: make eod-controller ARGS="status"
eod-controller:
	@python3 code/01_platform/04_scripts/eod_controller.py $(ARGS)

# Build the DDL apply contract image (code/01_platform/01_docker/ddl-apply/):
# multi-stage — maven builder compiles the engine, temurin-jre + python3
# runtime carries the classes, the 5 pinned jars, the orchestrator/smoke, the
# DDL corpus + manifest, and the matrix evidence. One-shot run inside the
# compose network: docker compose run --rm ddl-apply {validate|apply|smoke|self-test}.
# The stamps ride on the build command's own environment, exactly as `images`
# does: without them the image carries no label, the staleness checker falls back
# to its timestamp proxy, and an unstamped image reads FRESH by clock (CHG-124).
ddl-image:
	@stamps="$$(python3 code/01_platform/04_scripts/image_staleness_check.py --git-root . --print-stamps-env)" || exit 1; \
	[ -n "$$stamps" ] || { echo "ddl-image: no build stamps computed — refusing to build unstamped images"; exit 1; }; \
	$(STACK_LOCK) env $$stamps $(COMPOSE) build ddl-apply && \
	python3 code/01_platform/04_scripts/image_staleness_check.py --git-root . --compose code/01_platform/01_docker/docker-compose.yml --service ddl-apply --require-stamps

# Build the production Flink runtime image (code/01_platform/01_docker/flink-runtime/).
# WHY A SEPARATE TARGET: this image is NOT a docker-compose service and has no
# `build:` anywhere — docker-stack.yml cannot build at deploy time, so the image
# is produced here on a build host and referenced by digest via FLINK_IMAGE.
# The jars come from fetch-jars.sh, which verifies every one against a pinned
# SHA256 before the Dockerfile can see it; the staging tree is removed
# afterwards either way.
# Usage: make flink-image [FLINK_RUNTIME_TAG=trading-flink-runtime:0.1.0]
flink-image:
	@set -e; \
	dir=code/01_platform/01_docker/flink-runtime; \
	tag="$${FLINK_RUNTIME_TAG:-trading-flink-runtime:0.1.0}"; \
	ctx="$$dir/.buildctx"; \
	rm -rf "$$ctx"; mkdir -p "$$ctx"; \
	trap 'rm -rf "$$ctx"' EXIT; \
	bash "$$dir/fetch-jars.sh" --dest "$$ctx/jars"; \
	cp "$$dir/Dockerfile" "$$dir/core-site.xml" "$$dir/20-r2-secrets-from-file.sh" "$$ctx/"; \
	docker build -t "$$tag" "$$ctx"; \
	bash "$$dir/fetch-jars.sh" --verify "$$ctx/jars"; \
	echo ""; \
	echo "Built $$tag"; \
	echo "  image id: $$(docker image inspect "$$tag" --format '{{.Id}}')"; \
	echo "Next: push it, then pin the manifest digest in runtime.lock:"; \
	echo "  bash code/01_platform/04_scripts/digest-pin.sh <repo>:<tag>"

# Usage: make fluss-image [FLUSS_RUNTIME_TAG=trading-fluss-runtime:0.1.0]
# The Fluss server image: stock apache/fluss + the two lake plugins baked into
# /opt/fluss/plugins/iceberg/. Same reason as flink-image — a Swarm stack cannot
# `build:`, and the dev compose sources those jars from a gitignored host tree
# that does not exist on the production VM.
fluss-image:
	@set -e; \
	dir=code/01_platform/01_docker/fluss-runtime; \
	tag="$${FLUSS_RUNTIME_TAG:-trading-fluss-runtime:0.1.0}"; \
	ctx="$$dir/.buildctx"; \
	rm -rf "$$ctx"; mkdir -p "$$ctx"; \
	trap 'rm -rf "$$ctx"' EXIT; \
	bash "$$dir/fetch-jars.sh" --dest "$$ctx/jars"; \
	cp "$$dir/Dockerfile" "$$ctx/"; \
	docker build -t "$$tag" "$$ctx"; \
	bash "$$dir/fetch-jars.sh" --verify "$$ctx/jars"; \
	echo ""; \
	echo "Built $$tag"; \
	echo "  image id: $$(docker image inspect "$$tag" --format '{{.Id}}')"; \
	echo "Next: push it, then pin the manifest digest in runtime.lock:"; \
	echo "  bash code/01_platform/04_scripts/digest-pin.sh <repo>:<tag>"


# Non-root ownership contract gate (evidence_ownership_check.py): every
# apply.json the ddl-apply container wrote (owner == DDL_APPLY_UID) must be
# group-writable (setgid 2775 evidence root + umask 002 -> 664) and no record
# may be root-owned. Host-side records are out of scope. Also docs-audit C15
# and the Monday gate DDL step.
evidence-ownership-check:
	@python3 code/01_platform/04_scripts/evidence_ownership_check.py

# Phase 8: every guard fires on every run — the full Monday gate.
gate:
	bash code/01_platform/04_scripts/run-monday-gates.sh

# 01-foundation.md "Mandatory implementation order": enforce the 7-task
# sequence — refuse to proceed past a task whose acceptance checks are red
# or missing. Tasks run in order; the first failing task blocks all downstream.
gate-order:
	@python3 code/01_platform/04_scripts/implementation_gate.py

# Phase 8 G4: static script hygiene without needing the full gate.
static-check:
	@set -e; fail=0; for s in $$(find code -name '*.sh' -not -path '*/target/*' -not -path '*/third_party/*' | sort); do \
		bash -n "$$s" || fail=1; \
		if command -v shellcheck >/dev/null 2>&1; then \
			shellcheck -S warning "$$s" || fail=1; \
		fi; \
	done; \
	# Prometheus float trap guard (audit #8, 2026-08-28): parsing a Prometheus \
	# float like 127.0 with `grep -oE '[0-9]+$'` extracts only the last digit \
	# ("0"), silently zeroing sums and producing false "delta=0" conclusions. \
	# Flag the pattern ONLY when it appears near a metrics scrape (curl to a \
	# :9249/:9250/:9090 endpoint or a file named *metrics*) — the same pattern \
	# on a config file / test log (e.g. MAX_BRIDGE_RESTARTS=3) is fine. \
	if command -v rg >/dev/null 2>&1; then \
		tmp=$$(mktemp); \
		if rg -n 'grep -o[EP]?.*(\[0-9\]|\\d).*(9250|9249|9090|prom|metrics)|(9250|9249|9090|prom|metrics).*grep -o[EP]?.*(\[0-9\]|\\d)' code --glob '*.sh' -g '!**/target/**' >"$$tmp"; then \
			echo "static-check: Prometheus float-trap pattern found (grep -oE '[0-9]+$$'):" >&2; \
			cat "$$tmp" >&2; \
			fail=1; \
		fi; \
		rm -f "$$tmp"; \
	else \
		echo "static-check: WARN — rg (ripgrep) not installed; Prometheus float-trap scan SKIPPED" >&2; \
	fi; \
	echo "static-check: $$fail failures"; [ "$$fail" -eq 0 ]

# Load-test guards (audit #9, 2026-08-28): preflight asserts that catch the
# silent-failure class that cost ~6 retries + one wrong conclusion during the
# 20,480/s compute test. Fails fast with a clear message instead of running a
# load test that measures the wrong thing (idle faketool, stale broker on a
# busy port, wrong env var names, >1024 tokens, missing manifest/jar).
check-loadtest-env:
	@out=$$(mktemp); \
	bash code/01_platform/04_scripts/loadtest-run.sh --check-only >"$$out" 2>&1; rc=$$?; \
	head -20 "$$out"; \
	if [ "$$rc" -ne 0 ]; then \
		echo "check-loadtest-env: FAIL (rc=$$rc) — the preflight refused this environment:" >&2; \
		cat "$$out" >&2; \
		rm -f "$$out"; \
		exit "$$rc"; \
	fi; \
	rm -f "$$out"; \
	echo "check-loadtest-env: OK (preflight passed)" 

# Post-load-test hygiene: the ingestion container must be back on the real
# feed (no ARROW_FAKE_BROKER, no test tokens) after a host-side load run.
check-ingestion-clean:
	@set -e; \
	if ! cid=$$($(COMPOSE) ps -q ingestion 2>/dev/null); then \
		echo "check-ingestion-clean: FAIL — 'docker compose ps' failed (no daemon, or unreadable compose/env files)" >&2; \
		exit 1; \
	fi; \
	if [ -z "$$cid" ]; then \
		echo "check-ingestion-clean: FAIL — no running 'ingestion' container in this compose project" >&2; \
		exit 1; \
	fi; \
	FAKE=$$(docker exec "$$cid" sh -c 'echo "$$ARROW_FAKE_BROKER"') || { \
		echo "check-ingestion-clean: FAIL — 'docker exec' failed on $$cid (container starting or stopping?)" >&2; \
		exit 1; \
	}; \
	TOK=$$(docker exec "$$cid" sh -c 'echo "$$ARROW_INSTRUMENT_TOKENS"') || { \
		echo "check-ingestion-clean: FAIL — 'docker exec' failed on $$cid (container starting or stopping?)" >&2; \
		exit 1; \
	}; \
	if [ -n "$$FAKE" ]; then echo "check-ingestion-clean: FAIL — ARROW_FAKE_BROKER is set ($$FAKE)" >&2; exit 1; fi; \
	if echo "$$TOK" | grep -qE '^[0-9,]+\$$'; then echo "check-ingestion-clean: FAIL — test token list in container" >&2; exit 1; fi; \
	echo "check-ingestion-clean: OK (no fake broker, no test tokens)"

# Self-test for the load-test guards (no cluster needed).
test-loadtest-guards:
	@bash code/01_platform/04_scripts/test-loadtest-guards.sh

# R-209 regression gate: 20,480/s (1024 tokens × 20hz) for 5 min must run
# clean (0 UNSAFE halts, >= 5.5M appended). Seals the readiness-file fix so
# any future throughput regression fails the gate, not production. Requires
# the ingestion jar (make build) + a running Flink cluster (REST :8081).
loadtest-20k-regression:
	@set -e; \
	echo "=== R-209 regression: 20k/s × 5min (0 UNSAFE, >= 5.5M appended) ==="; \
	OUT=$$(bash code/01_platform/04_scripts/loadtest-run.sh 300 30 2>&1 | tail -5); \
	echo "$$OUT"; \
	DIR=$$(echo "$$OUT" | grep -oE 'logs/tracker-14/loadtest-[0-9-]+' | head -1); \
	[ -n "$$DIR" ] || { echo "loadtest-20k-regression: FAIL — no output dir" >&2; exit 1; }; \
	APPENDED=$$(grep -oE 'appended=[0-9]+' "$$DIR"/j1/java.out 2>/dev/null | tail -1 | cut -d= -f2); \
	UNSAFE=$$(grep -c 'UNSAFE' "$$DIR"/j1/java.out 2>/dev/null || echo 0); \
	echo "loadtest-20k-regression: appended=$${APPENDED:-0} unsafe=$$UNSAFE"; \
	[ "$${APPENDED:-0}" -ge 5500000 ] || { echo "loadtest-20k-regression: FAIL — appended $${APPENDED:-0} < 5.5M" >&2; exit 1; }; \
	[ "$$UNSAFE" -eq 0 ] || { echo "loadtest-20k-regression: FAIL — $$UNSAFE UNSAFE halts" >&2; exit 1; }; \
	echo "loadtest-20k-regression: PASS (appended=$$APPENDED, unsafe=0)"

# Foundation L388: docs must not silently contradict code. Runs the machine-
# verifiable invariant set established by the 2026-08-13 ground-truth audit.
# Freebuff-worktree bootstrap for docs-audit: C6 (test counts) reads
# module target/surefire-reports and C14 resolves evidence artifacts under
# logs/ — both gitignored, so a fresh worktree must symlink logs/ to the main
# project folder (repo convention; see CLAUDE.md symlinks) and copy the
# surefire reports from the main project's target/ dirs before this gate.
# P3-180: cd first — rustup resolves the toolchain from cwd, so --manifest-path from
# the repo root would silently bypass code/02_services/04_executor/rust-toolchain.toml.
#
# P6-864: every step runs even when an earlier one fails, and the target then fails if ANY
# did. As a bare sequence of recipe lines, a single formatting nit aborted make before
# docs_audit.py ever ran — silently suppressing the whole doc-vs-code audit.
docs-audit:
	@rc=0; \
	step() { printf '== T0-T8 hardening: %s ==\n' "$$1"; shift; "$$@" || rc=1; }; \
	step 'cargo clippy -D warnings' bash -c 'set -o pipefail; cd code/02_services/04_executor && cargo clippy --all-targets --features paper -- -D warnings 2>&1 | tail -20'; \
	step 'cargo fmt --check' bash -c 'set -o pipefail; cd code/02_services/04_executor && cargo fmt --check 2>&1 | tail -20'; \
	step 'go vet' bash -c 'set -o pipefail; cd code/02_services/06_execution_bridge/go-bridge && go vet ./... 2>&1 | tail -20'; \
	step 'docs-audit hardening checks done' python3 code/01_platform/04_scripts/docs_audit.py; \
	exit $$rc
# implementation dossiers or the authoritative upstream layers (decisions,
# requirements, architecture, contracts) reads feature_candles_15s as a LOG,
# Signal_Candidates as a KV table, or feature_candles_15s_current as live — or
# claims forming-bar postponed / ranking-reservation postponed / Trade_Decisions
# active — unless the claim is annotated historical/superseded (or carries a
# current-status marker). Exit 1 on un-annotated hits.
stale-tables:
	@python3 code/01_platform/04_scripts/stale_table_kind_scan.py --upstream

# One-command full doc audit (2026-08-16 consolidation): the three gates
# (stale-claim scanner --upstream, docs-audit, --ddl parity) plus the
# beyond-scanner sweeps (live Ranking/Reservations/Decisions claims vs the
# CHG-005 whitelist, stale 'pending implementation' prose in the upstream
# layers) plus the dossier-trio coherence checks (04-signal-job / 13 / 14
# must agree on the 2026-08-13 re-scope, the DEC-038 externalization landing,
# and the P11 status). Exit 0 only when every layer is green.
full-audit:
	@bash code/01_platform/04_scripts/full_audit.sh

# Foundation L548/553/554: pin discipline — matrix shape, corpus integrity,
# external-SNAPSHOT ban, platform version pins. CI SHALL run this.
pin-check:
	@bash code/01_platform/04_scripts/pin-check.sh

# Wave 36: the holistic measurement harness produced the end-to-end zero-loss
# evidence, and until now nothing named it — it had no target and no entry in
# docs/commands/COMMANDS.md, so it was only reachable by someone who already
# knew the path and its knobs. Needs the stack up (`make up`) and a QUIET
# cluster: the probes are invalid while it is deleting or re-electing.
holistic:
	@bash code/01_platform/04_scripts/holistic-measure.sh

# The same harness with short phases, for a quick verdict after a change.
holistic-quick:
	@SMOKE_S=60 MAIN_S=300 bash code/01_platform/04_scripts/holistic-measure.sh

clean:
	$(STACK_LOCK) $(COMPOSE) down -v
