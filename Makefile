# Trading_project — common dev commands (MVP scaffold)
# Run from the repo root: make <target>

COMPOSE := docker compose -f code/01_platform/01_docker/docker-compose.yml
# R-143: default to ONLINE maven (a fresh checkout has an empty ~/.m2 and -o
# fails obscurely). Set MVN_FLAGS=-o when the local cache is warm.
MVN := mvn $(MVN_FLAGS)

.PHONY: help env ddl up down logs build clean cep-check cep-check-module test test-ingestion test-audit-r2 execution-network-check gate gate-order static-check docs-audit stale-tables full-audit pin-check ddl-apply-smoke ddl-image evidence-ownership-check test-09 stack-selfcheck stack-config seed-dashboards rollout-savepoint chaos-suite check-image-stale branch-check proto

# Branch guard: low-latency work must happen on the low-latency branch.
# Any agent (human or AI) MUST run this before editing code. Fails (exit 1)
# with a clear message if the current branch is wrong.
branch-check:
	@branch=$$(git branch --show-current); \
	if [ "$$branch" != "low-latency-ingestion-based-project" ]; then \
		echo "ERROR: on branch '$$branch' — low-latency work is ONLY allowed on"; \
		echo "       'low-latency-ingestion-based-project'. Switch first or ask the operator."; \
		echo "       (AGENTS.md 'Branch Context'; contract §header)."; \
		exit 1; \
	fi; \
	echo "OK: on low-latency-ingestion-based-project"

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
	@echo "              and docs-audit C6 line N/N/N citations vs the truth 469/247/387)"
	@echo "              (forming-bar postponed, ranking/reservation postponed,"
	@echo "              Trade_Decisions active) without a status marker"
	@echo "  full-audit  run the whole doc audit in one command: the three gates (stale-tables,"
	@echo "              docs-audit, --ddl parity) + the beyond-scanner sweeps (live ranking/"
	@echo "              reservation claims, stale 'pending implementation' prose) + the"
	@echo "              dossier-trio coherence checks (04-signal-job / 13 / 14 agree on the"
	@echo "              re-scope, DEC-038 landing, and P11 status) — exit 0 only when all green"
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
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

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

# audit_r2.py unit tests (stdlib unittest — SigV4 golden vector, config
# parsing, provisioning/validation against an in-memory fake client).
test-audit-r2:
	python3 -m unittest discover -s code/01_platform/04_scripts/tests -v

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
ddl-image:
	@docker compose -f code/01_platform/01_docker/docker-compose.yml build ddl-apply

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
	if rg -n 'grep -o[E]?.*\[0-9\].*(9250|9249|9090|prom|metrics)|(9250|9249|9090|prom|metrics).*grep -o[E]?.*\[0-9\]' code --glob '*.sh' -g '!**/target/**' >/tmp/prom-float-trap.txt; then \
		echo "static-check: Prometheus float-trap pattern found (grep -oE '[0-9]+$$'):" >&2; \
		cat /tmp/prom-float-trap.txt >&2; \
		rm -f /tmp/prom-float-trap.txt; \
		fail=1; \
	fi; \
	rm -f /tmp/prom-float-trap.txt; \
	echo "static-check: $$fail failures"; [ "$$fail" -eq 0 ]

# Foundation L388: docs must not silently contradict code. Runs the machine-
# verifiable invariant set established by the 2026-08-13 ground-truth audit.
# Freebuff-worktree bootstrap for docs-audit: C6 (test counts) reads
# module target/surefire-reports and C14 resolves evidence artifacts under
# logs/ — both gitignored, so a fresh worktree must symlink logs/ to the main
# project folder (repo convention; see CLAUDE.md symlinks) and copy the
# surefire reports from the main project's target/ dirs before this gate.
docs-audit:
	@echo "== T0-T8 hardening: cargo clippy -D warnings =="
	@bash -c 'set -o pipefail; cargo clippy --manifest-path code/02_services/04_executor/Cargo.toml --all-targets -- -D warnings 2>&1 | tail -20'
	@echo "== T0-T8 hardening: cargo fmt --check =="
	@bash -c 'set -o pipefail; cargo fmt --manifest-path code/02_services/04_executor/Cargo.toml --check 2>&1 | tail -20'
	@echo "== T0-T8 hardening: go vet =="
	@bash -c 'set -o pipefail; cd code/02_services/06_execution_bridge/go-bridge && go vet ./... 2>&1 | tail -20'
	@echo "== T0-T8 hardening: docs-audit hardening checks done =="
	@python3 code/01_platform/04_scripts/docs_audit.py
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

clean:
	$(COMPOSE) down -v
