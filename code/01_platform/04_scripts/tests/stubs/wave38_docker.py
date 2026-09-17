#!/usr/bin/env python3
"""`docker` stub for the wave-38 tests: no daemon, no containers, exit 0.

capture_stall_diagnostics shells out to docker on the fail-fast path; the tests
are about the io-probe teardown, not the stall dump, so this keeps them
hermetic and fast (a real `docker ps` against a dead daemon is slow).

Behavior knobs (env; defaults preserve the original always-0 behavior):
  STUB_DOCKER_PS=<cid>     printed for `ps` queries (compose_cid needs one)
  STUB_DOCKER_INSPECT=<s>  printed for `inspect` ("" = not mounted)
  STUB_DOCKER_EXEC_RC=<n>  exit code for `exec` (1 = the ls found nothing)
"""
import os
import sys

argv = sys.argv[1:]
# NOTE: lib $COMPOSE invocations arrive as compose -f ... <subcommand>, so
# match by subcommand membership, not argv[0].
if "ps" in argv:
    # `docker ps -q ...` consumers expect empty output and rc 0 by default.
    cid = os.environ.get("STUB_DOCKER_PS", "")
    if cid:
        print(cid)
    sys.exit(0)
if "inspect" in argv:
    print(os.environ.get("STUB_DOCKER_INSPECT", ""))
    sys.exit(0)
if "exec" in argv:
    sys.exit(int(os.environ.get("STUB_DOCKER_EXEC_RC", "0")))
sys.exit(0)
