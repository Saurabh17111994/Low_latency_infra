#!/usr/bin/env python3
"""`docker` stub for the wave-38 tests: no daemon, no containers, exit 0.

capture_stall_diagnostics shells out to docker on the fail-fast path; the tests
are about the io-probe teardown, not the stall dump, so this keeps them
hermetic and fast (a real `docker ps` against a dead daemon is slow).
"""
import sys

if "ps" in sys.argv:
    # `docker ps -q --filter ...` consumers expect empty output and rc 0.
    sys.exit(0)
sys.exit(0)
