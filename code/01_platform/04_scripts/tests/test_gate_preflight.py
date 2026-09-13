"""Gate preflight: the drift parser and the compose-form invariant.

The recreate parser is tested against the real `up -d --dry-run` outputs captured
on 2026-09-13: the canonical env-file form reported no recreate action, while the
bare `-f` form reported recreate actions for six services (the mechanism that
killed gate attempts 24 and 26 at step 11). A parser that silently returned [] for
both would make the preflight a no-op, which is what these tests prevent.
"""

from __future__ import annotations

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gate_preflight  # noqa: E402  (path set above)


# Verbatim shape of `docker compose --env-file .env --env-file secrets.env -f ... up -d --dry-run`
CANONICAL_OUTPUT = """\
 DRY-RUN MODE -  Container 01_docker-zookeeper-1  Running
 DRY-RUN MODE -  Container 01_docker-minio-init-1  Created
 DRY-RUN MODE -  Container 01_docker-fluss-coordinator-1  Running
 DRY-RUN MODE -  Container 01_docker-fluss-tablet-1  Running
"""

# Verbatim shape of the bare `docker compose -f ...` form, which resolves a
# different secret set and therefore rebuilds the containers.
BARE_OUTPUT = """\
 DRY-RUN MODE -  Container 01_docker-zookeeper-1  Running
 DRY-RUN MODE -  Container 01_docker-minio-1  Recreate
 DRY-RUN MODE -  Container 01_docker-openobserve-1  Recreate
 DRY-RUN MODE -  Container 01_docker-fluss-coordinator-1  Recreate
 DRY-RUN MODE -  Container 01_docker-otel-collector-1  Recreate
 DRY-RUN MODE -  Container 01_docker-alert-consumer-1  Recreate
 DRY-RUN MODE -  Container 01_docker-minio-init-1  Recreate
 DRY-RUN MODE -  Container 01_docker-minio-1  Recreated
 DRY-RUN MODE -  Container 01_docker-openobserve-1  Recreated
 DRY-RUN MODE -  Container 01_docker-fluss-coordinator-1  Recreated
 DRY-RUN MODE -  Container 01_docker-minio-init-1  Recreated
"""


class TestRecreateParser(unittest.TestCase):
    def test_canonical_output_reports_no_drift(self):
        self.assertEqual([], gate_preflight.recreate_services(CANONICAL_OUTPUT))

    def test_bare_form_output_reports_each_service_once(self):
        self.assertEqual(
            ["01_docker-minio-1", "01_docker-openobserve-1",
             "01_docker-fluss-coordinator-1", "01_docker-otel-collector-1",
             "01_docker-alert-consumer-1", "01_docker-minio-init-1"],
            gate_preflight.recreate_services(BARE_OUTPUT),
        )

    def test_past_tense_lines_are_not_counted(self):
        only_recreated = " Container 01_docker-x-1  Recreated\n"
        self.assertEqual([], gate_preflight.recreate_services(only_recreated))

    def test_unrelated_output_is_not_mistaken_for_drift(self):
        self.assertEqual([], gate_preflight.recreate_services("Container x Running\n") )


class TestComposeFormInvariant(unittest.TestCase):
    """The preflight's compose form must stay identical to the Makefile's $(COMPOSE).

    A bare `-f` form reports 22 recreate actions where the canonical form reports
    none, so a drift between these two call sites silently restarts the cluster
    under the gate. Comparing the flag sets catches that at commit time.
    """

    def test_preflight_form_matches_makefile_compose(self):
        root = gate_preflight.PROJECT_ROOT
        with open(os.path.join(root, "Makefile"), encoding="utf-8") as handle:
            makefile = handle.read()
        match = re.search(r"^COMPOSE\s*:=\s*(.+)$", makefile, re.MULTILINE)
        self.assertIsNotNone(match, "COMPOSE := not found in the Makefile")

        makefile_argv = match.group(1).split()
        preflight_argv = gate_preflight.canonical_compose()
        self.assertEqual("docker", makefile_argv[0])
        self.assertEqual("compose", makefile_argv[1])
        self.assertEqual(_flags(preflight_argv), _flags(makefile_argv))


def _flags(argv: list[str]) -> set[tuple[str, str]]:
    """(flag, resolved path) pairs — absolute and relative spellings compare equal."""
    pairs = set()
    index = 0
    while index < len(argv) - 1:
        flag = argv[index]
        if flag in ("--env-file", "-f", "--project-directory", "-p"):
            value = argv[index + 1]
            if flag != "-p":
                value = os.path.realpath(value)
            pairs.add((flag, value))
            index += 2
            continue
        index += 1
    return pairs


if __name__ == "__main__":
    unittest.main()
