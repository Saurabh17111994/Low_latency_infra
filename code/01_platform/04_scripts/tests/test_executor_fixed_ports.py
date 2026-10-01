"""CHG-494: executor tests must not bind fixed TCP ports.

2026-10-01 (gate step 15): an unrelated local node tool held 127.0.0.1:18787
while the Nautilus suite ran; `send_command_round_trip_accepted` bound a
hardcoded port and failed with AddrInUse — 386/387 green. A dev machine's port
allocation must not be able to redden the gate: tests bind port 0 and use the
address the kernel hands back (the pattern every other bind in the crate uses).

Offline only: scans the executor sources, never runs cargo.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
SRC = ROOT / "code/02_services/04_executor/src"
FIXED_BIND = re.compile(r'(?:TcpListener::bind|serve_bridge)\(\s*"127\.0\.0\.1:(\d+)"')


class ExecutorFixedPortTests(unittest.TestCase):
    def test_no_test_binds_a_fixed_loopback_port(self):
        offenders = []
        for rs in sorted(SRC.rglob("*.rs")):
            for lineno, line in enumerate(rs.read_text().splitlines(), 1):
                for port in FIXED_BIND.findall(line):
                    if port != "0":
                        offenders.append(
                            f"{rs.relative_to(ROOT)}:{lineno}: 127.0.0.1:{port}"
                        )
        self.assertEqual(
            offenders, [],
            "bind 127.0.0.1:0 and use the bound address instead of a fixed port "
            "(a dev-machine port collision must never redden the suite):\n"
            + "\n".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
