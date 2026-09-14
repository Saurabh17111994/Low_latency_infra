"""P3-418: the audited Rust toolchain version is declared in three files.

`rust_toolchain_pin_check.sh` must reject any drift between them — and must
accept the real repository, so a future edit to one of the three is caught by
`make pin-check` and by this suite (which gate-fast runs).
"""

import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "rust_toolchain_pin_check.sh"
PIN = "1.97.1"


def build_root(tmp, pin=PIN, channel=PIN, rust_version=PIN):
    """Lay out a minimal repo root holding the three declarations."""
    root = Path(tmp)
    (root / "code/01_platform/04_scripts").mkdir(parents=True)
    (root / "code/02_services/04_executor").mkdir(parents=True)
    (root / "code/01_platform/04_scripts/versions.pin").write_text(
        f"NAUTILUS_RUST_TOOLCHAIN={pin}\n" if pin else "FLINK_VERSION=1.20.1\n"
    )
    (root / "code/02_services/04_executor/rust-toolchain.toml").write_text(
        f'[toolchain]\nchannel = "{channel}"\n' if channel else "[toolchain]\n"
    )
    (root / "code/02_services/04_executor/Cargo.toml").write_text(
        f'[package]\nrust-version = "{rust_version}"\n' if rust_version else "[package]\n"
    )
    return root


def run(root=None):
    cmd = ["bash", str(SCRIPT)] + ([str(root)] if root else [])
    return subprocess.run(cmd, capture_output=True, text=True)


class RustToolchainPinCheckTest(unittest.TestCase):
    def test_real_repo_declarations_agree(self):
        r = run()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("OK: NAUTILUS_RUST_TOOLCHAIN=", r.stdout)

    def test_cargo_toml_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = run(build_root(tmp, rust_version="1.96.0"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("disagree", r.stdout)
        self.assertIn("Cargo.toml=1.96.0", r.stdout)

    def test_toolchain_file_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = run(build_root(tmp, channel="1.97.0"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("rust-toolchain.toml=1.97.0", r.stdout)

    def test_missing_pin_key_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = run(build_root(tmp, pin=None))
        self.assertEqual(r.returncode, 1)
        self.assertIn("NAUTILUS_RUST_TOOLCHAIN missing", r.stdout)

    def test_missing_file_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = run(tmp)
        self.assertEqual(r.returncode, 1)
        self.assertIn("FAIL: missing", r.stdout)


if __name__ == "__main__":
    unittest.main()
