"""P5 hygiene batch (002/003/004/005/006/015-017/019/020/024/025) — ignore-file semantics.

Regression guards for the .gitignore/.dockerignore contract fixed 2026-09-09.
Each assert names a behavior that was broken before the fix; run from anywhere
(paths are resolved against the repo root).
"""
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]


def ignored(path: str) -> bool:
    """True when gitignore rules would exclude the (possibly hypothetical) path."""
    rc = subprocess.run(
        ["git", "check-ignore", "-q", "--no-index", path],
        cwd=ROOT, capture_output=True,
    ).returncode
    return rc == 0


class IgnoreHygieneTests(unittest.TestCase):
    def test_p5_005_env_full_coverage(self):
        self.assertTrue(ignored(".env"))
        self.assertTrue(ignored(".env.prod"))
        self.assertTrue(ignored("code/02_services/01_ingestion/.env"))
        self.assertFalse(ignored(".env.example"), ".env.example must stay visible")

    def test_p5_017_log_tsv_scoped_to_logs(self):
        self.assertFalse(ignored("src/foo.tsv"), "bare *.tsv swallowed fixtures repo-wide")
        self.assertFalse(ignored("src/foo.log"))
        self.assertTrue(ignored("logs/ingestion.log"))
        self.assertTrue(ignored("logs/soak/evidence.tsv"))

    def test_p5_019_root_local_agent_docs_only(self):
        self.assertTrue(ignored("CLAUDE.md"))
        self.assertTrue(ignored("TASK_CONTEXT.md"))
        self.assertFalse(ignored("code/01_platform/01_docker/CLAUDE.md"),
                         "nested shared docs must surface in git status")

    def test_p5_024_dotfile_project_names(self):
        self.assertTrue(ignored(".project"))
        self.assertTrue(ignored(".classpath"))
        self.assertFalse(ignored("foo.project"))

    def test_p5_006_no_cu_wildcard(self):
        self.assertFalse(ignored("CursorLogs"), "/Cu* swallowed future Cu* sources")
        self.assertFalse(ignored("Custom/"))
        self.assertTrue(ignored(".token-savior-cache.json"))

    def test_p5_003_004_no_schema_compat_reinclude(self):
        """The root .dockerignore must not re-include logs/ (runtime-only evidence)."""
        text = (ROOT / ".dockerignore").read_text()
        self.assertNotIn("!logs/schema-compat/", text)
        self.assertIn("**/logs/", text)
        self.assertIn("**/.env", text)          # P5-002 secrets
        self.assertIn("**/.DS_Store", text)     # P5-016 recursive patterns

    def test_p5_020_025_context_hygiene_files(self):
        self.assertIn("**/__pycache__/", (ROOT / "code" / ".dockerignore").read_text())
        executor = ROOT / "code" / "02_services" / "04_executor" / ".dockerignore"
        self.assertTrue(executor.is_file(),
                         "nautilus builds use context 02_services/04_executor — needs its own .dockerignore")
        self.assertIn("target/", executor.read_text())


if __name__ == "__main__":
    unittest.main()
