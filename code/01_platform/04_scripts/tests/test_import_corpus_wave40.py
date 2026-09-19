"""Wave 40 — retire the pre-v3 instruments loader; make the corpus pin strict.

Offline and hermetic: no cluster, no network, no writes to the repository. The
corpus-pin legs copy the real script into a temp repo skeleton (the script
derives REPO_ROOT from its own location), so the committed corpus and its pin
file are never touched.
"""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "code" / "01_platform" / "04_scripts"
IMPORT_SH = SCRIPTS / "import_instruments.sh"
CORPUS_PIN = SCRIPTS / "corpus-pin.sh"
REL_CORPUS = "code/02_services/01_ingestion/go-bridge/testdata/golden"
COMMANDS_MD = REPO / "docs" / "commands" / "COMMANDS.md"
INSTRUMENTS_README = REPO / "code" / "01_platform" / "05_instruments" / "README.md"


def run(args):
    return subprocess.run(
        ["bash", *[str(a) for a in args]], capture_output=True, text=True, timeout=120
    )


class RetiredImportScriptTest(unittest.TestCase):
    """P6-010/110..115/417..424/746/747 — the script is retired, not repaired."""

    def test_refuses_with_exit_2_and_says_nothing_was_imported(self):
        r = run([IMPORT_SH, "/tmp/w40-manifest.csv"])
        self.assertEqual(r.returncode, 2, r.stderr)
        self.assertEqual(r.stdout, "", "a refusal must not write to stdout")
        self.assertIn("Nothing was imported", r.stderr)

    def test_refuses_with_no_arguments_too(self):
        self.assertEqual(run([IMPORT_SH]).returncode, 2)

    def test_names_the_real_loader_and_its_config(self):
        r = run([IMPORT_SH, "x.csv"])
        self.assertIn("InstrumentManifestWriter", r.stderr)
        self.assertIn("INSTRUMENT_MANIFEST_PATH", r.stderr)

    def test_cannot_emit_sql(self):
        # The whole point of retiring rather than repairing: this file must never
        # generate or run a statement against the live table again.
        body = IMPORT_SH.read_text()
        self.assertNotIn("INSERT", body)
        self.assertNotIn("psql", body)

    def test_header_records_the_retirement_and_why(self):
        body = IMPORT_SH.read_text()
        self.assertIn("RETIRED", body)
        self.assertIn("20_instruments.sql", body)
        self.assertIn("InstrumentManifestWriter.ManifestEntry", body)
        self.assertIn("never in", body)  # the DDL's "never in DDL" contract note


class RetiredImportDocsTest(unittest.TestCase):
    """The two references that made this a documented trap, not dead code."""

    @staticmethod
    def _lines_mentioning(path, needle):
        return [ln for ln in path.read_text().splitlines() if needle in ln]

    def test_commands_md_no_longer_offers_it_as_a_working_import(self):
        hits = self._lines_mentioning(COMMANDS_MD, "import_instruments.sh")
        self.assertTrue(hits, "COMMANDS.md no longer mentions the script at all")
        for ln in hits:
            self.assertTrue(
                "retired" in ln.lower() or "InstrumentManifestWriter" in ln,
                f"COMMANDS.md still presents it as usable: {ln}",
            )

    def test_instruments_readme_points_at_the_loader(self):
        # Prose wraps, so the verdict may sit on a neighbouring line of the link.
        lines = INSTRUMENTS_README.read_text().splitlines()
        windows = [
            " ".join(lines[max(0, i - 1) : i + 2])
            for i, ln in enumerate(lines)
            if "import_instruments.sh" in ln
        ]
        self.assertTrue(windows, "the instruments README no longer mentions the script")
        for w in windows:
            self.assertTrue(
                "retired" in w.lower() or "InstrumentManifestWriter" in w,
                f"the README still presents it as usable: {w}",
            )


class _Skeleton:
    """A temp repo laid out so corpus-pin.sh finds its corpus and manifest."""

    def __init__(self, test):
        self.root = Path(tempfile.mkdtemp(prefix="w40-skel-"))
        test.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.scripts = self.root / "code" / "01_platform" / "04_scripts"
        self.corpus = self.root / REL_CORPUS
        self.scripts.mkdir(parents=True)
        self.corpus.mkdir(parents=True)
        self.script = self.scripts / "corpus-pin.sh"
        shutil.copy2(CORPUS_PIN, self.script)
        self.manifest = self.scripts / "corpus.sha256"

    def frame(self, name, body):
        p = self.corpus / name
        p.write_text(body)
        return p

    def run(self, *args):
        return run([self.script, *args])


class CorpusPinTest(unittest.TestCase):
    """P6-060/061/349 — the pin must see the whole corpus and never truncate."""

    def test_requires_exactly_one_argument(self):
        s = _Skeleton(self)
        self.assertEqual(s.run().returncode, 2, "no argument must be a usage error")
        self.assertEqual(s.run("--verify", "extra").returncode, 2)
        self.assertEqual(s.run("--regenerate", "extra").returncode, 2)
        self.assertEqual(s.run("--typo").returncode, 2)

    def test_verify_passes_on_the_committed_corpus(self):
        # The gate path: run-monday-gates.sh runs `make pin-check` -> --verify.
        r = run([CORPUS_PIN, "--verify"])
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_verify_detects_an_unpinned_file(self):
        s = _Skeleton(self)
        s.frame("a.frame", "frame-a\n")
        self.assertEqual(s.run("--regenerate").returncode, 0)
        self.assertEqual(s.run("--verify").returncode, 0)
        s.frame("c.frame", "frame-c\n")  # never pinned
        r = s.run("--verify")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("diverged", r.stderr)
        self.assertIn("c.frame", r.stderr)
        self.assertEqual(s.run("--regenerate").returncode, 0)
        self.assertEqual(s.run("--verify").returncode, 0, "re-pin must clear it")

    def test_verify_rejects_a_malformed_manifest(self):
        s = _Skeleton(self)
        s.frame("a.frame", "frame-a\n")
        s.run("--regenerate")
        s.manifest.write_text("not-a-hash  " + REL_CORPUS + "/a.frame\n")
        self.assertNotEqual(s.run("--verify").returncode, 0, "--strict must fail")

    def test_regenerate_is_atomic_when_hashing_fails(self):
        if __import__("os").geteuid() == 0:
            self.skipTest("chmod 000 does not block reads as root")
        s = _Skeleton(self)
        f = s.frame("a.frame", "frame-a\n")
        s.frame("b.frame", "frame-b\n")
        self.assertEqual(s.run("--regenerate").returncode, 0)
        before = s.manifest.read_bytes()
        f.chmod(0o000)
        try:
            self.assertNotEqual(s.run("--regenerate").returncode, 0)
        finally:
            f.chmod(0o644)
        self.assertEqual(
            s.manifest.read_bytes(), before, "a failed regenerate truncated the pin file"
        )
        leftovers = list(s.scripts.glob(".corpus.sha256.*"))
        self.assertEqual(leftovers, [], f"temp file left behind: {leftovers}")

    def test_regenerate_pins_dotfiles_and_is_deterministic(self):
        s = _Skeleton(self)
        s.frame("a.frame", "frame-a\n")
        s.frame(".hidden", "hidden\n")  # the old glob skipped dotfiles
        self.assertEqual(s.run("--regenerate").returncode, 0)
        first = s.manifest.read_bytes()
        self.assertIn(".hidden", first.decode())
        self.assertEqual(s.run("--regenerate").returncode, 0)
        self.assertEqual(s.manifest.read_bytes(), first, "order must be deterministic")

    def test_regenerate_refuses_an_empty_corpus(self):
        s = _Skeleton(self)
        self.assertEqual(s.run("--regenerate").returncode, 2)
        self.assertFalse(s.manifest.exists(), "an empty pin file must not be written")


if __name__ == "__main__":
    unittest.main()
