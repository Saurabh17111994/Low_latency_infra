#!/usr/bin/env python3
"""C3: the daily Fluss probe ships inside the ingestion image; day_run needs no host JDK.

Gate-discovered (test_*.py). Fails when:
  - the probe source stops being compiled into the ingestion image,
  - the runtime stage stops carrying every probe class,
  - day_run regresses to host `javac` + `target/cp.txt`,
  - the probe starts reaching Fluss over localhost instead of compose service DNS.
"""

from __future__ import annotations

import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[4]
DOCKERFILE = ROOT / "code/02_services/01_ingestion/Dockerfile"
DAY_RUN = ROOT / "code/01_platform/04_scripts/day_run.py"


class IngestionProbeImageTest(unittest.TestCase):
    def test_dockerfile_compiles_the_probe_against_the_shaded_jar(self) -> None:
        text = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn(
            "COPY 01_platform/04_scripts/fluss-probes/FlussReadLagProbe.java /src/probe/",
            text,
            "the probe source must be copied into the java-builder stage",
        )
        self.assertIn(
            "javac -cp /src/02_services/01_ingestion/target/ingestion.jar",
            text,
            "the probe must compile against the shaded ingestion jar",
        )

    def test_runtime_stage_carries_every_probe_class(self) -> None:
        text = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn(
            "COPY --from=java-builder /probe-classes/ /app/probe/",
            text,
            "the runtime stage must ship every compiled probe class: the probe "
            "loads its nested InputException at startup, so copying only "
            "FlussReadLagProbe.class crashes it with "
            "NoClassDefFoundError: FlussReadLagProbe$InputException",
        )
        self.assertNotIn(
            "/probe-classes/FlussReadLagProbe.class /app/probe/",
            text,
            "a single-class copy drops the nested classes the probe loads",
        )

    def test_day_run_probes_through_the_container(self) -> None:
        text = DAY_RUN.read_text(encoding="utf-8")
        self.assertIn('"/app/ingestion.jar:/app/probe"', text)
        self.assertIn("fluss-coordinator:9123", text)
        self.assertNotIn("javac", text, "day_run must never compile the probe on the host")
        self.assertNotIn("cp.txt", text, "day_run must not depend on target/cp.txt")
        self.assertNotIn("localhost:9123", text,
                         "the probe must reach Fluss by compose service DNS")


if __name__ == "__main__":
    unittest.main()
