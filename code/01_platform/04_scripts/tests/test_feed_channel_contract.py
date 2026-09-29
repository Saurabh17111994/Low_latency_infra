"""Dual-channel feed-contract guard (operator decision 2026-09-29).

The ingestion edge runs exactly ONE feed family per process, selectable with
``ARROW_FEED``: ``token`` (standard DataStream, ``wss://ds.arrow.trade``) or
``hft`` (``wss://socket.arrow.trade``). Both ride the same slot supervisor,
proto transport and Java admission path — there is no separate end-to-end
facility per channel; only the ingestion edge differs.

The deployment must be deterministic on the live channel (HFT is dead on this
account: ``PLAN_NOT_SUBSCRIBED``, probes 2026-09-24/26) while keeping the other
channel one env var away:

  * ``docker-compose.yml`` / ``docker-stack.yml`` pass ``ARROW_FEED`` through
    with the live-channel default (Swarm ignores ``env_file`` — without this
    key the deck would silently run the code default ``hft``),
  * the daily VM profile (``.env.vm.example``) and the runner's effective env
    (``day_run.py``) pin ``token``,
  * the Go selector keeps both families and checks the ``ARROW_HFT_URL``
    fake-broker override first,
  * Java keeps accepting both feed labels and quarantines anything else,
  * the dossier and the ingestion contract document the selector.

A future edit that drops the passthrough, unpins the VM, deletes a channel, or
lets the fake-broker override lose precedence fails here.
"""

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[4]
DOCKER = ROOT / "code/01_platform/01_docker"
COMPOSE = DOCKER / "docker-compose.yml"
STACK = DOCKER / "docker-stack.yml"
VM_ENV = DOCKER / ".env.vm.example"
DAY_RUN = ROOT / "code/01_platform/04_scripts/day_run.py"
SUPERVISOR = ROOT / "code/02_services/01_ingestion/go-bridge/supervisor.go"
SELECTOR_TEST = ROOT / "code/02_services/01_ingestion/go-bridge/token_slot_test.go"
SERVICE = ROOT / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java"
DOSSIER = ROOT / "docs/08_implementation/03-ingestion.md"
CONTRACT = ROOT / "docs/04_contracts/01-ingestion.md"


class FeedChannelContractTest(unittest.TestCase):

    def test_compose_pins_the_live_channel(self):
        self.assertIn("ARROW_FEED: ${ARROW_FEED:-token}", COMPOSE.read_text(),
                      "the compose base must pass ARROW_FEED through with the live default")

    def test_stack_pins_the_live_channel(self):
        # Swarm ignores env_file: without this explicit key the deck runs the
        # Go code default (hft), which is dead on this account.
        self.assertIn("ARROW_FEED: ${ARROW_FEED:-token}", STACK.read_text(),
                      "the production stack must pass ARROW_FEED through")

    def test_vm_profile_and_runner_pin_the_live_channel(self):
        self.assertIn("ARROW_FEED=token", VM_ENV.read_text(),
                      "the daily VM profile must pin the live channel")
        self.assertIn('"ARROW_FEED": "token"', DAY_RUN.read_text(),
                      "the runner's effective env must pin the live channel")

    def test_go_selector_keeps_both_channels_and_override_precedence(self):
        text = SUPERVISOR.read_text()
        self.assertRegex(text, r'feedHFT\s*=\s*"hft"', "the HFT family must stay defined")
        self.assertRegex(text, r'feedStandard\s*=\s*"token"', "the token family must stay defined")
        self.assertIn("ARROW_FEED", text, "the selector env key must stay")
        # The fake-broker override must be checked BEFORE the channel selector,
        # or the resilience ladders would be rerouted to a real socket.
        factory = text[text.index("func streamFactoryFor"):]
        self.assertLess(factory.index("ARROW_HFT_URL"), factory.index("feedUsesTokenStream"),
                        "ARROW_HFT_URL must win over ARROW_FEED")
        self.assertIn("ARROW_FEED", SELECTOR_TEST.read_text(),
                      "the Go selector test table must stay")

    def test_java_accepts_both_feed_labels(self):
        text = SERVICE.read_text()
        self.assertIn('"hft".equals(ev.getFeed())', text,
                      "the HFT label must stay accepted")
        self.assertIn('"token".equals(ev.getFeed())', text,
                      "the token label must stay accepted")

    def test_docs_document_the_selector(self):
        self.assertIn("ARROW_FEED", DOSSIER.read_text(),
                      "the ingestion dossier must document the selector")
        self.assertIn("ARROW_FEED=token", CONTRACT.read_text(),
                      "the ingestion contract must name the token-channel selection")


if __name__ == "__main__":
    unittest.main()
