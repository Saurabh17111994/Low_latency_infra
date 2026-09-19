import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "execution_network_check.py"
SPEC = importlib.util.spec_from_file_location("execution_network_check", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("could not load execution network checker")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_config():
    return {
        "networks": {
            "execution-net": {"internal": True},
            "arrow-egress": {"driver": "bridge"},
        },
        "services": {
            "execution-bridge": {
                "networks": ["execution-net", "arrow-egress"],
                "environment": {"EXECUTION_BRIDGE_MODE": "fake"},
            },
            "gateway": {"networks": ["execution-net"], "environment": {}},
            "rust-executor": {"networks": ["execution-net"], "environment": {}},
            "flink-jobmanager": {"networks": ["trading-net"], "environment": {}},
            "ingestion": {
                "networks": ["trading-net"],
                "environment": {"ARROW_APP_ID": "market-data-only"},
            },
        },
    }


class ExecutionNetworkCheckTest(unittest.TestCase):
    def test_accepts_bridge_only_order_egress(self):
        self.assertEqual(MODULE.validate_config(valid_config()), [])

    def test_rejects_non_bridge_egress_attachment(self):
        config = valid_config()
        config["services"]["gateway"]["networks"].append("arrow-egress")
        errors = MODULE.validate_config(config)
        self.assertTrue(any("gateway" in error for error in errors))

    def test_rejects_non_internal_execution_network(self):
        config = valid_config()
        config["networks"]["execution-net"]["internal"] = False
        # P6-835: bind to the rule under test — "some error" would also pass if an
        # unrelated check regressed.
        errors = MODULE.validate_config(config)
        self.assertTrue(any("execution-net must be an internal network" in e for e in errors),
                        errors)

    def test_rejects_arrow_credentials_outside_market_data_exception(self):
        config = valid_config()
        config["services"]["rust-executor"]["environment"] = {
            "ARROW_APP_SECRET": "must-not-be-here"
        }
        errors = MODULE.validate_config(config)
        # P6-836: the service name alone also matches unrelated failures; assert the
        # leaked key that this rule exists to catch.
        self.assertTrue(any("ARROW_APP_SECRET" in error for error in errors), errors)

    def test_rejects_published_bridge_port(self):
        config = valid_config()
        config["services"]["execution-bridge"]["ports"] = ["8787:8787"]
        errors = MODULE.validate_config(config)
        self.assertTrue(any("must not publish a host port" in e for e in errors), errors)


if __name__ == "__main__":
    unittest.main()
