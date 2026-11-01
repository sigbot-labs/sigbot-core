import importlib.util
import json
import pathlib
import unittest


class RuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = pathlib.Path(__file__).parents[2] / "src/sdk/runtime.py"
        spec = importlib.util.spec_from_file_location("sigbot_sdk", path)
        cls.sdk = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.sdk)

    def test_context_configuration_is_recursively_readonly_and_state_is_a_draft(self):
        original = {"configuration": {"nested": {"risk": 1}}, "state": {"user": {}}}
        context = self.sdk.Context(json.dumps(original))
        with self.assertRaises(TypeError):
            context.configuration.nested.risk = 2
        with self.assertRaises(TypeError):
            context.configuration = {}
        context.state["user"]["counter"] = 1
        result = json.loads(context.finish(self.sdk.Effects()))
        self.assertEqual(result["state"]["user"]["counter"], 1)
        self.assertEqual(original["state"]["user"], {})

    def test_market_and_compute_requests_are_deferred_effects(self):
        context = self.sdk.Context('{"state":{}}')
        self.assertIsNone(context.market.snapshot({"mode": {"kind": "LATEST"}}))
        self.assertIsNone(context.compute.submit({"max_rows": 4}))
        result = json.loads(context.finish(self.sdk.Effects()))
        self.assertEqual([request["kind"] for request in result["effects"]["requests"]], ["snapshot", "compute"])
        self.assertFalse(hasattr(context, "exchange"))

    def test_callback_must_return_effects_and_output_is_bounded(self):
        with self.assertRaises(TypeError):
            self.sdk.Context('{"state":{}}').finish({"intents": []})
        output = self.sdk.BoundedOutput()
        output.write("x" * 100000)
        output.write("secret")
        self.assertEqual(len(output.value), 65536)


if __name__ == "__main__":
    unittest.main()
