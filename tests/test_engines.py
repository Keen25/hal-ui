"""Coverage for the multi-engine registry (generic ACP agents).

Every non-Copilot/opencode engine is a spec-generic ``_acp_engine`` entry: its
model catalog and model switch are derived from the session/new response. The
end-to-end cases boot the real AcpProc against a tiny fake ACP agent so the
chosen wire method (set_config_option vs set_model) is asserted on the wire.

Run: python -m unittest discover -s tests   (from the repo root)
"""
import json
import os
import sys
import tempfile
import textwrap
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("HALUI_DEBUG", "0")

import server  # noqa: E402

MAJOR = {"copilot", "opencode", "claude", "codex", "gemini", "qwen", "cursor",
         "goose", "kimi", "auggie", "cline", "kilo", "vibe"}

FAKE_AGENT = textwrap.dedent(r'''
    import json, os, sys
    mode = os.environ["FAKE_MODE"]
    log = open(os.environ["FAKE_LOG"], "a")
    for line in sys.stdin:
        req = json.loads(line)
        log.write(json.dumps({"method": req.get("method"), "params": req.get("params")}) + "\n")
        log.flush()
        m, res = req.get("method"), {}
        if m == "session/new":
            res = {"sessionId": "s1"}
            if mode == "config":
                res["configOptions"] = [{"id": "mdl", "category": "model", "type": "select",
                                         "currentValue": "a",
                                         "options": [{"value": "a", "name": "A"},
                                                     {"value": "b", "name": "B"}]}]
            elif mode == "models":
                res["models"] = {"currentModelId": "a",
                                 "availableModels": [{"modelId": "a", "name": "A"},
                                                     {"modelId": "b", "name": "B"}]}
        if "id" in req:
            sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": res}) + "\n")
            sys.stdout.flush()
''')


class RegistryTest(unittest.TestCase):
    def test_major_engines_registered(self):
        self.assertTrue(MAJOR <= set(server.ENGINES))

    def test_descriptors_complete_and_consistent(self):
        for name, e in server.ENGINES.items():
            with self.subTest(engine=name):
                self.assertEqual(e["name"], name)
                self.assertTrue(e["label"] and e["bin_names"] and e["install"])
                argv = e["acp_argv"]("/bin/x")
                self.assertEqual(argv[0], "/bin/x")
                self.assertTrue(callable(e["parse_models"]))
                if not e["legacy_lane"]:
                    self.assertFalse(e["session_history"])

    def test_launch_argv_matches_acp_registry(self):
        expect = {"gemini": ["--acp"], "qwen": ["--acp"], "cursor": ["acp"],
                  "goose": ["acp"], "kimi": ["acp"], "auggie": ["--acp"],
                  "cline": ["--acp"], "kilo": ["acp"], "claude": [], "codex": [],
                  "vibe": []}
        for name, args in expect.items():
            self.assertEqual(server.ENGINES[name]["acp_argv"]("b"), ["b", *args])

    def test_cursor_does_not_claim_generic_agent_binary(self):
        # `agent` is a common unrelated wrapper name; never resolve it as Cursor.
        self.assertNotIn("agent", server.ENGINES["cursor"]["bin_names"])

    def test_acp_only_engine_has_no_legacy_argv(self):
        with self.assertRaises(RuntimeError):
            server._build_argv("hi", "sess", None, engine="gemini")

    def test_masked_config_advertises_install_state(self):
        cfg = server.masked_config()
        ids = {e["id"]: e for e in cfg["engines"]}
        self.assertTrue(MAJOR <= set(ids))
        for e in ids.values():
            self.assertIsInstance(e["installed"], bool)
            self.assertIn("install", e)
        self.assertFalse(ids["copilot"]["experimental"])
        self.assertTrue(ids["gemini"]["experimental"])


class ParseModelsTest(unittest.TestCase):
    def test_config_options_grouped(self):
        res = {"configOptions": [{"id": "model", "category": "model", "currentValue": "x/1",
                                  "options": [{"group": "x", "name": "X",
                                               "options": [{"value": "x/1", "name": "One"}]},
                                              {"value": "y/2", "name": "Two"}]}]}
        out = server._parse_models_acp(res)
        self.assertEqual([m["id"] for m in out["models"]], ["x/1", "y/2"])
        self.assertEqual(out["default"], "x/1")

    def test_models_state(self):
        res = {"models": {"currentModelId": "b",
                          "availableModels": [{"modelId": "a", "name": "A"},
                                              {"modelId": "b"}]}}
        out = server._parse_models_acp(res)
        self.assertEqual([m["id"] for m in out["models"]], ["a", "b"])
        self.assertEqual(out["models"][1]["name"], "b")
        self.assertEqual(out["default"], "b")

    def test_neither(self):
        self.assertIn("error", server._parse_models_acp({"sessionId": "s"}))

    def test_opencode_parser_unchanged_for_flat_options(self):
        res = {"configOptions": [{"id": "model", "currentValue": "p/m",
                                  "options": [{"value": "p/m", "name": "M"}]}]}
        self.assertEqual(server._parse_models_opencode(res),
                         {"models": [{"id": "p/m", "name": "M", "usage": "", "price": ""}],
                          "default": "p/m"})


class GenericModelSwitchE2ETest(unittest.TestCase):
    """Boot a real AcpProc against a fake generic ACP agent."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.script = os.path.join(self.tmp.name, "agent.py")
        self.log = os.path.join(self.tmp.name, "log.jsonl")
        with open(self.script, "w") as f:
            f.write(FAKE_AGENT)
        server.ENGINES["_fake"] = server._acp_engine(
            "_fake", "Fake", [sys.executable], [self.script], "n/a")
        os.environ["FAKE_LOG"] = self.log

    def tearDown(self):
        server.ENGINES.pop("_fake", None)
        self.tmp.cleanup()

    def _run(self, mode):
        os.environ["FAKE_MODE"] = mode
        p = server.AcpProc("b", cwd=self.tmp.name, engine="_fake")
        try:
            model = p.model
        finally:
            p.close()
        with open(self.log) as f:
            calls = [json.loads(ln) for ln in f]
        return model, calls

    def test_config_option_selector(self):
        model, calls = self._run("config")
        sw = [c for c in calls if c["method"] == "session/set_config_option"]
        self.assertEqual(sw[0]["params"], {"sessionId": "s1", "configId": "mdl", "value": "b"})
        self.assertEqual(model, "b")

    def test_models_state_uses_set_model(self):
        model, calls = self._run("models")
        sw = [c for c in calls if c["method"] == "session/set_model"]
        self.assertEqual(sw[0]["params"], {"sessionId": "s1", "modelId": "b"})
        self.assertEqual(model, "b")

    def test_no_model_surface_sends_nothing(self):
        model, calls = self._run("none")
        self.assertFalse([c for c in calls if "set_" in (c["method"] or "")])
        self.assertIsNone(model)


if __name__ == "__main__":
    unittest.main()
