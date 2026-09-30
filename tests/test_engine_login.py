"""Coverage for the engine sign-in job (/api/engine/login).

A fake ACP agent mimics Google Antigravity's consumer OAuth: session/new blocks
while a one-shot loopback listener waits for the browser redirect, and the
consent URL (carrying redirect_uri) is printed on stderr. A second mode mimics
agents that require an explicit ACP ``authenticate`` call first.

Run: python -m unittest discover -s tests   (from the repo root)
"""
import os
import sys
import tempfile
import textwrap
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("HALUI_DEBUG", "0")

import server  # noqa: E402

FAKE = textwrap.dedent(r'''
    import http.server, json, os, sys, threading, urllib.parse
    mode = os.environ["FAKE_LOGIN_MODE"]
    got = {}
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            got.update(urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query))
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
        def log_message(self, *a): pass
    authed = False
    def out(o):
        sys.stdout.write(json.dumps(o) + "\n"); sys.stdout.flush()
    for line in sys.stdin:
        req = json.loads(line); m = req.get("method"); rid = req.get("id")
        if m == "initialize":
            out({"jsonrpc": "2.0", "id": rid, "result": {"protocolVersion": 1,
                 "authMethods": [{"id": "gemini-api-key"}, {"id": "oauth-personal"}]}})
        elif m == "authenticate":
            authed = req["params"]["methodId"] == "oauth-personal"
            out({"jsonrpc": "2.0", "id": rid, "result": {}})
        elif m == "session/new":
            if mode == "explicit" and not authed:
                out({"jsonrpc": "2.0", "id": rid, "error": {"code": -32000,
                     "message": "Authentication required"}})
                continue
            if mode == "loopback":
                srv = http.server.HTTPServer(("127.0.0.1", 0), H)
                port = srv.server_address[1]
                print("Open the following link to authenticate the ACP server: "
                      "https://accounts.google.com/o/oauth2/v2/auth?client_id=x&redirect_uri="
                      + urllib.parse.quote(f"http://127.0.0.1:{port}/", safe=""),
                      file=sys.stderr, flush=True)
                srv.timeout = 20
                srv.handle_request()
                if got.get("code") != ["good"]:
                    out({"jsonrpc": "2.0", "id": rid, "error": {"code": -32000,
                         "message": "The authentication flow did not complete successfully."}})
                    continue
            out({"jsonrpc": "2.0", "id": rid, "result": {"sessionId": "s1"}})
''')


def _wait(pred, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        s = server._login_public()
        if pred(s):
            return s
        time.sleep(0.05)
    raise AssertionError(f"timed out; state={server._login_public()}")


class EngineLoginTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.script = os.path.join(self.tmp.name, "agent.py")
        with open(self.script, "w") as f:
            f.write(FAKE)
        server.ENGINES["_fakelogin"] = server._acp_engine(
            "_fakelogin", "Fake Login", [sys.executable], [self.script], "n/a")
        _wait(lambda s: not s["running"])

    def tearDown(self):
        server._engine_login_cancel()
        _wait(lambda s: not s["running"])
        server.ENGINES.pop("_fakelogin", None)
        self.tmp.cleanup()

    def test_already_signed_in(self):
        os.environ["FAKE_LOGIN_MODE"] = "ready"
        server._engine_login_start("_fakelogin", timeout=20)
        s = _wait(lambda s: s["done"])
        self.assertTrue(s["ok"], s)

    def test_loopback_flow_with_pasted_redirect(self):
        os.environ["FAKE_LOGIN_MODE"] = "loopback"
        server._engine_login_start("_fakelogin", timeout=20)
        s = _wait(lambda s: s["redirect_port"])
        self.assertIn("accounts.google.com", s["url"])
        port = s["redirect_port"]
        # Wrong host / wrong port / missing code are all refused.
        self.assertIn("error", server._engine_login_callback(f"http://evil.example:{port}/?code=good"))
        self.assertIn("error", server._engine_login_callback(f"http://127.0.0.1:{port + 1}/?code=good"))
        self.assertIn("error", server._engine_login_callback(f"http://127.0.0.1:{port}/?state=x"))
        out = server._engine_login_callback(f"http://127.0.0.1:{port}/?state=x&code=good")
        self.assertNotIn("error", out)
        s = _wait(lambda s: s["done"])
        self.assertTrue(s["ok"], s)

    def test_loopback_flow_failure_is_reported(self):
        os.environ["FAKE_LOGIN_MODE"] = "loopback"
        server._engine_login_start("_fakelogin", timeout=20)
        port = _wait(lambda s: s["redirect_port"])["redirect_port"]
        server._engine_login_callback(f"http://127.0.0.1:{port}/?code=bad")
        s = _wait(lambda s: s["done"])
        self.assertFalse(s["ok"])
        self.assertIn("did not complete", s["msg"])

    def test_explicit_authenticate_fallback(self):
        os.environ["FAKE_LOGIN_MODE"] = "explicit"
        server._engine_login_start("_fakelogin", timeout=20)
        s = _wait(lambda s: s["done"])
        self.assertTrue(s["ok"], s)
        self.assertTrue(any("authenticate(oauth-personal)" in l for l in s["log"]))

    def test_second_engine_refused_while_running(self):
        os.environ["FAKE_LOGIN_MODE"] = "loopback"
        server._engine_login_start("_fakelogin", timeout=20)
        _wait(lambda s: s["redirect_port"])
        self.assertIn("error", server._engine_login_start("copilot"))

    def test_callback_without_pending_login(self):
        self.assertIn("error", server._engine_login_callback("http://127.0.0.1:1/?code=x"))

    def test_unknown_engine(self):
        self.assertIn("error", server._engine_login_start("nope"))


if __name__ == "__main__":
    unittest.main()
