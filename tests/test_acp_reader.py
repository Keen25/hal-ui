"""Coverage for AcpProc._reader / _dispatch robustness (PR #6, finding F9).

A single malformed or unsupported ACP message must be logged and skipped, never
tear down the reader — otherwise a live opencode `acp` child is falsely reported
as exited mid-turn. These tests drive _reader against a fake process that replays
a realistic opencode ACP notification/request sequence with malformed payloads
mixed in, and assert the reader keeps going and marks the process dead only at
stdout EOF.

Run: python -m unittest discover -s tests   (from the repo root)
"""
import json
import os
import queue
import sys
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("HALUI_DEBUG", "0")  # keep test output quiet

import server  # noqa: E402
from server import AcpProc  # noqa: E402


class _FakeStdin:
    def __init__(self):
        self.written = []

    def write(self, s):
        self.written.append(s)

    def flush(self):
        pass


class _FakeStdout:
    """Iterable stdout that yields the given lines then EOF (StopIteration)."""

    def __init__(self, lines):
        self._it = iter(lines)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._it)


class _FakeProc:
    def __init__(self, lines):
        self.stdout = _FakeStdout(lines)
        self.stdin = _FakeStdin()


def _make_proc(lines, engine="opencode"):
    """Build an AcpProc without spawning a real child, wired to fake pipes."""
    p = object.__new__(AcpProc)
    p.engine = engine
    p.dead = False
    p.perm_mode = "auto"
    p._active_emit = None
    p._io_lock = threading.Lock()
    p._event_lock = threading.Lock()
    p._pending = {}
    p._event_q = queue.Queue()
    p.proc = _FakeProc([ln if ln.endswith("\n") else ln + "\n" for ln in lines])
    return p


def _drain(q):
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


class ReaderRobustnessTest(unittest.TestCase):
    def test_malformed_message_does_not_kill_reader(self):
        # A realistic opencode read-only investigation sequence: thinking, text,
        # two tool calls (glob + read) — with a malformed session/update (no
        # `update` key), a params-less notification, and a non-JSON line spliced
        # in. None of these may end the stream early.
        lines = [
            json.dumps({"jsonrpc": "2.0", "method": "session/update",
                        "params": {"update": {"sessionUpdate": "agent_thought_chunk"}}}),
            "this is not json at all",
            json.dumps({"jsonrpc": "2.0", "method": "session/update",
                        "params": {"update": {"sessionUpdate": "tool_call",
                                              "title": "glob", "kind": "read"}}}),
            # Malformed: session/update with no `update` — old code raised KeyError
            # here and tore down the reader (F9).
            json.dumps({"jsonrpc": "2.0", "method": "session/update",
                        "params": {}}),
            # Malformed: notification with no params at all.
            json.dumps({"jsonrpc": "2.0", "method": "session/update"}),
            json.dumps({"jsonrpc": "2.0", "method": "session/update",
                        "params": {"update": {"sessionUpdate": "tool_call",
                                              "title": "read", "kind": "read"}}}),
            json.dumps({"jsonrpc": "2.0", "method": "session/update",
                        "params": {"update": {"sessionUpdate": "agent_message_chunk",
                                              "content": {"text": "done"}}}}),
        ]
        p = _make_proc(lines)
        p._reader()  # runs to stdout EOF, then returns

        updates = _drain(p._event_q)
        kinds = [u.get("sessionUpdate") for u in updates]

        # All four well-formed updates survived, in order...
        self.assertEqual(
            kinds[:4],
            ["agent_thought_chunk", "tool_call", "tool_call", "agent_message_chunk"],
        )
        # ...and __dead__ is injected exactly once, only at the very end (EOF).
        self.assertEqual(kinds[-1], "__dead__")
        self.assertEqual(kinds.count("__dead__"), 1)
        self.assertTrue(p.dead)

    def test_agent_request_is_answered_and_stream_continues(self):
        # An agent->client permission request (auto mode) must be answered and
        # the reader must keep processing later notifications.
        lines = [
            json.dumps({"jsonrpc": "2.0", "id": 7,
                        "method": "session/request_permission",
                        "params": {"options": [
                            {"optionId": "allow_once", "kind": "allow_once",
                             "name": "Allow once"}]}}),
            json.dumps({"jsonrpc": "2.0", "method": "session/update",
                        "params": {"update": {"sessionUpdate": "agent_message_chunk",
                                              "content": {"text": "hi"}}}}),
        ]
        p = _make_proc(lines)
        p._reader()

        # The permission request got exactly one JSON-RPC reply.
        replies = [json.loads(s) for s in p.proc.stdin.written]
        perm = [r for r in replies if r.get("id") == 7]
        self.assertEqual(len(perm), 1)
        self.assertEqual(perm[0]["result"]["outcome"]["outcome"], "selected")
        self.assertEqual(perm[0]["result"]["outcome"]["optionId"], "allow_once")

        kinds = [u.get("sessionUpdate") for u in _drain(p._event_q)]
        self.assertIn("agent_message_chunk", kinds)
        self.assertEqual(kinds[-1], "__dead__")

    def test_response_delivered_to_pending_request(self):
        p = _make_proc([
            json.dumps({"jsonrpc": "2.0", "id": 3, "result": {"ok": True}}),
        ])
        waiter: queue.Queue = queue.Queue()
        p._pending[3] = waiter
        p._reader()
        resp = waiter.get_nowait()
        self.assertEqual(resp["result"], {"ok": True})

    def test_dead_only_on_eof_not_on_bad_message(self):
        # If the ONLY messages are malformed, the process must still be reported
        # alive until stdout EOF (i.e. no early __dead__ before the final one).
        p = _make_proc([
            json.dumps({"jsonrpc": "2.0", "method": "session/update", "params": {}}),
            "garbage",
            json.dumps({"jsonrpc": "2.0", "method": "totally/unknown",
                        "params": {"x": 1}}),
        ])
        p._reader()
        kinds = [u.get("sessionUpdate") for u in _drain(p._event_q)]
        self.assertEqual(kinds, ["__dead__"])  # nothing before EOF


class Cp1252StdoutDecodeTest(unittest.TestCase):
    """F9 round 2 — a Windows host decodes ACP stdout as cp1252 unless UTF-8 is
    pinned, so an engine's valid UTF-8 (`§`, `→`, emoji) raises UnicodeDecodeError
    inside the reader loop before per-message handling, faking a process exit.

    The child emits real UTF-8 bytes; cp1252 is the encoding a stock Windows
    Python resolves `text=True` to. The fix test drives server.TEXT_IO and
    asserts the bytes decode cleanly; the control drives the bare cp1252 path
    this fix replaces and asserts it raises — a regression that drops the
    explicit encoding turns the control green and fails the suite.
    """

    NON_ASCII = "§ → 😀 café \u201cquote\u201d"

    def _child_argv(self):
        # Emit one UTF-8 ACP-shaped line, independent of the child's own locale:
        # ensure_ascii=False keeps the non-ASCII chars, written as raw UTF-8.
        line = json.dumps({"jsonrpc": "2.0", "method": "session/update",
                           "params": {"update": {"sessionUpdate": "agent_message_chunk",
                                                  "content": {"text": self.NON_ASCII}}}},
                          ensure_ascii=False)
        code = ("import sys;"
                "sys.stdout.buffer.write(%r.encode('utf-8'));"
                "sys.stdout.buffer.write(b'\\n');sys.stdout.buffer.flush()" % line)
        return [sys.executable, "-c", code]

    def test_utf8_stdout_survives_with_pinned_encoding(self):
        import subprocess as sp
        p = sp.Popen(self._child_argv(), stdout=sp.PIPE, stderr=sp.DEVNULL,
                     text=True, bufsize=1, **server.TEXT_IO)
        try:
            line = p.stdout.readline()
        finally:
            p.stdout.close()
            p.wait(timeout=10)
        obj = json.loads(line)
        self.assertEqual(obj["params"]["update"]["content"]["text"], self.NON_ASCII)

    def test_control_cp1252_decode_raises_on_utf8(self):
        # The pre-fix pattern on Windows: text mode resolved to cp1252. The
        # emoji's UTF-8 bytes (0xF0 0x9F ...) hit an undefined cp1252 code point
        # and raise 'charmap' UnicodeDecodeError — Robert's reproduced failure.
        import subprocess as sp
        p = sp.Popen(self._child_argv(), stdout=sp.PIPE, stderr=sp.DEVNULL,
                     text=True, bufsize=1, encoding="cp1252")
        try:
            with self.assertRaises(UnicodeDecodeError):
                p.stdout.readline()
        finally:
            p.stdout.close()
            p.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
