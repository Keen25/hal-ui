#!/usr/bin/env python3
"""HAL-UI — Phase 1.

A zero-dependency web control plane for the GitHub Copilot CLI + the hive.

Seams:
  * Chat  -> drives `copilot -p <prompt> --output-format json --allow-all-tools`
             as a subprocess, relays its JSONL stream over SSE. Multi-turn memory
             via `-n <session>` (first turn) then `--resume=<session>`.
  * Hive  -> proxies the queue_server REST API on :5001 using the control token
             read from ~/.hive/worker.token (GET /status, POST /add).
  * GitHub-> shells authenticated `gh` (keyring) for the current user's open PRs
             and assigned issues.

Run:  python3 server.py --port 8900
Open: http://127.0.0.1:8900/
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import queue
import shutil
import subprocess
import threading
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse, quote

STATIC_DIR = Path(__file__).resolve().parent / "static"
COPILOT_BIN = shutil.which("copilot") or os.path.expanduser("~/.local/bin/copilot")
GH_BIN = shutil.which("gh") or "/usr/bin/gh"
AZ_BIN = shutil.which("az") or "/usr/bin/az"
HIVE_URL_DEFAULT = os.environ.get("HIVE_URL", "http://127.0.0.1:5001")
HIVE_TOKEN_FILE = os.path.expanduser("~/.hive/worker.token")
ADO_RESOURCE = "499b84ac-1321-427f-aa17-267ca6975798"  # Azure DevOps AAD app id
CONFIG_DIR = os.path.expanduser("~/.config/hal-ui")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

DEFAULT_CONFIG = {
    "ado_org": "https://dev.azure.com/1id",
    "ado_pat": "",          # optional; blank => use `az` AAD token
    "ado_identity": "",     # optional; blank => auto-resolve from ado_upn / az user
    "ado_upn": "",          # used to resolve identity; blank => `az account show` user
    "hive_url": HIVE_URL_DEFAULT,
    "hive_token": "",       # optional override; blank => ~/.hive/worker.token
    "default_model": "",
    "chat_fast": True,      # disable MCP servers in the chat lane for faster startup
}
_SECRET_KEYS = {"ado_pat", "hive_token"}
_config_lock = threading.Lock()


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    try:
        cfg.update(json.loads(Path(CONFIG_FILE).read_text()))
    except (OSError, json.JSONDecodeError):
        pass
    return cfg


def save_config(patch: dict) -> dict:
    with _config_lock:
        cfg = load_config()
        for k, v in patch.items():
            if k not in DEFAULT_CONFIG:
                continue
            if k in _SECRET_KEYS and (v is None or v == ""):
                continue  # blank secret => keep existing value
            cfg[k] = v
        os.makedirs(CONFIG_DIR, exist_ok=True)
        Path(CONFIG_FILE).write_text(json.dumps(cfg, indent=2))
        os.chmod(CONFIG_FILE, 0o600)
    return cfg


def masked_config() -> dict:
    cfg = load_config()
    out = {k: ("" if k in _SECRET_KEYS else v) for k, v in cfg.items()}
    for k in _SECRET_KEYS:
        out[k + "_set"] = bool(cfg.get(k))
    return out

_known_sessions: set[str] = set()
_sessions_lock = threading.Lock()

# --- tiny TTL cache for GitHub (gh calls are ~1s each) -----------------
_cache: dict[str, tuple[float, object]] = {}
_cache_lock = threading.Lock()


def cached(key: str, ttl: float, producer):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    with _cache_lock:
        _cache[key] = (now, val)
    return val


def _hive_token() -> str:
    cfg_tok = load_config().get("hive_token")
    if cfg_tok:
        return cfg_tok.strip()
    tok = os.environ.get("HIVE_TOKEN")
    if tok:
        return tok.strip()
    try:
        return Path(HIVE_TOKEN_FILE).read_text().strip()
    except OSError:
        return ""


def _hive_url() -> str:
    return (load_config().get("hive_url") or HIVE_URL_DEFAULT).rstrip("/")


def _hive_request(path: str, method: str = "GET", body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        _hive_url() + path, data=data, method=method,
        headers={
            "Authorization": f"Bearer {_hive_token()}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read() or b"{}")
        except Exception:
            return exc.code, {"error": exc.reason}
    except Exception as exc:  # pragma: no cover
        return 502, {"error": str(exc)}


def _gh_json(args: list[str]):
    try:
        out = subprocess.run(
            [GH_BIN, *args], capture_output=True, text=True, timeout=25,
        )
        if out.returncode != 0:
            return {"error": out.stderr.strip()[:300]}
        return json.loads(out.stdout or "[]")
    except Exception as exc:  # pragma: no cover
        return {"error": str(exc)}


def _github_mywork():
    prs = _gh_json([
        "search", "prs", "--author=@me", "--state=open", "--limit", "20",
        "--json", "number,title,repository,url,updatedAt,isDraft",
    ])
    issues = _gh_json([
        "search", "issues", "--assignee=@me", "--state=open",
        "--limit", "20", "--json", "number,title,repository,url,updatedAt",
    ])
    return {"prs": prs, "issues": issues}


# ---------------- Azure DevOps -----------------------------------------
_az_token: dict = {"tok": "", "exp": 0.0}
_az_lock = threading.Lock()


def _ado_auth_header() -> tuple[str, str | None]:
    """Return (Authorization header, error). Prefer PAT, else `az` AAD token."""
    pat = load_config().get("ado_pat")
    if pat:
        b = base64.b64encode(f":{pat}".encode()).decode()
        return f"Basic {b}", None
    with _az_lock:
        if _az_token["tok"] and _az_token["exp"] - time.time() > 120:
            return f"Bearer {_az_token['tok']}", None
        try:
            out = subprocess.run(
                [AZ_BIN, "account", "get-access-token", "--resource", ADO_RESOURCE,
                 "--query", "accessToken", "-o", "tsv"],
                capture_output=True, text=True, timeout=30,
            )
            if out.returncode != 0:
                return "", out.stderr.strip()[:200] or "az token failed"
            tok = out.stdout.strip()
            _az_token.update(tok=tok, exp=time.time() + 3000)
            return f"Bearer {tok}", None
        except Exception as exc:  # pragma: no cover
            return "", str(exc)


def _ado_request(url: str, method: str = "GET", body: dict | None = None):
    auth, err = _ado_auth_header()
    if err:
        return 401, {"error": f"ADO auth: {err}"}
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Authorization": auth, "Content-Type": "application/json",
                 "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read() or b"{}")
        except Exception:
            return exc.code, {"error": exc.reason}
    except Exception as exc:  # pragma: no cover
        return 502, {"error": str(exc)}


def _ado_identity(org: str) -> str:
    cfg = load_config()
    if cfg.get("ado_identity"):
        return cfg["ado_identity"]
    upn = cfg.get("ado_upn")
    if not upn:
        try:
            out = subprocess.run(
                [AZ_BIN, "account", "show", "--query", "user.name", "-o", "tsv"],
                capture_output=True, text=True, timeout=20,
            )
            upn = out.stdout.strip()
        except Exception:
            upn = ""
    if not upn:
        return ""
    vssps = org.replace("dev.azure.com", "vssps.dev.azure.com")
    code, data = _ado_request(
        f"{vssps}/_apis/identities?searchFilter=General&filterValue={upn}&api-version=7.0")
    if code == 200:
        vals = data.get("value") or []
        if vals:
            return vals[0].get("id", "")
    return ""


def _ado_mywork():
    cfg = load_config()
    org = (cfg.get("ado_org") or "").rstrip("/")
    if not org:
        return {"error": "ado_org not configured"}

    # 1) assigned work items (org-level WIQL -> batch details)
    wiql = {"query": (
        "SELECT [System.Id] FROM WorkItems WHERE [System.AssignedTo] = @Me "
        "AND [System.State] NOT IN ('Closed','Done','Removed','Resolved') "
        "ORDER BY [System.ChangedDate] DESC")}
    code, data = _ado_request(f"{org}/_apis/wit/wiql?api-version=7.0",
                              method="POST", body=wiql)
    workitems = []
    if code == 200:
        ids = [w["id"] for w in (data.get("workItems") or [])][:50]
        if ids:
            fields = ["System.Id", "System.Title", "System.WorkItemType",
                      "System.State", "System.TeamProject", "System.ChangedDate"]
            c2, d2 = _ado_request(f"{org}/_apis/wit/workitemsbatch?api-version=7.0",
                                  method="POST", body={"ids": ids, "fields": fields})
            if c2 == 200:
                for w in d2.get("value") or []:
                    f = w.get("fields", {})
                    proj = f.get("System.TeamProject", "")
                    workitems.append({
                        "id": w.get("id"),
                        "title": f.get("System.Title", ""),
                        "type": f.get("System.WorkItemType", ""),
                        "state": f.get("System.State", ""),
                        "project": proj,
                        "changed": f.get("System.ChangedDate", ""),
                        "url": f"{org}/{urllib.parse.quote(proj)}/_workitems/edit/{w.get('id')}",
                    })
            else:
                return {"error": f"workitemsbatch {c2}: {d2.get('error') or d2.get('message','')}"[:200]}
    else:
        return {"error": f"wiql {code}: {data.get('error') or data.get('message','')}"[:200]}

    # 2) my active pull requests (org-level PR search: as author AND as reviewer)
    prs = []
    ident = _ado_identity(org)
    if ident:
        def _fetch_prs(param, role):
            c, d = _ado_request(
                f"{org}/_apis/git/pullrequests?searchCriteria.{param}={ident}"
                f"&searchCriteria.status=active&api-version=7.0&$top=50")
            out = []
            if c == 200:
                for p in d.get("value") or []:
                    repo = p.get("repository", {})
                    prj = (repo.get("project") or {}).get("name", "")
                    prid = p.get("pullRequestId")
                    # vote of the current identity among reviewers (for reviewer rows)
                    vote = None
                    for rv in p.get("reviewers") or []:
                        if (rv.get("id") or "").lower() == ident.lower():
                            vote = rv.get("vote")
                            break
                    out.append({
                        "id": prid, "title": p.get("title", ""),
                        "repo": repo.get("name", ""), "project": prj,
                        "created": p.get("creationDate", ""),
                        "isDraft": p.get("isDraft", False),
                        "author": (p.get("createdBy") or {}).get("displayName", ""),
                        "role": role, "vote": vote,
                        "url": f"{org}/{urllib.parse.quote(prj)}/_git/"
                               f"{urllib.parse.quote(repo.get('name',''))}/pullrequest/{prid}",
                    })
            return out

        by_id = {}
        # author first so authored PRs keep role precedence, then reviewer
        for p in _fetch_prs("creatorId", "author"):
            by_id[p["id"]] = p
        for p in _fetch_prs("reviewerId", "reviewer"):
            if p["id"] in by_id:
                by_id[p["id"]]["role"] = "author+reviewer"
                by_id[p["id"]]["vote"] = p.get("vote")
            else:
                by_id[p["id"]] = p
        prs = sorted(by_id.values(), key=lambda x: x.get("created", ""), reverse=True)
    return {"workitems": workitems, "prs": prs,
            "identity_resolved": bool(ident)}


def _mcp_server_names() -> list[str]:
    """Names of configured MCP servers (read once, cached), used to disable them for speed."""
    global _mcp_names_cache
    try:
        return _mcp_names_cache
    except NameError:
        pass
    names: list[str] = []
    try:
        with open(os.path.expanduser("~/.copilot/mcp-config.json")) as fh:
            d = json.load(fh)
        names = list((d.get("mcpServers") or d or {}).keys())
    except Exception:
        names = []
    _mcp_names_cache = names
    return names


def _build_argv(prompt: str, session: str, model: str | None) -> list[str]:
    """Legacy per-turn spawn (used when chat_fast is OFF => full MCP available)."""
    argv = [COPILOT_BIN, "-p", prompt, "--output-format", "json", "--allow-all-tools"]
    with _sessions_lock:
        resume = session in _known_sessions
        _known_sessions.add(session)
    argv += (["--resume", session] if resume else ["-n", session])
    if model:
        argv += ["--model", model]
    return argv


# ======================================================================
# ACP persistent chat lane (chat_fast ON): one long-lived `copilot --acp`
# process per browser session. MCP stays cold (mcpServers:[]) so first turn
# boots once and later turns skip the ~5-7s CLI+MCP startup entirely.
# Protocol (JSON-RPC over stdio):
#   initialize -> session/new{cwd,mcpServers:[]} -> sessionId
#   session/set_model{modelId}    (optional)
#   session/prompt{sessionId,prompt:[{type:text,text}]} -> streams
#       session/update notifications, ends with the request's response (stopReason)
#   agent->client session/request_permission -> auto-answer allow_once
#   session/cancel{sessionId}     (notification) -> abort the running turn
# ======================================================================
class AcpProc:
    BOOT_TIMEOUT = 90

    def __init__(self, model: str | None = None):
        self.model = None
        self.session_id: str | None = None
        self.dead = False
        self._id = 0
        self._io_lock = threading.Lock()          # guards stdin writes + id counter
        self._pending: dict[int, queue.Queue] = {}  # req id -> Queue for its response
        self._turn_lock = threading.Lock()        # one turn at a time per process
        self._event_q: queue.Queue | None = None  # active turn's notification sink
        self._event_lock = threading.Lock()
        self.proc = subprocess.Popen(
            [COPILOT_BIN, "--acp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1, cwd=os.path.expanduser("~"))
        threading.Thread(target=self._reader, daemon=True).start()
        self._boot(model)

    # --- low level ----------------------------------------------------
    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _write(self, obj: dict):
        line = json.dumps(obj) + "\n"
        with self._io_lock:
            if self.proc.stdin:
                self.proc.stdin.write(line)
                self.proc.stdin.flush()

    def _request(self, method: str, params: dict, timeout: float = 120):
        rid = self._next_id()
        q: queue.Queue = queue.Queue()
        self._pending[rid] = q
        self._write({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        try:
            resp = q.get(timeout=timeout)
        finally:
            self._pending.pop(rid, None)
        if "error" in resp:
            raise RuntimeError(resp["error"])
        return resp.get("result", {})

    def _notify(self, method: str, params: dict):
        self._write({"jsonrpc": "2.0", "method": method, "params": params})

    def _reader(self):
        try:
            assert self.proc.stdout is not None
            for line in self.proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "method" in obj and "id" in obj:          # request from agent
                    self._on_agent_request(obj)
                elif "method" in obj:                         # notification
                    if obj["method"] == "session/update":
                        with self._event_lock:
                            if self._event_q is not None:
                                self._event_q.put(obj["params"]["update"])
                elif "id" in obj:                             # response to our request
                    q = self._pending.get(obj["id"])
                    if q is not None:
                        q.put(obj)
        except Exception:
            pass
        finally:
            self.dead = True
            with self._event_lock:
                if self._event_q is not None:
                    self._event_q.put({"sessionUpdate": "__dead__"})

    def _on_agent_request(self, obj: dict):
        method = obj.get("method", "")
        rid = obj["id"]
        if method == "session/request_permission":
            opts = obj.get("params", {}).get("options", []) or []
            pick = None
            for o in opts:
                oid = (o.get("optionId") or "").lower()
                if o.get("kind") == "allow_once" or oid == "allow_once":
                    pick = o
                    break
            if pick is None:
                for o in opts:
                    if "allow" in (o.get("optionId") or "").lower() or "allow" in (o.get("kind") or "").lower():
                        pick = o
                        break
            if pick is None and opts:
                pick = opts[0]
            result = ({"outcome": {"outcome": "selected", "optionId": pick["optionId"]}}
                      if pick else {"outcome": {"outcome": "cancelled"}})
            self._write({"jsonrpc": "2.0", "id": rid, "result": result})
        else:
            # unknown agent->client request: answer empty so the agent never blocks
            self._write({"jsonrpc": "2.0", "id": rid, "result": {}})

    # --- lifecycle ----------------------------------------------------
    def _boot(self, model: str | None):
        self._request("initialize", {
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}},
        }, timeout=self.BOOT_TIMEOUT)
        res = self._request("session/new", {
            "cwd": os.path.expanduser("~"), "mcpServers": [],
        }, timeout=self.BOOT_TIMEOUT)
        self.session_id = res.get("sessionId")
        if model:
            self.set_model(model)

    def set_model(self, model: str | None):
        if not model or model == self.model or not self.session_id:
            return
        try:
            self._request("session/set_model", {"sessionId": self.session_id, "modelId": model}, timeout=30)
            self.model = model
        except Exception:
            pass  # unknown model id => keep default

    def cancel(self):
        if self.session_id and not self.dead:
            try:
                self._notify("session/cancel", {"sessionId": self.session_id})
            except Exception:
                pass

    def close(self):
        try:
            self.cancel()
        except Exception:
            pass
        try:
            if self.proc.poll() is None:
                self.proc.terminate()
        except Exception:
            pass

    def prompt(self, text: str, emit, images: list | None = None):
        """Run one turn; translate ACP session/update stream to the SSE contract."""
        if self.dead or not self.session_id:
            emit("error", {"message": "acp session unavailable"})
            emit("done", {"code": 1})
            return
        blocks: list[dict] = []
        if text:
            blocks.append({"type": "text", "text": text})
        for im in (images or []):
            blocks.append({"type": "image", "mimeType": im.get("mime", "image/png"),
                           "data": im.get("data", "")})
        if not blocks:
            blocks = [{"type": "text", "text": text}]
        with self._turn_lock:
            q: queue.Queue = queue.Queue()
            with self._event_lock:
                self._event_q = q
            rid = self._next_id()
            resp_q: queue.Queue = queue.Queue()
            self._pending[rid] = resp_q
            full = []
            tools: list[str] = []
            try:
                self._write({"jsonrpc": "2.0", "id": rid, "method": "session/prompt",
                             "params": {"sessionId": self.session_id, "prompt": blocks}})
                while True:
                    try:
                        resp_q.get_nowait()
                        break
                    except queue.Empty:
                        pass
                    try:
                        upd = q.get(timeout=0.2)
                    except queue.Empty:
                        if self.dead:
                            emit("error", {"message": "acp process exited"})
                            break
                        continue
                    su = upd.get("sessionUpdate")
                    if su == "__dead__":
                        emit("error", {"message": "acp process exited"})
                        break
                    if su == "agent_message_chunk":
                        t = (upd.get("content") or {}).get("text", "")
                        if t:
                            full.append(t)
                            emit("delta", {"text": t})
                    elif su == "agent_thought_chunk":
                        emit("thinking", {})
                    elif su == "tool_call":
                        name = upd.get("title") or upd.get("kind") or "tool"
                        if name not in tools:
                            tools.append(name)
                        emit("tool", {"type": "tool_call", "name": name})
                    # tool_call_update / usage_update / config_option_update: ignored
            finally:
                self._pending.pop(rid, None)
                with self._event_lock:
                    self._event_q = None
            emit("message", {"text": "".join(full), "model": self.model, "tools": tools})
            emit("idle", {})
            emit("done", {"code": 0})


_acp_procs: dict[str, AcpProc] = {}
_acp_lock = threading.Lock()


def _acp_get(session: str, model: str | None) -> AcpProc:
    with _acp_lock:
        p = _acp_procs.get(session)
        if p is not None and not p.dead and p.proc.poll() is None:
            p.set_model(model)
            return p
        p = AcpProc(model)
        _acp_procs[session] = p
        return p


def _acp_stop(session: str):
    with _acp_lock:
        p = _acp_procs.get(session)
    if p is not None:
        p.cancel()


def _acp_close(session: str):
    with _acp_lock:
        p = _acp_procs.pop(session, None)
    with _live_lock:
        _live_turns.pop(session, None)
    if p is not None:
        p.close()


# --- live turn buffer (survive browser close / resume on reopen) -------
# A turn is driven by a background worker thread that appends SSE events into a
# per-session LiveTurn buffer instead of writing straight to one HTTP socket.
# The HTTP request that started the turn (and any later /api/chat/resume request)
# just relays from the buffer. A browser closing therefore detaches the viewer
# WITHOUT cancelling the turn; the worker keeps running and buffering, and a
# reopened tab replays the buffer from index 0 and continues live until done.
# The finished buffer is retained until the next turn replaces it, so a tab that
# reopens AFTER completion still gets the full transcript + terminal events.
class LiveTurn:
    def __init__(self, turn_id: str):
        self.turn_id = turn_id
        self.events: list[tuple[str, dict]] = []
        self.done = False
        self.canceller = None   # set by the runner: stops the turn (Stop button)
        self.cond = threading.Condition()

    def append(self, event: str, data: dict):
        with self.cond:
            self.events.append((event, data))
            self.cond.notify_all()

    def finish(self):
        with self.cond:
            self.done = True
            self.cond.notify_all()

    def read_from(self, idx: int):
        """Generator: yield buffered events from idx, blocking for new ones
        until the turn is done. Never holds the lock while yielding."""
        while True:
            with self.cond:
                while idx >= len(self.events) and not self.done:
                    self.cond.wait(timeout=0.5)
                batch = self.events[idx:]
                idx = len(self.events)
                done = self.done
            for ev in batch:
                yield ev
            if done and not batch:
                return


_live_turns: dict[str, LiveTurn] = {}
_live_lock = threading.Lock()


def _run_acp_turn(session, model, prompt, images, lt: LiveTurn):
    """Fast lane: run one turn on the persistent ACP process, buffering events."""
    try:
        acp = _acp_get(session, model)
    except Exception as exc:
        lt.append("error", {"message": f"acp launch failed: {exc}"})
        lt.append("done", {"code": 1})
        return
    lt.canceller = acp.cancel
    try:
        acp.prompt(prompt, lt.append, images=images)
    except Exception as exc:
        lt.append("error", {"message": str(exc)})
        lt.append("done", {"code": 1})


def _run_subprocess_turn(session, model, prompt, lt: LiveTurn):
    """Legacy lane (chat_fast OFF => full MCP): spawn `copilot -p ...` per turn,
    parse its JSONL, buffer events. Survives browser close because it runs in the
    turn worker thread, not the HTTP request."""
    emit = lt.append
    argv = _build_argv(prompt, session, model)
    try:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                cwd=os.path.expanduser("~"), text=True, bufsize=1)
    except Exception as exc:
        emit("error", {"message": f"launch failed: {exc}"})
        emit("done", {})
        return
    lt.canceller = proc.kill
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            etype = obj.get("type", "")
            data = obj.get("data", {})
            if etype == "assistant.message_delta":
                emit("delta", {"text": data.get("deltaContent", "")})
            elif etype == "assistant.message":
                tr = data.get("toolRequests") or []
                emit("message", {"text": data.get("content", ""),
                                 "model": data.get("model"),
                                 "tools": [t.get("name") for t in tr]})
            elif etype == "assistant.turn_start":
                emit("turn_start", {})
            elif etype == "assistant.idle":
                emit("idle", {})
            elif etype == "model.call_start":
                emit("thinking", {})
            elif etype.startswith("mcp.") or "tool" in etype:
                emit("tool", {"type": etype})
        proc.wait(timeout=5)
    except Exception as exc:
        emit("error", {"message": str(exc)})
    finally:
        if proc.poll() is None:
            proc.kill()
        err = proc.stderr.read()[-400:] if proc.stderr else ""
        emit("done", {"code": proc.returncode, "stderr": err.strip()})


def _start_turn(session: str, model: str | None, prompt: str, images: list) -> LiveTurn:
    """Begin a turn in a background worker thread that buffers its SSE events into
    a LiveTurn. Returns the buffer immediately so the HTTP request can relay it.
    Because the turn runs off-thread, a browser close does not end it — a reopened
    tab reattaches via /api/chat/resume. Replaces any prior buffer for the session.
    Lane choice mirrors _sse_chat: ACP fast lane, or legacy subprocess when
    chat_fast is off (images always force the ACP lane)."""
    turn_id = uuid.uuid4().hex
    lt = LiveTurn(turn_id)
    with _live_lock:
        _live_turns[session] = lt
    fast = bool(load_config().get("chat_fast", True)) or bool(images)
    lt.append("start", {"session": session, "acp": fast, "turn": turn_id})

    def worker():
        try:
            if fast:
                _run_acp_turn(session, model, prompt, images, lt)
            else:
                _run_subprocess_turn(session, model, prompt, lt)
        finally:
            lt.finish()

    threading.Thread(target=worker, name=f"turn-{session}", daemon=True).start()
    return lt


def _turn_stop(session: str):
    """Stop the in-flight turn for a session (Stop button): cancel the ACP prompt
    or kill the subprocess, and also cancel the persistent ACP session."""
    with _live_lock:
        lt = _live_turns.get(session)
    if lt is not None and lt.canceller is not None:
        try:
            lt.canceller()
        except Exception:
            pass
    _acp_stop(session)


# --- image attachment store (paste-to-chat) ---------------------------
# Browser POSTs base64 image data -> we hand back a short id which the SSE
# GET /api/chat references (query strings can't carry a whole image). Entries
# are single-use and expire, so nothing lingers in memory.
_attach_store: dict[str, tuple[str, str, float]] = {}  # id -> (mime, b64data, ts)
_attach_lock = threading.Lock()
ATTACH_TTL = 600.0
ATTACH_MAX_BYTES = 16 * 1024 * 1024  # cap per image (base64 length)


def _attach_put(mime: str, data: str) -> str:
    now = time.time()
    with _attach_lock:
        for k in [k for k, v in _attach_store.items() if now - v[2] > ATTACH_TTL]:
            _attach_store.pop(k, None)
        aid = base64.urlsafe_b64encode(os.urandom(9)).decode().rstrip("=")
        _attach_store[aid] = (mime or "image/png", data, now)
    return aid


def _attach_take(ids: list[str]) -> list[dict]:
    out: list[dict] = []
    with _attach_lock:
        for aid in ids:
            v = _attach_store.pop(aid, None)  # single-use
            if v is not None:
                out.append({"mime": v[0], "data": v[1]})
    return out


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    # --- helpers -------------------------------------------------------
    def _json(self, code: int, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, rel: str):
        if rel in ("", "/"):
            rel = "index.html"
        target = (STATIC_DIR / rel.lstrip("/")).resolve()
        if not str(target).startswith(str(STATIC_DIR)) or not target.is_file():
            self.send_error(404, "Not found")
            return
        body = target.read_bytes()
        ctype = {".html": "text/html; charset=utf-8", ".js": "text/javascript",
                 ".css": "text/css"}.get(target.suffix, "application/octet-stream")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # --- SSE chat ------------------------------------------------------
    def _relay_live(self, lt: "LiveTurn", from_idx: int):
        """Stream a LiveTurn buffer to this client from from_idx, blocking for
        new events until the turn completes. A write failure means the viewer
        detached (tab closed) — we return quietly; the turn keeps running."""
        try:
            for event, data in lt.read_from(from_idx):
                self.wfile.write(
                    f"event: {event}\ndata: {json.dumps(data)}\n\n".encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def _sse_resume(self, qs: dict):
        """Reattach a reopened tab to an in-flight (or just-finished) turn and
        replay its buffered events, then continue live until done."""
        session = (qs.get("session", ["default"])[0]).strip() or "default"
        turn = (qs.get("turn", [""])[0]).strip()
        try:
            from_idx = int(qs.get("from", ["0"])[0])
        except ValueError:
            from_idx = 0
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        with _live_lock:
            lt = _live_turns.get(session)
        if lt is None or (turn and lt.turn_id != turn):
            # nothing to resume: server restarted, or a newer/different turn.
            try:
                self.wfile.write(
                    b'event: done\ndata: {"code": 0, "stale": true}\n\n')
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        self._relay_live(lt, from_idx)

    def _sse_chat(self, qs: dict):
        prompt = (qs.get("prompt", [""])[0]).strip()
        session = (qs.get("session", ["default"])[0]).strip() or "default"
        model = (qs.get("model", [""])[0]).strip() or None
        attach_ids = [a for a in (qs.get("attach", [""])[0] or "").split(",") if a]
        images = _attach_take(attach_ids) if attach_ids else []
        if not prompt and not images:
            self.send_error(400, "missing prompt")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        # The turn runs in a background worker that buffers its events (LiveTurn);
        # this request just relays them. If the browser closes we stop relaying but
        # DO NOT cancel — the worker keeps running and a reopened tab reattaches via
        # /api/chat/resume. Lane (ACP vs subprocess) is chosen inside _start_turn.
        lt = _start_turn(session, model, prompt, images)
        self._relay_live(lt, 0)

    # --- routing -------------------------------------------------------
    def do_GET(self):
        p = urlparse(self.path)
        if p.path == "/api/chat":
            self._sse_chat(parse_qs(p.query))
        elif p.path == "/api/chat/resume":
            self._sse_resume(parse_qs(p.query))
        elif p.path == "/api/chat/stop":
            session = (parse_qs(p.query).get("session", [""])[0] or "").strip()
            _turn_stop(session)
            self._json(200, {"stopped": bool(session)})
        elif p.path == "/api/chat/close":
            session = (parse_qs(p.query).get("session", [""])[0] or "").strip()
            _acp_close(session)
            self._json(200, {"closed": bool(session)})
        elif p.path == "/api/hive/status":
            code, data = _hive_request("/status")
            self._json(200 if code == 200 else 502, data)
        elif p.path == "/api/hive/task":
            tid = (parse_qs(p.query).get("id", [""])[0] or "").strip()
            if not tid:
                self._json(400, {"error": "id is required"})
            else:
                code, data = _hive_request("/task/" + quote(tid, safe=""))
                self._json(200 if code == 200 else (code or 502), data)
        elif p.path == "/api/github/mywork":
            self._json(200, cached("mywork", 30.0, _github_mywork))
        elif p.path == "/api/ado/mywork":
            self._json(200, cached("ado_mywork", 30.0, _ado_mywork))
        elif p.path == "/api/settings":
            self._json(200, masked_config())
        else:
            self._serve_static(p.path)

    def do_POST(self):
        p = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            body = {}
        if p.path == "/api/chat/attach":
            data = (body.get("data") or "").strip()
            mime = (body.get("mime") or "image/png").strip()
            if not data:
                self._json(400, {"error": "data (base64) is required"})
                return
            if len(data) > ATTACH_MAX_BYTES:
                self._json(413, {"error": "image too large"})
                return
            self._json(200, {"id": _attach_put(mime, data)})
        elif p.path == "/api/hive/add":
            tid = (body.get("id") or "").strip()
            prompt = (body.get("prompt") or "").strip()
            if not tid or not prompt:
                self._json(400, {"error": "id and prompt are required"})
                return
            payload = {"id": tid, "prompt": prompt,
                       "priority": int(body.get("priority", 5)),
                       "kind": body.get("kind", "work")}
            for k in ("requires", "complexity", "model"):
                if body.get(k) not in (None, ""):
                    payload[k] = body[k]
            code, data = _hive_request("/add", method="POST", body=payload)
            self._json(code, data)
        elif p.path == "/api/settings":
            save_config(body if isinstance(body, dict) else {})
            # settings that affect ADO identity/token invalidate caches
            with _cache_lock:
                _cache.pop("ado_mywork", None)
            with _az_lock:
                _az_token.update(tok="", exp=0.0)
            self._json(200, masked_config())
        else:
            self.send_error(404, "Not found")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8900)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    if not Path(COPILOT_BIN).exists():
        raise SystemExit(f"copilot binary not found at {COPILOT_BIN}")
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    tok = "ok" if _hive_token() else "MISSING"
    print(f"HAL-UI Phase 2 on http://{args.host}:{args.port}/  "
          f"(copilot={COPILOT_BIN}, hive={_hive_url()} token={tok})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
