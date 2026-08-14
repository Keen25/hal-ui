#!/usr/bin/env python3
"""HAL-UI — Phase 1.

A zero-dependency web control plane for the GitHub Copilot CLI + the hive.

Seams:
  * Chat  -> drives `copilot -p <prompt> --output-format json --allow-all-tools`
             as a subprocess, relays its JSONL stream over SSE. Multi-turn memory
             via a stable per-session `--session-id <uuid>` (idempotent create
             on the first turn, resume thereafter).
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
import collections
import concurrent.futures
import json
import os
import queue
import re
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
GH_BIN = shutil.which("gh") or "/usr/bin/gh"
AZ_BIN = shutil.which("az") or "/usr/bin/az"


# --- Engine abstraction: which coding-agent CLI drives the chat lanes --------
# hal-ui was born Copilot-only. Both Copilot and opencode expose the SAME ACP
# (Zed Agent Client Protocol) over stdio, so the persistent chat lane (chat_fast
# ON, the default) is engine-agnostic — only the launch argv differs
# (`copilot --acp` vs `opencode acp`) and the JSON-RPC method vocabulary is
# shared. The legacy per-turn lane (chat_fast OFF) parses Copilot's proprietary
# `--output-format json` stream, so it stays Copilot-only; engines that don't set
# ``legacy_lane`` are forced onto the ACP lane. Likewise session history
# (ACP session/list + the on-disk ~/.copilot/session-state transcripts) is a
# Copilot-only surface, gated by ``session_history``.
def _resolve_bin(names: list[str], fallbacks: list[str]) -> str | None:
    """Resolve a runnable binary from PATH names then explicit fallbacks.

    Returns the resolved path, or None if nothing runnable was found — callers
    that need a definite string for display fall back themselves. Resolution is
    intentionally re-runnable (not cached) so a binary installed *after* the
    server started is picked up on the next call instead of staying rejected
    until a restart."""
    for n in names:
        p = shutil.which(n)
        if p:
            return p
    for f in fallbacks:
        fp = os.path.expanduser(f)
        if os.path.isfile(fp) and os.access(fp, os.X_OK):
            return fp
    return None


def _engine_bin(name: str) -> str:
    """Live-resolved path to an engine's binary (re-runs _resolve_bin every
    call so a freshly installed CLI is found without a restart). Falls back to
    the first configured name if nothing is runnable yet, so argv construction
    still yields a string (the launch then fails cleanly with a clear error)."""
    e = ENGINES[name]
    return _resolve_bin(e["bin_names"], e["bin_fallbacks"]) or e["bin_names"][0]


def _parse_models_copilot(res: dict) -> dict:
    """Copilot advertises its catalog under session/new ``models`` —
    availableModels + currentModelId, filtered by copilotEnablement."""
    models = res.get("models", {}) or {}
    out = []
    for m in models.get("availableModels", []) or []:
        meta = m.get("_meta") or {}
        if meta.get("copilotEnablement") not in (None, "enabled"):
            continue
        out.append({"id": m.get("modelId", ""), "name": m.get("name", ""),
                    "usage": meta.get("copilotUsage", ""),
                    "price": meta.get("copilotPriceCategory", "")})
    if not out:
        return {"error": "no models returned"}
    return {"models": out, "default": models.get("currentModelId", "")}


def _parse_models_opencode(res: dict) -> dict:
    """opencode advertises its catalog under session/new ``configOptions`` — a
    select with id "model" (value/name pairs + currentValue), not Copilot's
    ``models.availableModels``."""
    out, default = [], ""
    for opt in res.get("configOptions", []) or []:
        if opt.get("id") != "model" and opt.get("category") != "model":
            continue
        default = opt.get("currentValue", "") or default
        for o in opt.get("options", []) or []:
            out.append({"id": o.get("value", ""), "name": o.get("name", ""),
                        "usage": "", "price": ""})
        break
    if not out:
        return {"error": "no models returned"}
    return {"models": out, "default": default}


ENGINES: dict[str, dict] = {
    "copilot": {
        "name": "copilot",
        "label": "GitHub Copilot CLI",
        # Command names (PATH) + explicit fallbacks, resolved live via
        # _engine_bin so a post-startup install is picked up without a restart.
        "bin_names": ["copilot"],
        "bin_fallbacks": ["~/.local/bin/copilot"],
        "acp_argv": lambda b: [b, "--acp"],
        # Per-engine ACP model-switch op (method, params) for a live session —
        # Copilot uses session/set_model{modelId}.
        "set_model_op": lambda sid, model: (
            "session/set_model", {"sessionId": sid, "modelId": model}),
        # Legacy per-turn spawn (chat_fast OFF); parsed by _run_subprocess_turn.
        "oneshot_argv": lambda b, prompt, sid, model: (
            [b, "-p", prompt, "--output-format", "json", "--allow-all-tools",
             "--session-id", sid] + (["--model", model] if model else [])),
        "parse_models": _parse_models_copilot,
        "legacy_lane": True,       # supports the chat_fast-OFF subprocess lane
        "session_history": True,   # supports ACP session/list + session-state
        # On-disk transcript store scanned by the History full-text search.
        "session_state_dir": "~/.copilot/session-state",
        # Copilot's permission option vocabulary is known, so when no exact
        # allow_once is offered we may fall back to any allow* / first option.
        "permissive_fallback": True,
    },
    "opencode": {
        "name": "opencode",
        "label": "opencode",
        "bin_names": ["opencode"],
        "bin_fallbacks": ["~/.opencode/bin/opencode", "~/.local/bin/opencode"],
        "acp_argv": lambda b: [b, "acp"],
        # opencode v1.18.18 does not honor session/set_model; it changes the
        # active model through session/set_config_option{configId:"model",value}.
        "set_model_op": lambda sid, model: (
            "session/set_config_option",
            {"sessionId": sid, "configId": "model", "value": model}),
        # opencode's `run --format json` stream schema differs from Copilot's, so
        # the legacy lane is disabled below and this argv is never taken today;
        # kept for a future per-engine stream adapter.
        "oneshot_argv": lambda b, prompt, sid, model: (
            [b, "run", "--format", "json"]
            + (["--model", model] if model else []) + [prompt]),
        "parse_models": _parse_models_opencode,
        "legacy_lane": False,      # run-stream JSON schema differs -> ACP lane only
        # opencode exposes ACP session/list, but the History full-text search
        # reads the on-disk transcript store, which is Copilot-specific — so the
        # whole history surface stays gated off until an opencode store adapter
        # exists.
        "session_history": False,
        "session_state_dir": None,
        # Unverified option vocabulary: never auto-pick a broad/persistent grant.
        "permissive_fallback": False,
    },
}


def _engine_name(cfg: dict | None = None) -> str:
    """The active engine id, validated against ENGINES. Tolerates a missing or
    non-string ``engine`` config value (defaults to copilot) so a hand-edited
    config or crafted request can never crash engine resolution."""
    raw = (cfg or load_config()).get("engine")
    name = raw.lower() if isinstance(raw, str) else ""
    return name if name in ENGINES else "copilot"


def _engine(cfg: dict | None = None) -> dict:
    """The active engine descriptor (from config ``engine``; default copilot)."""
    return ENGINES[_engine_name(cfg)]


def _engine_argv(name: str) -> list[str]:
    """ACP-launch argv for a specific engine name (bin live-resolved). Callers
    that captured an engine snapshot pass it here so a concurrent engine switch
    can't change the argv out from under them mid-launch."""
    e = ENGINES[name]
    return e["acp_argv"](_engine_bin(name))


def _acp_launch_argv() -> list[str]:
    """argv that starts the active engine as an ACP agent over stdio."""
    return _engine_argv(_engine_name())
def _resolve_sf() -> tuple[str, str]:
    """Locate the `sf` CLI and the bin dir that must be on PATH to run it.

    Under systemd the service PATH is only /usr/local/bin:/usr/bin, which
    contains neither the nvm-installed `sf` nor the `node` its shebang
    (`#!/usr/bin/env -S node`) needs. shutil.which("sf") therefore fails and the
    CR tab silently disappears even though the sf auth token is valid on disk.
    Fall back to the nvm node bin dirs so the integration works regardless of
    the inherited PATH. Returns (sf_path, bin_dir); bin_dir is "" when unknown."""
    found = shutil.which("sf")
    if found:
        return found, os.path.dirname(found)
    import glob as _glob
    # Prefer the highest node version that ships an sf binary.
    cands = sorted(_glob.glob(os.path.expanduser("~/.nvm/versions/node/*/bin/sf")),
                   reverse=True)
    for c in cands:
        if os.path.exists(c):
            return c, os.path.dirname(c)
    return "sf", ""


SF_BIN, SF_BIN_DIR = _resolve_sf()
SF_EXPECTED_ORG_ID = "00D30000000n44BEAQ"   # Quest/One Identity Salesforce org


def _sf_env(base: dict | None = None) -> dict:
    """Environment for every `sf` invocation.

    Two things a background/systemd service needs that the ambient env lacks:
      1. PATH must include the nvm bin dir so `sf` and the `node` its shebang
         invokes are resolvable (systemd PATH is only /usr/local/bin:/usr/bin).
      2. The generic file keychain must be forced. By default `sf` stores its
         AES key in the OS keyring via `secret-tool`, which needs a live DBus
         session + unlocked wallet. Without them the token can't be decrypted
         and `sf` reports AuthDecryptError ("secret-tool: Cannot autolaunch
         D-Bus without X11 $DISPLAY"). SF_USE_GENERIC_UNIX_KEYCHAIN moves the
         key to ~/.sfdx/key.json so reads work headlessly regardless of the
         keyring/DBus state. (Both the sf and legacy sfdx var names are set.)

    The token must be (re)authorized once after this flag is in effect so it
    gets encrypted with the file key rather than the keyring key."""
    env = (base if base is not None else os.environ).copy()
    if SF_BIN_DIR:
        env["PATH"] = SF_BIN_DIR + os.pathsep + env.get("PATH", "")
    env.setdefault("SF_USE_GENERIC_UNIX_KEYCHAIN", "true")
    env.setdefault("SFDX_USE_GENERIC_UNIX_KEYCHAIN", "true")
    return env
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
    "engine": "copilot",    # coding-agent CLI backing the chat lanes: copilot | opencode
    "chat_fast": True,      # disable MCP servers in the chat lane for faster startup
    "chat_idle_timeout": 900,  # legacy lane: reap a copilot -p turn after this many
                               # seconds with zero output (wedged subprocess). 0 = off.
    "acp_idle_timeout": 900,   # ACP fast lane: abort + reap a turn after this many
                               # seconds with zero streamed output. 0 = off.
    "acp_cancel_grace": 8,     # seconds to wait for a cancelled ACP turn to stop
                               # before force-killing the wedged process. 0 = off.
    "acp_reap_idle": 1800,     # background reaper: close an ACP process untouched
                               # for this many seconds (abandoned session). 0 = off.
    "chat_permission_mode": "auto",  # ACP permission policy: auto (accept offered
                               # allow_once) | prompt (ask the browser each time).
    "chat_permission_timeout": 120,  # seconds to wait for a browser permission
                               # decision in prompt mode before cancelling (fail-safe).
    "chat_cwd": "",            # server default working directory for new chat sessions
                               # (validated; blank => user home).
    "sf_org": "",           # Salesforce target org username/alias; blank => auto-resolve
    "sf_products": "ActiveRolesServer,PasswordManager",  # default R&D queues for the CR tab
}
_SECRET_KEYS = {"ado_pat", "hive_token"}
_config_lock = threading.Lock()
_cfg_cache: dict | None = None       # last parsed config
_cfg_cache_mtime: float = -1.0       # mtime it was parsed at (-1 = none/no file)


def load_config() -> dict:
    """Load config as a fresh dict (DEFAULT_CONFIG merged with the on-disk file).

    Backed by an mtime-keyed in-memory cache so the hot paths that resolve the
    active engine per request (review finding N8) don't re-read + re-parse the
    file every call; an external edit still takes effect because a changed mtime
    invalidates the cache. Returns a copy so callers can't mutate the cache."""
    global _cfg_cache, _cfg_cache_mtime
    try:
        mtime = os.path.getmtime(CONFIG_FILE)
    except OSError:
        mtime = -1.0
    cached_cfg = _cfg_cache
    if cached_cfg is not None and mtime == _cfg_cache_mtime:
        return dict(cached_cfg)
    cfg = dict(DEFAULT_CONFIG)
    try:
        cfg.update(json.loads(Path(CONFIG_FILE).read_text()))
    except (OSError, json.JSONDecodeError):
        pass
    _cfg_cache, _cfg_cache_mtime = dict(cfg), mtime
    return cfg


def save_config(patch: dict) -> dict:
    global _cfg_cache, _cfg_cache_mtime
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
        # Refresh the cache under the write's new mtime so the very next
        # load_config() returns this value without a re-read.
        try:
            _cfg_cache, _cfg_cache_mtime = dict(cfg), os.path.getmtime(CONFIG_FILE)
        except OSError:
            _cfg_cache = None
    return cfg


def masked_config() -> dict:
    cfg = load_config()
    out = {k: ("" if k in _SECRET_KEYS else v) for k, v in cfg.items()}
    for k in _SECRET_KEYS:
        out[k + "_set"] = bool(cfg.get(k))
    out["hive_enabled"] = _hive_enabled()
    out["sf_enabled"] = _sf_enabled()
    # Advertise the engine registry so the Settings dropdown is generated from
    # the backend (single source of truth) instead of a hardcoded list that can
    # drift as engines are added/removed.
    out["engine"] = _engine_name(cfg)
    out["engines"] = [{"id": e["name"], "label": e["label"]}
                      for e in ENGINES.values()]
    # Capability flags for the active engine so the frontend can adapt (e.g.
    # explain an empty History tab, or disable the fast-lane toggle when the
    # legacy lane is unavailable) instead of hardcoding engine-specific UI.
    _e = _engine(cfg)
    out["session_history"] = bool(_e.get("session_history"))
    out["legacy_lane"] = bool(_e.get("legacy_lane"))
    # Effective (validated) server default working directory for new chats, so
    # the frontend can show it and offer a per-session override (R3).
    out["chat_cwd"] = _validated_cwd(cfg.get("chat_cwd"))
    out["chat_permission_mode"] = _permission_mode(cfg.get("chat_permission_mode"))
    return out


def _validated_cwd(raw: str | None) -> str:
    """Resolve a working directory to an existing absolute path, or fall back to
    the user home. Guards against a hand-edited/crafted value pointing at a
    missing path or a non-directory (R3): an invalid cwd would otherwise make the
    engine's session/new fail on every turn."""
    home = os.path.expanduser("~")
    if not isinstance(raw, str) or not raw.strip():
        return home
    path = os.path.abspath(os.path.expanduser(raw.strip()))
    try:
        return path if os.path.isdir(path) else home
    except Exception:
        return home


def _permission_mode(raw: str | None) -> str:
    """The ACP permission policy, validated (R1). ``prompt`` surfaces each
    permission request to the browser; anything else is the auto default."""
    return "prompt" if isinstance(raw, str) and raw.strip().lower() == "prompt" else "auto"

# Fixed namespace so a given browser session always maps to the same copilot
# session UUID. Using a stable UUID (via --session-id) makes each turn an
# idempotent create-or-resume, which survives hal-ui restarts and avoids the
# duplicate-name ambiguity that -n/--resume-by-name produced.
_COPILOT_SESSION_NS = uuid.UUID("5f3d9b2e-1c4a-4e77-9a10-7b6c8d2e4f01")


def _copilot_session_id(session: str) -> str:
    return str(uuid.uuid5(_COPILOT_SESSION_NS, session))

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


def _hive_reachable() -> bool:
    """Probe whether a hive is actually running at the configured URL.

    A URL string alone isn't proof of deployment: `hive_url` defaults to a
    localhost address, so a plain "is it configured" check left the Hive tab
    visible on every box even when no hive was ever stood up. Here we make one
    short, cached request: any HTTP answer (200, or even 401/404) means a hive
    is deployed and the tab should show; a connection-level failure (refused /
    timeout / DNS) means it isn't, so the tab stays hidden. Cached briefly so
    the probe adds no latency to repeated /api/settings reads and, when a hive
    is down, costs at most one short timeout per TTL window."""
    url = _hive_url()

    def _probe() -> bool:
        req = urllib.request.Request(
            url + "/status", method="GET",
            headers={"Authorization": f"Bearer {_hive_token()}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=1.5):
                return True
        except urllib.error.HTTPError:
            return True  # the hive answered (even an auth/404 error) => deployed
        except Exception:
            return False  # refused / timeout / DNS => not deployed

    return cached("hive_reachable:" + url, 20.0, _probe)


def _hive_enabled() -> bool:
    """Hive integration is optional: enabled only when a hive URL is configured
    AND a hive is actually reachable there. Clear the Hive URL in Settings to
    decommission the hive; either way, if no hive is deployed the tab, buttons,
    and API routes stay disabled."""
    if not (load_config().get("hive_url") or "").strip():
        return False
    return _hive_reachable()


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


_GH_PR_FIELDS = "number,title,repository,url,updatedAt,isDraft"
# role precedence: authored PRs win, then review-requested, then assignee.
_GH_PR_ROLES = [
    ("--author=@me", "author"),
    ("--review-requested=@me", "reviewer"),
    ("--assignee=@me", "assignee"),
]


def _github_mywork():
    # Merge PRs across every relationship (author / reviewer / assignee) so
    # review-requested and assigned PRs surface, not just the ones we authored.
    by_key: dict = {}
    order: list = []
    prs_err = ""
    for flag, role in _GH_PR_ROLES:
        rows = _gh_json([
            "search", "prs", flag, "--state=open", "--limit", "20",
            "--json", _GH_PR_FIELDS,
        ])
        if isinstance(rows, dict) and rows.get("error"):
            prs_err = prs_err or rows["error"]
            continue
        for p in rows or []:
            repo = (p.get("repository") or {}).get("nameWithOwner", "")
            key = f"{repo}#{p.get('number')}"
            if key in by_key:
                existing = by_key[key]
                if role not in existing["_roles"]:
                    existing["_roles"].append(role)
            else:
                p["_roles"] = [role]
                by_key[key] = p
                order.append(key)
    prs = []
    for key in order:
        p = by_key[key]
        roles = p.pop("_roles", [])
        p["role"] = "+".join(roles)
        prs.append(p)
    if not prs and prs_err:
        prs = {"error": prs_err}
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


# ADO thread statuses that count as "resolved" for the comment badge.
_ADO_RESOLVED_STATUS = {"fixed", "wontfix", "closed", "bydesign"}


def _pr_thread_stats(org: str, pr: dict) -> None:
    """Fetch a PR's comment threads and annotate it in place with the last
    activity time and resolved/total user-discussion comment counts.

    The ADO PR search API returns neither a last-updated time nor comment
    counts, so we derive both from the threads endpoint (one call per PR):
      - updated: newest lastUpdatedDate across all non-deleted threads
        (system threads included, since pushes/votes are activity), falling
        back to the PR creation date.
      - threads_total / threads_resolved: only threads containing a real user
        comment (commentType 'text'); system-only threads are excluded, and a
        thread counts as resolved when its status is fixed/wontFix/closed/byDesign.
    """
    repo_id = pr.get("repo_id")
    if not repo_id:
        return
    c, d = _ado_request(
        f"{org}/_apis/git/repositories/{urllib.parse.quote(str(repo_id))}"
        f"/pullRequests/{pr['id']}/threads?api-version=7.0")
    if c != 200:
        return
    total = resolved = 0
    latest = ""
    for th in d.get("value") or []:
        if th.get("isDeleted"):
            continue
        upd = th.get("lastUpdatedDate") or th.get("publishedDate") or ""
        if upd > latest:
            latest = upd
        comments = th.get("comments") or []
        is_user = any((c or {}).get("commentType", "text") != "system"
                      and not (c or {}).get("isDeleted") for c in comments)
        if not is_user:
            continue
        total += 1
        if (th.get("status") or "").lower() in _ADO_RESOLVED_STATUS:
            resolved += 1
    pr["updated"] = latest or pr.get("created", "")
    pr["threads_total"] = total
    pr["threads_resolved"] = resolved


def _enrich_pr_threads(org: str, prs: list) -> None:
    """Populate updated time + comment counts for every PR concurrently."""
    if not prs:
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda p: _pr_thread_stats(org, p), prs))


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
                        "repo_id": repo.get("id", ""),
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
        _enrich_pr_threads(org, prs)
    return {"workitems": workitems, "prs": prs,
            "identity_resolved": bool(ident)}


# ======================================================================
# Salesforce Collaboration Requests (CRs). Support escalates customer cases to
# R&D as CRs (custom object Collaboration_Request__c). We shell out to the `sf`
# CLI (already authenticated to the Quest org) and surface three views in the UI:
#   - Mine       : CRs owned by the authenticated Salesforce user
#   - Unassigned : CRs still owned by a queue (Owner.Type == 'Queue')
#   - All        : every open R&D CR for the selected product queues
# The filter mirrors the Salesforce list view the team uses:
#   Status != 'Completed'  AND  Type__c = 'R&D'  AND  R_D_Queue__c IN (products)
# ======================================================================
_SF_DEFAULT_PRODUCTS = ["ActiveRolesServer", "PasswordManager"]
_SF_CR_FIELDS = ("Id,Name,Status__c,CR_Priority__c,Case_Subject__c,"
                 "Support_Product__c,R_D_Queue__c,CreatedDate,LastModifiedDate,"
                 "Case_Number__c,External_Tracking_ID__c,OwnerId,Owner.Type,"
                 "Owner.Name,At_Risk__c,Case_Escalated__c")


def _soql_esc(s: str) -> str:
    """Escape a value for inlining into a SOQL string literal."""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _sf_prio_n(priority) -> int:
    """Sortable priority: first integer in the value (e.g. 'Level 2'->2, 'P1'->1);
    missing/unknown sinks to the bottom."""
    m = re.search(r"(\d+)", str(priority or ""))
    return int(m.group(1)) if m else 99


def _sf_run(args: list, timeout: float = 60) -> dict:
    """Run an `sf` subcommand with --json and return the parsed object, or
    {'error': ...} on any failure."""
    try:
        out = subprocess.run([SF_BIN, *args, "--json"],
                             capture_output=True, text=True, timeout=timeout,
                             env=_sf_env())
    except FileNotFoundError:
        return {"error": "sf CLI not found"}
    except subprocess.TimeoutExpired:
        return {"error": "sf CLI timed out"}
    except Exception as exc:
        return {"error": str(exc)}
    try:
        return json.loads(out.stdout or "{}")
    except json.JSONDecodeError:
        return {"error": (out.stderr or out.stdout or "sf: no output").strip()[:200]}


def _sf_org() -> str:
    """Resolve (and cache) the authenticated Salesforce org username. Config
    sf_org overrides; otherwise pick the Connected org matching the expected
    orgId, falling back to any Connected org."""
    cfg = load_config()
    if cfg.get("sf_org"):
        return cfg["sf_org"]

    def producer():
        d = _sf_run(["org", "list"])
        res = d.get("result") or {}
        fallback = ""
        for grp in ("nonScratchOrgs", "sandboxes", "scratchOrgs", "other"):
            for o in res.get(grp) or []:
                if o.get("connectedStatus") != "Connected":
                    continue
                uname = o.get("username") or o.get("alias") or ""
                if o.get("orgId") == SF_EXPECTED_ORG_ID:
                    return uname
                fallback = fallback or uname
        return fallback

    return cached("sf_org", 300.0, producer)


def _sf_enabled() -> bool:
    """True when the sf CLI is present and an org is resolvable => show the CR tab."""
    if not (shutil.which("sf") or (SF_BIN and os.path.sep in SF_BIN and os.path.exists(SF_BIN))):
        return False
    return bool(_sf_org())


def _sf_cache_clear() -> None:
    """Drop every cached Salesforce-derived value so status/enabled/products
    re-evaluate after a (re)authentication."""
    with _cache_lock:
        for k in [k for k in _cache if k.startswith("sf_")]:
            _cache.pop(k, None)


def _desktop_env() -> dict:
    """Best-effort DISPLAY/XAUTHORITY for the logged-in HAL desktop so a
    subprocess (the sf web-login browser) can open a window. The systemd user
    service inherits no DISPLAY, so we lift it from a live desktop process (same
    uid => readable), falling back to :0 + the newest /run/user xauth cookie."""
    try:
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue
            try:
                raw = Path(f"/proc/{pid}/environ").read_bytes()
            except OSError:
                continue
            e = dict(kv.split("=", 1) for kv in
                     raw.decode("utf-8", "ignore").split("\0") if "=" in kv)
            if e.get("DISPLAY"):
                out = {"DISPLAY": e["DISPLAY"]}
                if e.get("XAUTHORITY"):
                    out["XAUTHORITY"] = e["XAUTHORITY"]
                return out
    except OSError:
        pass
    out = {"DISPLAY": ":0"}
    import glob as _glob
    xs = sorted(_glob.glob(f"/run/user/{os.getuid()}/xauth_*"))
    if xs:
        out["XAUTHORITY"] = xs[-1]
    return out


def _sf_status() -> dict:
    """Salesforce connection status for the Settings panel. Reports the org
    matching the expected Quest orgId (or any authorized org), including broken
    states like AuthDecryptError so the UI can prompt a reconnect."""
    present = bool(shutil.which("sf") or
                   (SF_BIN and os.path.sep in SF_BIN and os.path.exists(SF_BIN)))
    if not present:
        return {"cli": False, "connected": False, "status": "sf CLI not found",
                "org": "", "orgId": "", "instanceUrl": ""}
    d = _sf_run(["org", "list"])
    if "result" not in d:
        return {"cli": True, "connected": False,
                "status": d.get("error") or "org list failed",
                "org": "", "orgId": "", "instanceUrl": ""}
    res = d["result"] or {}
    chosen = anyorg = None
    for grp in ("nonScratchOrgs", "sandboxes", "scratchOrgs", "other"):
        for o in res.get(grp) or []:
            anyorg = anyorg or o
            if o.get("orgId") == SF_EXPECTED_ORG_ID:
                chosen = o
                break
        if chosen:
            break
    o = chosen or anyorg or {}
    st = o.get("connectedStatus") or ("unknown" if o else "no authorized orgs")
    return {"cli": True, "connected": st == "Connected", "status": st,
            "org": o.get("username") or o.get("alias") or "",
            "orgId": o.get("orgId") or "", "instanceUrl": o.get("instanceUrl") or ""}


_sf_login_lock = threading.Lock()
_sf_login_state = {"running": False, "url": "", "done": False,
                   "ok": False, "msg": ""}


def _sf_reconnect_start() -> dict:
    """Kick off `sf org login web` for the Quest org in the background. Opens a
    browser on the HAL desktop (DISPLAY lifted from the live session) and also
    returns the login URL so it can be completed manually. Returns as soon as
    the URL is known (or ~8s), leaving the flow running; SF caches are cleared
    when it finishes so the CR tab reappears without a restart."""
    with _sf_login_lock:
        if _sf_login_state["running"]:
            return {"started": True, "already": True, "url": _sf_login_state["url"]}
        _sf_login_state.update(running=True, url="", done=False, ok=False, msg="")
    url_evt = threading.Event()

    def worker():
        env = _sf_env()
        env.update(_desktop_env())
        cmd = [SF_BIN, "org", "login", "web",
               "--instance-url", "https://login.salesforce.com", "--alias", "90729"]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, env=env)
        except Exception as exc:
            with _sf_login_lock:
                _sf_login_state.update(running=False, done=True, ok=False,
                                       msg=str(exc)[:200])
            url_evt.set()
            return
        for line in proc.stdout or []:
            m = re.search(r"https?://\S+", line)
            if m:
                with _sf_login_lock:
                    if not _sf_login_state["url"]:
                        _sf_login_state["url"] = m.group(0)
                url_evt.set()
        try:
            rc = proc.wait(timeout=300)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = -1
        _sf_cache_clear()
        with _sf_login_lock:
            _sf_login_state.update(running=False, done=True, ok=(rc == 0),
                                   msg="authorized" if rc == 0 else
                                       ("timed out" if rc == -1 else f"exit {rc}"))
        url_evt.set()

    threading.Thread(target=worker, daemon=True).start()
    url_evt.wait(timeout=8)
    with _sf_login_lock:
        return {"started": True, "url": _sf_login_state["url"],
                "done": _sf_login_state["done"], "ok": _sf_login_state["ok"],
                "msg": _sf_login_state["msg"]}


def _sf_status_payload() -> dict:
    """Connection status plus any in-flight login progress (single poll source
    for the Settings panel)."""
    s = dict(cached("sf_status", 8.0, _sf_status))
    with _sf_login_lock:
        s["login"] = {"running": _sf_login_state["running"],
                      "url": _sf_login_state["url"],
                      "done": _sf_login_state["done"],
                      "ok": _sf_login_state["ok"], "msg": _sf_login_state["msg"]}
    return s


def _sf_query(soql: str, timeout: float = 60):
    """Run a SOQL query via the sf CLI. Returns (records, error_or_None)."""
    org = _sf_org()
    if not org:
        return None, "no Salesforce org (run: sf org login web)"
    d = _sf_run(["data", "query", "-o", org, "--query", soql], timeout=timeout)
    if "result" not in d:
        return None, (d.get("message") or d.get("name") or d.get("error")
                      or "sf query failed")[:200]
    return (d["result"].get("records") or []), None


def _sf_identity() -> str:
    """The authenticated user's Salesforce User Id (owner of the 'Mine' view)."""
    def producer():
        org = _sf_org()
        if not org:
            return ""
        disp = _sf_run(["org", "display", "-o", org])
        uname = ((disp.get("result") or {}).get("username")) or org
        recs, err = _sf_query(
            f"SELECT Id FROM User WHERE Username = '{_soql_esc(uname)}' LIMIT 1")
        if err or not recs:
            return ""
        return recs[0].get("Id", "")
    return cached("sf_identity", 600.0, producer)


def _sf_lightning_base() -> str:
    """Lightning base URL for building CR record links (…/lightning/r/…)."""
    def producer():
        org = _sf_org()
        if not org:
            return ""
        disp = _sf_run(["org", "display", "-o", org])
        inst = ((disp.get("result") or {}).get("instanceUrl")) or ""
        if not inst:
            return ""
        # https://<host>.my.salesforce.com -> https://<host>.lightning.force.com
        return inst.replace(".my.salesforce.com", ".lightning.force.com")
    return cached("sf_lightning", 600.0, producer)


def _sf_products() -> dict:
    """Distinct R&D queues among open R&D CRs (with counts) for the product picker."""
    recs, err = _sf_query(
        "SELECT R_D_Queue__c FROM Collaboration_Request__c "
        "WHERE Status__c != 'Completed' AND Type__c = 'R&D' "
        "AND R_D_Queue__c != null LIMIT 5000")
    if err:
        return {"error": err}
    counts: dict[str, int] = {}
    for r in recs:
        q = r.get("R_D_Queue__c")
        if q:
            counts[q] = counts.get(q, 0) + 1
    items = sorted(({"queue": k, "count": v} for k, v in counts.items()),
                   key=lambda x: (-x["count"], x["queue"].lower()))
    return {"products": items, "default": _SF_DEFAULT_PRODUCTS}


def _sf_yes(v) -> bool:
    """These SF fields come back as strings ('YES'/'NO', sometimes HTML-wrapped
    like '<img ... alt="Yes"/> YES'), not booleans. Treat only a real YES as true."""
    return "YES" in str(v or "").upper()


def _sf_cr_row(r: dict, base: str) -> dict:
    owner = r.get("Owner") or {}
    rid = r.get("Id") or ""
    return {
        "id": rid,
        "name": r.get("Name") or "",
        "status": r.get("Status__c") or "",
        "priority": r.get("CR_Priority__c") or "",
        "prio_n": _sf_prio_n(r.get("CR_Priority__c")),
        "subject": r.get("Case_Subject__c") or "",
        "product": r.get("Support_Product__c") or "",
        "queue": r.get("R_D_Queue__c") or "",
        "created": r.get("CreatedDate") or "",
        "modified": r.get("LastModifiedDate") or "",
        "case": r.get("Case_Number__c") or "",
        "ado": r.get("External_Tracking_ID__c") or "",
        "atRisk": _sf_yes(r.get("At_Risk__c")),
        "escalated": _sf_yes(r.get("Case_Escalated__c")),
        "owner": owner.get("Name") or "",
        "ownerType": owner.get("Type") or "",
        "url": (f"{base}/lightning/r/Collaboration_Request__c/{rid}/view"
                if base and rid else ""),
    }


def _sf_crs(products: list) -> dict:
    """Fetch open R&D CRs for the given R&D queues and partition into
    mine / unassigned / all."""
    products = [p.strip() for p in products if p and p.strip()]
    if not products:
        products = list(_SF_DEFAULT_PRODUCTS)
    in_list = ", ".join("'" + _soql_esc(p) + "'" for p in products)
    soql = (f"SELECT {_SF_CR_FIELDS} FROM Collaboration_Request__c "
            f"WHERE Status__c != 'Completed' AND Type__c = 'R&D' "
            f"AND R_D_Queue__c IN ({in_list}) "
            f"ORDER BY CreatedDate ASC LIMIT 500")
    recs, err = _sf_query(soql, timeout=90)
    if err:
        return {"error": err, "products": products}
    me = _sf_identity()
    base = _sf_lightning_base()
    mine, unassigned, allrows = [], [], []
    for r in recs:
        row = _sf_cr_row(r, base)
        allrows.append(row)
        if me and r.get("OwnerId") == me:
            mine.append(row)
        if (r.get("Owner") or {}).get("Type") == "Queue":
            unassigned.append(row)
    return {"mine": mine, "unassigned": unassigned, "all": allrows,
            "products": products, "identity_resolved": bool(me)}


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


def _build_argv(prompt: str, session: str, model: str | None,
                copilot_id: str | None = None, engine: str | None = None) -> list[str]:
    """Legacy per-turn spawn (used when chat_fast is OFF => full MCP available).

    Resumes by a stable per-session UUID via --session-id: copilot creates the
    session on the first turn and resumes it thereafter. This is idempotent and
    unambiguous, unlike resuming by name (-n/--resume=<name>), which broke when a
    hal-ui restart re-created a session and left two copilot sessions sharing a
    name ("Multiple sessions match the name ...").

    When copilot_id is given (a session revived from disk in the History tab), we
    resume THAT real copilot session id directly so the conversation continues
    with its full server-side context, instead of our uuid5-derived id.

    ``engine`` is the caller's snapshot (F1): the legacy argv must match the
    engine the lane was chosen for, never a concurrently-switched one."""
    sid = copilot_id or _copilot_session_id(session)
    e = ENGINES[engine] if engine else _engine()
    return e["oneshot_argv"](_engine_bin(e["name"]), prompt, sid, model)


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
def _classify_acp_error(err) -> tuple[str, str]:
    """Map a JSON-RPC error from session/prompt to (kind, message) so the UI can
    distinguish an authentication / model / provider / protocol failure (R2)."""
    if isinstance(err, dict):
        msg = str(err.get("message") or err.get("data") or "prompt failed")
    else:
        msg = str(err or "prompt failed")
    low = msg.lower()
    if any(k in low for k in ("auth", "unauthor", "token", "login", "credential", "api key")):
        kind = "authentication"
    elif any(k in low for k in ("model", "unknown model", "no such model")):
        kind = "model"
    elif any(k in low for k in ("provider", "upstream", "rate limit", "quota",
                                 "overloaded", "server error", "503", "429")):
        kind = "provider"
    else:
        kind = "protocol"
    return kind, msg


class AcpProc:
    BOOT_TIMEOUT = 90

    def __init__(self, model: str | None = None, load_id: str | None = None,
                 cwd: str | None = None, engine: str | None = None):
        self.model = None
        # Bind the proc to a single engine snapshot: the caller passes the engine
        # it resolved so a settings save landing between resolution and launch
        # can't spawn one CLI while tagging the proc as another (review finding
        # N5). argv below is built from this same captured name.
        self.engine = engine or _engine_name()
        self.session_id: str | None = None
        self.dead = False
        self.cwd = _validated_cwd(cwd) if cwd else os.path.expanduser("~")
        # Per-session permission policy (R1): "auto" accepts an offered
        # allow_once; "prompt" surfaces each request to the browser. Set by the
        # caller from the chat request; independent of the agent's own mode.
        self.perm_mode = "auto"
        self.last_used = time.time()   # updated each turn; drives idle reaping (R4)
        self._id = 0
        self._io_lock = threading.Lock()          # guards stdin writes + id counter
        self._pending: dict[int, queue.Queue] = {}  # req id -> Queue for its response
        self._turn_lock = threading.Lock()        # one turn at a time per process
        self._event_q: queue.Queue | None = None  # active turn's notification sink
        self._event_lock = threading.Lock()
        self._active_emit = None                  # active turn's emit (for R1 prompts)
        self._perm_q: queue.Queue = queue.Queue() # browser permission decisions (R1)
        self._awaiting_perm = False               # true while blocked on a browser decision
        self._stderr_tail: collections.deque = collections.deque(maxlen=50)
        self.proc = subprocess.Popen(
            _engine_argv(self.engine), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1, cwd=self.cwd)
        threading.Thread(target=self._reader, daemon=True).start()
        # Persistent stderr drainer (R4): a chatty engine (MCP logs/warnings) can
        # fill the 64 KB stderr pipe, block the child on write(), and wedge stdout
        # forever. Continuously drain it, keeping only a short tail for diagnostics.
        threading.Thread(target=self._drain_stderr, daemon=True).start()
        try:
            self._boot(model, load_id, cwd)
        except Exception:
            # A failed handshake (missing/incompatible engine, timeout) must not
            # leak the child process + its reader thread — tear it down before
            # propagating so the caller can surface a launch error cleanly.
            self.dead = True
            try:
                if self.proc.poll() is None:
                    self.proc.terminate()
                    try:
                        self.proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.proc.kill()
                        # Reap after SIGKILL so no zombie lingers until GC.
                        try:
                            self.proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pass
            except Exception:
                pass
            raise

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

    def _drain_stderr(self):
        """Continuously drain the child's stderr so it can never fill its pipe and
        block the child on write() (R4). Keeps only a short rolling tail, used to
        enrich error messages."""
        try:
            if self.proc.stderr is None:
                return
            for line in self.proc.stderr:
                line = line.rstrip("\n")
                if line:
                    self._stderr_tail.append(line)
        except Exception:
            pass

    def provide_permission(self, option_id: str | None):
        """Deliver a browser permission decision (R1) to a turn blocked in
        prompt mode. ``None``/unknown id => the request is cancelled (fail-safe)."""
        self._perm_q.put(option_id)

    def _on_agent_request(self, obj: dict):
        method = obj.get("method", "")
        rid = obj["id"]
        if method == "session/request_permission":
            opts = obj.get("params", {}).get("options", []) or []
            # R1: in prompt mode, surface the request to the browser and block
            # (bounded) for an explicit decision instead of auto-selecting.
            if getattr(self, "perm_mode", "auto") == "prompt" and self._active_emit:
                self._prompt_permission(rid, opts)
                return
            # Always prefer an exact allow_once (single-turn grant).
            pick = None
            for o in opts:
                oid = (o.get("optionId") or "").lower()
                if o.get("kind") == "allow_once" or oid == "allow_once":
                    pick = o
                    break
            if pick is None:
                if ENGINES.get(self.engine, {}).get("permissive_fallback"):
                    # Engine with a known option vocabulary (e.g. Copilot):
                    # fall back to any "allow"* option, then the first option.
                    for o in opts:
                        if ("allow" in (o.get("optionId") or "").lower()
                                or "allow" in (o.get("kind") or "").lower()):
                            pick = o
                            break
                    if pick is None and opts:
                        pick = opts[0]
                # Otherwise the option vocabulary is unverified: never auto-pick a
                # broad/persistent (allow_always) or unknown-first option — cancel
                # instead of guessing (fail-safe).
            result = ({"outcome": {"outcome": "selected", "optionId": pick["optionId"]}}
                      if pick else {"outcome": {"outcome": "cancelled"}})
            self._write({"jsonrpc": "2.0", "id": rid, "result": result})
        else:
            # unknown agent->client request: answer empty so the agent never blocks
            self._write({"jsonrpc": "2.0", "id": rid, "result": {}})

    def _prompt_permission(self, rid, opts: list):
        """Ask the browser to resolve a permission request (R1). Runs on the
        reader thread; the agent is blocked awaiting our response, so blocking
        here (bounded) is safe. On timeout or an unknown reply we cancel."""
        valid = {o.get("optionId") for o in opts if o.get("optionId")}
        # Drop any stale decision from an earlier request.
        try:
            while True:
                self._perm_q.get_nowait()
        except queue.Empty:
            pass
        self._awaiting_perm = True
        try:
            self._active_emit("permission", {
                "id": str(rid),
                "options": [{"optionId": o.get("optionId"),
                             "name": o.get("name") or o.get("optionId"),
                             "kind": o.get("kind")} for o in opts],
            })
            try:
                timeout = float(load_config().get("chat_permission_timeout", 120) or 0) or None
            except (TypeError, ValueError):
                timeout = 120.0
            try:
                chosen = self._perm_q.get(timeout=timeout)
            except queue.Empty:
                chosen = None
        finally:
            self._awaiting_perm = False
        if chosen in valid:
            result = {"outcome": {"outcome": "selected", "optionId": chosen}}
        else:
            chosen = None
            result = {"outcome": {"outcome": "cancelled"}}
        if self._active_emit:
            self._active_emit("permission_resolved", {"id": str(rid), "optionId": chosen})
        self._write({"jsonrpc": "2.0", "id": rid, "result": result})

    # --- lifecycle ----------------------------------------------------
    def _boot(self, model: str | None, load_id: str | None = None,
              cwd: str | None = None):
        self._request("initialize", {
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}},
        }, timeout=self.BOOT_TIMEOUT)
        wd = self.cwd
        if load_id:
            # Resume an existing on-disk session (revived from the History tab).
            # session/load replays the full history as session/update notifications
            # before returning; we have no active turn sink yet, so that replay is
            # harmlessly dropped here — the transcript is shown from /api/session/
            # transcript instead. The session is left ready to continue.
            self._request("session/load", {
                "sessionId": load_id, "cwd": wd, "mcpServers": [],
            }, timeout=self.BOOT_TIMEOUT)
            self.session_id = load_id
        else:
            res = self._request("session/new", {
                "cwd": wd, "mcpServers": [],
            }, timeout=self.BOOT_TIMEOUT)
            self.session_id = res.get("sessionId")
        if model:
            self.set_model(model)

    def set_model(self, model: str | None):
        if not model or model == self.model or not self.session_id:
            return
        # Model-switch wire op is engine-specific (Copilot: session/set_model;
        # opencode: session/set_config_option), so it lives in the descriptor of
        # the engine THIS proc was launched under (review finding N3).
        op = ENGINES.get(self.engine, {}).get("set_model_op")
        if not op:
            return
        method, params = op(self.session_id, model)
        try:
            self._request(method, params, timeout=30)
            self.model = model
        except Exception:
            pass  # unknown model id => keep default

    def cancel(self):
        if self.session_id and not self.dead:
            try:
                self._notify("session/cancel", {"sessionId": self.session_id})
            except Exception:
                pass
        # Unblock a turn parked on a browser permission decision so it can wind
        # down instead of hanging until the permission timeout (R1/R4).
        if getattr(self, "_awaiting_perm", False):
            try:
                self._perm_q.put(None)
            except Exception:
                pass

    def kill(self):
        """Bounded terminate -> wait -> kill -> wait, then close pipes (F2/R4).
        Idempotent; used by close() and by the wedged-turn force path."""
        self.dead = True
        p = self.proc
        try:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
                    try:
                        p.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
        except Exception:
            pass
        for stream in (p.stdin, p.stdout, p.stderr):
            try:
                if stream:
                    stream.close()
            except Exception:
                pass

    def close(self):
        """Fully tear down the process (F2). Previously this only sent
        terminate() with no wait/kill, so an engine that ignored SIGTERM stayed
        alive indefinitely after being dropped from tracking. Now we cancel the
        turn, unblock any permission wait, then reap the process and close its
        pipes."""
        try:
            self.cancel()
        except Exception:
            pass
        self.kill()

    def prompt(self, text: str, emit, images: list | None = None):
        """Run one turn; translate ACP session/update stream to the SSE contract.

        Robustness:
          * R2 — the final session/prompt response is inspected, not discarded:
            a JSON-RPC error emits an ``error`` + ``done`` code 1 (classified as
            auth/model/provider/protocol) while still preserving partial streamed
            text; a ``cancelled`` stopReason is reported as a cancel, not code 0.
          * R4 — an idle turn (no streamed output for ``acp_idle_timeout``) is
            force-killed and reported instead of hanging the session forever.
        """
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
        try:
            idle_timeout = float(load_config().get("acp_idle_timeout", 900) or 0)
        except (TypeError, ValueError):
            idle_timeout = 900.0
        with self._turn_lock:
            self.last_used = time.time()
            self._active_emit = emit
            q: queue.Queue = queue.Queue()
            with self._event_lock:
                self._event_q = q
            rid = self._next_id()
            resp_q: queue.Queue = queue.Queue()
            self._pending[rid] = resp_q
            full = []
            tools: list[str] = []
            resp = None
            failed = None          # (kind, message) for a JSON-RPC failure
            wedged = False
            last_activity = time.monotonic()

            def _apply(upd) -> bool:
                """Handle one session/update. Returns False if the stream is
                dead (agent exited) and the turn must stop."""
                su = upd.get("sessionUpdate")
                if su == "__dead__":
                    emit("error", {"message": "acp process exited"})
                    return False
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
                return True

            try:
                self._write({"jsonrpc": "2.0", "id": rid, "method": "session/prompt",
                             "params": {"sessionId": self.session_id, "prompt": blocks}})
                while True:
                    try:
                        resp = resp_q.get_nowait()
                        # The response can race ahead of trailing session/update
                        # notifications the reader already queued. Drain them so
                        # partial (and final) streamed text is never lost (R2).
                        while True:
                            try:
                                _apply(q.get_nowait())
                            except queue.Empty:
                                break
                        break
                    except queue.Empty:
                        pass
                    try:
                        upd = q.get(timeout=0.2)
                    except queue.Empty:
                        if self.dead:
                            emit("error", {"message": "acp process exited"})
                            break
                        # R4 idle-timeout: a wedged turn produces no updates and
                        # never returns a response. Don't count time spent parked
                        # on a browser permission decision (R1).
                        if (idle_timeout and not self._awaiting_perm
                                and time.monotonic() - last_activity >= idle_timeout):
                            wedged = True
                            emit("error", {"message":
                                 f"acp turn idle >{int(idle_timeout)}s — aborted"})
                            try:
                                self.cancel()
                            except Exception:
                                pass
                            break
                        continue
                    last_activity = time.monotonic()
                    if not _apply(upd):
                        break
            finally:
                self._pending.pop(rid, None)
                with self._event_lock:
                    self._event_q = None
                self._active_emit = None
            # R2: inspect the session/prompt outcome instead of assuming success.
            stop_reason = None
            if isinstance(resp, dict):
                if "error" in resp:
                    err = resp.get("error") or {}
                    kind, msg = _classify_acp_error(err)
                    failed = (kind, msg)
                else:
                    stop_reason = (resp.get("result") or {}).get("stopReason")
            # Always surface whatever text streamed so far (partial on failure).
            emit("message", {"text": "".join(full), "model": self.model, "tools": tools})
            if wedged:
                # Force-terminate the unresponsive process so a reused session
                # rebuilds a clean one (R4). Pool eviction happens in the caller.
                self.kill()
                emit("done", {"code": 1, "reason": "idle_timeout"})
            elif failed is not None:
                kind, msg = failed
                emit("error", {"message": f"{kind} error: {msg}", "kind": kind})
                emit("done", {"code": 1, "reason": kind})
            elif stop_reason == "cancelled":
                emit("done", {"code": 0, "reason": "cancelled"})
            else:
                emit("idle", {})
                emit("done", {"code": 0, "reason": stop_reason or "end_turn"})


_acp_procs: dict[str, AcpProc] = {}
_acp_lock = threading.Lock()


def _acp_get(session: str, model: str | None,
             load_id: str | None = None, cwd: str | None = None,
             engine: str | None = None, perm_mode: str | None = None) -> AcpProc:
    # Use the caller's engine snapshot (F1) so a settings save landing mid-turn
    # can't launch/tag this proc under a different engine than the lane the turn
    # was planned for. Falls back to the live engine only when no snapshot given.
    cur = engine or _engine_name()
    want_cwd = _validated_cwd(cwd) if cwd else None
    with _acp_lock:
        p = _acp_procs.get(session)
        # Reuse only a live proc that still matches the active engine AND the
        # requested working directory — a proc launched under a since-changed
        # engine or a different cwd must be torn down and rebuilt so the session
        # doesn't keep talking to the old CLI binary / wrong directory.
        if p is not None and not p.dead and p.proc.poll() is None \
                and getattr(p, "engine", cur) == cur \
                and (want_cwd is None or getattr(p, "cwd", None) == want_cwd):
            if perm_mode is not None:
                p.perm_mode = perm_mode
            p.set_model(model)
            p.last_used = time.time()
            return p
        if p is not None:
            try:
                p.close()
            except Exception:
                pass
        p = AcpProc(model, load_id=load_id, cwd=cwd, engine=cur)
        if perm_mode is not None:
            p.perm_mode = perm_mode
        _acp_procs[session] = p
        return p


def _acp_stop(session: str):
    with _acp_lock:
        p = _acp_procs.get(session)
    if p is not None:
        p.cancel()


def _acp_provide_permission(session: str, option_id: str | None) -> bool:
    """Route a browser permission decision (R1) to the session's ACP proc.
    Returns False if there is no live proc to receive it."""
    with _acp_lock:
        p = _acp_procs.get(session)
    if p is None or p.dead:
        return False
    p.provide_permission(option_id)
    return True


def _acp_close(session: str):
    with _acp_lock:
        p = _acp_procs.pop(session, None)
    with _live_lock:
        _live_turns.pop(session, None)
    if p is not None:
        p.close()


def _acp_reap_idle() -> int:
    """Close ACP processes that no session has touched for ``acp_reap_idle``
    seconds (R4): a browser can walk away leaving a live engine + its threads
    parked forever. Skips procs with a turn in flight. Returns the count closed."""
    try:
        ttl = float(load_config().get("acp_reap_idle", 1800) or 0)
    except (TypeError, ValueError):
        ttl = 1800.0
    if ttl <= 0:
        return 0
    now = time.time()
    with _live_lock:
        busy = {s for s, lt in _live_turns.items() if lt is not None and not lt.done}
    with _acp_lock:
        idle = [(s, p) for s, p in _acp_procs.items()
                if s not in busy and now - getattr(p, "last_used", now) > ttl]
        for s, _ in idle:
            _acp_procs.pop(s, None)
    for _, p in idle:
        try:
            p.close()
        except Exception:
            pass
    return len(idle)


def _acp_reaper_loop():
    """Background daemon: periodically reap abandoned ACP processes (R4)."""
    while True:
        time.sleep(60)
        try:
            _acp_reap_idle()
        except Exception:
            pass


def _acp_close_stale(keep_engine: str) -> int:
    """Tear down only cached ACP processes NOT bound to ``keep_engine``. Called
    on an engine change so sessions stop talking to the previous engine's CLI,
    while any process already on the new engine (and other engines' in-flight
    turns are unavoidable collateral only if they differ) survives. Selecting
    and detaching under _acp_lock makes this atomic against a concurrent
    _acp_get that may have just created a new-engine process.

    Returns the count closed."""
    with _acp_lock:
        stale = [(s, p) for s, p in _acp_procs.items()
                 if getattr(p, "engine", keep_engine) != keep_engine]
        for s, _ in stale:
            _acp_procs.pop(s, None)
    with _live_lock:
        for sess, _ in stale:
            _live_turns.pop(sess, None)
    for _, p in stale:
        try:
            p.close()
        except Exception:
            pass
    return len(stale)


def _list_models(engine: str | None = None) -> dict:
    """Return the live model catalog the engine exposes over ACP.

    We run a short one-shot `<engine> acp` handshake (initialize + session/new
    with no MCP servers) and parse its catalog — the same source the interactive
    `/model` picker uses — so the web UI's model lists never go stale. Cached by
    the caller. On any failure returns {"error": ...} and the client keeps its
    built-in fallback list. `engine` is the caller's snapshot so the cache key
    and the launched CLI never disagree across a concurrent switch (finding N4)."""
    eng = ENGINES[engine] if engine else _engine()
    try:
        p = subprocess.Popen(eng["acp_argv"](_engine_bin(eng["name"])),
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             text=True, bufsize=1)
    except Exception as exc:
        return {"error": f"launch failed: {exc}"}
    _id = 0

    def send(method, params):
        nonlocal _id
        _id += 1
        p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": _id,
                                  "method": method, "params": params}) + "\n")
        p.stdin.flush()
        return _id

    def read_until(target, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            line = p.stdout.readline()
            if not line:
                break
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("id") == target and ("result" in obj or "error" in obj):
                return obj
            if obj.get("method") and "id" in obj:  # unblock agent->client requests
                p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": obj["id"],
                                          "result": {}}) + "\n")
                p.stdin.flush()
        return None

    try:
        send("initialize", {"protocolVersion": 1,
                            "clientCapabilities": {"fs": {"readTextFile": False,
                                                          "writeTextFile": False}}})
        read_until(_id)
        i2 = send("session/new", {"cwd": os.path.expanduser("~"), "mcpServers": []})
        res = (read_until(i2) or {}).get("result", {})
        # Model-catalog shape is engine-specific (Copilot's models.availableModels
        # vs opencode's configOptions), so parsing lives in the engine descriptor.
        return eng["parse_models"](res)
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        try:
            p.kill()
        except Exception:
            pass



def _acp_oneshot(engine: str | None = None):
    """Spawn a short-lived `<engine> acp` and return (proc, send, pump) helpers
    for one-off queries (session/list, session/load). pump(target) reads lines
    until the response to request id `target` arrives, auto-answering any
    agent->client requests, and returns (result_obj, notifications) where
    notifications is the list of session/update payloads seen meanwhile.
    `engine` pins the launch to the caller's snapshot (finding N4)."""
    argv = _engine_argv(engine) if engine else _acp_launch_argv()
    p = subprocess.Popen(argv, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                         text=True, bufsize=1)
    state = {"id": 0}

    def send(method, params):
        state["id"] += 1
        p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": state["id"],
                                  "method": method, "params": params}) + "\n")
        p.stdin.flush()
        return state["id"]

    def pump(target, timeout=120):
        notes = []
        end = time.time() + timeout
        while time.time() < end:
            line = p.stdout.readline()
            if not line:
                break
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("method") == "session/update":
                upd = (obj.get("params") or {}).get("update")
                if upd is not None:
                    notes.append(upd)
                continue
            if obj.get("method") and "id" in obj:      # agent->client request
                p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": obj["id"],
                                          "result": {}}) + "\n")
                p.stdin.flush()
                continue
            if obj.get("id") == target and ("result" in obj or "error" in obj):
                return obj, notes
        return None, notes

    return p, send, pump


def _list_sessions(cap: int = 200, engine: str | None = None) -> dict:
    """Enumerate on-disk copilot sessions via ACP session/list (paginated).

    Returns every session copilot knows about — console, other tools, and ones
    hal-ui created — newest first, so the History tab can surface them. Cached by
    the caller. On failure returns {"error": ...}. `engine` is the caller's
    snapshot so gating, cache key, and launch stay consistent (finding N4)."""
    eng = ENGINES[engine] if engine else _engine()
    if not eng.get("session_history", False):
        return {"sessions": []}   # engine has no ACP session/list surface
    try:
        p, send, pump = _acp_oneshot(engine=eng["name"])
    except Exception as exc:
        return {"error": f"launch failed: {exc}"}
    try:
        send("initialize", {"protocolVersion": 1,
                            "clientCapabilities": {"fs": {"readTextFile": False,
                                                          "writeTextFile": False}}})
        pump(1)
        out, cursor, guard = [], None, 0
        while guard < 20 and len(out) < cap:
            guard += 1
            params = {"cursor": cursor} if cursor else {}
            i = send("session/list", params)
            resp, _ = pump(i)
            res = (resp or {}).get("result", {}) or {}
            for s in res.get("sessions", []) or []:
                out.append({"id": s.get("sessionId", ""),
                            "title": s.get("title", "") or "",
                            "cwd": s.get("cwd", "") or "",
                            "updatedAt": s.get("updatedAt", "")})
            cursor = res.get("nextCursor")
            if not cursor:
                break
        out.sort(key=lambda s: s.get("updatedAt") or "", reverse=True)
        return {"sessions": out[:cap]}
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        try:
            p.kill()
        except Exception:
            pass


# Full-text search over on-disk sessions. copilot stores each session's raw
# event stream at ~/.copilot/session-state/<id>/events.jsonl; the user/assistant
# message bodies live there. session/list only returns titles, so to let the
# History search match transcript CONTENT (e.g. "coffee") we scan those files.
# The store is engine-specific (see ENGINES[...]["session_state_dir"]).
_BODY_CAP = 60_000   # per-session text budget so the index stays memory-bounded


def _session_state_dir() -> str | None:
    """Absolute on-disk transcript dir for the active engine, or None if the
    engine has no Copilot-style session-state store."""
    d = _engine().get("session_state_dir")
    return os.path.expanduser(d) if d else None


def _session_body(session_id: str) -> str:
    """Concatenated user+assistant message text for one on-disk session (capped)."""
    base = _session_state_dir()
    if not base:
        return ""
    path = os.path.join(base, session_id, "events.jsonl")
    parts: list[str] = []
    total = 0
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if '"user.message"' not in line and '"assistant.message"' not in line:
                    continue
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if o.get("type") not in ("user.message", "assistant.message"):
                    continue
                d = o.get("data") or o
                c = d.get("content")
                if isinstance(c, str) and c:
                    parts.append(c)
                    total += len(c)
                    if total >= _BODY_CAP:
                        break
    except FileNotFoundError:
        return ""
    except Exception:
        return ""
    return "\n".join(parts)


def _search_sessions(query: str, cap: int = 100) -> dict:
    """Search on-disk sessions by transcript body, title and cwd. Returns matches
    newest-first with a highlighted-ready snippet around the first body hit.

    Metadata (title/cwd/updatedAt) comes from the cached session list so results
    line up with the browse view; the body scan reads events.jsonl per session."""
    q = (query or "").strip()
    if not q:
        return {"sessions": []}
    ql = q.lower()
    eng = _engine_name()
    meta = cached("sessions:" + eng, 20.0, lambda: _list_sessions(engine=eng))
    if isinstance(meta, dict) and meta.get("error"):
        return {"error": meta["error"]}
    out = []
    for s in (meta.get("sessions") or []):
        sid = s.get("id") or ""
        title = s.get("title") or ""
        cwd = s.get("cwd") or ""
        snippet = ""
        hit = False
        body = _session_body(sid)
        i = body.lower().find(ql)
        if i >= 0:
            hit = True
            start = max(0, i - 40)
            snippet = ("…" if start > 0 else "") + body[start:i + len(q) + 70].replace("\n", " ")
        elif ql in title.lower() or ql in cwd.lower():
            hit = True
            snippet = (title[:120] or cwd[:120])
        if hit:
            out.append({"id": sid, "title": title, "cwd": cwd,
                        "updatedAt": s.get("updatedAt", ""), "snippet": snippet})
        if len(out) >= cap:
            break
    return {"sessions": out}


# Agent chunks copilot injects that are pure plumbing, not conversation.
_TRANSCRIPT_NOISE = ("Info: Remote control connected",
                     "Warning: Failed to connect to MCP server")


def _load_transcript(session_id: str, cwd: str | None = None) -> dict:
    """Replay a copilot session's full history via ACP session/load and coalesce
    it into the client transcript shape: [{role:'user'|'bot', text, tools?}].

    Consecutive assistant chunks fold into one bubble; tool_call titles attach to
    the assistant turn that issued them; user chunks start a user bubble."""
    try:
        p, send, pump = _acp_oneshot()
    except Exception as exc:
        return {"error": f"launch failed: {exc}"}
    try:
        send("initialize", {"protocolVersion": 1,
                            "clientCapabilities": {"fs": {"readTextFile": False,
                                                          "writeTextFile": False}}})
        pump(1)
        wd = cwd or os.path.expanduser("~")
        i = send("session/load", {"sessionId": session_id, "cwd": wd, "mcpServers": []})
        resp, notes = pump(i, timeout=180)
        if resp is None or "error" in (resp or {}):
            return {"error": (resp or {}).get("error", "load failed") if resp else "load timeout"}

        messages: list[dict] = []
        cur = {"role": None, "text": "", "tools": []}

        def flush():
            if cur["role"] is None:
                return
            txt = cur["text"].strip()
            if cur["role"] == "bot":
                if txt or cur["tools"]:
                    messages.append({"role": "bot", "text": txt, "tools": list(cur["tools"])})
            else:
                if txt:
                    messages.append({"role": "user", "text": txt})
            cur["role"], cur["text"], cur["tools"] = None, "", []

        def content_text(c):
            if isinstance(c, dict):
                return c.get("text", "") or ""
            return ""

        for u in notes:
            kind = u.get("sessionUpdate")
            if kind == "agent_message_chunk":
                t = content_text(u.get("content"))
                if any(t.startswith(n) for n in _TRANSCRIPT_NOISE):
                    continue
                if cur["role"] != "bot":
                    flush(); cur["role"] = "bot"
                cur["text"] += t
            elif kind == "user_message_chunk":
                t = content_text(u.get("content"))
                if cur["role"] != "user":
                    flush(); cur["role"] = "user"
                cur["text"] += t
            elif kind == "tool_call":
                if cur["role"] != "bot":
                    flush(); cur["role"] = "bot"
                title = u.get("title") or u.get("kind") or "tool"
                cur["tools"].append(title)
            # tool_call_update / plan / thought: ignored for the transcript
        flush()
        return {"messages": messages}
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        try:
            p.kill()
        except Exception:
            pass


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

    def read_from(self, idx: int, idle_ping: float = 15.0):
        """Generator: yield buffered events from idx, blocking for new ones
        until the turn is done. Never holds the lock while yielding. When no
        new event arrives within idle_ping seconds (and the turn isn't done),
        yields a ("__ping__", None) sentinel so the relay can emit an SSE
        keepalive — this surfaces a half-open connection to the browser as an
        error instead of a silently-stuck stream."""
        last = time.monotonic()
        while True:
            with self.cond:
                while idx >= len(self.events) and not self.done:
                    self.cond.wait(timeout=0.5)
                    if (time.monotonic() - last >= idle_ping
                            and idx >= len(self.events) and not self.done):
                        break
                batch = self.events[idx:]
                idx = len(self.events)
                done = self.done
            if batch:
                last = time.monotonic()
                for ev in batch:
                    yield ev
            elif not done:
                last = time.monotonic()
                yield ("__ping__", None)
            if done and not batch:
                return


_live_turns: dict[str, LiveTurn] = {}
_live_lock = threading.Lock()


def _run_acp_turn(session, model, prompt, images, lt: LiveTurn,
                  copilot_id=None, copilot_cwd=None, engine=None,
                  perm_mode=None, cwd=None):
    """Fast lane: run one turn on the persistent ACP process, buffering events.
    When copilot_id is set (a revived on-disk session) the process boots by
    session/load-ing that id so the turn continues its real context.

    ``engine``/``cwd``/``perm_mode`` are the caller's snapshot (F1/R1/R3) so the
    proc launched/reused matches the lane the turn was planned for."""
    try:
        acp = _acp_get(session, model, load_id=copilot_id,
                       cwd=copilot_cwd or cwd, engine=engine, perm_mode=perm_mode)
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
    finally:
        # R4: a wedged/dead proc is evicted from the pool so the next turn on
        # this session rebuilds a clean one instead of reusing a corpse.
        if acp.dead:
            with _acp_lock:
                if _acp_procs.get(session) is acp:
                    _acp_procs.pop(session, None)


def _run_subprocess_turn(session, model, prompt, lt: LiveTurn, copilot_id=None,
                         engine=None, cwd=None):
    """Legacy lane (chat_fast OFF => full MCP): spawn `copilot -p ...` per turn,
    parse its JSONL, buffer events. Survives browser close because it runs in the
    turn worker thread, not the HTTP request.

    Two robustness guards keep a turn from wedging the whole session forever:
      * a concurrent stderr drainer — a chatty copilot (MCP logs/warnings) can
        otherwise fill the 64 KB stderr pipe, block the child on write(), and
        stall stdout indefinitely (the process sits in Sl and the UI shows
        "working" with nothing ever completing);
      * an idle-timeout reaper — if no stdout arrives for chat_idle_timeout
        seconds the subprocess is killed and the turn is closed, so the browser
        stops waiting and queued messages can drain."""
    emit = lt.append
    argv = _build_argv(prompt, session, model, copilot_id=copilot_id, engine=engine)
    try:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                cwd=_validated_cwd(cwd), text=True, bufsize=1)
    except Exception as exc:
        emit("error", {"message": f"launch failed: {exc}"})
        emit("done", {})
        return
    lt.canceller = proc.kill
    try:
        idle_timeout = float(load_config().get("chat_idle_timeout", 900) or 0)
    except (TypeError, ValueError):
        idle_timeout = 900.0

    # Drain stderr concurrently so a full pipe can never deadlock the child.
    err_tail: list[str] = []
    def _drain_err():
        try:
            assert proc.stderr is not None
            for chunk in iter(lambda: proc.stderr.read(4096), ""):
                err_tail.append(chunk)
                if len(err_tail) > 8:
                    del err_tail[0]
        except Exception:
            pass
    threading.Thread(target=_drain_err, name=f"cerr-{session}", daemon=True).start()

    # Pump stdout lines into a queue so the consumer can apply an idle timeout
    # (a blocking `for line in proc.stdout` gives us no way to notice a wedge).
    lines: queue.Queue = queue.Queue()
    def _pump():
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                lines.put(line)
        except Exception:
            pass
        finally:
            lines.put(None)  # EOF sentinel
    threading.Thread(target=_pump, name=f"cout-{session}", daemon=True).start()

    timed_out = False
    try:
        while True:
            try:
                line = lines.get(timeout=idle_timeout if idle_timeout > 0 else None)
            except queue.Empty:
                timed_out = True
                break
            if line is None:
                break  # EOF: process finished
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
    except Exception as exc:
        emit("error", {"message": str(exc)})
    finally:
        if timed_out:
            # Append a note without wiping any partial streamed text.
            emit("delta", {"text": f"\n\n⚠️ _Turn reaped: no output for "
                                   f"{int(idle_timeout)}s; the copilot subprocess "
                                   f"was wedged and has been killed. Please retry._"})
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
        err = ("".join(err_tail))[-400:].strip()
        code = proc.returncode if proc.returncode is not None else -1
        emit("done", {"code": code, "stderr": err, "reaped": timed_out})


def _start_turn(session: str, model: str | None, prompt: str, images: list,
                copilot_id: str | None = None, copilot_cwd: str | None = None,
                cwd: str | None = None, perm_mode: str | None = None) -> LiveTurn:
    """Begin a turn in a background worker thread that buffers its SSE events into
    a LiveTurn. Returns the buffer immediately so the HTTP request can relay it.
    Because the turn runs off-thread, a browser close does not end it — a reopened
    tab reattaches via /api/chat/resume. Replaces any prior buffer for the session.
    Lane choice mirrors _sse_chat: ACP fast lane, or legacy subprocess when
    chat_fast is off (images always force the ACP lane).

    copilot_id (optional) pins this session to a real on-disk copilot session id
    revived from the History tab so the turn resumes that conversation.

    Concurrency guard: if a turn for this session is still running, we do NOT
    spawn a second one — a duplicate `copilot -p` on the same --session-id can
    deadlock or corrupt that session. We return the in-flight buffer so the new
    request just relays it (the frontend already serialises via its busy flag;
    this closes a race from a reload/resume edge case)."""
    turn_id = uuid.uuid4().hex
    lt = LiveTurn(turn_id)
    with _live_lock:
        prev = _live_turns.get(session)
        if prev is not None and not prev.done:
            return prev
        _live_turns[session] = lt
    # F1: snapshot the engine ONCE and derive the lane from that same snapshot,
    # then thread it through the worker. Previously `fast` was computed from the
    # live engine while the worker re-resolved it later, so a settings save
    # landing between could run the ACP lane's `fast` flag against the other
    # engine's argv (e.g. parse `opencode run` output as Copilot JSONL).
    cfg = load_config()
    eng_name = _engine_name(cfg)
    eng = ENGINES[eng_name]
    eff_cwd = cwd if cwd else _validated_cwd(cfg.get("chat_cwd"))
    eff_perm = perm_mode if perm_mode is not None else _permission_mode(cfg.get("chat_permission_mode"))
    # Engines without a legacy per-turn parser (e.g. opencode) always take the
    # ACP lane — their `run` stream schema isn't the Copilot JSONL that
    # _run_subprocess_turn expects.
    fast = (bool(cfg.get("chat_fast", True)) or bool(images)
            or not eng.get("legacy_lane", True))
    lt.append("start", {"session": session, "acp": fast, "turn": turn_id,
                        "engine": eng_name, "cwd": eff_cwd})

    def worker():
        try:
            if fast:
                _run_acp_turn(session, model, prompt, images, lt,
                              copilot_id=copilot_id, copilot_cwd=copilot_cwd,
                              engine=eng_name, perm_mode=eff_perm, cwd=eff_cwd)
            else:
                _run_subprocess_turn(session, model, prompt, lt,
                                     copilot_id=copilot_id, engine=eng_name, cwd=eff_cwd)
        finally:
            lt.finish()

    threading.Thread(target=worker, name=f"turn-{session}", daemon=True).start()
    return lt


def _turn_stop(session: str):
    """Stop the in-flight turn for a session (Stop button): cancel the ACP prompt
    or kill the subprocess, and also cancel the persistent ACP session.

    Bounded cancellation (R4): after signalling cancel we give the turn a short
    grace period to settle; if it doesn't, we force-terminate the ACP process and
    evict it from the pool so a wedged agent can't hold the session hostage."""
    with _live_lock:
        lt = _live_turns.get(session)
    if lt is not None and lt.canceller is not None:
        try:
            lt.canceller()
        except Exception:
            pass
    _acp_stop(session)
    try:
        grace = float(load_config().get("acp_cancel_grace", 8) or 0)
    except (TypeError, ValueError):
        grace = 8.0
    if grace <= 0:
        return

    def _watch():
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            with _live_lock:
                cur = _live_turns.get(session)
            if cur is None or cur.done:
                return
            time.sleep(0.2)
        # Still running after the grace window: force-kill + evict the ACP proc.
        with _acp_lock:
            p = _acp_procs.pop(session, None)
        if p is not None:
            try:
                p.kill()
            except Exception:
                pass

    threading.Thread(target=_watch, name=f"cancel-{session}", daemon=True).start()


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
                if event == "__ping__":
                    self.wfile.write(b": ping\n\n")   # SSE comment: keepalive only
                else:
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
        copilot_id = (qs.get("copilot", [""])[0]).strip() or None
        copilot_cwd = (qs.get("cwd", [""])[0]).strip() or None
        # R3: per-session working directory override (distinct from `cwd`, which
        # is the on-disk cwd of a revived Copilot session). R1: per-session
        # permission policy override. Both fall back to the server defaults.
        wd = (qs.get("wd", [""])[0]).strip() or None
        perm = (qs.get("perm", [""])[0]).strip() or None
        perm_mode = _permission_mode(perm) if perm else None
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
        lt = _start_turn(session, model, prompt, images,
                         copilot_id=copilot_id, copilot_cwd=copilot_cwd,
                         cwd=wd, perm_mode=perm_mode)
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
        elif p.path == "/api/chat/permission":
            # R1: deliver a browser permission decision to a turn parked in
            # prompt mode. `option` blank/absent => cancel (fail-safe).
            qp = parse_qs(p.query)
            session = (qp.get("session", [""])[0] or "").strip()
            option = (qp.get("option", [""])[0] or "").strip() or None
            ok = _acp_provide_permission(session, option) if session else False
            self._json(200, {"delivered": ok})
        elif p.path == "/api/hive/status":
            if not _hive_enabled():
                self._json(404, {"error": "hive disabled"})
            else:
                code, data = _hive_request("/status")
                self._json(200 if code == 200 else 502, data)
        elif p.path == "/api/hive/task":
            if not _hive_enabled():
                self._json(404, {"error": "hive disabled"})
                return
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
        elif p.path == "/api/sf/products":
            if not _sf_enabled():
                self._json(404, {"error": "salesforce disabled"})
            else:
                self._json(200, cached("sf_products", 300.0, _sf_products))
        elif p.path == "/api/sf/status":
            self._json(200, _sf_status_payload())
        elif p.path == "/api/sf/crs":
            if not _sf_enabled():
                self._json(404, {"error": "salesforce disabled"})
            else:
                raw = (parse_qs(p.query).get("products", [""])[0] or "").strip()
                products = [x for x in raw.split(",") if x] if raw else []
                key = "sf_crs:" + ",".join(sorted(products))
                self._json(200, cached(key, 30.0, lambda: _sf_crs(products)))
        elif p.path == "/api/models":
            # Snapshot the engine ONCE so the cache key and the producer's
            # launch/parse agree even if a settings save lands mid-request; an
            # in-flight old-engine producer can't repopulate the new key (N4).
            eng = _engine_name()
            self._json(200, cached("models:" + eng, 600.0,
                                   lambda: _list_models(eng)))
        elif p.path == "/api/sessions":
            eng = _engine_name()
            self._json(200, cached("sessions:" + eng, 20.0,
                                   lambda: _list_sessions(engine=eng)))
        elif p.path == "/api/sessions/search":
            if not _engine().get("session_history", False):
                self._json(200, {"sessions": []})
            else:
                qs = parse_qs(p.query)
                q = (qs.get("q", [""])[0]).strip()
                self._json(200, _search_sessions(q))
        elif p.path == "/api/session/transcript":
            if not _engine().get("session_history", False):
                self._json(404, {"error": "session history not supported for this engine"})
                return
            qs = parse_qs(p.query)
            sid = (qs.get("id", [""])[0]).strip()
            cwd = (qs.get("cwd", [""])[0]).strip() or None
            if not sid:
                self._json(400, {"error": "missing id"})
            else:
                self._json(200, _load_transcript(sid, cwd))
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
            if not _hive_enabled():
                self._json(404, {"error": "hive disabled"})
                return
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
        elif p.path == "/api/sf/reconnect":
            self._json(200, _sf_reconnect_start())
        elif p.path == "/api/settings":
            patch = body if isinstance(body, dict) else {}
            prev_engine = _engine_name()
            # Validate only a genuine engine CHANGE. The frontend sends `engine`
            # on every save, so gating on presence alone would reject unrelated
            # setting changes whenever the active engine's binary is missing
            # (review finding N1). An unchanged engine needs no re-check.
            if "engine" in patch:
                name = patch["engine"]
                changing = not isinstance(name, str) or name.lower() != prev_engine
                if changing:
                    if not isinstance(name, str) or name.lower() not in ENGINES:
                        self._json(400, {"error": f"unknown engine: {name!r}"})
                        return
                    # Live-resolve so an engine installed AFTER startup is
                    # accepted without a restart (review finding N2).
                    if not _resolve_bin(ENGINES[name.lower()]["bin_names"],
                                        ENGINES[name.lower()]["bin_fallbacks"]):
                        self._json(400, {"error": f"{name} binary not found (install it or check PATH)"})
                        return
            # R3: reject a working directory that isn't an existing directory so
            # the user learns at save time instead of every turn failing later.
            if patch.get("chat_cwd"):
                cw = patch["chat_cwd"]
                if not isinstance(cw, str) or not os.path.isdir(
                        os.path.abspath(os.path.expanduser(cw.strip()))):
                    self._json(400, {"error": f"working directory not found: {cw!r}"})
                    return
            # R1: normalise the permission mode to the known vocabulary.
            if "chat_permission_mode" in patch:
                patch["chat_permission_mode"] = _permission_mode(patch.get("chat_permission_mode"))
            save_config(patch)
            # settings that affect ADO identity/token invalidate caches
            with _cache_lock:
                _cache.pop("ado_mywork", None)
            with _az_lock:
                _az_token.update(tok="", exp=0.0)
            # On an engine change, retire only the previous engine's ACP procs so
            # the switched session re-homes to the new CLI while sessions already
            # on the new engine keep running (review finding N3). Model/history
            # caches are engine-qualified (N4), so no cross-engine eviction is
            # needed — the new engine simply reads its own keys.
            if _engine_name() != prev_engine:
                _acp_close_stale(_engine_name())
            self._json(200, masked_config())
        else:
            self.send_error(404, "Not found")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8900)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    eng = _engine()
    bin_path = _engine_bin(eng["name"])
    if not (bin_path and Path(bin_path).exists()):
        raise SystemExit(f"{eng['label']} binary not found "
                         f"(tried {eng['bin_names']} + {eng['bin_fallbacks']})")
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    # R4: background reaper for abandoned ACP processes.
    threading.Thread(target=_acp_reaper_loop, name="acp-reaper", daemon=True).start()
    tok = "ok" if _hive_token() else "MISSING"
    print(f"HAL-UI Phase 2 on http://{args.host}:{args.port}/  "
          f"(engine={eng['label']} bin={bin_path}, hive={_hive_url()} token={tok})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
