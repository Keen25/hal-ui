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
COPILOT_BIN = shutil.which("copilot") or os.path.expanduser("~/.local/bin/copilot")
GH_BIN = shutil.which("gh") or "/usr/bin/gh"
AZ_BIN = shutil.which("az") or "/usr/bin/az"
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
SF_EXPECTED_ORG_ID = "00Da500001UOLuLEAX"   # One Identity Salesforce org (new RDE instance; was Quest 00D30000000n44BEAQ)


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
HIVE_TOKEN_FILE = os.path.expanduser("~/.hive/worker.token")
ADO_RESOURCE = "499b84ac-1321-427f-aa17-267ca6975798"  # Azure DevOps AAD app id
CONFIG_DIR = os.path.expanduser("~/.config/hal-ui")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

DEFAULT_CONFIG = {
    "ado_org": "https://dev.azure.com/1id",
    "ado_pat": "",          # optional; blank => use `az` AAD token
    "ado_identity": "",     # optional; blank => auto-resolve from ado_upn / az user
    "ado_upn": "",          # used to resolve identity; blank => `az account show` user
    # Empty by default so a fresh install does not auto-wire a hive (no Hive tab
    # until the user opts in via Settings). Existing installs keep their saved
    # hive_url on upgrade (config.json layers over these defaults). Setting the
    # HIVE_URL env var still seeds it for headless/scripted deploys.
    "hive_url": os.environ.get("HIVE_URL", ""),
    "hive_token": "",       # optional override; blank => ~/.hive/worker.token
    "default_model": "",
    "chat_fast": True,      # disable MCP servers in the chat lane for faster startup
    "chat_mcp_servers": [],  # names (from ~/.copilot/mcp-config.json) to keep warm in
                              # the fast ACP lane too, e.g. ["one-identity"]. Each boot
                              # pays that server's startup cost once; [] keeps the
                              # original all-cold fast-lane behavior.
    "chat_idle_timeout": 900,  # legacy lane: reap a copilot -p turn after this many
                               # seconds with zero output (wedged subprocess). 0 = off.
    "sf_org": "",           # Salesforce target org username/alias; blank => auto-resolve
    "sf_products": "ActiveRolesServer,PasswordManager",  # default R&D queues for the CR tab
}
_SECRET_KEYS = {"ado_pat", "hive_token"}
_SECRET_ENC_SUFFIX = "_enc"
# Secrets can be supplied out-of-band via the environment (e.g. systemd
# EnvironmentFile) so they never have to touch disk. Env always wins and is
# never persisted back into config.json.
_SECRET_ENV = {
    "ado_pat": ("HALUI_ADO_PAT", "AZURE_DEVOPS_EXT_PAT"),
    "hive_token": ("HALUI_HIVE_TOKEN",),
}
SECRET_KEY_FILE = os.path.join(CONFIG_DIR, ".secret.key")
_config_lock = threading.Lock()
_secret_key_lock = threading.Lock()
_SECRET_TOKEN_PREFIX = "HALUI1:"  # versioned; lets the format evolve later


def _chmod_600(path: str) -> None:
    """Best-effort tighten to owner rw. On POSIX this is 0600; on Windows
    os.chmod only toggles the read-only bit (0600 keeps it writable) and the
    file inherits the profile dir's ACL — no exception either way. Swallow
    OSError so an exotic filesystem can never crash a config write."""
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _secret_key() -> bytes | None:
    """Return the 32-byte master key from a 0600 key file, generating it on
    first use. Stdlib only (no third-party crypto) so this behaves identically
    on the Linux and Windows (WinSW) hosts. Returns None only if the key cannot
    be read or created (e.g. read-only FS), in which case callers fall back to
    plaintext-at-0600 rather than crashing."""
    import base64 as _b64
    with _secret_key_lock:
        try:
            os.makedirs(CONFIG_DIR, exist_ok=True)
            if os.path.exists(SECRET_KEY_FILE):
                return _b64.urlsafe_b64decode(Path(SECRET_KEY_FILE).read_text().strip())
            import secrets as _secrets
            key = _secrets.token_bytes(32)
            Path(SECRET_KEY_FILE).write_text(_b64.urlsafe_b64encode(key).decode())
            _chmod_600(SECRET_KEY_FILE)
            return key
        except (OSError, ValueError):
            return None


def _subkeys(key: bytes) -> tuple[bytes, bytes]:
    import hmac as _hmac
    import hashlib as _hashlib
    enc = _hmac.new(key, b"halui-enc", _hashlib.sha256).digest()
    mac = _hmac.new(key, b"halui-mac", _hashlib.sha256).digest()
    return enc, mac


def _keystream(enc_key: bytes, nonce: bytes, n: int) -> bytes:
    """HMAC-SHA256 in counter mode: block_i = HMAC(enc_key, nonce||counter_be4).
    A fresh random nonce per message keeps the keystream unique."""
    import hmac as _hmac
    import hashlib as _hashlib
    out = bytearray()
    counter = 0
    while len(out) < n:
        block = _hmac.new(enc_key, nonce + counter.to_bytes(4, "big"),
                          _hashlib.sha256).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:n])


def _encrypt_secret(val: str) -> str | None:
    """Encrypt-then-MAC a secret with the stdlib HMAC keystream cipher.
    Returns a `HALUI1:<b64>` token, or None if no key is available."""
    if not val:
        return None
    key = _secret_key()
    if not key:
        return None
    try:
        import base64 as _b64
        import hmac as _hmac
        import hashlib as _hashlib
        import secrets as _secrets
        enc_key, mac_key = _subkeys(key)
        nonce = _secrets.token_bytes(16)
        pt = val.encode("utf-8")
        ct = bytes(a ^ b for a, b in zip(pt, _keystream(enc_key, nonce, len(pt))))
        tag = _hmac.new(mac_key, nonce + ct, _hashlib.sha256).digest()
        return _SECRET_TOKEN_PREFIX + _b64.urlsafe_b64encode(nonce + ct + tag).decode()
    except Exception:
        return None


def _decrypt_secret(token: str) -> str:
    """Inverse of _encrypt_secret. Returns "" on any tampering/format/key
    mismatch (e.g. an encrypted config copied to a host without its key)."""
    if not token or not token.startswith(_SECRET_TOKEN_PREFIX):
        return ""
    key = _secret_key()
    if not key:
        return ""
    try:
        import base64 as _b64
        import hmac as _hmac
        import hashlib as _hashlib
        raw = _b64.urlsafe_b64decode(token[len(_SECRET_TOKEN_PREFIX):])
        if len(raw) < 16 + 32:
            return ""
        nonce, body, tag = raw[:16], raw[16:-32], raw[-32:]
        enc_key, mac_key = _subkeys(key)
        want = _hmac.new(mac_key, nonce + body, _hashlib.sha256).digest()
        if not _hmac.compare_digest(want, tag):
            return ""  # tampered or wrong key
        pt = bytes(a ^ b for a, b in zip(body, _keystream(enc_key, nonce, len(body))))
        return pt.decode("utf-8")
    except Exception:
        return ""


def _env_secret(key: str) -> str | None:
    for name in _SECRET_ENV.get(key, ()):
        v = os.environ.get(name)
        if v:
            return v.strip()
    return None


def load_config() -> dict:
    """Effective config with secrets resolved to plaintext for runtime use.

    Secret precedence: environment override > encrypted `<key>_enc` on disk >
    legacy plaintext `<key>` on disk. The returned dict never contains the
    internal `_enc` keys — callers keep reading `cfg["ado_pat"]` as before."""
    cfg = dict(DEFAULT_CONFIG)
    try:
        raw = json.loads(Path(CONFIG_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        raw = {}
    for k, v in raw.items():
        if k in _SECRET_KEYS or k.endswith(_SECRET_ENC_SUFFIX):
            continue  # secrets resolved below; never surface stored ciphertext
        cfg[k] = v
    for k in _SECRET_KEYS:
        env_v = _env_secret(k)
        if env_v is not None:
            cfg[k] = env_v
            continue
        enc = raw.get(k + _SECRET_ENC_SUFFIX)
        if enc:
            cfg[k] = _decrypt_secret(enc)
        else:
            cfg[k] = raw.get(k, DEFAULT_CONFIG.get(k, ""))  # legacy plaintext
    return cfg


def _write_config(cfg: dict) -> None:
    """Serialize config to disk with secrets encrypted, never in plaintext.
    A secret sourced purely from the environment is not persisted, but any
    ciphertext already on disk is preserved so unsetting the env var later does
    not silently lose a stored secret."""
    try:
        prior = json.loads(Path(CONFIG_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        prior = {}
    out = {}
    for k, v in cfg.items():
        if k in _SECRET_KEYS or k.endswith(_SECRET_ENC_SUFFIX):
            continue
        out[k] = v
    for k in _SECRET_KEYS:
        val = cfg.get(k)
        enc_key = k + _SECRET_ENC_SUFFIX
        if val and _env_secret(k) == val:
            # Provided by env: keep it out of the file, but retain any existing
            # ciphertext so the stored secret survives the env var going away.
            if prior.get(enc_key):
                out[enc_key] = prior[enc_key]
            continue
        if not val:
            continue
        enc = _encrypt_secret(val)
        if enc:
            out[enc_key] = enc
        else:
            out[k] = val  # no key available: plaintext, still 0600 best-effort
    os.makedirs(CONFIG_DIR, exist_ok=True)
    Path(CONFIG_FILE).write_text(json.dumps(out, indent=2))
    _chmod_600(CONFIG_FILE)


def save_config(patch: dict) -> dict:
    with _config_lock:
        cfg = load_config()
        for k, v in patch.items():
            if k not in DEFAULT_CONFIG:
                continue
            if k in _SECRET_KEYS and (v is None or v == ""):
                continue  # blank secret => keep existing value
            cfg[k] = v
        _write_config(cfg)
    return cfg


def _migrate_secrets() -> None:
    """One-shot on startup: if config.json still holds a plaintext secret,
    rewrite it in encrypted form so the PAT/token no longer sits in cleartext."""
    try:
        raw = json.loads(Path(CONFIG_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        return
    has_plaintext = any(k in raw for k in _SECRET_KEYS)
    if has_plaintext and _secret_key() is not None:
        with _config_lock:
            _write_config(load_config())


def masked_config() -> dict:
    cfg = load_config()
    out = {k: ("" if k in _SECRET_KEYS else v) for k, v in cfg.items()}
    for k in _SECRET_KEYS:
        out[k + "_set"] = bool(cfg.get(k))
    out["hive_enabled"] = _hive_enabled()
    out["sf_enabled"] = _sf_enabled()
    return out

# Fixed namespace so a given browser session always maps to the same copilot
# session UUID. Using a stable UUID (via --session-id) makes each turn an
# idempotent create-or-resume, which survives hal-ui restarts and avoids the
# duplicate-name ambiguity that -n/--resume-by-name produced.
_COPILOT_SESSION_NS = uuid.UUID("5f3d9b2e-1c4a-4e77-9a10-7b6c8d2e4f01")


def _copilot_session_id(session: str) -> str:
    return str(uuid.uuid5(_COPILOT_SESSION_NS, session))


# --- ACP <-> legacy session bridge ------------------------------------
# The legacy lane pins a deterministic uuid5 per browser label, so it resumes
# the same on-disk copilot session every turn. The ACP fast lane instead calls
# session/new, which mints a RANDOM v4 id that previously lived only in the
# in-memory _acp_procs dict — so an AcpProc death, a hal-ui restart, or a
# chat_fast lane toggle orphaned the conversation and the next turn started
# empty (the "lost context mid-turn" bug). This tiny disk map remembers the
# real copilot id last used for a label and lets EITHER lane resume it, closing
# that leak.
_SESSION_MAP_FILE = os.path.join(CONFIG_DIR, "sessions.json")
_session_map_lock = threading.Lock()


def _session_map_load() -> dict:
    try:
        return json.loads(Path(_SESSION_MAP_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _session_map_get(label: str) -> str | None:
    """The real copilot session id last used for this browser label, but only if
    that session still exists on disk (a stale id would make the ACP lane's
    session/load fail)."""
    if not label:
        return None
    with _session_map_lock:
        sid = _session_map_load().get(label)
    if sid and os.path.isdir(os.path.join(_SESSION_STATE_DIR, sid)):
        return sid
    return None


def _session_map_put(label: str, sid: str | None) -> None:
    if not label or not sid:
        return
    with _session_map_lock:
        m = _session_map_load()
        if m.get(label) == sid:
            return
        m[label] = sid
        try:
            os.makedirs(CONFIG_DIR, exist_ok=True)
            Path(_SESSION_MAP_FILE).write_text(json.dumps(m, indent=2))
            _chmod_600(_SESSION_MAP_FILE)
        except OSError:
            pass


# --- tiny TTL cache for GitHub (gh calls are ~1s each) -----------------
_cache: dict[str, tuple[float, object]] = {}
_cache_lock = threading.Lock()


def cached(key: str, ttl: float, producer, keep=None):
    """Memoize `producer()` for `ttl` seconds.

    `keep`, if given, is called with the produced value and must return True
    for the result to be stored. Transient error payloads (e.g. an ADO auth
    blip) can thus be excluded so they don't pin a stale error for the full
    TTL — the next poll retries instead.
    """
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    if keep is None or keep(val):
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
    # No localhost fallback: an empty hive_url means hive is disabled (see
    # _hive_enabled), so callers are gated and must not invent a default.
    return (load_config().get("hive_url") or "").strip().rstrip("/")


def _hive_enabled() -> bool:
    """Hive integration is optional: enabled only when a hive URL is configured.
    Clear the Hive URL in Settings to decommission the hive (hides its tab and
    buttons, and disables the hive API routes)."""
    return bool((load_config().get("hive_url") or "").strip())


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


def _github_gists():
    # `gh api` paginates newest-first; 50 is plenty for a control-plane list.
    raw = _gh_json(["api", "/gists?per_page=50"])
    if isinstance(raw, dict) and raw.get("error"):
        return {"gists": raw}
    gists = []
    for g in raw or []:
        files = list((g.get("files") or {}).keys())
        gists.append({
            "id": g.get("id"),
            "description": (g.get("description") or "").strip(),
            "public": bool(g.get("public")),
            "url": g.get("html_url"),
            "files": files,
            "comments": g.get("comments") or 0,
            "createdAt": g.get("created_at"),
            "updatedAt": g.get("updated_at"),
        })
    return {"gists": gists}


def _github_gist_detail(gid: str):
    """Return one gist's files with contents for inline preview.

    `gh api /gists/<id>` embeds file `content` inline (GitHub truncates very
    large files and instead exposes a `raw_url`; we fetch that so preview is
    complete). Errors surface as {"error": ...}.
    """
    if not gid or not re.fullmatch(r"[0-9a-zA-Z]+", gid):
        return {"error": "bad gist id"}
    raw = _gh_json(["api", f"/gists/{gid}"])
    if isinstance(raw, dict) and raw.get("error"):
        return raw
    files = []
    for name, f in (raw.get("files") or {}).items():
        content = f.get("content") or ""
        if f.get("truncated") and f.get("raw_url"):
            fetched = _gh_json(["api", f.get("raw_url")])
            # raw_url returns text, not JSON; _gh_json yields {"error":...} on
            # a decode failure but the stdout is the file — grab it directly.
            content = _gh_raw(f.get("raw_url")) if isinstance(fetched, dict) and fetched.get("error") else content
        files.append({
            "name": name,
            "language": f.get("language") or "",
            "size": f.get("size") or 0,
            "type": f.get("type") or "",
            "content": content,
        })
    return {
        "id": raw.get("id"),
        "description": (raw.get("description") or "").strip(),
        "public": bool(raw.get("public")),
        "url": raw.get("html_url"),
        "files": files,
    }


def _gh_raw(path: str) -> str:
    """Fetch a gh api path expecting a raw (non-JSON) text body."""
    try:
        out = subprocess.run([GH_BIN, "api", path], capture_output=True,
                             text=True, timeout=25)
        return out.stdout if out.returncode == 0 else ""
    except Exception:  # pragma: no cover
        return ""


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


def _ado_parse_json(status: int, raw: bytes):
    """Parse an ADO response body as JSON, or surface a readable error.

    ADO occasionally answers with a non-JSON body (an HTML sign-in/gateway
    page from an auth blip or proxy). Report the real HTTP status plus a short
    snippet instead of leaking a cryptic `json.JSONDecodeError` string, so the
    UI shows something diagnosable rather than a misleading "502 Expecting
    value: line 3 column 1".
    """
    try:
        return status, json.loads(raw or b"{}")
    except (json.JSONDecodeError, ValueError):
        snippet = (raw or b"").decode("utf-8", "replace").strip()[:120]
        return status, {"error": f"non-JSON response (HTTP {status}): {snippet}"}


# Conditions worth one automatic retry: transient gateway/throttling statuses.
_ADO_RETRY_STATUS = {429, 500, 502, 503, 504}


def _ado_request(url: str, method: str = "GET", body: dict | None = None):
    auth, err = _ado_auth_header()
    if err:
        return 401, {"error": f"ADO auth: {err}"}
    data = json.dumps(body).encode() if body is not None else None
    last = (502, {"error": "unknown"})
    for attempt in range(2):
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={"Authorization": auth, "Content-Type": "application/json",
                     "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=25) as resp:
                status, parsed = _ado_parse_json(resp.status, resp.read())
                # A 200 with a non-JSON body is a transient blip worth retrying.
                if "error" in parsed and status == 200 and attempt == 0:
                    last = (status, parsed)
                    time.sleep(0.4)
                    continue
                return status, parsed
        except urllib.error.HTTPError as exc:
            try:
                last = _ado_parse_json(exc.code, exc.read())
            except Exception:
                last = (exc.code, {"error": exc.reason})
            if exc.code in _ADO_RETRY_STATUS and attempt == 0:
                time.sleep(0.4)
                continue
            return last
        except Exception as exc:
            last = (502, {"error": str(exc)})
            if attempt == 0:
                time.sleep(0.4)
                continue
            return last
    return last


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


def _ado_my_testcases():
    """All Test Cases assigned to the current user (every state), org-wide.

    Test Cases live in the Test plans area but are ordinary work items, so a
    plain WIQL on WorkItemType='Test Case' + AssignedTo=@Me returns them. We
    surface Automation Status too so a tester can see auto-vs-manual at a glance.
    """
    cfg = load_config()
    org = (cfg.get("ado_org") or "").rstrip("/")
    if not org:
        return {"error": "ado_org not configured"}

    wiql = {"query": (
        "SELECT [System.Id] FROM WorkItems "
        "WHERE [System.WorkItemType] = 'Test Case' "
        "AND [System.AssignedTo] = @Me "
        "ORDER BY [System.ChangedDate] DESC")}
    code, data = _ado_request(f"{org}/_apis/wit/wiql?api-version=7.0",
                              method="POST", body=wiql)
    if code != 200:
        return {"error": f"wiql {code}: {data.get('error') or data.get('message','')}"[:200]}

    testcases = []
    ids = [w["id"] for w in (data.get("workItems") or [])]
    fields = ["System.Id", "System.Title", "System.State",
              "System.AreaPath", "System.TeamProject",
              "Microsoft.VSTS.TCM.AutomationStatus", "System.ChangedDate"]
    # workitemsbatch caps at 200 ids per call.
    for i in range(0, len(ids), 200):
        chunk = ids[i:i + 200]
        c2, d2 = _ado_request(f"{org}/_apis/wit/workitemsbatch?api-version=7.0",
                              method="POST", body={"ids": chunk, "fields": fields})
        if c2 != 200:
            return {"error": f"workitemsbatch {c2}: {d2.get('error') or d2.get('message','')}"[:200]}
        for w in d2.get("value") or []:
            f = w.get("fields", {})
            proj = f.get("System.TeamProject", "")
            testcases.append({
                "id": w.get("id"),
                "title": f.get("System.Title", ""),
                "state": f.get("System.State", ""),
                "auto": f.get("Microsoft.VSTS.TCM.AutomationStatus", ""),
                "area": f.get("System.AreaPath", ""),
                "project": proj,
                "changed": f.get("System.ChangedDate", ""),
                "url": f"{org}/{urllib.parse.quote(proj)}/_workitems/edit/{w.get('id')}",
            })
    return {"testcases": testcases}


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


def _mcp_config_raw() -> dict:
    """Raw `~/.copilot/mcp-config.json` mcpServers dict (read once, cached)."""
    global _mcp_config_cache
    try:
        return _mcp_config_cache
    except NameError:
        pass
    servers: dict = {}
    try:
        with open(os.path.expanduser("~/.copilot/mcp-config.json")) as fh:
            d = json.load(fh)
        servers = d.get("mcpServers") or d or {}
    except Exception:
        servers = {}
    _mcp_config_cache = servers
    return servers


def _mcp_server_names() -> list[str]:
    """Names of all MCP servers configured for the CLI, for the settings picker."""
    return list(_mcp_config_raw().keys())


def _acp_mcp_servers(names: list[str] | None = None) -> list[dict]:
    """Convert selected entries from `~/.copilot/mcp-config.json` (copilot CLI
    format) into the ACP `session/new.mcpServers` shape: [{name, command, args,
    env: [{name,value}]}] for local/stdio servers, [{name, type:"http", url}]
    for http ones. Unknown/missing names are silently skipped so a stale saved
    selection never breaks session boot. names=None reads the live setting."""
    if names is None:
        names = load_config().get("chat_mcp_servers", [])
    raw = _mcp_config_raw()
    out: list[dict] = []
    for name in names:
        cfg = raw.get(name)
        if not cfg:
            continue
        if cfg.get("type") == "http":
            entry = {"name": name, "type": "http", "url": cfg.get("url", "")}
            if cfg.get("headers"):
                entry["headers"] = cfg["headers"]
            out.append(entry)
        elif cfg.get("command"):
            out.append({
                "name": name,
                "command": cfg["command"],
                "args": cfg.get("args", []),
                "env": [{"name": k, "value": v} for k, v in (cfg.get("env") or {}).items()],
            })
    return out


def _build_argv(prompt: str, session: str, model: str | None,
                copilot_id: str | None = None) -> list[str]:
    """Legacy per-turn spawn (used when chat_fast is OFF => full MCP available).

    Resumes by a stable per-session UUID via --session-id: copilot creates the
    session on the first turn and resumes it thereafter. This is idempotent and
    unambiguous, unlike resuming by name (-n/--resume=<name>), which broke when a
    hal-ui restart re-created a session and left two copilot sessions sharing a
    name ("Multiple sessions match the name ...").

    When copilot_id is given (a session revived from disk in the History tab), we
    resume THAT real copilot session id directly so the conversation continues
    with its full server-side context, instead of our uuid5-derived id."""
    sid = copilot_id or _copilot_session_id(session)
    argv = [COPILOT_BIN, "-p", prompt, "--output-format", "json",
            "--allow-all-tools", "--session-id", sid]
    if model:
        argv += ["--model", model]
    return argv


# ======================================================================
# ACP persistent chat lane (chat_fast ON): one long-lived `copilot --acp`
# process per browser session. MCP stays cold by default (mcpServers:[]) so
# first turn boots once and later turns skip the ~5-7s CLI+MCP startup
# entirely. Config's chat_mcp_servers can opt specific servers back in (see
# _acp_mcp_servers) when a session needs live org/topic context (e.g. the
# One Identity MCP server) — that server's startup cost is then paid once,
# at boot, not per turn.
# Protocol (JSON-RPC over stdio):
#   initialize -> session/new{cwd,mcpServers:[...configured...]} -> sessionId
#   session/set_model{modelId}    (optional)
#   session/prompt{sessionId,prompt:[{type:text,text}]} -> streams
#       session/update notifications, ends with the request's response (stopReason)
#   agent->client session/request_permission -> auto-answer allow_once
#   session/cancel{sessionId}     (notification) -> abort the running turn
# ======================================================================
class AcpProc:
    BOOT_TIMEOUT = 90

    def __init__(self, model: str | None = None, load_id: str | None = None,
                 cwd: str | None = None):
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
        self._boot(model, load_id, cwd)

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
    def _boot(self, model: str | None, load_id: str | None = None,
              cwd: str | None = None):
        self._request("initialize", {
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}},
        }, timeout=self.BOOT_TIMEOUT)
        wd = cwd or os.path.expanduser("~")
        # Fast lane stays cold by default (mcpServers: []); config's
        # chat_mcp_servers opts specific servers (e.g. "one-identity") back in
        # when a session needs live org/topic context, at the cost of that
        # server's startup time, paid once here at boot.
        mcp_servers = _acp_mcp_servers()
        if load_id:
            # Resume an existing on-disk session (revived from the History tab).
            # session/load replays the full history as session/update notifications
            # before returning; we have no active turn sink yet, so that replay is
            # harmlessly dropped here — the transcript is shown from /api/session/
            # transcript instead. The session is left ready to continue.
            self._request("session/load", {
                "sessionId": load_id, "cwd": wd, "mcpServers": mcp_servers,
            }, timeout=self.BOOT_TIMEOUT)
            self.session_id = load_id
        else:
            res = self._request("session/new", {
                "cwd": wd, "mcpServers": mcp_servers,
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


def _acp_get(session: str, model: str | None,
             load_id: str | None = None, cwd: str | None = None) -> AcpProc:
    with _acp_lock:
        p = _acp_procs.get(session)
        if p is not None and not p.dead and p.proc.poll() is None:
            p.set_model(model)
            return p
        p = AcpProc(model, load_id=load_id, cwd=cwd)
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


def _list_models() -> dict:
    """Return the live model catalog copilot exposes over ACP.

    We run a short one-shot `copilot --acp` handshake (initialize + session/new
    with no MCP servers) and read models.availableModels + currentModelId — the
    same source the interactive `/model` picker uses — so the web UI's model
    lists never go stale. Cached by the caller. On any failure returns
    {"error": ...} and the client keeps its built-in fallback list."""
    try:
        p = subprocess.Popen([COPILOT_BIN, "--acp"], stdin=subprocess.PIPE,
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
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        try:
            p.kill()
        except Exception:
            pass



def _acp_oneshot():
    """Spawn a short-lived `copilot --acp` and return (proc, send, pump) helpers
    for one-off queries (session/list, session/load). pump(target) reads lines
    until the response to request id `target` arrives, auto-answering any
    agent->client requests, and returns (result_obj, notifications) where
    notifications is the list of session/update payloads seen meanwhile."""
    p = subprocess.Popen([COPILOT_BIN, "--acp"], stdin=subprocess.PIPE,
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


def _list_sessions(cap: int = 200) -> dict:
    """Enumerate on-disk copilot sessions via ACP session/list (paginated).

    Returns every session copilot knows about — console, other tools, and ones
    hal-ui created — newest first, so the History tab can surface them. Cached by
    the caller. On failure returns {"error": ...}."""
    try:
        p, send, pump = _acp_oneshot()
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
_SESSION_STATE_DIR = os.path.expanduser("~/.copilot/session-state")
_BODY_CAP = 60_000   # per-session text budget so the index stays memory-bounded


def _session_body(session_id: str) -> str:
    """Concatenated user+assistant message text for one on-disk session (capped)."""
    path = os.path.join(_SESSION_STATE_DIR, session_id, "events.jsonl")
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
    meta = cached("sessions", 20.0, _list_sessions)
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
                  copilot_id=None, copilot_cwd=None):
    """Fast lane: run one turn on the persistent ACP process, buffering events.
    When copilot_id is set (a revived on-disk session) the process boots by
    session/load-ing that id so the turn continues its real context."""
    try:
        acp = _acp_get(session, model, load_id=copilot_id, cwd=copilot_cwd)
    except Exception as exc:
        lt.append("error", {"message": f"acp launch failed: {exc}"})
        lt.append("done", {"code": 1})
        return
    # Remember the real copilot id (session/new mints a random v4) so any later
    # turn — in either lane, after a restart or lane toggle — resumes it.
    _session_map_put(session, acp.session_id)
    lt.canceller = acp.cancel
    try:
        acp.prompt(prompt, lt.append, images=images)
    except Exception as exc:
        lt.append("error", {"message": str(exc)})
        lt.append("done", {"code": 1})


def _run_subprocess_turn(session, model, prompt, lt: LiveTurn, copilot_id=None):
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
    # The id this turn actually resumes/creates (revive id, bridged id, or the
    # deterministic uuid5). Persist it so the ACP lane can resume it too.
    sid = copilot_id or _copilot_session_id(session)
    _session_map_put(session, sid)
    argv = _build_argv(prompt, session, model, copilot_id=copilot_id)
    try:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                cwd=os.path.expanduser("~"), text=True, bufsize=1)
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
                copilot_id: str | None = None, copilot_cwd: str | None = None) -> LiveTurn:
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
    # Bridge: with no explicit revive id, resume whatever real copilot session
    # this label last used (ACP v4 or legacy uuid5). This is what survives an
    # AcpProc death, a hal-ui restart, or a chat_fast lane toggle. An explicit
    # copilot_id (History-tab revive) always wins.
    if copilot_id is None:
        copilot_id = _session_map_get(session)
    fast = bool(load_config().get("chat_fast", True)) or bool(images)
    lt.append("start", {"session": session, "acp": fast, "turn": turn_id})

    def worker():
        try:
            if fast:
                _run_acp_turn(session, model, prompt, images, lt,
                              copilot_id=copilot_id, copilot_cwd=copilot_cwd)
            else:
                _run_subprocess_turn(session, model, prompt, lt, copilot_id=copilot_id)
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
                 ".css": "text/css", ".json": "application/json",
                 ".webmanifest": "application/manifest+json",
                 ".png": "image/png", ".svg": "image/svg+xml",
                 ".ico": "image/x-icon", ".map": "application/json",
                 ".woff2": "font/woff2"}.get(target.suffix, "application/octet-stream")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        # The service worker must control the root scope; let it be served from /.
        if target.name == "sw.js":
            self.send_header("Service-Worker-Allowed", "/")
            self.send_header("Cache-Control", "no-cache")
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
                         copilot_id=copilot_id, copilot_cwd=copilot_cwd)
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
            self._json(200, cached("mywork", 30.0, _github_mywork,
                                   keep=lambda v: "error" not in v))
        elif p.path == "/api/github/gists":
            self._json(200, cached("gists", 30.0, _github_gists,
                                   keep=lambda v: not (isinstance(v.get("gists"), dict))))
        elif p.path == "/api/github/gist":
            gid = (parse_qs(p.query).get("id", [""])[0] or "").strip()
            if not gid:
                self._json(400, {"error": "id is required"})
            else:
                self._json(200, cached("gist:" + gid, 60.0,
                                       lambda: _github_gist_detail(gid),
                                       keep=lambda v: "error" not in v))
        elif p.path == "/api/ado/mywork":
            self._json(200, cached("ado_mywork", 30.0, _ado_mywork,
                                   keep=lambda v: "error" not in v))
        elif p.path == "/api/ado/testcases":
            self._json(200, cached("ado_testcases", 30.0, _ado_my_testcases,
                                   keep=lambda v: "error" not in v))
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
            self._json(200, cached("models", 600.0, _list_models))
        elif p.path == "/api/sessions":
            self._json(200, cached("sessions", 20.0, _list_sessions))
        elif p.path == "/api/sessions/search":
            qs = parse_qs(p.query)
            q = (qs.get("q", [""])[0]).strip()
            self._json(200, _search_sessions(q))
        elif p.path == "/api/session/transcript":
            qs = parse_qs(p.query)
            sid = (qs.get("id", [""])[0]).strip()
            cwd = (qs.get("cwd", [""])[0]).strip() or None
            if not sid:
                self._json(400, {"error": "missing id"})
            else:
                self._json(200, _load_transcript(sid, cwd))
        elif p.path == "/api/settings":
            self._json(200, masked_config())
        elif p.path == "/api/settings/mcp-servers":
            self._json(200, {"names": _mcp_server_names()})
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
            save_config(body if isinstance(body, dict) else {})
            # settings that affect ADO identity/token invalidate caches
            with _cache_lock:
                _cache.pop("ado_mywork", None)
                _cache.pop("ado_testcases", None)
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
    _migrate_secrets()  # encrypt any legacy plaintext PAT/token left in config.json
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    tok = "ok" if _hive_token() else "MISSING"
    hive_desc = f"{_hive_url()} token={tok}" if _hive_enabled() else "disabled"
    print(f"HAL-UI Phase 2 on http://{args.host}:{args.port}/  "
          f"(copilot={COPILOT_BIN}, hive={hive_desc})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
