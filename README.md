# HAL-UI

A zero-dependency, single-file web control plane for the **GitHub Copilot CLI** —
turns the console agent into a browser cockpit with konsole-style session tabs,
native Hive control, and GitHub + Azure DevOps work views.

Two files. No build step. No `pip install`, no `npm install`. Just
`python3 server.py` and a browser tab.

## Features

- **Unread notifications** — a background poller (~90s) watches GitHub, ADO,
  CRs and Hive; when an item changes since you last looked, its row gets a red
  dot, its tab shows a red count badge, and the browser tab title shows the
  grand total (`(3) HAL-UI`). Change detection is per-item: PR/issue/CR/work-item
  `updatedAt`/`changed`/`modified` timestamps, or Hive task status. Leaving a
  view (or clicking a row) marks its items seen; first run per view is a silent
  baseline (no flood). Seen-state persists in `localStorage`.
- **Streaming multi-session chat** — konsole-style tabs, each a live Copilot CLI
  session. Token-by-token streaming over SSE, per-session message queue (sends
  while busy are queued, not dropped), copy-to-clipboard on replies.
- **Native Hive control** — fleet + queue view, task-detail/logs drawer, and a
  "→ Hive" button that turns the current chat into a queued task. The Hive tab,
  its "→ Hive" button, and the hive API routes only appear when a hive is
  actually deployed: the server probes the configured `hive_url` and treats any
  HTTP response (even auth/404) as live, hiding everything when the address is
  unreachable or the URL is cleared.
- **GitHub "My Work"** — assigned PRs and issues via the `gh` CLI.
- **Azure DevOps** — assigned PBIs and PRs (as author *and* reviewer, role-tagged)
  via `az` + ADO REST.
- **Open-as-session** — any ADO/GitHub item spawns a context-pinned chat session
  ("this is what we'll discuss") without dumping boilerplate in the composer.
- **Settings pane** — wire ADO org/PAT/UPN, Hive URL/token, default model, the
  `chat_fast` toggle, the default working directory, and the tool-permission
  policy. Stored server-side at `~/.config/hal-ui/config.json` (0600).
- **Persistent ACP chat lane** — with `chat_fast` on, each session runs one
  long-lived `copilot --acp` process (Agent Client Protocol over stdio). The CLI
  boots once; later turns skip startup entirely (cold ~10s → warm ~2-3s to first
  token). Tool-permission requests are auto-approved (`allow_once`) by default,
  or surfaced to the browser for an explicit decision in **prompt** mode.
- **Rich markdown rendering** — HAL responses render as formatted HTML (GitHub
  tables, headings, bold/italic, inline + fenced code, lists, blockquotes, links)
  via a small zero-dependency, XSS-safe renderer. The copy button puts **rich
  HTML** on the clipboard (`text/html` + `text/plain`), so tables and formatting
  survive a paste into Teams, ADO, or email.
- **Paste images into chat** — paste a screenshot/image into the composer; it
  stages as a thumbnail and is sent to HAL as an ACP image block (the model sees
  the actual image). Images route through the ACP lane automatically.
- **Stop a running turn** — a Stop button (and `session/cancel`) aborts the
  in-flight turn instantly; queued messages are cleared.
- **Fast chat toggle** — `chat_fast` ON = ACP persistent lane (fast, no MCP);
  OFF = legacy per-turn spawn with full MCP servers available mid-chat.
- **Per-chat working directory & permissions** — each chat can override the
  server-default working directory (validated; blank = home) and the
  tool-permission policy (auto-accept vs ask-me-each-time) from a toolbar above
  the composer. Both are shown live, retained with browser-archived sessions,
  and passed through the engine-snapshot lane so a mid-turn settings change can
  never mismatch the running turn.
- **Turn lifecycle hardening** — the ACP `session/prompt` response is inspected
  (auth / model / provider / protocol errors surface with a nonzero exit and
  partial text preserved, cancellation is distinguished from failure); an idle
  turn is force-aborted after `acp_idle_timeout`; a cancelled-but-wedged process
  is force-killed after `acp_cancel_grace` and evicted; abandoned ACP processes
  are reaped after `acp_reap_idle`; stderr is drained continuously so a chatty
  engine can't deadlock on a full pipe.
- **Pluggable engine** — the `engine` setting picks which coding-agent CLI backs
  chat: `copilot` (default), `opencode`, or any of the other major ACP agents
  (Claude Code, Codex, Google Antigravity, Gemini CLI, Qwen Code, Cursor, goose, Kimi, Auggie,
  Cline, Kilo, Mistral Vibe — see [Chat engines](#chat-engines)). All speak the
  **same ACP** (Zed Agent Client Protocol) over stdio, so the fast lane is
  engine-agnostic — only the launch argv differs (`copilot --acp` vs
  `opencode acp` vs `gemini --acp` …). The legacy per-turn lane and the on-disk
  history search parse Copilot's proprietary JSON, so they stay Copilot-only;
  any other engine transparently forces the ACP fast lane.
- **Hive drawer auto-poll** — an open task drawer refreshes progress + log tail
  every 4s while the task is non-terminal, then stops.

## Architecture

```
Browser (vanilla HTML/CSS/JS, EventSource/SSE)
        │  /api/chat, /api/hive, /api/ado, /api/github, /api/settings
        ▼
server.py (Python stdlib http.server, threaded)
   ├── copilot CLI   ── JSON events ──► re-emitted as SSE
   ├── gh CLI             (GitHub My Work)
   ├── az + ADO REST      (PBIs / PRs)
   └── Hive REST :5001    (fleet / queue)
```

### Backend — `server.py`
- Python 3 **stdlib only**: `http.server.ThreadingHTTPServer`, thread-per-request.
- **Engine abstraction** (`ENGINES` registry): each entry maps a CLI to its ACP
  launch argv, legacy oneshot argv, and capability flags (`legacy_lane`,
  `session_history`). Copilot and opencode have hand-tuned entries; every other
  agent is a one-line `_acp_engine(...)` call (launch command + install hint)
  whose model catalog and model switch are read from the spec-standard
  `session/new` response (`configOptions` model selector →
  `session/set_config_option`, else `models` → `session/set_model`).
- Chat, fast lane (`chat_fast` on): persistent ACP process per session
  (`<engine> acp` — JSON-RPC over stdio: `initialize` → `session/new` →
  `session/prompt`, streamed `session/update` → SSE). `session/cancel` powers
  Stop (bounded, escalating to force-kill); permission requests are auto-answered
  `allow_once` or routed to the browser (`/api/chat/permission`) in prompt mode.
  The engine, working directory, and permission mode are snapshotted per turn so
  a concurrent settings change can't cross lanes.
- Chat, legacy lane (`chat_fast` off, Copilot only): per-turn
  `copilot -p … --output-format json` spawn with full MCP servers.
- Data: `gh` CLI (TTL-cached), `az account get-access-token` → ADO REST, Hive REST.
- Config: `~/.config/hal-ui/config.json` (0600); secrets masked on read, preserved
  if POSTed blank.

### Frontend — `static/index.html`
- Vanilla HTML/CSS/JS, no framework, no bundler — served straight off disk.
- `SessionManager` in `localStorage`; `EventSource` for streaming.
- Views: Chat · Hive · GitHub · ADO · Settings.

## Run

```bash
python3 server.py --port 8900 --host 127.0.0.1
# open http://127.0.0.1:8900
```

### Autostart (systemd user service)

Runs HAL-UI on login/boot and restarts it on crash — no manual launch needed.
`hal-ui.service` (in this repo) is a linger-friendly user unit:

```bash
mkdir -p ~/.config/systemd/user
cp hal-ui.service ~/.config/systemd/user/
loginctl enable-linger "$USER"          # start at boot without an active login
systemctl --user daemon-reload
systemctl --user enable --now hal-ui.service
```

Manage it:

```bash
systemctl --user status hal-ui        # state + pid
systemctl --user restart hal-ui       # after a server.py change
journalctl --user -u hal-ui -f        # live logs (replaces server.log)
```

On **Windows**, HAL-UI runs the same `server.py` under a
[WinSW](https://github.com/winsw/winsw)-registered Windows service (service id
`hal-ui`), managed with `Restart-Service hal-ui` / `Get-Service hal-ui`.

## Updating

Pull the latest committed version and make it live in **one command**. The
updater fast-forwards the checkout (`git pull --ff-only`), restarts the service
so `server.py` changes take effect, then health-checks the running server. It
refuses to run over uncommitted local edits (commit or stash them first), and is
safe to re-run.

**Linux / macOS** (systemd user service):

```bash
~/hal-ui/update.sh
```

**Windows** (WinSW service, PowerShell 5.1+):

```powershell
powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\hal-ui\update.ps1"
```

Then **hard-refresh the browser tab (Ctrl+Shift+R)** to load the new client
(static files are served off disk, so the client needs no restart — only a
refresh; `server.py` changes need the service restart the script performs).

### Manual update

If you prefer to do it by hand, the script is just:

```bash
# Linux / macOS
cd ~/hal-ui && git pull --ff-only && systemctl --user restart hal-ui
```

```powershell
# Windows
cd $env:USERPROFILE\hal-ui ; git pull --ff-only ; Restart-Service hal-ui
```

### Troubleshooting

- **"You have uncommitted changes"** — the updater won't clobber local edits.
  `git stash` (keep them) or `git checkout .` (discard them), then re-run.
- **`git pull --ff-only` fails** — your local `master` has diverged from
  `origin/master`. Inspect with `git status` / `git log --oneline origin/master..HEAD`
  and reconcile (rebase or reset) before updating.
- **Different service name / port** — override via env before running:
  `HALUI_SERVICE=my-unit HALUI_HEALTH=http://127.0.0.1:9000/ ~/hal-ui/update.sh`
  (PowerShell: `$env:HALUI_SERVICE`, `$env:HALUI_HEALTH`, or the `-Service` /
  `-Health` parameters).
- **Service didn't come back** — check logs: `journalctl --user -u hal-ui -n50`
  (Linux) or the WinSW `*.out.log` / `*.err.log` next to the service exe (Windows).
- **Salesforce shows `AuthDecryptError`** — `sf` stores its token-encryption key
  in the OS keyring (`secret-tool`), which a headless/systemd service can't reach
  (`Cannot autolaunch D-Bus without X11 $DISPLAY`). HAL-UI forces `sf`'s file
  keychain via `SF_USE_GENERIC_UNIX_KEYCHAIN=true` (set in `hal-ui.service` and
  injected into every `sf` call). The on-disk token must be re-encrypted with the
  file key **once** after adopting this: restart the service, then click
  **Reconnect** in Settings (or run `SF_USE_GENERIC_UNIX_KEYCHAIN=true sf org
  login web --alias 90729`). Thereafter the CR tab survives restarts and locked
  keyrings. Note: a token authorized *under the flag* can't be read *without* it
  (and vice-versa) — keep the flag set everywhere `sf` is invoked.

### Requirements
- Python 3.10+
- [`copilot`](https://github.com/github/copilot-cli) CLI on `PATH` (default engine)
- Any other supported agent CLI — **optional**, only if you select that engine
  (see below)
- `gh` CLI (for GitHub views), `az` CLI (for ADO views) — optional per view

## Chat engines

The **engine** setting (Settings pane) picks which coding-agent CLI backs chat.
Both engines speak the same ACP fast lane; the default is unchanged from a
Copilot-only install.

| Engine | Fast lane (ACP) | Legacy per-turn lane | On-disk history |
|--------|:---------------:|:--------------------:|:---------------:|
| `copilot` (default) | ✅ | ✅ (full MCP) | ✅ |
| `opencode` | ✅ | — (auto-forced to ACP) | — |
| any other engine below | ✅ | — (auto-forced to ACP) | — |

Other ACP agents (launch commands from the
[ACP registry](https://github.com/agentclientprotocol/registry)). They are marked
**experimental** in the dropdown: protocol handling is spec-generic, but they
haven't been smoke-tested against HAL-UI. Sign in to each CLI once in a terminal
before selecting it — HAL-UI does not drive ACP authentication.

| Engine id | Agent | Launched as | Install |
|-----------|-------|-------------|---------|
| `claude` | Claude Code (ACP adapter) | `claude-agent-acp` | `npm i -g @agentclientprotocol/claude-agent-acp` |
| `codex` | OpenAI Codex (ACP adapter) | `codex-acp` | `npm i -g @agentclientprotocol/codex-acp` |
| `antigravity` | Google Antigravity | `agy_acp_server.par --uid=` | see below |
| `gemini` | Gemini CLI | `gemini --acp` | `npm i -g @google/gemini-cli` |
| `qwen` | Qwen Code | `qwen --acp` | `npm i -g @qwen-code/qwen-code` |
| `cursor` | Cursor Agent | `cursor-agent acp` | `curl https://cursor.com/install -fsS \| bash` |
| `goose` | goose | `goose acp` | [goose install docs](https://block.github.io/goose/docs/getting-started/installation) |
| `kimi` | Kimi CLI | `kimi acp` | `uv tool install kimi-cli` |
| `auggie` | Augment Auggie | `auggie --acp` | `npm i -g @augmentcode/auggie` |
| `cline` | Cline CLI | `cline --acp` | `npm i -g cline` |
| `kilo` | Kilo Code CLI | `kilo acp` | `npm i -g @kilocode/cli` |
| `vibe` | Mistral Vibe | `vibe-acp` | `uv tool install mistral-vibe` |

**Sign-in.** Some agents need a one-time browser login (OAuth) and block the ACP
handshake until it's done. Use **Settings → Engine sign-in → Sign in / test**:
HAL-UI starts the agent in the background, shows its consent link and status,
and reports the agent's own error if it fails. The agent redirects the browser
to a one-shot `http://127.0.0.1:<port>/` listener on the HAL-UI host, so:

- browser on the HAL-UI host — just approve; it completes by itself;
- browser on another device — approve, then copy the (unreachable)
  `http://127.0.0.1:<port>/?…&code=…` address from the address bar into the
  paste box that appears; HAL-UI forwards it to the listener locally (only that
  exact loopback port is accepted).

If the agent has no default auth method configured, HAL-UI calls ACP
`authenticate` with its OAuth method. The agent caches the credentials, so
later chats start headless. Headless alternative: `python3 server.py --login
<engine>` prints the link in a terminal. The same button also works as a quick
"can this engine start a session?" check for any engine.

**Google Antigravity** (smoke-tested with ACP server 1.2.1: sign-in, model
catalog, model switch, streamed turns). The `agy` CLI has no ACP mode and its login is **not**
shared: Google ships a separate ACP server with its own OAuth token
(`~/.gemini/antigravity-acp/acp_token.json`). Install it, then sign in as above:

```bash
mkdir -p ~/.local/share/antigravity-acp && cd ~/.local/share/antigravity-acp
curl -fsSLO https://dl.google.com/agy-extensions/releases/linux/agy-acp-server-1.2.1-linux-x86_64.zip
unzip -o agy-acp-server-*.zip && rm agy-acp-server-*.zip
```

HAL-UI finds it in `~/.local/share/antigravity-acp/` (or `antigravity-acp` /
`agy_acp_server.par` on `PATH`). API-key or enterprise auth instead of Google
login: set `auth.type` in `~/.gemini/antigravity-acp/settings.json`
(`gemini-api-key` + `GEMINI_API_KEY`, `agent-platform`, `oauth-business`).

The engine can be switched in **Settings → Engine** or from the **engine**
dropdown in the chat toolbar (installed engines only; same global setting,
applies from the next turn). The Settings dropdown lists every engine, flags the ones not found on this host
(they can't be selected; the install command is shown), and the binary is
re-resolved on every save, so installing one while HAL-UI runs needs no restart.

The legacy lane and the **on-disk** history search parse Copilot's proprietary
JSON, so they stay Copilot-only; selecting opencode transparently forces the ACP
fast lane. The History tab itself still works under opencode for your
**browser-local archived** sessions — only the on-disk transcript section is
disabled (with an in-tab note). Switching engines retires only the previous
engine's live chat processes; sessions already on the new engine keep running.

**To use opencode:**
1. Install the CLI: `curl -fsSL https://opencode.ai/install | bash` (or
   `npm i -g opencode-ai`). It need not be on `PATH` — HAL-UI also finds
   `~/.opencode/bin/opencode`. Installing it while HAL-UI is already running is
   fine; the binary is re-resolved on each save (no restart needed).
2. Settings → **Engine → opencode** → Save. Chat re-homes to opencode on the
   next turn; the model picker follows the new engine.
3. Free "OpenCode Zen" models work without auth, so you can smoke-test right
   away; sign in via `opencode auth login` for the full catalog.

Saving an engine whose binary can't be found is rejected (the prior engine is
kept), so you can't strand chat on a CLI that isn't installed.

## Roadmap
- Convert `~/.copilot/mcp-config.json` to ACP `mcpServers` so the fast lane can
  keep chosen MCP servers warm (best of both lanes).
- Multi-turn ACP session persistence across server restarts (`session/load`).
- Per-turn token/cost usage surfaced in the chat UI (ACP `usage_update`).
