# HAL-UI

A zero-dependency, single-file web control plane for the **GitHub Copilot CLI** —
turns the console agent into a browser cockpit with konsole-style session tabs,
native Hive control, and GitHub + Azure DevOps work views.

Two files. No build step. No `pip install`, no `npm install`. Just
`python3 server.py` and a browser tab.

## Features

- **Streaming multi-session chat** — konsole-style tabs, each a live Copilot CLI
  session. Token-by-token streaming over SSE, per-session message queue (sends
  while busy are queued, not dropped), copy-to-clipboard on replies.
- **Native Hive control** — fleet + queue view, task-detail/logs drawer, and a
  "→ Hive" button that turns the current chat into a queued task.
- **GitHub "My Work"** — assigned PRs and issues via the `gh` CLI.
- **Azure DevOps** — assigned PBIs and PRs (as author *and* reviewer, role-tagged)
  via `az` + ADO REST.
- **Open-as-session** — any ADO/GitHub item spawns a context-pinned chat session
  ("this is what we'll discuss") without dumping boilerplate in the composer.
- **Settings pane** — wire ADO org/PAT/UPN, Hive URL/token, default model, and the
  `chat_fast` toggle. Stored server-side at `~/.config/hal-ui/config.json` (0600).
- **Fast chat lane** — `chat_fast` disables MCP servers for the chat process,
  cutting per-message startup latency (~12s → ~5-6s).

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
- Chat: shells to `copilot -p … --output-format json --allow-all-tools`
  (`-n <session>` first turn, `--resume <session>` after), parses the CLI JSON
  event stream, re-emits as Server-Sent Events.
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

### Requirements
- Python 3.10+
- [`copilot`](https://github.com/github/copilot-cli) CLI on `PATH`
- `gh` CLI (for GitHub views), `az` CLI (for ADO views) — optional per view

## Roadmap
- `--acp` persistent Copilot process per session (warm MCP, sub-second later turns).
- Stop/abort a running turn; drawer auto-poll for running Hive tasks.
