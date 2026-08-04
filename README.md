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
- **Persistent ACP chat lane** — with `chat_fast` on, each session runs one
  long-lived `copilot --acp` process (Agent Client Protocol over stdio). The CLI
  boots once; later turns skip startup entirely (cold ~10s → warm ~2-3s to first
  token). Tool-permission requests are auto-approved (`allow_once`).
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
- Chat, fast lane (`chat_fast` on): persistent `copilot --acp` process per session
  (JSON-RPC over stdio — `initialize` → `session/new` → `session/prompt`, streamed
  `session/update` → SSE). `session/cancel` powers Stop; permission requests
  auto-answered `allow_once`.
- Chat, legacy lane (`chat_fast` off): per-turn `copilot -p … --output-format json`
  spawn with full MCP servers.
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
- Convert `~/.copilot/mcp-config.json` to ACP `mcpServers` so the fast lane can
  keep chosen MCP servers warm (best of both lanes).
- Multi-turn ACP session persistence across server restarts (`session/load`).
- Per-turn token/cost usage surfaced in the chat UI (ACP `usage_update`).
