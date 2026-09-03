#!/usr/bin/env bash
# Update HAL-UI to the latest committed version and make it live.
#
# One command:  ~/hal-ui/update.sh
#
# What it does:
#   1. fast-forwards the checkout to origin/master (git pull --ff-only)
#   2. restarts the systemd user service (picks up server.py changes)
#   3. health-checks the running server
#
# It is safe to re-run. It refuses to clobber local edits: if the working
# tree is dirty it stops and tells you, rather than losing your changes.
#
# Env overrides:
#   HALUI_SERVICE   systemd user unit name         (default: hal-ui)
#   HALUI_HEALTH    URL to health-check after boot (default: http://127.0.0.1:8900/)
set -euo pipefail

SERVICE="${HALUI_SERVICE:-hal-ui}"
HEALTH="${HALUI_HEALTH:-http://127.0.0.1:8900/}"

# Run from the repo this script lives in, regardless of the caller's cwd.
cd "$(dirname "$(readlink -f "$0")")"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "✗ $(pwd) is not a git checkout — nothing to update." >&2
  exit 1
fi

# Refuse to update over uncommitted changes.
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "✗ You have uncommitted changes. Commit or stash them first:" >&2
  echo "    git -C \"$(pwd)\" stash        # shelve them" >&2
  echo "    git -C \"$(pwd)\" checkout .   # or discard them" >&2
  exit 1
fi

before="$(git rev-parse --short HEAD)"
echo "→ Fetching and fast-forwarding ($(git rev-parse --abbrev-ref HEAD))…"
git pull --ff-only
after="$(git rev-parse --short HEAD)"

if [ "$before" = "$after" ]; then
  echo "✓ Already up to date at $after."
else
  echo "✓ Updated $before → $after:"
  git --no-pager log --oneline "$before..$after" | sed 's/^/    /'
fi

# Restart the service if it is installed; otherwise tell the user how to run it.
if systemctl --user list-unit-files "$SERVICE.service" >/dev/null 2>&1 \
   && systemctl --user cat "$SERVICE" >/dev/null 2>&1; then
  echo "→ Restarting $SERVICE…"
  systemctl --user restart "$SERVICE"
  sleep 1
  state="$(systemctl --user is-active "$SERVICE" || true)"
  echo "  service: $state"
  [ "$state" = "active" ] || { echo "✗ $SERVICE is not active — check: journalctl --user -u $SERVICE -n50" >&2; exit 1; }
else
  echo "! No '$SERVICE' user service found. Start it manually:" >&2
  echo "    python3 \"$(pwd)/server.py\" --port 8900 --host 127.0.0.1" >&2
fi

# Health check.
code="$(curl -s -o /dev/null -w '%{http_code}' "$HEALTH" || echo 000)"
if [ "$code" = "200" ]; then
  echo "✓ Live: $HEALTH ($code)"
  echo "  Hard-refresh the browser tab (Ctrl+Shift+R) to load the new client."
else
  echo "✗ Health check failed: $HEALTH returned $code" >&2
  exit 1
fi

# Prerequisite (non-fatal): the Salesforce/CR tab and the Settings Reconnect
# button need the `sf` CLI. Detect it the way server.py's _resolve_sf() does
# (PATH, an nvm-installed sf, or the npm global prefix); if missing, install it
# via npm and re-check. A failed or impossible install only warns — it never
# fails the update.
_sf_present() {
  command -v sf >/dev/null 2>&1 && return 0
  ls "$HOME"/.nvm/versions/node/*/bin/sf >/dev/null 2>&1 && return 0
  local pref; pref="$(npm prefix -g 2>/dev/null)"
  [ -n "$pref" ] && [ -x "$pref/bin/sf" ] && return 0
  return 1
}
if _sf_present; then
  :
elif command -v npm >/dev/null 2>&1; then
  echo "→ Salesforce CLI ('sf') not found — installing via npm…"
  if npm install --global @salesforce/cli && _sf_present; then
    echo "✓ Installed sf ($(command -v sf 2>/dev/null || echo 'restart shell to pick up PATH'))."
  else
    echo "! Failed to install 'sf' automatically. Install manually:" >&2
    echo "    npm install --global @salesforce/cli" >&2
  fi
else
  echo "! Salesforce CLI ('sf') not found and npm is unavailable — the CR tab" >&2
  echo "  will show 'sf CLI not found'. Install Node/npm, then:" >&2
  echo "    npm install --global @salesforce/cli" >&2
fi
