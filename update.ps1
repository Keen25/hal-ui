<#
.SYNOPSIS
  Update HAL-UI to the latest committed version and make it live (Windows).

.DESCRIPTION
  One command:  powershell -ExecutionPolicy Bypass -File %USERPROFILE%\hal-ui\update.ps1

  What it does:
    1. fast-forwards the checkout to origin/master (git pull --ff-only)
    2. restarts the WinSW-registered Windows service (picks up server.py changes)
    3. health-checks the running server

  Safe to re-run. Refuses to clobber local edits: if the working tree is dirty
  it stops rather than losing your changes.

.PARAMETER Service
  WinSW service id/name to restart. Default: hal-ui (or $env:HALUI_SERVICE).

.PARAMETER Health
  URL to health-check after boot. Default: http://127.0.0.1:8900/ (or $env:HALUI_HEALTH).

.NOTES
  Targets Windows PowerShell 5.1. Run as the same user the service runs under
  (or elevated) so Restart-Service has rights.
#>
param(
  [string]$Service = $(if ($env:HALUI_SERVICE) { $env:HALUI_SERVICE } else { "hal-ui" }),
  [string]$Health  = $(if ($env:HALUI_HEALTH)  { $env:HALUI_HEALTH }  else { "http://127.0.0.1:8900/" })
)

# Run from the repo this script lives in, regardless of the caller's cwd.
Set-Location -Path $PSScriptRoot

# --- must be a git checkout -------------------------------------------------
git rev-parse --is-inside-work-tree 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
  Write-Host ("FAIL: {0} is not a git checkout - nothing to update." -f (Get-Location)) -ForegroundColor Red
  exit 1
}

# --- refuse to update over uncommitted changes ------------------------------
git diff --quiet
$dirty = ($LASTEXITCODE -ne 0)
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { $dirty = $true }
if ($dirty) {
  Write-Host "FAIL: You have uncommitted changes. Commit or stash them first:" -ForegroundColor Red
  Write-Host ("    git -C `"{0}`" stash        # shelve them" -f (Get-Location))
  Write-Host ("    git -C `"{0}`" checkout .   # or discard them" -f (Get-Location))
  exit 1
}

$branch = (git rev-parse --abbrev-ref HEAD).Trim()
$before = (git rev-parse --short HEAD).Trim()
Write-Host ("-> Fetching and fast-forwarding ({0})..." -f $branch)
git pull --ff-only
if ($LASTEXITCODE -ne 0) {
  Write-Host "FAIL: git pull --ff-only failed (diverged history or no network)." -ForegroundColor Red
  exit 1
}
$after = (git rev-parse --short HEAD).Trim()

if ($before -eq $after) {
  Write-Host ("OK: Already up to date at {0}." -f $after) -ForegroundColor Green
} else {
  Write-Host ("OK: Updated {0} -> {1}:" -f $before, $after) -ForegroundColor Green
  git --no-pager log --oneline ("{0}..{1}" -f $before, $after) | ForEach-Object { Write-Host ("    {0}" -f $_) }
}

# --- restart the WinSW service ---------------------------------------------
$svc = Get-Service -Name $Service -ErrorAction SilentlyContinue
if ($null -ne $svc) {
  Write-Host ("-> Restarting service '{0}'..." -f $Service)
  try {
    Restart-Service -Name $Service -Force -ErrorAction Stop
  } catch {
    Write-Host ("FAIL: could not restart '{0}': {1}" -f $Service, $_.Exception.Message) -ForegroundColor Red
    Write-Host ("      Try elevated, or:  {0}.exe restart" -f $Service)
    exit 1
  }
  Start-Sleep -Seconds 1
  $state = (Get-Service -Name $Service).Status
  Write-Host ("   service: {0}" -f $state)
  if ($state -ne "Running") {
    Write-Host ("FAIL: '{0}' is not Running. Check the WinSW log next to the exe." -f $Service) -ForegroundColor Red
    exit 1
  }
} else {
  Write-Host ("! No '{0}' service found. Start it manually:" -f $Service) -ForegroundColor Yellow
  Write-Host ("    python `"{0}\server.py`" --port 8900 --host 127.0.0.1" -f $PSScriptRoot)
}

# --- health check -----------------------------------------------------------
$code = 0
try {
  $resp = Invoke-WebRequest -UseBasicParsing -Uri $Health -TimeoutSec 5
  $code = [int]$resp.StatusCode
} catch {
  $code = 0
}
if ($code -eq 200) {
  Write-Host ("OK: Live: {0} ({1})" -f $Health, $code) -ForegroundColor Green
  Write-Host "    Hard-refresh the browser tab (Ctrl+Shift+R) to load the new client."
} else {
  Write-Host ("FAIL: Health check failed: {0} returned {1}" -f $Health, $code) -ForegroundColor Red
  exit 1
}

# --- prerequisite (non-fatal): install the sf CLI if missing -----------------
# The Salesforce/CR tab and the Settings Reconnect button need the `sf` CLI.
# Detect it beyond PATH (npm global prefix / %AppData%\npm hold sf.cmd, which is
# often not on a service/shell PATH), install via npm when truly absent, then
# re-check so we don't reinstall on every run. A failed/impossible install only
# warns.
function Test-SfInstalled {
  if ($null -ne (Get-Command sf -ErrorAction SilentlyContinue)) { return $true }
  $dirs = @()
  try { $p = (npm prefix -g 2>$null); if ($p) { $dirs += $p.Trim() } } catch {}
  if ($env:APPDATA) { $dirs += (Join-Path $env:APPDATA 'npm') }
  foreach ($d in $dirs) {
    foreach ($n in @('sf.cmd', 'sf.exe', 'sf.ps1', 'sf')) {
      if (Test-Path (Join-Path $d $n)) { return $true }
    }
  }
  return $false
}

if (Test-SfInstalled) {
  # present - nothing to do
} elseif ($null -ne (Get-Command npm -ErrorAction SilentlyContinue)) {
  Write-Host "-> Salesforce CLI ('sf') not found - installing via npm..."
  npm install --global @salesforce/cli
  if (($LASTEXITCODE -eq 0) -and (Test-SfInstalled)) {
    Write-Host "OK: Installed sf." -ForegroundColor Green
  } else {
    Write-Host "! Failed to install 'sf' automatically. Install manually:" -ForegroundColor Yellow
    Write-Host "    npm install --global @salesforce/cli"
  }
} else {
  Write-Host "! Salesforce CLI ('sf') not found and npm is unavailable - the CR tab" -ForegroundColor Yellow
  Write-Host "  will show 'sf CLI not found'. Install Node/npm, then:"
  Write-Host "    npm install --global @salesforce/cli"
}
