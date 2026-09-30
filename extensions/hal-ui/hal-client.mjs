/**
 * Thin REST client for a locally running HAL-UI (`server.py`).
 *
 * HAL-UI already is the backend: it fronts the Hive queue, Salesforce, ADO and
 * GitHub behind one loopback HTTP API. This module is only transport plus
 * lifecycle, so the extension never re-implements any of that logic.
 *
 * Deliberately dependency-free: Node 18+ ships global `fetch` and
 * `AbortSignal.timeout`, and the SDK resolves no third-party modules for us.
 */

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const DEFAULT_BASE = "http://127.0.0.1:8900";

/** Match update.sh / update.ps1 so one env var configures every HAL-UI tool. */
const SERVICE = (process.env.HALUI_SERVICE || "hal-ui").trim();

const REQUEST_TIMEOUT_MS = 20_000;
const HEALTH_TIMEOUT_MS = 1_500;
const START_TIMEOUT_MS = 15_000;
const POLL_INTERVAL_MS = 250;

/** Base URL of the running HAL-UI, without a trailing slash. */
export function baseUrl() {
    const raw = (process.env.HALUI_URL || "").trim() || DEFAULT_BASE;
    return raw.replace(/\/+$/, "");
}

/** URL probed for liveness. Same meaning as HALUI_HEALTH in update.sh / update.ps1. */
export function healthUrl() {
    return (process.env.HALUI_HEALTH || "").trim() || `${baseUrl()}/`;
}

/** True when `url` points at this machine, so starting a local service can help. */
export function isLoopback(url) {
    try {
        const host = new URL(url).hostname.toLowerCase();
        return host === "localhost" || host === "[::1]" || /^127(\.\d{1,3}){3}$/.test(host);
    } catch {
        return false;
    }
}

/** Error carrying an actionable hint, so the agent can tell the user what to fix. */
export class HalError extends Error {
    constructor(message, { status = 0, hint = "" } = {}) {
        super(message);
        this.name = "HalError";
        this.status = status;
        this.hint = hint;
    }

    toString() {
        return this.hint ? `${this.message}\nHint: ${this.hint}` : this.message;
    }
}

/**
 * HAL-UI returns 404 for features that are switched off rather than missing,
 * so turn that into the setting the user actually has to change.
 */
function hintFor(path, status) {
    if (status !== 404) return "";
    if (path.startsWith("/api/sf/")) {
        return "Salesforce is disabled. Install the `sf` CLI and connect an org in HAL-UI Settings.";
    }
    return "";
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function request(path, { method = "GET", body, timeoutMs = REQUEST_TIMEOUT_MS } = {}) {
    const url = baseUrl() + path;
    let res;
    try {
        res = await fetch(url, {
            method,
            headers: body === undefined ? undefined : { "Content-Type": "application/json" },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: AbortSignal.timeout(timeoutMs),
        });
    } catch (err) {
        throw new HalError(`cannot reach HAL-UI at ${baseUrl()} (${err.message})`, {
            hint: startCommand(),
        });
    }

    const text = await res.text();
    let data;
    try {
        data = text ? JSON.parse(text) : {};
    } catch {
        throw new HalError(`HAL-UI returned non-JSON from ${path} (HTTP ${res.status})`, {
            status: res.status,
        });
    }

    if (!res.ok) {
        throw new HalError(`${path} failed: ${data?.error || `HTTP ${res.status}`}`, {
            status: res.status,
            hint: hintFor(path, res.status),
        });
    }
    return data;
}

function startCommand() {
    return process.platform === "win32"
        ? `Start-Service ${SERVICE}`
        : `systemctl --user start ${SERVICE}`;
}

async function probe(method) {
    try {
        const res = await fetch(healthUrl(), {
            method,
            signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS),
        });
        if (method === "GET") await res.body?.cancel();
        return res;
    } catch {
        return null;
    }
}

/** True when HAL-UI answers on the configured health URL. */
export async function isUp() {
    const head = await probe("HEAD");
    if (head?.ok) return true;
    // A server that answered at all but rejected HEAD (older HAL-UI builds have
    // no do_HEAD and reply 501) is still up; confirm with GET. fetch() resolves
    // on any HTTP status, so this must key off the status, not a throw.
    if (!head) return false;
    return Boolean((await probe("GET"))?.ok);
}

/**
 * Ensure HAL-UI is reachable, starting its service once if it is not.
 *
 * Only ever *starts* the service — never restarts or stops it — so opening a
 * canvas can't disturb a session the user already has running in a browser.
 */
export async function ensureUp() {
    if (await isUp()) return { started: false };

    if (!isLoopback(baseUrl()) || !isLoopback(healthUrl())) {
        throw new HalError(`HAL-UI is not reachable at ${healthUrl()}`, {
            hint: "HALUI_URL / HALUI_HEALTH point at a remote host, so it cannot be started from here.",
        });
    }

    // SERVICE is spliced into a PowerShell command line on Windows, so refuse
    // anything that isn't a plain service name rather than risk injection.
    if (!/^[\w.-]+$/.test(SERVICE)) {
        throw new HalError(`refusing to start service with unsafe name ${JSON.stringify(SERVICE)}`, {
            hint: "Set HALUI_SERVICE to a plain service name (letters, digits, . _ -).",
        });
    }

    const [cmd, args] =
        process.platform === "win32"
            ? ["powershell", ["-NoProfile", "-Command", `Start-Service -Name '${SERVICE}'`]]
            : ["systemctl", ["--user", "start", SERVICE]];

    try {
        await execFileAsync(cmd, args, { timeout: START_TIMEOUT_MS });
    } catch (err) {
        throw new HalError(`HAL-UI is not running and \`${startCommand()}\` failed: ${err.message}`, {
            hint: `Start it by hand: ${startCommand()}`,
        });
    }

    const deadline = Date.now() + START_TIMEOUT_MS;
    while (Date.now() < deadline) {
        await sleep(POLL_INTERVAL_MS);
        if (await isUp()) return { started: true };
    }

    throw new HalError(`HAL-UI did not come up at ${baseUrl()} after starting \`${SERVICE}\``, {
        hint:
            process.platform === "win32"
                ? "Check the WinSW *.err.log next to the service exe."
                : `Check logs: journalctl --user -u ${SERVICE} -n50`,
    });
}

export const salesforce = {
    status: () => request("/api/sf/status"),
    products: () => request("/api/sf/products"),
    crs: (products = []) =>
        request(
            products.length
                ? `/api/sf/crs?products=${encodeURIComponent(products.join(","))}`
                : "/api/sf/crs",
        ),
};

export const settings = () => request("/api/settings");
