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
    const raw = (process.env.HALUI_URL || process.env.HALUI_HEALTH || DEFAULT_BASE).trim();
    return raw.replace(/\/+$/, "");
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

/** True when HAL-UI answers on the configured base URL. */
export async function isUp() {
    try {
        const res = await fetch(baseUrl() + "/", {
            method: "HEAD",
            signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS),
        });
        return res.ok;
    } catch {
        // Older HAL-UI builds have no do_HEAD and answer 501; fall back to GET.
        try {
            const res = await fetch(baseUrl() + "/", {
                signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS),
            });
            return res.ok;
        } catch {
            return false;
        }
    }
}

/**
 * Ensure HAL-UI is reachable, starting its service once if it is not.
 *
 * Only ever *starts* the service — never restarts or stops it — so opening a
 * canvas can't disturb a session the user already has running in a browser.
 */
export async function ensureUp() {
    if (await isUp()) return { started: false };

    if (process.env.HALUI_URL && !/^https?:\/\/(127\.0\.0\.1|localhost)(:|\/|$)/.test(baseUrl())) {
        throw new HalError(`HAL-UI is not reachable at ${baseUrl()}`, {
            hint: "HALUI_URL points at a remote host, so it cannot be started from here.",
        });
    }

    const [cmd, args] =
        process.platform === "win32"
            ? ["powershell", ["-NoProfile", "-Command", `Start-Service ${SERVICE}`]]
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
