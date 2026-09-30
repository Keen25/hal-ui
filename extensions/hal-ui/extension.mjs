/**
 * HAL-UI extension for GitHub Copilot.
 *
 * Provides a canvas that embeds HAL-UI in the Copilot side panel, plus the one
 * data surface Copilot has no native equivalent for: Salesforce Collaboration
 * Request triage.
 *
 * The Hive is deliberately NOT exposed here. It is served by hal-hive's own MCP
 * server (`hive_mcp.py`, tools `hive-status`, `hive-task`, ...), which talks to
 * the queue directly. Proxying it through this extension would add a hop, make
 * a dashboard a required link in the hive's control path, and give the model two
 * routes to the same truth. The Hive tab stays reachable through the canvas.
 *
 * Azure DevOps and GitHub "My Work" are likewise not exposed as tools: Copilot
 * already ships an Azure DevOps plugin and a native GitHub My Work view.
 */

import { createCanvas, joinSession } from "@github/copilot-sdk/extension";
import { HalError, baseUrl, ensureUp, salesforce } from "./hal-client.mjs";

/** Top-level HAL-UI tabs, as named by `data-view` in static/index.html. */
const VIEWS = {
    chat: "Chat",
    hive: "Hive",
    github: "GitHub",
    gists: "Gists",
    ado: "Azure DevOps",
    tests: "Test Cases",
    sf: "Collaboration Requests",
    history: "History",
    settings: "Settings",
};

function clamp(value, min, max, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.min(max, Math.max(min, Math.trunc(n)));
}

function render(value) {
    return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

/** Run a tool body, turning HalError into a failure result with its hint. */
async function run(fn) {
    try {
        return render(await fn());
    } catch (err) {
        const message = err instanceof HalError ? err.toString() : err?.message || String(err);
        return { textResultForLlm: message, resultType: "failure" };
    }
}

// ---------------------------------------------------------------- canvas

function canvasUrl(view) {
    // Carry the view in the query *and* the fragment: a host that reloads the
    // frame keeps the query, a host that only rewrites the fragment triggers
    // HAL-UI's hashchange router. Either way the right tab opens.
    const encoded = encodeURIComponent(view);
    return `${baseUrl()}/?view=${encoded}#view=${encoded}`;
}

const canvasInputSchema = {
    type: "object",
    additionalProperties: false,
    properties: {
        view: {
            type: "string",
            enum: Object.keys(VIEWS),
            description: "Which HAL-UI tab to open. Defaults to hive.",
        },
    },
};

let session;

// -------------------------------------------------------------- the tools

const tools = [
    {
        name: "hal_sf_crs",
        description:
            "Salesforce Collaboration Requests (CRs) escalated from Support, split into ones assigned to you and ones still unassigned in the queue. Each row carries the CR name, subject, product, status, priority, linked Salesforce case, linked ADO work item id, escalation/at-risk flags and a deep link. Use this for 'check the queue', 'what CRs are unassigned', or to look up a specific CR.",
        parameters: {
            type: "object",
            properties: {
                products: {
                    type: "array",
                    items: { type: "string" },
                    description:
                        "Filter to these product names, e.g. ['Active Roles','Password Manager']. Omit for all.",
                },
                scope: {
                    type: "string",
                    enum: ["all", "mine", "unassigned"],
                    description: "Which bucket to return. Default all.",
                },
                limit: {
                    type: "number",
                    description: "Maximum CRs per bucket. Default 50.",
                },
            },
        },
        handler: async (args = {}) =>
            run(async () => {
                const products = Array.isArray(args.products)
                    ? args.products.map((p) => String(p).trim()).filter(Boolean)
                    : [];
                const limit = clamp(args.limit, 1, 200, 50);
                const scope = String(args.scope || "all").toLowerCase();
                if (!["all", "mine", "unassigned"].includes(scope)) {
                    throw new Error(`invalid scope ${JSON.stringify(args.scope)}: use all, mine or unassigned`);
                }

                const data = await salesforce.crs(products);
                const out = { products: products.length ? products : "all" };

                if (scope === "all" || scope === "mine") {
                    const mine = data.mine ?? [];
                    out.mine_total = mine.length;
                    out.mine = mine.slice(0, limit);
                }
                if (scope === "all" || scope === "unassigned") {
                    const queue = data.unassigned ?? [];
                    out.unassigned_total = queue.length;
                    out.unassigned = queue.slice(0, limit);
                }
                return out;
            }),
    },
];

// ------------------------------------------------------------- lifecycle

session = await joinSession({
    tools,
    canvases: [
        createCanvas({
            id: "hal-ui",
            displayName: "HAL-UI",
            description:
                "Open the HAL-UI dashboard (read-only view): hive queue and fleet, Salesforce Collaboration Requests, ADO and GitHub work views. Hive control is not available here; use hal-hive's MCP server.",
            inputSchema: canvasInputSchema,
            actions: [
                {
                    name: "show_hal_view",
                    description:
                        "Switch the HAL-UI canvas to a specific tab (hive, sf, ado, github, gists, tests, history, settings, chat).",
                    inputSchema: {
                        type: "object",
                        additionalProperties: false,
                        required: ["view"],
                        properties: {
                            view: { type: "string", enum: Object.keys(VIEWS) },
                        },
                    },
                    handler: async (ctx) =>
                        session.rpc.canvas.open({
                            extensionId: ctx.extensionId,
                            canvasId: ctx.canvasId,
                            instanceId: ctx.instanceId,
                            input: { view: ctx.input?.view || "hive" },
                        }),
                },
            ],
            open: async (ctx) => {
                const requested = String(ctx.input?.view || "hive");
                const view = Object.hasOwn(VIEWS, requested) ? requested : "hive";
                // Start HAL-UI's service if it is down, but never stop or
                // restart it — the user may have the same instance open in a
                // browser tab, and this canvas is only one more client.
                await ensureUp();
                return { title: "HAL-UI", status: VIEWS[view], url: canvasUrl(view) };
            },
        }),
    ],
});

await session.log(`HAL-UI extension ready (${baseUrl()})`, { ephemeral: true });
