/**
 * HAL-UI extension for GitHub Copilot.
 *
 * Surfaces the two halves of HAL-UI that Copilot has no native equivalent for
 * — the Hive task queue and the Salesforce Collaboration Request triage view —
 * as a canvas plus agent-callable tools.
 *
 * Azure DevOps and GitHub "My Work" are intentionally NOT exposed as tools:
 * Copilot already ships an Azure DevOps plugin and a native GitHub My Work
 * view, so duplicating them would only collide with existing tool names and
 * split the user's workflow. Both remain reachable through the canvas.
 */

import { createCanvas, joinSession } from "@github/copilot-sdk/extension";
import { HalError, baseUrl, ensureUp, hive, salesforce } from "./hal-client.mjs";

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

/** Task states HAL-UI itself treats as in-flight (mirrors ACTIVE in index.html). */
const ACTIVE_STATES = new Set([
    "queued",
    "pending",
    "claimed",
    "running",
    "probing",
    "reviewing",
    "needs_attention",
    "merging",
]);

/** Fields worth showing per task in a queue listing. */
const TASK_SUMMARY_FIELDS = [
    "id",
    "status",
    "kind",
    "title",
    "priority",
    "complexity",
    "model",
    "worker",
    "branch",
    "requires",
    "created_at",
    "heartbeat_at",
    "done_at",
    "error",
];

/**
 * Task-detail fields to drop: secrets, and `*_json` strings that merely
 * duplicate an already-parsed object.
 */
const TASK_DETAIL_OMIT = new Set(["lease_token", "progress_json", "usage_json"]);

const PROMPT_PREVIEW_CHARS = 400;

function pick(obj, fields) {
    const out = {};
    for (const f of fields) {
        if (obj?.[f] !== undefined && obj?.[f] !== null) out[f] = obj[f];
    }
    return out;
}

function clamp(value, min, max, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.min(max, Math.max(min, Math.trunc(n)));
}

function truncate(text, max) {
    if (typeof text !== "string" || text.length <= max) return text;
    return `${text.slice(0, max)}… [truncated, ${text.length} chars total]`;
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

/** `task-` + 8 hex chars, matching the ids the Hive already issues. */
function generateTaskId() {
    const hex = Math.trunc(Math.random() * 0xffffffff)
        .toString(16)
        .padStart(8, "0");
    return `task-${hex.slice(0, 8)}`;
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
        name: "hal_hive_status",
        description:
            "Snapshot of the HAL hive: task counts by state, the worker fleet (host, lane, capability tags, online status, current task) and the tasks themselves. Defaults to in-flight tasks only because the queue holds hundreds of finished ones. Use this to answer 'what is the hive doing', 'is anything stuck', or 'which workers are online'.",
        parameters: {
            type: "object",
            properties: {
                status: {
                    type: "string",
                    description:
                        "Return only tasks in this exact state, e.g. claimed, needs_attention, merged, dead. Overrides the default in-flight filter.",
                },
                all: {
                    type: "boolean",
                    description:
                        "Include finished tasks (merged, cancelled, dead) as well as in-flight ones. Default false.",
                },
                limit: {
                    type: "number",
                    description: "Maximum tasks to return. Default 25, max 200.",
                },
                include_workers: {
                    type: "boolean",
                    description: "Include the worker fleet. Default true.",
                },
            },
        },
        handler: async (args = {}) =>
            run(async () => {
                const data = await hive.status();
                const limit = clamp(args.limit, 1, 200, 25);
                const wanted = String(args.status || "").trim().toLowerCase();

                let tasks = Array.isArray(data.tasks) ? data.tasks : [];
                const total = tasks.length;

                if (wanted) {
                    tasks = tasks.filter((t) => String(t.status || "").toLowerCase() === wanted);
                } else if (!args.all) {
                    tasks = tasks.filter((t) => ACTIVE_STATES.has(String(t.status || "").toLowerCase()));
                }

                const result = {
                    counts: data.counts ?? {},
                    usage_totals: data.usage_totals ?? {},
                    tasks_total: total,
                    tasks_matched: tasks.length,
                    tasks_shown: Math.min(tasks.length, limit),
                    tasks: tasks.slice(0, limit).map((t) => {
                        const row = pick(t, TASK_SUMMARY_FIELDS);
                        if (t.progress) {
                            row.progress = pick(t.progress, ["phase", "turn", "elapsed_s", "last_tool"]);
                        }
                        return row;
                    }),
                };

                if (args.include_workers !== false) {
                    result.workers = (data.workers ?? []).map((w) =>
                        pick(w, [
                            "worker_id",
                            "hostname",
                            "lane",
                            "host",
                            "online",
                            "status",
                            "current_task",
                            "caps",
                            "last_seen",
                            "age_s",
                        ]),
                    );
                }
                return result;
            }),
    },
    {
        name: "hal_hive_task",
        description:
            "Full detail for one HAL hive task: status, worker, lease, attempts, progress, error, verification state and the tail of its log. Use this to diagnose a task that is stuck, failed, or sitting in needs_attention.",
        parameters: {
            type: "object",
            properties: {
                id: { type: "string", description: "Task id, e.g. task-41a317e3." },
                include_prompt: {
                    type: "boolean",
                    description:
                        "Include the full task prompt instead of a short preview. Prompts are long; default false.",
                },
            },
            required: ["id"],
        },
        handler: async (args = {}) =>
            run(async () => {
                const id = String(args.id || "").trim();
                if (!id) throw new HalError("id is required");

                const task = await hive.task(id);
                const out = {};
                for (const [key, value] of Object.entries(task)) {
                    if (TASK_DETAIL_OMIT.has(key)) continue;
                    out[key] = value;
                }
                if (!args.include_prompt && typeof out.prompt === "string") {
                    out.prompt = truncate(out.prompt, PROMPT_PREVIEW_CHARS);
                }
                return out;
            }),
    },
    {
        name: "hal_hive_add",
        description:
            "Queue a coding task on the HAL hive. This is a WRITE that dispatches real work to a worker, so confirm the prompt and the capability requirements with the user before calling it. Capability tags in `requires` must be satisfied by a worker (e.g. os:windows, build:dotnet), otherwise the task will never be claimed — check hal_hive_status for the fleet's advertised caps first.",
        parameters: {
            type: "object",
            properties: {
                prompt: {
                    type: "string",
                    description: "The full task prompt handed to the worker's Copilot CLI.",
                },
                id: {
                    type: "string",
                    description: "Task id. Generated as task-<8 hex> when omitted.",
                },
                complexity: {
                    type: "number",
                    description:
                        "1-3, selecting the model tier. Omit to let the hive choose automatically.",
                },
                priority: {
                    type: "number",
                    description: "Queue priority; lower runs sooner. Default 5.",
                },
                kind: { type: "string", description: "Task kind. Default 'work'." },
                requires: {
                    type: "array",
                    items: { type: "string" },
                    description:
                        "Capability tags a worker must advertise, e.g. ['os:windows','build:dotnet'].",
                },
                model: {
                    type: "string",
                    description: "Pin an exact model instead of deriving one from complexity.",
                },
            },
            required: ["prompt"],
        },
        handler: async (args = {}) =>
            run(async () => {
                const prompt = String(args.prompt || "").trim();
                if (!prompt) throw new HalError("prompt is required");

                const payload = { id: String(args.id || "").trim() || generateTaskId(), prompt };
                if (args.priority !== undefined) payload.priority = clamp(args.priority, 0, 99, 5);
                if (args.kind) payload.kind = String(args.kind);
                if (args.model) payload.model = String(args.model);
                if (args.complexity !== undefined) payload.complexity = clamp(args.complexity, 1, 3, 2);
                if (Array.isArray(args.requires) && args.requires.length) {
                    // HAL-UI's own Add form posts a space-separated string; the
                    // hive canonicalises either shape, so match the proven path.
                    payload.requires = args.requires.map((t) => String(t).trim()).filter(Boolean).join(" ");
                }

                const data = await hive.add(payload);
                return { queued: payload.id, requires: payload.requires ?? null, response: data };
            }),
    },
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
                "Open the HAL-UI control plane: hive queue and fleet, Salesforce Collaboration Requests, ADO and GitHub work views.",
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
