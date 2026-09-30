import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";

import { baseUrl, healthUrl, isLoopback, isUp } from "./hal-client.mjs";

function listen(handler) {
    return new Promise((resolve) => {
        const server = http.createServer(handler).listen(0, "127.0.0.1", () => {
            resolve({ server, url: `http://127.0.0.1:${server.address().port}` });
        });
    });
}

beforeEach(() => {
    delete process.env.HALUI_URL;
    delete process.env.HALUI_HEALTH;
});

describe("baseUrl / healthUrl", () => {
    test("defaults to loopback 8900", () => {
        assert.equal(baseUrl(), "http://127.0.0.1:8900");
        assert.equal(healthUrl(), "http://127.0.0.1:8900/");
    });

    test("HALUI_HEALTH never becomes the API base", () => {
        process.env.HALUI_HEALTH = "http://127.0.0.1:9999/healthz";
        assert.equal(baseUrl(), "http://127.0.0.1:8900");
        assert.equal(healthUrl(), "http://127.0.0.1:9999/healthz");
    });

    test("HALUI_URL drives both, trailing slashes trimmed", () => {
        process.env.HALUI_URL = "http://localhost:1234//";
        assert.equal(baseUrl(), "http://localhost:1234");
        assert.equal(healthUrl(), "http://localhost:1234/");
    });
});

describe("isLoopback", () => {
    for (const url of ["http://127.0.0.1:8900", "http://127.1.2.3/", "http://localhost", "http://[::1]:8900/"]) {
        test(`accepts ${url}`, () => assert.equal(isLoopback(url), true));
    }
    for (const url of ["http://example.com", "http://127.0.0.1.evil.com/", "http://10.0.0.1", "not a url"]) {
        test(`rejects ${url}`, () => assert.equal(isLoopback(url), false));
    }
});

describe("isUp", () => {
    let legacy, modern, down;

    before(async () => {
        // Mimics a HAL-UI build without do_HEAD: stdlib replies 501 to HEAD.
        legacy = await listen((req, res) => {
            res.writeHead(req.method === "HEAD" ? 501 : 200).end("ok");
        });
        modern = await listen((req, res) => res.writeHead(200).end());
        down = await listen((req, res) => res.writeHead(503).end());
    });

    after(() => {
        for (const s of [legacy, modern, down]) s.server.close();
    });

    test("true when HEAD succeeds", async () => {
        process.env.HALUI_URL = modern.url;
        assert.equal(await isUp(), true);
    });

    test("falls back to GET when HEAD is answered with 501", async () => {
        process.env.HALUI_URL = legacy.url;
        assert.equal(await isUp(), true);
    });

    test("false when the server answers with an error", async () => {
        process.env.HALUI_URL = down.url;
        assert.equal(await isUp(), false);
    });

    test("false when nothing is listening", async () => {
        process.env.HALUI_URL = "http://127.0.0.1:1";
        assert.equal(await isUp(), false);
    });
});
