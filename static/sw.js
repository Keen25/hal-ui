/* HAL-UI service worker.
 *
 * HAL-UI is a live control plane: /api/* is SSE + realtime JSON and must never
 * be cached. This worker only makes the app installable and gives an offline
 * shell — it precaches the static shell (index.html, icons, manifest), serves
 * navigations network-first (falling back to the cached shell when offline),
 * and serves other same-origin static assets cache-first. Any /api/ request or
 * cross-origin request is left entirely to the network (no respondWith).
 */
const VERSION = "hal-ui-v1";
const SHELL = [
  "/",
  "/index.html",
  "/manifest.webmanifest",
  "/icon-192.png",
  "/icon-512.png",
  "/icon-maskable-512.png",
  "/apple-touch-icon.png",
  "/favicon.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(VERSION).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== VERSION).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;

  const url = new URL(req.url);
  // Only ever touch our own origin.
  if (url.origin !== self.location.origin) return;
  // Never intercept the live control-plane API (SSE + realtime JSON).
  if (url.pathname.startsWith("/api/")) return;

  // Navigations: network-first, fall back to the cached app shell offline.
  if (req.mode === "navigate") {
    event.respondWith(
      fetch(req)
        .then((res) => {
          const copy = res.clone();
          caches.open(VERSION).then((c) => c.put("/index.html", copy)).catch(() => {});
          return res;
        })
        .catch(() => caches.match("/index.html").then((r) => r || caches.match("/")))
    );
    return;
  }

  // Other same-origin static assets: cache-first, then network (and cache it).
  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((res) => {
        if (res.ok && res.type === "basic") {
          const copy = res.clone();
          caches.open(VERSION).then((c) => c.put(req, copy)).catch(() => {});
        }
        return res;
      });
    })
  );
});
