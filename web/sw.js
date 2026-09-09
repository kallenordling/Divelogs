/* Service worker: makes DeepLog installable and usable without a signal.
 *
 * Three strategies, chosen by what the request is:
 *
 *   app shell        cache first, refreshed in the background — the page must
 *                    open instantly on a boat with no bars
 *   site catalogues  same, but they are half a megabyte and change rarely
 *   Supabase         network only, never cached: dives must be current, and
 *                    caching an authenticated response is a way to leak it
 *
 * Bump VERSION when the shell changes; old caches are dropped on activate.
 */
'use strict';

const VERSION = 'deeplog-v1';
const SHELL = `${VERSION}-shell`;
const DATA = `${VERSION}-data`;
const RUNTIME = `${VERSION}-runtime`;

const SHELL_FILES = [
  './',
  './index.html',
  './css/app.css',
  './js/util.js',
  './js/api.js',
  './js/catalogue.js',
  './js/charts.js',
  './js/ui.js',
  './js/views.js',
  './js/map.js',
  './js/logdive.js',
  './js/app.js',
  './manifest.webmanifest',
  './icons/icon-192.png',
  './icons/icon-512.png',
  './icons/apple-touch-icon.png',
];

// Large and slow-changing; fetched on install so the first offline run works.
const DATA_FILES = ['./dive_sites.json', './finnish_sites.json'];

self.addEventListener('install', (e) => {
  e.waitUntil((async () => {
    const shell = await caches.open(SHELL);
    // addAll fails the whole install if any single file 404s, so add them
    // individually and let the rest through.
    await Promise.all(SHELL_FILES.map((f) =>
      shell.add(new Request(f, { cache: 'reload' })).catch(() => {})));

    const data = await caches.open(DATA);
    await Promise.all(DATA_FILES.map((f) => data.add(f).catch(() => {})));

    self.skipWaiting();
  })());
});

self.addEventListener('activate', (e) => {
  e.waitUntil((async () => {
    const keep = new Set([SHELL, DATA, RUNTIME]);
    for (const k of await caches.keys()) if (!keep.has(k)) await caches.delete(k);
    await self.clients.claim();
  })());
});

/** Serve from cache at once, and refresh the entry for next time. */
async function staleWhileRevalidate(request, cacheName) {
  const cache = await caches.open(cacheName);
  const hit = await cache.match(request, { ignoreSearch: false });

  const network = fetch(request).then((res) => {
    if (res && res.ok) cache.put(request, res.clone());
    return res;
  }).catch(() => null);

  return hit || (await network) || Response.error();
}

self.addEventListener('fetch', (e) => {
  const { request } = e;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // Never touch the API: responses are per-user and must be current.
  if (url.hostname.endsWith('supabase.co')) return;

  // Navigations: cache first, falling back to the cached shell when offline,
  // so a deep link still opens the app rather than the browser's error page.
  if (request.mode === 'navigate') {
    e.respondWith((async () => {
      try {
        const res = await fetch(request);
        const cache = await caches.open(SHELL);
        cache.put('./index.html', res.clone());
        return res;
      } catch {
        return (await caches.match('./index.html')) ||
               (await caches.match('./')) ||
               new Response('DeepLog is offline and has nothing cached yet.',
                            { status: 503, headers: { 'Content-Type': 'text/plain' } });
      }
    })());
    return;
  }

  if (url.origin === self.location.origin) {
    const isData = DATA_FILES.some((f) => url.pathname.endsWith(f.replace('./', '')));
    e.respondWith(staleWhileRevalidate(request, isData ? DATA : SHELL));
    return;
  }

  // Map tiles, Leaflet and the web font: keep whatever we have seen before,
  // so a previously visited area of the map still draws offline.
  e.respondWith(staleWhileRevalidate(request, RUNTIME));
});

// Lets the page trigger an immediate update rather than waiting for a reload.
self.addEventListener('message', (e) => {
  if (e.data === 'skip-waiting') self.skipWaiting();
});
