/* Verifies the installable-app behaviour in real Chrome: the manifest parses
 * as Chrome itself parses it, the service worker installs and precaches, and
 * the app still opens with the network switched off.
 *
 * Needs Chrome; it serves web/ itself, so nothing else has to be running.
 *
 *     node web/test/pwa.check.js
 */
'use strict';

const { spawn } = require('child_process');
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const WEB = path.join(__dirname, '..');
const PORT = 8231;
const CHROME = process.env.CHROME || '/usr/bin/google-chrome';
const TYPES = { '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
                '.json': 'application/json', '.webmanifest': 'application/manifest+json',
                '.png': 'image/png', '.svg': 'image/svg+xml' };

const server = http.createServer((req, res) => {
  let p = new URL(req.url, 'http://x').pathname;
  if (p === '/') p = '/index.html';
  const file = path.join(WEB, p);
  if (!file.startsWith(WEB) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    res.writeHead(404); return res.end('not found');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'text/plain' });
  fs.createReadStream(file).pipe(res);
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const devtools = (p) => new Promise((res, rej) => http.get(
  { host: '127.0.0.1', port: 9351, path: p }, (r) => {
    let b = ''; r.on('data', (c) => b += c); r.on('end', () => res(JSON.parse(b)));
  }).on('error', rej));

(async () => {
  await new Promise((r) => server.listen(PORT, r));

  const chrome = spawn(CHROME, ['--headless=new', '--remote-debugging-port=9351',
    '--no-sandbox', '--disable-gpu', '--window-size=390,844', 'about:blank'], { stdio: 'ignore' });

  const done = (code) => { try { chrome.kill(); } catch {} server.close(); process.exit(code); };

  let list;
  for (let i = 0; i < 50; i++) {
    try { list = await devtools('/json/list'); break; } catch { await sleep(300); }
  }
  if (!list) { console.error('Chrome did not start (set CHROME= to override)'); return done(1); }

  const ws = new WebSocket(list.find((t) => t.type === 'page').webSocketDebuggerUrl,
                           { perMessageDeflate: false });
  await new Promise((r) => ws.on('open', r));

  let id = 0; const pending = new Map(); const errs = [];
  ws.on('message', (raw) => {
    const m = JSON.parse(raw);
    if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); }
    if (m.method === 'Runtime.exceptionThrown') {
      errs.push((m.params.exceptionDetails.exception || {}).description || 'exception');
    }
  });
  const send = (method, params = {}) => new Promise((res) => {
    const i = ++id; pending.set(i, res); ws.send(JSON.stringify({ id: i, method, params }));
  });
  const js = async (e) => {
    const r = await send('Runtime.evaluate',
      { expression: e, awaitPromise: true, returnByValue: true });
    return r.result && r.result.result && r.result.result.value;
  };

  await send('Runtime.enable'); await send('Page.enable'); await send('Network.enable');
  await send('Page.navigate', { url: `http://localhost:${PORT}/` });
  await sleep(2600);

  let failed = 0;
  const ok = (n, v, extra = '') => {
    if (!v) failed++;
    console.log(`${v ? 'ok  ' : 'FAIL'}  ${n}${v ? '' : '   << ' + extra}`);
  };

  const man = (await send('Page.getAppManifest')).result || {};
  ok('manifest parses with no errors', !!man.url && !(man.errors || []).length,
     JSON.stringify(man.errors || []));
  const parsed = JSON.parse(man.data || '{}');
  ok('installable display mode', parsed.display === 'standalone', parsed.display);
  ok('theme colour matches the app', parsed.theme_color === '#07141F', parsed.theme_color);
  ok('512px icon present', (parsed.icons || []).some((i) => i.sizes === '512x512'));
  ok('maskable icon present', (parsed.icons || []).some((i) => i.purpose === 'maskable'));

  const reg = await js(`(async () => {
    const r = await navigator.serviceWorker.getRegistration();
    return r ? (r.active ? 'active' : 'pending') : 'none';
  })()`);
  ok('service worker active', reg === 'active', String(reg));

  await sleep(1600);
  const c = JSON.parse(await js(`(async () => {
    const names = await caches.keys();
    let n = 0;
    for (const k of names) n += (await (await caches.open(k)).keys()).length;
    return JSON.stringify({ names, n });
  })()`) || '{}');
  ok('shell and data precached', c.n >= 14, JSON.stringify(c));

  ok('site catalogue cached', await js(`(async () => {
    for (const k of await caches.keys()) {
      if (await (await caches.open(k)).match('./finnish_sites.json')) return true;
    }
    return false;
  })()`) === true);

  // Cut the network entirely and reload.
  await send('Network.emulateNetworkConditions',
    { offline: true, latency: 0, downloadThroughput: 0, uploadThroughput: 0 });
  await send('Page.reload');
  await sleep(3000);

  ok('app opens offline',
     /DeepLog/.test(await js(`(document.querySelector('.brand')||{}).textContent || ''`)));
  ok('catalogue readable offline',
     (await js(`(async () => (await (await fetch('finnish_sites.json')).json()).sites.length)()`)) > 300);

  console.log('exceptions:', errs.length ? errs.join(' | ') : 'none');
  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  done(failed ? 1 : 0);
})().catch((e) => { console.error('FAILED', e.message); process.exit(1); });
