// Smoke-tests the DEPLOYED site, not the working copy: fetches the live page
// and its live data files, runs them in jsdom, points the map at the Gulf of
// Finland and reports what it actually draws.
//
// The other suites prove the code is right; this proves what is on the server
// is right, which is a different question and the one worth asking when the
// page looks wrong in a browser.
//
//     node web/test/live.check.js
//
// Exits non-zero if the default view draws no dive sites, or draws any clubs.
const { JSDOM, VirtualConsole } = require('jsdom');
const https = require('https');

const BASE = 'https://kallenordling.github.io/Divelogs/web/';

const get = (url) => new Promise((resolve, reject) => {
  https.get(url, (res) => {
    if (res.statusCode !== 200) return reject(new Error(url + ' -> ' + res.statusCode));
    let b = ''; res.setEncoding('utf8');
    res.on('data', (c) => b += c);
    res.on('end', () => resolve(b));
  }).on('error', reject);
});

(async () => {
  const [html, osmRaw, finRaw] = await Promise.all([
    get(BASE), get(BASE + 'dive_sites.json'), get(BASE + 'finnish_sites.json'),
  ]);
  console.log(`fetched live page (${html.length}B), ` +
              `dive_sites.json (${osmRaw.length}B), finnish_sites.json (${finRaw.length}B)`);

  const drawn = { catalogue: [], mine: [] };
  const errors = [];
  const handlers = {};
  let mapObj = null;

  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));

  const stripped = html
    .replace(/<script src="https:\/\/unpkg[^<]*<\/script>/g, '')
    .replace(/<link rel="stylesheet" href="https:\/\/unpkg[^>]*>/g, '');

  const dom = new JSDOM(stripped, {
    runScripts: 'dangerously', pretendToBeVisual: true, url: BASE, virtualConsole: vc,
    beforeParse(win) {
      let layers = 0;
      win.L = {
        tileLayer: () => ({ addTo() { return this; } }),
        layerGroup: () => {
          const isCat = ++layers === 1;
          const l = { addTo: () => l, clearLayers() { (isCat ? drawn.catalogue : drawn.mine).length = 0; }, _cat: isCat };
          return l;
        },
        map: () => {
          mapObj = {
            _b: { s: -90, w: -180, n: 90, e: 180 },
            setView: () => mapObj, fitBounds: () => mapObj, invalidateSize: () => mapObj,
            getBounds: () => ({ contains: ([lat, lon]) =>
              lat >= mapObj._b.s && lat <= mapObj._b.n && lon >= mapObj._b.w && lon <= mapObj._b.e }),
            on: (ev, fn) => { handlers[ev] = fn; return mapObj; },
          };
          return mapObj;
        },
        circleMarker: (latlng, opts) => ({
          addTo(layer) { (layer && layer._cat ? drawn.catalogue : drawn.mine).push({ latlng, opts, popup: null }); return this; },
          bindPopup(h) {
            const arr = drawn.catalogue.length ? drawn.catalogue : drawn.mine;
            arr[arr.length - 1].popup = h; return this;
          },
        }),
      };
      win.onerror = (m) => errors.push(String(m));
      win.fetch = async (url) => {
        const u = String(url);
        if (u.includes('grant_type=password')) {
          return { ok: true, status: 200, json: async () => ({
            access_token: 'AT', refresh_token: 'RT', user: { email: 'd@e.com', id: 'u' } }) };
        }
        if (u.includes('finnish_sites.json')) return { ok: true, status: 200, json: async () => JSON.parse(finRaw) };
        if (u.includes('dive_sites.json'))    return { ok: true, status: 200, json: async () => JSON.parse(osmRaw) };
        if (u.includes('/rest/v1/dives'))     return { ok: true, status: 200, json: async () => [] };
        return { ok: false, status: 404, json: async () => ({}) };
      };
    },
  });

  const { window } = dom;
  const tick = (n = 6) => new Promise((r) => setTimeout(r, 40 * n));
  await tick();
  window.document.getElementById('email').value = 'd@e.com';
  window.document.getElementById('password').value = 'x';
  window.document.getElementById('btn-signin').click();
  await tick();
  window.document.querySelector('nav button[data-tab="map"]').click();
  await tick(10);

  // Point the map at the Gulf of Finland and redraw, as panning there would.
  mapObj._b = { s: 59.7, w: 23.5, n: 60.6, e: 26.0 };
  handlers.moveend();

  const kinds = {};
  for (const m of drawn.catalogue) {
    const p = m.popup || '';
    const kind = /Dive club/.test(p) ? 'club'
               : /Accommodation/.test(p) ? 'lodging'
               : /Boat site|Sea shore site|Quarry|Mine|Lake site/.test(p) ? 'finnish site'
               : 'osm site';
    kinds[kind] = (kinds[kind] || 0) + 1;
  }

  console.log('\n=== Gulf of Finland viewport, default toggles ===');
  console.log('markers drawn:', drawn.catalogue.length);
  for (const [k, v] of Object.entries(kinds)) console.log(`   ${k}: ${v}`);
  console.log('\nfirst 12 names drawn:');
  for (const m of drawn.catalogue.slice(0, 12)) {
    console.log('   ' + (m.popup || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 70));
  }
  console.log('\nnote:', window.document.getElementById('map-note').textContent);
  console.log('count label:', window.document.getElementById('cat-count').textContent);
  console.log('runtime errors:', errors.length ? errors.join(' | ') : 'none');

  const clubs = kinds.club || 0;
  const sites = (kinds['finnish site'] || 0) + (kinds['osm site'] || 0);
  console.log(`\nVERDICT: ${sites} dive sites, ${clubs} clubs drawn by default`);
  process.exit(sites > 0 && clubs === 0 ? 0 : 1);
})().catch((e) => { console.error('FAILED:', e.message); process.exit(2); });
