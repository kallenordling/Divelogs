// Exercises the Map tab, which the other tests skip by stubbing Leaflet away.
// A fake L records what the page asks it to draw.
const { JSDOM, VirtualConsole } = require('jsdom');
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8')
  .replace(/<script src="https:\/\/unpkg[^<]*<\/script>/g, '')
  .replace(/<link rel="stylesheet" href="https:\/\/unpkg[^>]*>/g, '');

const CATALOGUE = { sites: [
  { name: 'Ojamon kaivoslampi', lat: 60.239814, lon: 24.034529, country: 'Finland', desc: 'Dive site' },
  { name: 'Vetokannas',         lat: 59.989997, lon: 24.417312, country: 'Finland', desc: 'Dive site' },
  { name: 'Kuru',               lat: 62.120610, lon: 23.917669, country: 'Finland', desc: 'Wreck' },
  { name: 'Blue Hole',          lat: 27.849700, lon: 34.533900, country: 'Egypt',   desc: 'Dive site' },
] };

function makeLeaflet(record) {
  const layer = () => {
    const l = {
      addTo() { return l; },
      clearLayers() { record.cleared++; },
    };
    return l;
  };
  const marked = (store) => (latlng, opts) => {
    store.push({ latlng, opts });
    return {
      addTo() { return this; },
      bindPopup(html) { record.popups.push(html); return this; },
    };
  };

  const L = {
    tileLayer: () => ({ addTo() { return this; } }),
    layerGroup: () => {
      // Two layers are created; tell them apart by creation order.
      record.layers++;
      const isCatalogue = record.layers === 1;
      const l = {
        addTo() { return l; },
        clearLayers() { (isCatalogue ? record.catalogue : record.mine).length = 0; },
        _catalogue: isCatalogue,
      };
      return l;
    },
  };

  L.map = () => {
    const m = {
      _bounds: { south: -90, west: -180, north: 90, east: 180 },
      setView(c, z) { record.setView = { c, z }; return m; },
      fitBounds(pts) { record.fitBounds = pts; return m; },
      invalidateSize() { record.invalidated++; return m; },
      getBounds() {
        const b = m._bounds;
        return { contains: ([lat, lon]) =>
          lat >= b.south && lat <= b.north && lon >= b.west && lon <= b.east };
      },
      on(ev, fn) { record.handlers[ev] = fn; return m; },
    };
    record.map = m;
    return m;
  };

  // circleMarker is routed to whichever layer it is added to.
  L.circleMarker = (latlng, opts) => ({
    addTo(layer) {
      (layer && layer._catalogue ? record.catalogue : record.mine).push({ latlng, opts });
      return this;
    },
    bindPopup(html) { record.popups.push(html); return this; },
  });
  L.marker = marked(record.mine);
  return L;
}

async function run(dives, { withLeaflet = true } = {}) {
  const record = { catalogue: [], mine: [], popups: [], layers: 0, cleared: 0,
                   invalidated: 0, fitBounds: null, setView: null, handlers: {}, map: null };
  const errors = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));

  const dom = new JSDOM(html, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/web/', virtualConsole: vc,
    beforeParse(win) {
      if (withLeaflet) win.L = makeLeaflet(record);
      win.onerror = (m) => errors.push(String(m));
      win.addEventListener('unhandledrejection', (e) =>
        errors.push('unhandled rejection: ' + (e.reason && e.reason.message)));
      win.fetch = async (url, opts = {}) => {
        const u = String(url);
        if (u.includes('grant_type=password')) {
          return { ok: true, status: 200, json: async () => ({
            access_token: 'AT1', refresh_token: 'RT1',
            user: { email: 'd@e.com', id: 'u1' } }) };
        }
        if (u.includes('dive_sites.json')) {
          return { ok: true, status: 200, json: async () => CATALOGUE };
        }
        if (u.includes('/rest/v1/dives')) {
          return { ok: true, status: 200, json: async () => dives };
        }
        return { ok: false, status: 404, json: async () => ({}) };
      };
    },
  });

  const { window } = dom;
  const tick = (n = 4) => new Promise((r) => setTimeout(r, 30 * n));
  await tick();

  window.document.getElementById('email').value = 'd@e.com';
  window.document.getElementById('password').value = 'x';
  window.document.getElementById('btn-signin').click();
  await tick();

  window.document.querySelector('nav button[data-tab="map"]').click();
  await tick();

  return {
    record, errors, window, tick,
    note: window.document.getElementById('map-note').textContent,
    catCount: window.document.getElementById('cat-count').textContent,
  };
}

const dive = (o) => Object.assign({
  date: '2025-05-01', time: '10:00:00', site_name: null, device_name: 'Teric',
  maxdepth: 20, avgdepth: 10, duration: 1800, divemode: 'OC',
  site_lat: null, site_lon: null, gasmixes: [], tanks: [],
  samples: [[0, 0, 10], [60000, 20, 8]],
}, o);

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  // ── The catalogue is the point of the map ────────────────────────────────
  let r = await run([]);
  ok('no runtime errors drawing the map', r.errors.length === 0, r.errors.join(' | '));
  ok('catalogue sites drawn with no dives logged at all',
     r.record.catalogue.length === CATALOGUE.sites.length,
     'drew ' + r.record.catalogue.length);
  ok('Finnish sites are on the map',
     r.record.popups.some((p) => p.includes('Ojamon kaivoslampi')) &&
     r.record.popups.some((p) => p.includes('Vetokannas')),
     r.record.popups.join(' | ').slice(0, 160));
  ok('a global site is on the map too',
     r.record.popups.some((p) => p.includes('Blue Hole')), 'no non-Finnish site drawn');
  ok('popup carries country and kind',
     r.record.popups.some((p) => p.includes('Finland') && p.includes('Wreck')),
     r.record.popups.find((p) => p.includes('Kuru')) || 'none');
  ok('note counts the catalogue', /4 known dive sites/.test(r.note), r.note);
  ok('note credits OpenStreetMap', /OpenStreetMap/.test(r.note) && /ODbL/.test(r.note), r.note);
  ok('note says no dives of your own are placed',
     /None of your own dives/.test(r.note), r.note);
  ok('catalogue count reported', /in view/.test(r.catCount), r.catCount);
  ok('invalidateSize called for the hidden container', r.record.invalidated > 0);

  // ── Your own dives sit on top, styled differently ────────────────────────
  r = await run([dive({ site_name: 'Vetokannas' })]);
  ok('catalogue still drawn alongside your dives', r.record.catalogue.length === 4,
     String(r.record.catalogue.length));
  ok('your dive drawn as its own marker', r.record.mine.length === 1,
     JSON.stringify(r.record.mine.map((m) => m.latlng)));
  ok('your dives use the accent colour',
     r.record.mine[0].opts.fillColor === '#4fc3f7', JSON.stringify(r.record.mine[0].opts));
  ok('catalogue sites are visually distinct',
     r.record.catalogue[0].opts.fillColor !== r.record.mine[0].opts.fillColor);
  ok('your dive placed from the catalogue by name',
     Math.abs(r.record.mine[0].latlng[0] - 59.989997) < 1e-6,
     JSON.stringify(r.record.mine[0].latlng));
  ok('note reports your own sites', /Your dives: 1 site/.test(r.note), r.note);
  ok('map opens on your dives', Array.isArray(r.record.fitBounds));

  // ── Several dives at one site collapse to one marker ─────────────────────
  r = await run([
    dive({ site_name: 'Vetokannas', date: '2025-05-01' }),
    dive({ site_name: 'Vetokannas', date: '2025-06-01', maxdepth: 30 }),
  ]);
  ok('one marker per site, not per dive', r.record.mine.length === 1,
     String(r.record.mine.length));
  ok('popup counts the dives',
     r.record.popups.some((p) => p.includes('2 dives logged')),
     r.record.popups.filter((p) => p.includes('logged')).join(' | '));
  ok('popup reports the deepest',
     r.record.popups.some((p) => p.includes('30.0 m')), 'no depth in popup');

  // ── Dives that cannot be placed are explained ────────────────────────────
  r = await run([dive({ site_name: 'Some private quarry' }), dive({})]);
  ok('unplaceable dives draw no personal markers', r.record.mine.length === 0);
  ok('note explains the unmatched name', /not in the catalogue/.test(r.note), r.note);
  ok('note points at Set sites for dives with none',
     /have no site/.test(r.note) && /Set sites/.test(r.note), r.note);
  ok('catalogue still shown regardless', r.record.catalogue.length === 4);

  // ── Only what is in view is drawn, and panning redraws ───────────────────
  r = await run([]);
  r.record.map._bounds = { south: 59, west: 23, north: 61, east: 25 };  // southern Finland
  r.record.handlers.moveend();
  ok('pan redraws for the new viewport', r.record.catalogue.length === 2,
     'drew ' + r.record.catalogue.length + ' expected the 2 southern Finnish sites');
  ok('out-of-view sites are dropped',
     !r.record.catalogue.some((m) => m.latlng[0] > 61), 'Kuru should be out of view');

  // ── The toggle hides the catalogue ───────────────────────────────────────
  r = await run([]);
  const toggle = r.window.document.getElementById('cat-toggle');
  toggle.checked = false;
  toggle.dispatchEvent(new r.window.Event('change'));
  await r.tick(1);
  ok('toggle hides the catalogue', r.record.catalogue.length === 0,
     String(r.record.catalogue.length));
  ok('toggle state reported',
     r.window.document.getElementById('cat-count').textContent === 'hidden',
     r.window.document.getElementById('cat-count').textContent);

  // ── Search jumps to a site ───────────────────────────────────────────────
  r = await run([]);
  const search = r.window.document.getElementById('map-search');
  search.value = 'kuru';
  search.dispatchEvent(new r.window.KeyboardEvent('keydown', { key: 'Enter' }));
  await r.tick(1);
  ok('search jumps to a single hit',
     r.record.setView && Math.abs(r.record.setView.c[0] - 62.120610) < 1e-5,
     JSON.stringify(r.record.setView));

  search.value = 'finland';
  search.dispatchEvent(new r.window.KeyboardEvent('keydown', { key: 'Enter' }));
  await r.tick(1);
  ok('searching a country fits all its sites',
     Array.isArray(r.record.fitBounds) && r.record.fitBounds.length === 3,
     JSON.stringify(r.record.fitBounds));

  search.value = 'nowhere at all';
  search.dispatchEvent(new r.window.KeyboardEvent('keydown', { key: 'Enter' }));
  await r.tick(1);
  ok('a search with no hits says so',
     /Nothing in the catalogue matches/.test(
       r.window.document.getElementById('map-note').textContent),
     r.window.document.getElementById('map-note').textContent);

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
