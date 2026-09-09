// The map: catalogue markers, your own dives, layer chips, the site sheet and
// search. Leaflet is replaced by a fake that records what it is asked to draw.
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const HTML = buildHtml();

const OSM = { sites: [
  { name: 'Vetokannas', lat: 59.989997, lon: 24.417312, country: 'Finland', desc: 'Dive Site' },
  { name: 'Blue Hole', lat: 27.8497, lon: 34.5339, country: 'Egypt', desc: 'Dive Site' },
  { name: 'Ojamon kaivoslampi', lat: 60.239814, lon: 24.034529, country: 'Finland' },
] };
const FIN = { sites: [
  { name: 'Kronprins Gustav Adolf', lat: 60.054217, lon: 24.932098, country: 'Finland',
    desc: 'Boat site', kind: 'site', notes: 'Swedish ship of the line, sank 1788.' },
  { name: 'Ojamon kaivos', lat: 60.240429, lon: 24.031069, country: 'Finland',
    desc: 'Mine', kind: 'site' },
  { name: 'Kemin Urheilusukeltajat ry', lat: 65.7358, lon: 24.5657, country: 'Finland',
    desc: 'Dive club', kind: 'club' },
] };

const dive = (o = {}) => Object.assign({
  id: 1, date: '2025-05-01', time: '10:00:00', site_name: null,
  maxdepth: 20, avgdepth: 10, duration: 1800, divemode: 'OC',
  site_lat: null, site_lon: null, gasmixes: [], tanks: [],
  samples: [[0, 0, 8], [60000, 20, 6]],
}, o);

function fakeLeaflet(rec) {
  const L = {
    tileLayer: () => ({ addTo() { return this; } }),
    layerGroup: () => {
      const isCat = ++rec.layers === 1;
      const l = {
        addTo: () => l, _cat: isCat,
        clearLayers() { (isCat ? rec.catalogue : rec.mine).length = 0; },
      };
      return l;
    },
    circleMarker: (latlng, opts) => {
      let store = null;
      const m = {
        addTo(layer) {
          store = layer && layer._cat ? rec.catalogue : rec.mine;
          store.push({ latlng, opts, on: null });
          return m;
        },
        on(ev, fn) { if (store) store[store.length - 1].on = fn; return m; },
      };
      return m;
    },
  };
  L.control = { zoom: () => ({ addTo() { return this; } }) };
  L.map = () => {
    const m = {
      _b: { s: -90, w: -180, n: 90, e: 180 },
      setView(c, z) { rec.setView = { c, z }; return m; },
      fitBounds(p) { rec.fitBounds = p; return m; },
      invalidateSize() { rec.invalidated++; return m; },
      getBounds: () => ({ contains: ([lat, lon]) =>
        lat >= m._b.s && lat <= m._b.n && lon >= m._b.w && lon <= m._b.e }),
      on(ev, fn) { rec.handlers[ev] = fn; return m; },
    };
    rec.map = m;
    return m;
  };
  return L;
}

function boot(dives) {
  const rec = { catalogue: [], mine: [], layers: 0, invalidated: 0,
                fitBounds: null, setView: null, handlers: {}, map: null };
  const errors = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));

  const dom = new JSDOM(HTML, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/web/', virtualConsole: vc,
    beforeParse(win) {
      win.L = fakeLeaflet(rec);
      win.scrollTo = () => {};
      win.localStorage.setItem('deeplog.session', JSON.stringify({
        access_token: 'AT1', refresh_token: 'RT1', email: 'd@e.com', user_id: 'u1' }));
      win.fetch = async (url) => {
        const u = String(url);
        if (u.includes('finnish_sites.json')) return { ok: true, status: 200, json: async () => FIN };
        if (u.includes('dive_sites.json')) return { ok: true, status: 200, json: async () => OSM };
        if (u.includes('/rest/v1/dives')) return { ok: true, status: 200, json: async () => dives };
        return { ok: false, status: 404, json: async () => ({}) };
      };
    },
  });
  const tick = (n = 5) => new Promise((r) => setTimeout(r, 30 * n));
  return { window: dom.window, rec, errors, tick };
}

async function openMap(b) {
  await b.tick(6);
  b.window.document.querySelector('[data-nav="map"]').click();
  await b.tick(5);
  return b;
}

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  // ── The catalogue is the point of the map ────────────────────────────────
  {
    const b = await openMap(boot([]));
    const { rec, errors, window } = b;

    ok('no runtime errors drawing the map', errors.length === 0, errors.join(' | '));
    // 3 OSM − 1 duplicate (Ojamon) = 2, plus the 2 Finnish dive sites = 4.
    ok('catalogue drawn with no dives at all', rec.catalogue.length === 4,
       'drew ' + rec.catalogue.length);
    ok('Finnish wreck is on the map',
       rec.catalogue.some((m) => Math.abs(m.latlng[0] - 60.054217) < 1e-5), 'wreck missing');
    ok('a worldwide site is on the map too',
       rec.catalogue.some((m) => Math.abs(m.latlng[0] - 27.8497) < 1e-4), 'Blue Hole missing');
    ok('duplicate across sources drawn once',
       rec.catalogue.filter((m) => Math.abs(m.latlng[1] - 24.03) < 0.02).length === 1,
       'Ojamon drawn twice');
    ok('map invalidates size for the hidden container', rec.invalidated > 0);

    const note = () => window.document.getElementById('map-note').textContent;
    ok('note counts what is in view', /4 dive sites in view/.test(note()), note());
    ok('note credits both sources',
       /Google My Maps/.test(note()) && /OpenStreetMap/.test(note()), note());
    ok('note says none of your dives are placed', /None of your own dives/.test(note()), note());

    const doc = window.document;
    doc.querySelector('[data-layer="services"]').click();
    await b.tick(2);
    ok('clubs appear when the chip is on', rec.catalogue.length === 5, String(rec.catalogue.length));
    ok('clubs are styled apart from dive sites',
       rec.catalogue.some((m) => m.opts.fillColor === '#4E6C86'),
       JSON.stringify(rec.catalogue.map((m) => m.opts.fillColor)));
    ok('note separates sites from clubs', /dive sites?, 1 club/.test(note()), note());

    doc.querySelector('[data-layer="sites"]').click();
    await b.tick(2);
    ok('turning dive sites off leaves only clubs', rec.catalogue.length === 1,
       String(rec.catalogue.length));
    doc.querySelector('[data-layer="sites"]').click();
    doc.querySelector('[data-layer="services"]').click();
    await b.tick(2);

    rec.map._b = { s: 59.5, w: 23.5, n: 60.6, e: 25.5 };
    rec.handlers.moveend();
    ok('panning redraws for the viewport', rec.catalogue.length === 3,
       'drew ' + rec.catalogue.length);
  }

  // ── Your dives sit on top, distinctly ────────────────────────────────────
  {
    const b = await openMap(boot([
      dive({ site_name: 'Vetokannas' }),
      dive({ id: 2, site_name: 'Vetokannas', date: '2025-06-01', maxdepth: 30 }),
    ]));
    const { rec, window } = b;

    ok('one marker per site, not per dive', rec.mine.length === 1,
       JSON.stringify(rec.mine.map((m) => m.latlng)));
    ok('your dives use the accent colour', rec.mine[0].opts.fillColor === '#39C6E8',
       JSON.stringify(rec.mine[0].opts));
    ok('your markers are stronger than catalogue ones',
       rec.mine[0].opts.radius > rec.catalogue[0].opts.radius,
       `${rec.mine[0].opts.radius} vs ${rec.catalogue[0].opts.radius}`);
    ok('placed from the catalogue by name',
       Math.abs(rec.mine[0].latlng[0] - 59.989997) < 1e-6, JSON.stringify(rec.mine[0].latlng));
    ok('map opens on your own diving', Array.isArray(rec.fitBounds));
    ok('note reports your sites',
       /dived 1 of them across 2 dives/i.test(window.document.getElementById('map-note').textContent),
       window.document.getElementById('map-note').textContent);

    rec.mine[0].on();
    await b.tick(2);
    const sheet = window.document.querySelector('.sheet');
    ok('clicking a marker opens a sheet', !!sheet, 'no sheet');
    ok('sheet names the site', /Vetokannas/.test(sheet.textContent));
    ok('sheet counts your dives', /Your dives/.test(sheet.textContent),
       sheet.textContent.trim().slice(0, 120));
    ok('sheet reports the deepest', /30/.test(sheet.textContent));

    sheet.querySelector('[data-act="view"]').click();
    await b.tick(3);
    ok('"View site" opens the site view',
       !window.document.getElementById('view-site').classList.contains('hidden'));
    ok('the site view carries the temperature profile',
       /chart/.test(window.document.getElementById('view-site').innerHTML));
  }

  // ── An undived catalogue site offers to log one ──────────────────────────
  {
    const b = await openMap(boot([]));
    const { rec, window } = b;
    const wreck = rec.catalogue.find((m) => Math.abs(m.latlng[0] - 60.054217) < 1e-5);
    wreck.on();
    await b.tick(2);
    const sheet = window.document.querySelector('.sheet');
    ok('undived site sheet says so', /Not dived yet/.test(sheet.textContent),
       sheet.textContent.trim().slice(0, 120));
    ok('undived site offers to log a dive', !!sheet.querySelector('[data-act="log"]'));

    sheet.querySelector('[data-act="log"]').click();
    await b.tick(3);
    const form = window.document.querySelector('#dive-form');
    ok('logging from the map opens the form', !!form, 'no form');
    ok('the form is pre-filled with that site',
       form && form.querySelector('#f-site').value === 'Kronprins Gustav Adolf',
       form ? form.querySelector('#f-site').value : 'no form');
  }

  // ── Search ───────────────────────────────────────────────────────────────
  {
    const b = await openMap(boot([]));
    const { rec, window } = b;
    const s = window.document.getElementById('map-search');

    s.value = 'kronprins';
    s.dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Enter' }));
    await b.tick(2);
    ok('search jumps to a single hit',
       rec.setView && Math.abs(rec.setView.c[0] - 60.054217) < 1e-5, JSON.stringify(rec.setView));

    s.value = 'finland';
    s.dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Enter' }));
    await b.tick(2);
    ok('searching a country fits all its sites',
       Array.isArray(rec.fitBounds) && rec.fitBounds.length === 4,
       JSON.stringify(rec.fitBounds));

    s.value = 'nowhere at all';
    s.dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Enter' }));
    await b.tick(2);
    ok('a search with no hits says so',
       /Nothing in the catalogue matches/.test(
         window.document.getElementById('toast-host').textContent),
       window.document.getElementById('toast-host').textContent);
  }

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
