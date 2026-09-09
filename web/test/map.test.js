// Exercises the Map tab, which the other tests skip by stubbing Leaflet away.
// A fake L records what the page asks it to draw.
const { JSDOM, VirtualConsole } = require('jsdom');
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8')
  .replace(/<script src="https:\/\/unpkg[^<]*<\/script>/g, '')
  .replace(/<link rel="stylesheet" href="https:\/\/unpkg[^>]*>/g, '');

const CATALOGUE = { sites: [
  { name: 'Ojamon kaivoslampi', lat: 60.239814, lon: 24.034529, country: 'Finland' },
  { name: 'Vetokannas',         lat: 59.989997, lon: 24.417312, country: 'Finland' },
] };

function makeLeaflet(record) {
  const layer = () => ({
    addTo() { return this; }, clearLayers() { record.cleared++; }, _l: true,
  });
  const L = {
    map: () => ({
      setView() { return this; },
      fitBounds(pts) { record.fitBounds = pts; },
      invalidateSize() { record.invalidated++; },
    }),
    tileLayer: () => ({ addTo() { return this; } }),
    layerGroup: layer,
    marker(latlng) {
      record.markers.push(latlng);
      return { addTo() { return this; }, bindPopup(html) { record.popups.push(html); return this; } };
    },
  };
  // map() returns an object literal whose methods use `this`; bind it properly.
  L.map = () => {
    const m = {
      setView() { return m; },
      fitBounds(pts) { record.fitBounds = pts; return m; },
      invalidateSize() { record.invalidated++; return m; },
    };
    return m;
  };
  return L;
}

async function run(dives, { withLeaflet = true } = {}) {
  const record = { markers: [], popups: [], cleared: 0, invalidated: 0, fitBounds: null };
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
    record, errors,
    note: window.document.getElementById('map-note').textContent,
    mapHidden: window.document.getElementById('tab-map').classList.contains('hidden'),
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

  // 1. A dive carrying its own coordinates.
  let r = await run([dive({ site_name: 'Boat wreck', site_lat: 60.1, site_lon: 24.9 })]);
  ok('no runtime errors drawing the map', r.errors.length === 0, r.errors.join(' | '));
  ok('marker placed from the row coordinates', r.record.markers.length === 1,
     JSON.stringify(r.record.markers));
  ok('popup names the site', (r.record.popups[0] || '').includes('Boat wreck'), r.record.popups[0]);
  ok('map fitted to the markers', Array.isArray(r.record.fitBounds));
  ok('invalidateSize called for the hidden container', r.record.invalidated > 0);
  ok('note reports what was shown', /1 site/.test(r.note), r.note);

  // 2. The case that left the real map empty: a name, no coordinates, but the
  //    catalogue knows the site.
  r = await run([dive({ site_name: 'Vetokannas' })]);
  ok('catalogue supplies the missing position', r.record.markers.length === 1,
     JSON.stringify(r.record.markers) + ' note=' + r.note);
  ok('placed at the catalogue coordinates',
     r.record.markers[0] && Math.abs(r.record.markers[0][0] - 59.989997) < 1e-6,
     JSON.stringify(r.record.markers));

  // 3. Case-and-accent-insensitive name matching.
  r = await run([dive({ site_name: '  ojamon KAIVOSLAMPI ' })]);
  ok('name match ignores case and padding', r.record.markers.length === 1,
     JSON.stringify(r.record.markers) + ' note=' + r.note);

  // 4. Names nowhere to be found, and dives with no site: explained, not blank.
  r = await run([dive({ site_name: 'Some private quarry' }), dive({})]);
  ok('unplaceable dives draw no markers', r.record.markers.length === 0);
  ok('note explains the unmatched name', /not in the catalogue/.test(r.note), r.note);
  ok('note counts dives with no site', /1 dive\(s\) have no site at all/.test(r.note), r.note);

  // 5. Several dives at one site collapse to a single marker.
  r = await run([
    dive({ site_name: 'Vetokannas', date: '2025-05-01' }),
    dive({ site_name: 'Vetokannas', date: '2025-06-01', maxdepth: 30 }),
  ]);
  ok('one marker per site, not per dive', r.record.markers.length === 1,
     JSON.stringify(r.record.markers));
  ok('popup counts the dives', (r.record.popups[0] || '').includes('2 dives'), r.record.popups[0]);
  ok('popup reports the deepest', (r.record.popups[0] || '').includes('30.0 m'), r.record.popups[0]);

  // 6. No dives at all.
  r = await run([]);
  ok('empty log says so', /No dives yet/.test(r.note), r.note);
  ok('no errors with an empty log', r.errors.length === 0, r.errors.join(' | '));

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
