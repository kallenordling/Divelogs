// Covers assigning a dive site from the web app: the picker, the PATCH it
// sends, bulk selection, and what happens when RLS refuses the write.
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
] };

const baseDive = (id, extra = {}) => Object.assign({
  id, date: `2025-0${id}-01`, time: '10:00:00', site_name: null,
  device_name: 'Teric', maxdepth: 20, avgdepth: 10, duration: 1800, divemode: 'OC',
  site_lat: null, site_lon: null, gasmixes: [], tanks: [],
  samples: [[0, 0, 10], [60000, 20, 8]],
}, extra);

async function boot({ dives, patchStatus = 204 }) {
  const patches = [];
  const errors = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));

  const dom = new JSDOM(html, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/web/', virtualConsole: vc,
    beforeParse(win) {
      win.L = undefined;
      win.alert = () => {};
      win.fetch = async (url, opts = {}) => {
        const u = String(url);
        if (u.includes('grant_type=password')) {
          return { ok: true, status: 200, json: async () => ({
            access_token: 'AT1', refresh_token: 'RT1', user: { email: 'd@e.com', id: 'u1' } }) };
        }
        if (u.includes('dive_sites.json')) {
          return { ok: true, status: 200, json: async () => CATALOGUE };
        }
        if (u.includes('/rest/v1/dives') && (opts.method || 'GET') === 'PATCH') {
          patches.push({ url: u, body: JSON.parse(opts.body), headers: opts.headers });
          if (patchStatus >= 300) {
            return { ok: false, status: patchStatus,
                     json: async () => ({ message: 'permission denied for table dives' }) };
          }
          return { ok: true, status: 204, json: async () => null };
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
  return { window, patches, errors, tick };
}

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  // ── 1. Bulk: select every dive without a site, then set one ───────────────
  {
    const { window, patches, errors, tick } = await boot({
      dives: [baseDive(1), baseDive(2), baseDive(3, { site_name: 'Already set' })],
    });
    const $ = (s) => window.document.querySelector(s);

    $('#btn-select').click(); await tick();
    ok('selection bar appears', !!$('.selbar'));
    ok('apply disabled with nothing selected', $('#sel-apply').disabled);

    $('#sel-none').click(); await tick();
    ok('"without a site" picks only the siteless dives',
       window.document.querySelectorAll('.dive-card.picked').length === 2,
       String(window.document.querySelectorAll('.dive-card.picked').length));
    ok('apply enabled once dives are selected', !$('#sel-apply').disabled);

    $('#sel-apply').click(); await tick();
    ok('picker opens', !!$('#site-modal'));
    ok('picker says how many dives it will change',
       $('.modal-box h3').textContent.includes('2 dives'), $('.modal-box h3').textContent);

    $('#pick-q').value = 'vetok'; $('#pick-q').dispatchEvent(new window.Event('input'));
    await tick();
    const rows = window.document.querySelectorAll('.pick-row');
    ok('search narrows the catalogue', rows.length === 1, 'rows=' + rows.length);

    rows[0].click(); await tick();
    ok('modal closes after choosing', !$('#site-modal'));
    ok('one PATCH per selected dive', patches.length === 2, 'patches=' + patches.length);
    ok('PATCH targets the dive by id',
       patches.every((p) => /dives\?id=eq\.[12]$/.test(p.url)), patches.map((p) => p.url).join(' '));
    ok('PATCH writes name and coordinates',
       patches[0].body.site_name === 'Vetokannas' &&
       Math.abs(patches[0].body.site_lat - 59.989997) < 1e-6 &&
       Math.abs(patches[0].body.site_lon - 24.417312) < 1e-6,
       JSON.stringify(patches[0].body));
    ok('PATCH does not touch the dive already having a site',
       !patches.some((p) => p.url.endsWith('id=eq.3')));
    ok('confirmation shown', /Set .Vetokannas./.test($('#selbar-host').textContent),
       $('#selbar-host').textContent.trim());
    ok('list now shows the new site',
       window.document.querySelector('.dive-card').textContent.includes('Vetokannas'));
    ok('no runtime errors', errors.length === 0, errors.join(' | '));

    // The dive is now placeable, so the Sites tab picks it up.
    window.document.querySelector('nav button[data-tab="sites"]').click();
    await tick();
    ok('assigned dives appear under Sites',
       window.document.querySelector('#site-list').textContent.includes('Vetokannas'),
       window.document.querySelector('#site-list').textContent.slice(0, 120));
  }

  // ── 2. A single dive, from its detail view ────────────────────────────────
  {
    const { window, patches, tick } = await boot({ dives: [baseDive(7)] });
    const $ = (s) => window.document.querySelector(s);

    window.document.querySelector('.dive-card').click(); await tick();
    ok('detail offers to set a site', !!$('#btn-set-site'));
    $('#btn-set-site').click(); await tick();
    ok('picker opens for one dive',
       $('.modal-box h3').textContent.includes('1 dive'), $('.modal-box h3').textContent);

    window.document.querySelectorAll('.pick-row')[0].click(); await tick();
    ok('single dive patched once', patches.length === 1, 'patches=' + patches.length);
    ok('detail re-rendered with the site',
       $('#detail-body').textContent.includes('Ojamon kaivoslampi'));
  }

  // ── 3. A site that is not in the catalogue ────────────────────────────────
  {
    const { window, patches, tick } = await boot({ dives: [baseDive(9)] });
    const $ = (s) => window.document.querySelector(s);
    $('#btn-select').click(); await tick();
    $('#sel-none').click(); await tick();
    $('#sel-apply').click(); await tick();

    $('#pick-name').value = 'Kalle’s secret quarry';
    $('#pick-lat').value = '61.5';
    $('#pick-lon').value = '25.5';
    $('#pick-custom').click(); await tick();

    ok('custom site is patched', patches.length === 1, 'patches=' + patches.length);
    ok('custom name and coordinates sent',
       patches[0].body.site_name === 'Kalle’s secret quarry' &&
       patches[0].body.site_lat === 61.5 && patches[0].body.site_lon === 25.5,
       JSON.stringify(patches[0].body));
  }

  // ── 4. A custom site with no coordinates ──────────────────────────────────
  {
    const { window, patches, tick } = await boot({ dives: [baseDive(11)] });
    const $ = (s) => window.document.querySelector(s);
    $('#btn-select').click(); await tick();
    $('#sel-none').click(); await tick();
    $('#sel-apply').click(); await tick();
    $('#pick-name').value = 'Unknown shore';
    $('#pick-custom').click(); await tick();

    ok('coordinates sent as null when omitted',
       patches[0].body.site_lat === null && patches[0].body.site_lon === null,
       JSON.stringify(patches[0].body));
    ok('warned that it will not appear on the map',
       /will not appear on the map/.test($('#selbar-host').textContent),
       $('#selbar-host').textContent.trim());
  }

  // ── 5. RLS refuses the write ──────────────────────────────────────────────
  {
    const { window, tick } = await boot({ dives: [baseDive(13)], patchStatus: 403 });
    const $ = (s) => window.document.querySelector(s);
    $('#btn-select').click(); await tick();
    $('#sel-none').click(); await tick();
    $('#sel-apply').click(); await tick();
    window.document.querySelectorAll('.pick-row')[0].click(); await tick();

    const text = window.document.getElementById('tab-dives').textContent;
    ok('a refused write is explained, not swallowed',
       /update policy/.test(text), text.slice(0, 200));
  }

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
