// Site discovery: the catalogue list, its filters, and one site's view.
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const HTML = buildHtml();

const OSM = { sites: [
  { name: 'Blue Hole', lat: 27.8497, lon: 34.5339, country: 'Egypt', desc: 'Dive Site' },
  { name: 'Zeelandbrug', lat: 51.6, lon: 3.88, country: 'Nederland', desc: 'Dive Site' },
] };
const FIN = { sites: [
  { name: 'Kronprins Gustav Adolf', lat: 60.054217, lon: 24.932098, country: 'Finland',
    desc: 'Boat site', kind: 'site', notes: 'Swedish ship of the line, sank 1788.',
    source: 'https://example.invalid/kga' },
  { name: 'Ojamon kaivos', lat: 60.240429, lon: 24.031069, country: 'Finland',
    region: 'Uusimaa', desc: 'Mine', kind: 'site' },
  { name: 'Vetokannas', lat: 59.989997, lon: 24.417312, country: 'Finland',
    desc: 'Sea shore site', kind: 'site' },
  { name: 'Kemin Urheilusukeltajat ry', lat: 65.7358, lon: 24.5657, country: 'Finland',
    desc: 'Dive club', kind: 'club' },
] };

const dive = (o = {}) => Object.assign({
  id: 1, date: '2025-05-01', time: '10:00:00', site_name: 'Vetokannas',
  maxdepth: 18, avgdepth: 9, duration: 2400, divemode: 'OC', temp_min: 6, temp_max: 12,
  site_lat: null, site_lon: null, gasmixes: [], tanks: [],
  samples: Array.from({ length: 30 }, (_, i) =>
    [i * 60000, Math.sin(i / 30 * Math.PI) * 18, 6 + i * 0.2]),
}, o);

function boot(dives) {
  const errors = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));
  const dom = new JSDOM(HTML, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/web/', virtualConsole: vc,
    beforeParse(win) {
      win.L = undefined;
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
  return { window: dom.window, errors, tick };
}

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  const { window, errors, tick } = boot([
    dive(),
    dive({ id: 2, date: '2025-06-02', maxdepth: 22, temp_min: 8 }),
    dive({ id: 3, date: '2025-07-03', site_name: 'Ojamon kaivos', maxdepth: 30 }),
  ]);
  await tick(6);
  const doc = window.document;
  const $ = (s) => doc.querySelector(s);

  doc.querySelector('[data-nav="sites"]').click();
  await tick(4);

  ok('no runtime errors on the sites screen', errors.length === 0, errors.join(' | '));
  ok('page invites exploration', /Explore dive sites/.test($('#view-sites').textContent));

  const cards = () => [...doc.querySelectorAll('#view-sites .site-card')];
  ok('catalogue and dived sites both listed', cards().length === 5,
     String(cards().length) + ' cards');
  ok('clubs are not offered as dive sites',
     !$('#view-sites').textContent.includes('Urheilusukeltajat'), 'club listed as a site');

  // Sites you have dived come first, and say so.
  ok('your own sites lead the list', /Vetokannas/.test(cards()[0].textContent),
     cards()[0].textContent.trim().slice(0, 60));
  ok('a dived site is marked with its count',
     /Dived 2 times/.test(cards()[0].textContent), cards()[0].textContent.trim());
  ok('an undived site says so',
     cards().some((c) => /Not dived/.test(c.textContent)), 'no "not dived" chip');
  ok('dived sites show their deepest',
     /18|22/.test(cards()[0].querySelector('.right').textContent),
     cards()[0].querySelector('.right').textContent.trim());
  ok('site type shown as a chip', /Mine|Sea shore|Boat|Wreck/.test($('#view-sites').textContent));

  // Search
  const s = $('#site-search');
  s.value = 'egypt';
  s.dispatchEvent(new window.Event('input'));
  await tick(3);
  ok('search matches on country', cards().length === 1 && /Blue Hole/.test(cards()[0].textContent),
     cards().map((c) => c.querySelector('.name').textContent).join(','));
  ok('search keeps focus', doc.activeElement && doc.activeElement.id === 'site-search',
     doc.activeElement ? doc.activeElement.id : 'none');

  s.value = '';
  s.dispatchEvent(new window.Event('input'));
  await tick(3);

  // Filters
  doc.querySelector('[data-filter="dived"]').click();
  await tick(3);
  ok('"Dived" filter keeps only visited sites', cards().length === 2, String(cards().length));
  doc.querySelector('[data-filter="not"]').click();
  await tick(3);
  ok('"Not dived" filter is the complement', cards().length === 3, String(cards().length));
  doc.querySelector('[data-filter="Mine"]').click();
  await tick(3);
  ok('type filter narrows to that kind', cards().length === 1 && /Ojamon/.test(cards()[0].textContent),
     cards().map((c) => c.querySelector('.name').textContent).join(','));
  doc.querySelector('[data-filter="all"]').click();
  await tick(3);

  // ── One site, dived ──────────────────────────────────────────────────────
  cards().find((c) => /Vetokannas/.test(c.textContent)).click();
  await tick(3);
  ok('opening a site shows the site view', !$('#view-site').classList.contains('hidden'));

  const site = $('#view-site');
  ok('site view names the site', /Vetokannas/.test(site.querySelector('.page-title').textContent));
  ok('site view counts your dives', /Dived 2 times/.test(site.textContent), site.textContent.slice(0, 200));
  ok('site view shows dives, deepest, total time, last dived',
     site.querySelectorAll('.summary .cell').length === 4);
  ok('site view draws the temperature profile',
     !!site.querySelector('.chart svg') && /stroke="rgb\(/.test(site.querySelector('.chart').innerHTML),
     'no coloured profile');
  ok('site view reports coldest and warmest', /Coldest/.test(site.textContent) && /Warmest/.test(site.textContent));
  ok('site view lists the dives there',
     site.querySelectorAll('tr[data-dive]').length === 2,
     String(site.querySelectorAll('tr[data-dive]').length));

  // A dive row opens that dive.
  site.querySelector('tr[data-dive]').click();
  await tick(3);
  ok('a dive row opens the dive', !$('#view-dive').classList.contains('hidden'));

  // ── One site, never dived ────────────────────────────────────────────────
  doc.querySelector('[data-nav="sites"]').click();
  await tick(4);
  cards().find((c) => /Kronprins/.test(c.textContent)).click();
  await tick(3);
  const s2 = $('#view-site');
  ok('an undived site still opens', /Kronprins Gustav Adolf/.test(s2.textContent));
  ok('it says there are no dives yet', /No dives at this site yet/.test(s2.textContent),
     s2.textContent.slice(0, 200));
  ok('it offers to log one', !!s2.querySelector('[data-act="log"]'));
  ok('it shows the catalogue description', /sank 1788/.test(s2.textContent));
  ok('it links to the source', /example\.invalid/.test(s2.innerHTML));
  ok('no runtime errors on an undived site', errors.length === 0, errors.join(' | '));

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
