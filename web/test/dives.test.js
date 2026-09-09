// The dive log home screen, the dive cards and the dive detail view.
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const HTML = buildHtml();

const CATALOGUE = { sites: [
  { name: 'Ojamon kaivos', lat: 60.240429, lon: 24.031069, country: 'Finland',
    region: 'Uusimaa', desc: 'Mine', kind: 'site' },
  { name: 'Vetokannas', lat: 59.989997, lon: 24.417312, country: 'Finland', desc: 'Sea shore site', kind: 'site' },
] };

const dive = (o = {}) => Object.assign({
  id: 1, date: '2025-05-01', time: '10:30:00', site_name: 'Vetokannas',
  device_name: 'Shearwater Teric', maxdepth: 18.4, avgdepth: 9.2, duration: 2880,
  divemode: 'OC', temp_min: 6.5, temp_max: 9, temp_surface: 18,
  site_lat: null, site_lon: null,
  gasmixes: [{ o2: 32, he: 0, n2: 68 }],
  tanks: [{ gasmix: 0, volume: 12, workpressure: 232, start: 210, end: 60 }],
  samples: Array.from({ length: 40 }, (_, i) =>
    [i * 60000, Math.sin(i / 40 * Math.PI) * 18.4, 6.5 + i * 0.06]),
}, o);

function boot(dives, { patches = [], deletes = [] } = {}) {
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
      win.fetch = async (url, opts = {}) => {
        const u = String(url), m = opts.method || 'GET';
        if (u.includes('finnish_sites.json')) return { ok: true, status: 200, json: async () => CATALOGUE };
        if (u.includes('dive_sites.json')) return { ok: true, status: 200, json: async () => ({ sites: [] }) };
        if (u.includes('/rest/v1/dives')) {
          if (m === 'PATCH') { patches.push({ url: u, body: JSON.parse(opts.body) }); return { ok: true, status: 204, json: async () => null }; }
          if (m === 'DELETE') { deletes.push(u); return { ok: true, status: 204, json: async () => null }; }
          // The real query is order=date.desc,time.desc; mirror it, or the
          // ordering assertions below test nothing.
          const sorted = [...dives].sort((a, b) =>
            (b.date + (b.time || '')).localeCompare(a.date + (a.time || '')));
          return { ok: true, status: 200, json: async () => sorted };
        }
        return { ok: false, status: 404, json: async () => ({}) };
      };
    },
  });
  const tick = (n = 5) => new Promise((r) => setTimeout(r, 30 * n));
  return { window: dom.window, errors, tick, patches, deletes };
}

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  // ── Empty state ──────────────────────────────────────────────────────────
  {
    const { window, tick } = boot([]);
    await tick();
    const t = window.document.getElementById('view-dives').textContent;
    ok('empty log is motivating, not blank', /No dives yet/.test(t) && /next adventure/.test(t), t.slice(0, 120));
    ok('empty log offers to log one',
       !!window.document.querySelector('#view-dives [data-act="log"]'));
  }

  // ── Summary and cards ────────────────────────────────────────────────────
  {
    const { window, errors, tick } = boot([
      dive(),
      dive({ id: 2, date: '2025-07-14', site_name: 'Ojamon kaivos', maxdepth: 28.4, duration: 3600 }),
      dive({ id: 3, date: '2024-06-01', site_name: null, maxdepth: 12, duration: 1800,
             gasmixes: [], tanks: [], samples: [] }),
    ]);
    await tick(6);
    const doc = window.document;
    const view = doc.getElementById('view-dives');

    ok('no runtime errors rendering the log', errors.length === 0, errors.join(' | '));
    ok('page is titled "Your diving"', /Your diving/.test(view.textContent));

    const cells = [...view.querySelectorAll('.summary .cell')];
    ok('summary has four figures', cells.length === 4, String(cells.length));
    ok('total dives counted', cells[0].textContent.includes('3'), cells[0].textContent.trim());
    ok('total time summed', /1h 5[0-9]m|2h/.test(cells[1].textContent), cells[1].textContent.trim());
    ok('max depth is the deepest', cells[2].textContent.includes('28.4'), cells[2].textContent.trim());
    ok('last dive is the most recent', cells[3].textContent.includes('2025'), cells[3].textContent.trim());

    const cards = [...view.querySelectorAll('.dive-card')];
    ok('a card per dive', cards.length === 3, String(cards.length));

    // Hierarchy: site, then place, then date, then the metrics.
    const first = cards[0];
    ok('card leads with the site', first.querySelector('.site').textContent.trim() === 'Ojamon kaivos',
       first.querySelector('.site').textContent);
    ok('card shows region and country from the catalogue',
       first.querySelector('.place').textContent.includes('Uusimaa') &&
       first.querySelector('.place').textContent.includes('Finland'),
       first.querySelector('.place').textContent);
    ok('card shows the date', /2025/.test(first.querySelector('.when').textContent));
    ok('card shows depth, time, water and gas',
       first.querySelectorAll('.stats .metric').length === 4,
       String(first.querySelectorAll('.stats .metric').length));
    ok('gas is named the way divers say it', /EAN32/.test(first.textContent), first.textContent.slice(0, 200));
    ok('card carries a profile sparkline', !!first.querySelector('.spark svg'));
    ok('a dive with no samples has no sparkline',
       !cards[2].querySelector('.spark svg'), 'sparkline drawn for an empty profile');
    ok('unnamed site falls back', cards[2].textContent.includes('Unnamed site'));

    // Numbering runs from the oldest dive, like the phone app.
    ok('oldest dive is #1', cards[2].querySelector('.badge-n').textContent === '#1',
       cards[2].querySelector('.badge-n').textContent);
    ok('newest dive is #3', cards[0].querySelector('.badge-n').textContent === '#3',
       cards[0].querySelector('.badge-n').textContent);

    // Search
    const s = doc.getElementById('dive-search');
    s.value = 'ojamon';
    s.dispatchEvent(new window.Event('input'));
    await tick(2);
    ok('search filters the list', doc.querySelectorAll('.dive-card').length === 1,
       String(doc.querySelectorAll('.dive-card').length));
    ok('search keeps focus for continued typing',
       doc.activeElement && doc.activeElement.id === 'dive-search',
       doc.activeElement ? doc.activeElement.id : 'none');

    s.value = 'nothing here';
    s.dispatchEvent(new window.Event('input'));
    await tick(2);
    ok('a search with no hits explains itself',
       /Nothing matches/.test(doc.getElementById('view-dives').textContent));
  }

  // ── Detail view ──────────────────────────────────────────────────────────
  {
    const { window, errors, tick, patches, deletes } = boot([dive()]);
    await tick(6);
    const doc = window.document;

    doc.querySelector('.dive-card').click();
    await tick(2);
    ok('opening a dive shows the detail view',
       !doc.getElementById('view-dive').classList.contains('hidden'));

    const body = doc.getElementById('view-dive');
    ok('detail leads with the site name', /Vetokannas/.test(body.querySelector('.page-title').textContent));
    ok('detail shows the long date', /May 2025/.test(body.textContent), body.textContent.slice(0, 200));

    const big = [...body.querySelectorAll('.metric-lg')];
    ok('four headline metrics', big.length === 4, String(big.length));
    ok('max depth is prominent', big[0].textContent.includes('18.4'), big[0].textContent.trim());
    ok('duration is prominent', big[1].textContent.includes('48 min'), big[1].textContent.trim());
    ok('water temperature is prominent', big[2].textContent.includes('6.5'), big[2].textContent.trim());
    ok('gas is prominent', big[3].textContent.includes('EAN32'), big[3].textContent.trim());

    ok('detail draws the full profile', !!body.querySelector('.chart svg'));
    ok('profile marks the deepest point', /circle/.test(body.querySelector('.chart').innerHTML));
    ok('gas table present', /Mix/.test(body.textContent) && /68/.test(body.textContent));
    ok('cylinder table computes gas used', /150/.test(body.textContent), 'no used-gas figure');
    ok('site section offers to set a site', !!body.querySelector('[data-act="setsite"]'));
    ok('no runtime errors on detail', errors.length === 0, errors.join(' | '));

    // Back
    body.querySelector('[data-back]').click();
    await tick(2);
    ok('back returns to the log', !doc.getElementById('view-dives').classList.contains('hidden'));

    // Delete, via a confirmation
    doc.querySelector('.dive-card').click();
    await tick(2);
    doc.querySelector('#view-dive [data-act="delete"]').click();
    await tick(2);
    ok('delete asks first', !!doc.querySelector('.sheet'), 'no confirmation');
    doc.querySelector('.sheet [data-act="yes"]').click();
    await tick(3);
    ok('delete issues a DELETE for that dive', deletes.length === 1 && /id=eq\.1/.test(deletes[0]),
       deletes.join(','));
    ok('deleted dive leaves the log', doc.querySelectorAll('.dive-card').length === 0,
       String(doc.querySelectorAll('.dive-card').length));
  }

  // ── Escaping ─────────────────────────────────────────────────────────────
  {
    const { window, tick } = boot([dive({ site_name: '<img src=x onerror="window.__pwned=1">' })]);
    await tick(6);
    ok('a hostile site name is escaped, not executed',
       window.__pwned === undefined &&
       window.document.querySelector('.dive-card').textContent.includes('<img'));
  }

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
