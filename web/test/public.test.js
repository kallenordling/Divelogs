// Exploring without an account, and the water averaged across divers.
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const HTML = buildHtml();

const FIN = { sites: [
  { name: 'Vetokannas', lat: 59.989997, lon: 24.417312, country: 'Finland',
    desc: 'Sea shore site', kind: 'site' },
] };

// Two divers, same site, same days, different readings. The site page must
// show the average of the two, not either one.
const WATER = [];
for (const [date, a, b] of [['2026-06-08', 18, 20], ['2026-07-01', 20, 22]]) {
  for (let depth = 0; depth <= 20; depth += 5) {
    const mine = a - depth * 0.4;
    const theirs = b - depth * 0.4;
    WATER.push({ date, depth_m: depth, temp_c: (mine + theirs) / 2, readings: 2 });
  }
}

function boot({ signedIn = false, water = WATER, viewMissing = false } = {}) {
  const errors = [];
  const asked = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));
  const dom = new JSDOM(HTML, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/', virtualConsole: vc,
    beforeParse(win) {
      win.L = undefined;
      win.scrollTo = () => {};
      if (signedIn) {
        win.localStorage.setItem('deeplog.session', JSON.stringify({
          access_token: 'AT1', refresh_token: 'RT1', email: 'd@e.com', user_id: 'u1' }));
      }
      win.fetch = async (url, opts = {}) => {
        const u = String(url);
        asked.push({ url: u, auth: (opts.headers || {}).Authorization });
        if (u.includes('finnish_sites.json')) return { ok: true, status: 200, json: async () => FIN };
        if (u.includes('dive_sites.json')) return { ok: true, status: 200, json: async () => ({ sites: [] }) };
        if (u.includes('site_places')) {
          return { ok: true, status: 200,
                   json: async () => [{ site_name: 'Hidden Reef', lat: 61.1, lon: 25.5 }] };
        }
        if (u.includes('site_water')) {
          if (viewMissing) {
            return { ok: false, status: 404,
                     json: async () => ({ message: 'relation "site_water" does not exist' }) };
          }
          return { ok: true, status: 200, json: async () => water };
        }
        if (u.includes('/rest/v1/dives')) return { ok: true, status: 200, json: async () => [] };
        return { ok: false, status: 404, json: async () => ({}) };
      };
    },
  });
  const tick = (n = 5) => new Promise((r) => setTimeout(r, 30 * n));
  return { window: dom.window, errors, asked, tick };
}

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  // ── A visitor with no account ───────────────────────────────────────────
  const { window, errors, asked, tick } = boot();
  await tick(5);
  const doc = window.document;
  const $ = (s) => doc.querySelector(s);

  ok('a visitor lands on the sign-in screen',
     !$('#login-view').classList.contains('hidden'));
  ok('and is offered a way in without an account', !!$('#btn-explore'));

  $('#btn-explore').click();
  await tick(5);

  ok('exploring opens the app', $('#login-view').classList.contains('hidden') &&
     !$('#app-view').classList.contains('hidden'));
  ok('a visitor starts at the sites, not an empty log',
     !$('#view-sites').classList.contains('hidden'), 'wrong tab');
  ok('logging a dive is not offered without an account',
     $('#nav-log').classList.contains('hidden') && $('#tab-log').classList.contains('hidden'));

  const cards = () => [...doc.querySelectorAll('#view-sites .site-card')];
  ok('the catalogue is browsable', cards().some((c) => /Vetokannas/.test(c.textContent)),
     String(cards().length));
  ok('sites divers logged but no catalogue lists are there too',
     cards().some((c) => /Hidden Reef/.test(c.textContent)),
     cards().map((c) => c.textContent.trim().split('\n')[0]).join(' | '));

  cards().find((c) => /Vetokannas/.test(c.textContent)).click();
  await tick(5);
  const site = $('#view-site');
  ok('a visitor can open a site', /Vetokannas/.test(site.textContent));
  ok('the shared water is drawn', !!site.querySelector('.chart svg'), 'no chart');
  ok('and is described as shared', /Averaged across every diver/.test(site.textContent),
     site.textContent.slice(0, 300));
  ok('the fit is offered to visitors too', !!site.querySelector('[data-act="fit"]'));
  ok('no personal dive list is shown', !/Dives here/.test(site.textContent));

  // The two divers recorded 18 and 20 °C at the surface on 8 June; the page
  // must show 19, and the chart must be coloured from the averages.
  ok('the warmest shown is the average, not the warmest diver',
     /\b19\b/.test(site.textContent) && !/\b20 °C/.test(site.textContent),
     site.textContent.slice(site.textContent.indexOf('Warmest') - 40, site.textContent.indexOf('Warmest') + 40));

  ok('the shared views are read with the public key',
     asked.filter((a) => a.url.includes('site_water')).every((a) => /Bearer eyJ/.test(a.auth || '')),
     JSON.stringify(asked.filter((a) => a.url.includes('site_water'))[0] || {}));
  ok('a visitor never asks for private dives',
     !asked.some((a) => /\/rest\/v1\/dives/.test(a.url)),
     asked.map((a) => a.url).filter((u) => u.includes('rest')).join(' | '));

  doc.querySelector('[data-nav="dives"]').click();
  await tick(3);
  ok('the log invites signing in', /Sign in to keep your own log/.test($('#view-dives').textContent));
  $('#view-dives [data-act="signin"]').click();
  await tick(2);
  ok('and that returns to the sign-in screen', !$('#login-view').classList.contains('hidden'));

  ok('no runtime errors while exploring', errors.length === 0, errors.join(' | '));

  // ── Without the shared views, nothing breaks ────────────────────────────
  const plain = boot({ viewMissing: true });
  await plain.tick(5);
  plain.window.document.querySelector('#btn-explore').click();
  await plain.tick(5);
  const pdoc = plain.window.document;
  [...pdoc.querySelectorAll('#view-sites .site-card')]
    .find((c) => /Vetokannas/.test(c.textContent)).click();
  await plain.tick(4);
  ok('a database without the shared views still opens sites',
     /Vetokannas/.test(pdoc.querySelector('#view-site').textContent));
  ok('and says nothing has been recorded there',
     /Nothing recorded at this site yet/.test(pdoc.querySelector('#view-site').textContent),
     pdoc.querySelector('#view-site').textContent.slice(0, 200));
  ok('no runtime errors without the views', plain.errors.length === 0, plain.errors.join(' | '));

  console.log(failed ? `\n${failed} failed` : '\nall passed');
  process.exit(failed ? 1 : 0);
})();
