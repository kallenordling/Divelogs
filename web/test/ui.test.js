// Loads web/index.html in jsdom with a stubbed Supabase, then drives the real
// UI: sign in, list dives, open a dive, check the profile chart and stats.
const { JSDOM, VirtualConsole } = require('jsdom');
const fs = require('fs');

const html = fs.readFileSync(require('path').join(__dirname, '..', 'index.html'), 'utf8')
  // Leaflet is loaded from a CDN; jsdom has no network here and the map tab is
  // not what this test is checking.
  .replace(/<script src="https:\/\/unpkg[^<]*<\/script>/g, '<script>window.L=undefined;</script>')
  .replace(/<link rel="stylesheet" href="https:\/\/unpkg[^>]*>/g, '');

const DIVES = [
  {
    date: '2025-07-14', time: '11:20:00', site_name: 'Ojamon kaivoslampi',
    device_name: 'Shearwater Teric', maxdepth: 28.4, avgdepth: 14.2, duration: 2760,
    divemode: 'OC', temp_min: 4.5, temp_surface: 21.0,
    site_lat: 60.239814, site_lon: 24.034529,
    gasmixes: [{ o2: 32, he: 0, n2: 68 }],
    tanks: [{ volume: 12, workpressure: 232, start: 210, end: 70 }],
    samples: Array.from({ length: 60 }, (_, i) => [i * 30000, Math.sin(i / 60 * Math.PI) * 28.4, 4.5 + i * 0.02]),
  },
  {
    // Deliberately awkward: JSON columns as strings, no site, no temperature.
    date: '2024-06-01', time: '09:00:00', site_name: null,
    device_name: null, maxdepth: 12.0, avgdepth: 0, duration: 1800,
    divemode: 'OC', temp_min: null, temp_surface: null,
    site_lat: null, site_lon: null,
    gasmixes: '[{"o2":21,"he":0,"n2":79}]',
    tanks: '[]',
    samples: '[[0,0,0],[60000,12,0],[120000,0,0]]',
  },
];

const vc = new VirtualConsole();
const errors = [];
vc.on('jsdomError', (e) => errors.push('jsdomError: ' + e.message));
vc.on('error', (...a) => errors.push('console.error: ' + a.join(' ')));

const dom = new JSDOM(html, {
  runScripts: 'dangerously',
  pretendToBeVisual: true,
  url: 'https://example.github.io/deeplog/',
  virtualConsole: vc,
});
const { window } = dom;

// ── Stub the backend ────────────────────────────────────────────────────────
const calls = [];
window.fetch = async (url, opts = {}) => {
  calls.push({ url: String(url), method: opts.method || 'GET', headers: opts.headers || {} });
  const u = String(url);

  if (u.includes('/auth/v1/token?grant_type=password')) {
    const body = JSON.parse(opts.body);
    if (body.password !== 'correct-horse') {
      return { ok: false, status: 400, json: async () => ({ code: 400, msg: 'Invalid login credentials' }) };
    }
    return { ok: true, status: 200, json: async () => ({
      access_token: 'AT1', refresh_token: 'RT1', user: { email: body.email, id: 'user-1' } }) };
  }
  if (u.includes('/rest/v1/dives')) {
    if ((opts.headers || {}).Authorization !== 'Bearer AT1') {
      return { ok: false, status: 401, json: async () => ({}) };
    }
    return { ok: true, status: 200, json: async () => DIVES };
  }
  return { ok: false, status: 404, json: async () => ({}) };
};

const $ = (s) => window.document.querySelector(s);
const tick = () => new Promise((r) => setTimeout(r, 30));

const results = [];
function check(name, cond, extra = '') {
  results.push({ name, pass: !!cond, extra });
}

(async () => {
  await tick();

  // 1. Starts on the login screen.
  check('login screen shown at boot', !$('#login-view').classList.contains('hidden'));
  check('app hidden at boot', $('#app-view').classList.contains('hidden'));

  // 2. Wrong password surfaces the server's message.
  $('#email').value = 'diver@example.com';
  $('#password').value = 'wrong';
  $('#btn-signin').click();
  await tick(); await tick();
  check('bad password shows server message',
    $('#login-msg').textContent.includes('Invalid login credentials'),
    $('#login-msg').textContent);
  check('bad password keeps you on login', !$('#login-view').classList.contains('hidden'));

  // 3. Correct password signs in and loads dives.
  $('#password').value = 'correct-horse';
  $('#btn-signin').click();
  await tick(); await tick(); await tick();
  check('signed in shows app', !$('#app-view').classList.contains('hidden'));
  check('email shown in header', $('#who').textContent === 'diver@example.com', $('#who').textContent);
  check('session persisted', !!window.localStorage.getItem('deeplog.session'));

  const cards = window.document.querySelectorAll('.dive-card');
  check('both dives listed', cards.length === 2, 'got ' + cards.length);

  // Newest first, numbered from the oldest.
  check('newest dive first', cards[0].textContent.includes('Ojamon'), cards[0].textContent.trim());
  check('dive numbered from oldest', cards[0].querySelector('.dive-num').textContent === '2',
    cards[0].querySelector('.dive-num').textContent);
  check('unnamed site falls back', cards[1].textContent.includes('Unnamed site'));
  check('duration formatted', cards[0].textContent.includes('46:00'), cards[0].textContent.trim());

  // 4. Search filters.
  $('#search').value = 'ojamon';
  $('#search').dispatchEvent(new window.Event('input'));
  await tick();
  check('search is case-insensitive', window.document.querySelectorAll('.dive-card').length === 1);
  $('#search').value = '';
  $('#search').dispatchEvent(new window.Event('input'));
  await tick();

  // 5. Detail view with a real profile.
  window.document.querySelectorAll('.dive-card')[0].click();
  await tick();
  const detail = $('#detail-body').innerHTML;
  check('detail tab shown', !$('#tab-detail').classList.contains('hidden'));
  check('profile chart drawn', detail.includes('<svg') && detail.includes('stroke="#4fc3f7"'));
  check('temperature series drawn', detail.includes('stroke="#ffb74d"'));
  check('gas named from o2', detail.includes('EAN32'), 'no EAN32 in detail');
  check('cylinder gas used computed', detail.includes('140 bar'), 'no used-gas figure');
  check('max depth shown', detail.includes('28.4'));

  // 6. A dive whose JSON columns came back as strings must still render.
  $('#btn-back').click();
  await tick();
  window.document.querySelectorAll('.dive-card')[1].click();
  await tick();
  const d2 = $('#detail-body').innerHTML;
  check('string JSON columns parsed', d2.includes('Air'), 'gas table missing for string columns');
  check('profile from string samples', d2.includes('<svg'));
  check('no temp series when flat', !d2.includes('stroke="#ffb74d"'));

  // 7. Stats.
  $('#btn-back').click();
  window.document.querySelector('nav button[data-tab="stats"]').click();
  await tick();
  const stats = $('#stat-tiles').textContent;
  check('dive count', stats.includes('2'), stats);
  check('total time underwater', stats.includes('1') && stats.includes('h'), stats);
  check('deepest shown', stats.includes('28.4'), stats);
  check('per-year breakdown', $('#per-year').textContent.includes('2025') &&
                              $('#per-year').textContent.includes('2024'));

  // 8. Expired token: the client refreshes, and gives up cleanly if it cannot.
  window.localStorage.setItem('deeplog.session', JSON.stringify({ access_token: 'STALE', email: 'x' }));

  // 9. Sign out clears the stored session.
  $('#btn-signout').click();
  await tick();
  check('sign out clears session', !window.localStorage.getItem('deeplog.session'));
  check('sign out returns to login', !$('#login-view').classList.contains('hidden'));

  // 10. XSS: a site name must not become live markup.
  DIVES[0].site_name = '<img src=x onerror="window.__pwned=1">';
  $('#email').value = 'diver@example.com';
  $('#password').value = 'correct-horse';
  $('#btn-signin').click();
  await tick(); await tick(); await tick();
  check('site name is escaped', window.__pwned === undefined &&
        window.document.querySelector('.dive-card').textContent.includes('<img'));

  // ── Report ────────────────────────────────────────────────────────────────
  let failed = 0;
  for (const r of results) {
    if (!r.pass) failed++;
    console.log(`${r.pass ? 'ok  ' : 'FAIL'}  ${r.name}${r.pass ? '' : '   << ' + r.extra}`);
  }
  if (errors.length) {
    console.log('\nRuntime errors:');
    errors.forEach((e) => console.log('  ' + e));
  }
  console.log(`\n${results.length - failed}/${results.length} passed` +
              (errors.length ? `, ${errors.length} runtime error(s)` : ''));
  process.exit(failed || errors.length ? 1 : 0);
})();
