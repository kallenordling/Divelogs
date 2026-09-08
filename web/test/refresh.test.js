// Boots web/index.html with a stored session whose access token is expired,
// and checks the client refreshes it rather than dumping the user at login.
const { JSDOM, VirtualConsole } = require('jsdom');
const fs = require('fs');

const html = fs.readFileSync(require('path').join(__dirname, '..', 'index.html'), 'utf8')
  .replace(/<script src="https:\/\/unpkg[^<]*<\/script>/g, '<script>window.L=undefined;</script>')
  .replace(/<link rel="stylesheet" href="https:\/\/unpkg[^>]*>/g, '');

function run(scenario, refreshWorks) {
  const vc = new VirtualConsole();
  const log = [];

  // Declared before the JSDOM is built: beforeParse runs during construction,
  // and the page's boot code fetches synchronously from there.
  async function stub(url, opts = {}) {
    const u = String(url);
    const auth = (opts.headers || {}).Authorization;

    if (u.includes('grant_type=refresh_token')) {
      log.push('refresh');
      if (!refreshWorks) {
        return { ok: false, status: 400, json: async () => ({ msg: 'Invalid Refresh Token' }) };
      }
      return { ok: true, status: 200, json: async () => ({
        access_token: 'FRESH', refresh_token: 'RT-NEW',
        user: { email: 'diver@example.com', id: 'u1' } }) };
    }
    if (u.includes('/rest/v1/dives')) {
      log.push('dives:' + (auth === 'Bearer FRESH' ? 'fresh' : 'stale'));
      if (auth !== 'Bearer FRESH') return { ok: false, status: 401, json: async () => ({}) };
      return { ok: true, status: 200, json: async () => ([{
        date: '2025-01-02', time: '10:00:00', site_name: 'Vetokannas',
        maxdepth: 18, avgdepth: 9, duration: 2400, divemode: 'OC',
        gasmixes: [], tanks: [], samples: [[0, 0, 0], [60000, 18, 5]] }]) };
    }
    return { ok: false, status: 404, json: async () => ({}) };
  }

  const dom = new JSDOM(html, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/deeplog/', virtualConsole: vc,
    beforeParse(win) {
      win.localStorage.setItem('deeplog.session', JSON.stringify({
        access_token: 'EXPIRED', refresh_token: 'RT-OLD', email: 'diver@example.com',
      }));
      // Must be in place before the page's boot code runs.
      win.fetch = (...a) => stub(...a);
    },
  });
  const { window } = dom;

  return new Promise((resolve) => setTimeout(() => {
    const doc = window.document;
    resolve({
      scenario,
      log,
      onApp: !doc.getElementById('app-view').classList.contains('hidden'),
      onLogin: !doc.getElementById('login-view').classList.contains('hidden'),
      cards: doc.querySelectorAll('.dive-card').length,
      stored: window.localStorage.getItem('deeplog.session'),
    });
  }, 200));
}

(async () => {
  let failed = 0;
  const ok = (name, cond, extra = '') => {
    if (!cond) failed++;
    console.log(`${cond ? 'ok  ' : 'FAIL'}  ${name}${cond ? '' : '   << ' + extra}`);
  };

  const a = await run('refresh succeeds', true);
  ok('stale token triggers a refresh', a.log.includes('refresh'), JSON.stringify(a.log));
  ok('request retried with the new token', a.log.includes('dives:fresh'), JSON.stringify(a.log));
  ok('user lands in the app, not at login', a.onApp && !a.onLogin);
  ok('dives rendered after refresh', a.cards === 1, 'cards=' + a.cards);
  ok('new refresh token stored',
     a.stored && JSON.parse(a.stored).refresh_token === 'RT-NEW', String(a.stored));

  const b = await run('refresh fails', false);
  ok('failed refresh returns to login', b.onLogin && !b.onApp);
  ok('failed refresh clears the stored session', !b.stored, String(b.stored));

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
