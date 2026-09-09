// Sign in, sign up, sign out, session persistence and token refresh.
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const HTML = buildHtml();

function boot({ dives = [], stored = null, refreshWorks = true, password = 'correct-horse' } = {}) {
  const log = [];
  const errors = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));

  async function stub(url, opts = {}) {
    const u = String(url);
    const body = opts.body ? JSON.parse(opts.body) : null;

    if (u.includes('grant_type=password')) {
      log.push('signin');
      if (body.password !== password) {
        return { ok: false, status: 400, json: async () => ({ msg: 'Invalid login credentials' }) };
      }
      return { ok: true, status: 200, json: async () => ({
        access_token: 'AT1', refresh_token: 'RT1',
        user: { email: body.email, id: 'u1' } }) };
    }
    if (u.includes('grant_type=refresh_token')) {
      log.push('refresh');
      if (!refreshWorks) return { ok: false, status: 400, json: async () => ({ msg: 'bad token' }) };
      return { ok: true, status: 200, json: async () => ({
        access_token: 'FRESH', refresh_token: 'RT2',
        user: { email: 'diver@example.com', id: 'u1' } }) };
    }
    if (u.includes('/auth/v1/signup')) {
      log.push('signup');
      return { ok: true, status: 200, json: async () => ({
        access_token: 'AT1', refresh_token: 'RT1',
        user: { email: body.email, id: 'u1' } }) };
    }
    if (u.includes('dive_sites.json') || u.includes('finnish_sites.json')) {
      return { ok: true, status: 200, json: async () => ({ sites: [] }) };
    }
    if (u.includes('/rest/v1/dives')) {
      const auth = (opts.headers || {}).Authorization;
      log.push('dives:' + (auth === 'Bearer FRESH' ? 'fresh' : auth === 'Bearer AT1' ? 'ok' : 'stale'));
      if (auth !== 'Bearer AT1' && auth !== 'Bearer FRESH') {
        return { ok: false, status: 401, json: async () => ({}) };
      }
      return { ok: true, status: 200, json: async () => dives };
    }
    return { ok: false, status: 404, json: async () => ({}) };
  }

  const dom = new JSDOM(HTML, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/web/', virtualConsole: vc,
    beforeParse(win) {
      win.L = undefined;
      if (stored) win.localStorage.setItem('deeplog.session', JSON.stringify(stored));
      win.fetch = (...a) => stub(...a);
      win.scrollTo = () => {};
    },
  });
  const tick = (n = 4) => new Promise((r) => setTimeout(r, 30 * n));
  return { window: dom.window, log, errors, tick };
}

const dive = (o = {}) => Object.assign({
  id: 1, date: '2025-05-01', time: '10:00:00', site_name: 'Vetokannas',
  maxdepth: 18, avgdepth: 9, duration: 2400, divemode: 'OC', temp_min: 6,
  gasmixes: [], tanks: [], samples: [[0, 0, 6], [60000, 18, 6]],
}, o);

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  // ── Starts signed out ────────────────────────────────────────────────────
  {
    const { window, tick } = boot();
    await tick();
    const $ = (s) => window.document.querySelector(s);
    ok('login shown at boot', !$('#login-view').classList.contains('hidden'));
    ok('app hidden at boot', $('#app-view').classList.contains('hidden'));

    // Wrong password surfaces the server's own message.
    $('#email').value = 'diver@example.com';
    $('#password').value = 'wrong';
    $('#btn-signin').click();
    await tick();
    ok('bad password shows the server message',
       $('#login-msg').textContent.includes('Invalid login credentials'),
       $('#login-msg').textContent);
    ok('stays on the login screen', !$('#login-view').classList.contains('hidden'));

    // Correct password gets in.
    $('#password').value = 'correct-horse';
    $('#btn-signin').click();
    await tick(6);
    ok('signs in', !$('#app-view').classList.contains('hidden'));
    ok('session persisted', !!window.localStorage.getItem('deeplog.session'));
    ok('account button shows the initial', $('#btn-account').textContent === 'D',
       $('#btn-account').textContent);
    ok('lands on the dive log', !$('#view-dives').classList.contains('hidden'));
  }

  // ── Enter submits ────────────────────────────────────────────────────────
  {
    const { window, tick } = boot();
    await tick();
    const $ = (s) => window.document.querySelector(s);
    $('#email').value = 'diver@example.com';
    $('#password').value = 'correct-horse';
    $('#password').dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Enter' }));
    await tick(6);
    ok('Enter in the password field signs in', !$('#app-view').classList.contains('hidden'));
  }

  // ── Sign up ──────────────────────────────────────────────────────────────
  {
    const { window, log, tick } = boot();
    await tick();
    const $ = (s) => window.document.querySelector(s);
    $('#email').value = 'new@example.com';
    $('#password').value = 'whatever';
    $('#btn-signup').click();
    await tick(6);
    ok('sign up calls the signup endpoint', log.includes('signup'), log.join(','));
    ok('sign up enters the app', !$('#app-view').classList.contains('hidden'));
  }

  // ── Sign out ─────────────────────────────────────────────────────────────
  {
    const { window, tick } = boot({ dives: [dive()], stored: {
      access_token: 'AT1', refresh_token: 'RT1', email: 'diver@example.com', user_id: 'u1' } });
    await tick(6);
    const $ = (s) => window.document.querySelector(s);
    ok('stored session skips the login screen', !$('#app-view').classList.contains('hidden'));

    $('#btn-account').click();
    await tick();
    ok('account opens a confirm sheet', !!$('.sheet'), 'no sheet');
    $('.sheet [data-act="yes"]').click();
    await tick();
    ok('sign out clears the session', !window.localStorage.getItem('deeplog.session'));
    ok('sign out returns to login', !$('#login-view').classList.contains('hidden'));
  }

  // ── Expired token is refreshed, once ─────────────────────────────────────
  {
    const { window, log, tick } = boot({ dives: [dive()], stored: {
      access_token: 'EXPIRED', refresh_token: 'RT-OLD', email: 'diver@example.com' } });
    await tick(6);
    const $ = (s) => window.document.querySelector(s);
    ok('stale token triggers a refresh', log.includes('refresh'), log.join(','));
    ok('request retried with the fresh token', log.includes('dives:fresh'), log.join(','));
    ok('user lands in the app, not at login', !$('#app-view').classList.contains('hidden'));
    ok('rotated refresh token stored',
       JSON.parse(window.localStorage.getItem('deeplog.session')).refresh_token === 'RT2');
  }

  // ── A refresh that fails signs out cleanly ───────────────────────────────
  {
    const { window, tick } = boot({ refreshWorks: false, stored: {
      access_token: 'EXPIRED', refresh_token: 'RT-OLD', email: 'diver@example.com' } });
    await tick(6);
    const $ = (s) => window.document.querySelector(s);
    ok('failed refresh returns to login', !$('#login-view').classList.contains('hidden'));
    ok('failed refresh clears the session', !window.localStorage.getItem('deeplog.session'));
  }

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
