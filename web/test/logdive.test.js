// Logging a dive by hand and editing one: validation, what is written, and
// that only columns the table actually has are sent.
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const HTML = buildHtml();

const FIN = { sites: [
  { name: 'Vetokannas', lat: 59.989997, lon: 24.417312, country: 'Finland',
    desc: 'Sea shore site', kind: 'site' },
  { name: 'Ojamon kaivos', lat: 60.240429, lon: 24.031069, country: 'Finland',
    desc: 'Mine', kind: 'site' },
] };

const existing = {
  id: 7, date: '2025-05-01', time: '10:30:00', site_name: 'Vetokannas',
  device_name: 'Shearwater Teric', maxdepth: 18.4, avgdepth: 9.2, duration: 2880,
  divemode: 'OC', temp_min: 6.5, temp_max: 9, temp_surface: 18,
  site_lat: 59.989997, site_lon: 24.417312,
  gasmixes: [{ o2: 32, he: 0, n2: 68 }],
  tanks: [{ gasmix: 0, volume: 12, workpressure: 232, start: 210, end: 60 }],
  samples: [[0, 0, 6.5], [60000, 18.4, 6.5]],
};

function boot(dives, { postStatus = 200 } = {}) {
  const posts = [], patches = [];
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
        if (u.includes('finnish_sites.json')) return { ok: true, status: 200, json: async () => FIN };
        if (u.includes('dive_sites.json')) return { ok: true, status: 200, json: async () => ({ sites: [] }) };
        if (u.includes('/rest/v1/dives')) {
          if (m === 'POST') {
            const body = JSON.parse(opts.body);
            posts.push(body);
            if (postStatus >= 300) {
              return { ok: false, status: postStatus,
                       json: async () => ({ message: 'new row violates row-level security policy' }) };
            }
            return { ok: true, status: 201, json: async () => [{ ...body, id: 99 }] };
          }
          if (m === 'PATCH') { patches.push(JSON.parse(opts.body)); return { ok: true, status: 204, json: async () => null }; }
          return { ok: true, status: 200, json: async () => dives };
        }
        return { ok: false, status: 404, json: async () => ({}) };
      };
    },
  });
  const tick = (n = 5) => new Promise((r) => setTimeout(r, 30 * n));
  return { window: dom.window, posts, patches, errors, tick };
}

const set = (doc, id, v) => {
  const el = doc.querySelector('#' + id);
  el.value = String(v);
  el.dispatchEvent(new doc.defaultView.Event('input'));
};
const submit = (doc) =>
  doc.querySelector('#dive-form')
     .dispatchEvent(new doc.defaultView.Event('submit', { cancelable: true, bubbles: true }));

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  // ── The form opens from every entry point ────────────────────────────────
  {
    const b = boot([]);
    await b.tick(6);
    const doc = b.window.document;

    doc.querySelector('#nav-log').click();
    await b.tick(2);
    ok('desktop "Log dive" opens the form', !!doc.querySelector('#dive-form'));
    doc.querySelector('#f-cancel').click();
    await b.tick(1);
    ok('cancel closes it', !doc.querySelector('#dive-form'));

    doc.querySelector('#tab-log').click();
    await b.tick(2);
    ok('the mobile centre button opens it too', !!doc.querySelector('#dive-form'));

    // Sections, so it reads as a sequence rather than one wall of inputs.
    const sections = [...doc.querySelectorAll('#dive-form .micro')].map((n) => n.textContent);
    ok('the form is split into named sections', sections.length >= 4, sections.join(','));
    ok('sections cover dive, profile, conditions and gas',
       /Dive/.test(sections[0]) && /Profile/.test(sections[1]) &&
       /Conditions/.test(sections[2]) && /Gas/.test(sections[3]), sections.join(','));
    ok('required fields are marked',
       doc.querySelectorAll('#dive-form .field.required').length >= 4,
       String(doc.querySelectorAll('#dive-form .field.required').length));
    ok('date defaults to today',
       doc.querySelector('#f-date').value === new Date().toISOString().slice(0, 10),
       doc.querySelector('#f-date').value);
    ok('numeric fields ask for a numeric keypad',
       doc.querySelector('#f-depth').getAttribute('inputmode') === 'decimal');
  }

  // ── Validation ───────────────────────────────────────────────────────────
  {
    const b = boot([]);
    await b.tick(6);
    const doc = b.window.document;
    doc.querySelector('#nav-log').click();
    await b.tick(2);

    submit(doc);
    await b.tick(2);
    ok('a site is required', /site name/i.test(doc.querySelector('#f-msg').textContent),
       doc.querySelector('#f-msg').textContent);
    ok('nothing is posted while invalid', b.posts.length === 0);

    set(doc, 'f-site', 'Vetokannas');
    submit(doc);
    await b.tick(2);
    ok('depth is required', /maximum depth/i.test(doc.querySelector('#f-msg').textContent),
       doc.querySelector('#f-msg').textContent);

    set(doc, 'f-depth', '18');
    submit(doc);
    await b.tick(2);
    ok('duration is required', /how long/i.test(doc.querySelector('#f-msg').textContent),
       doc.querySelector('#f-msg').textContent);

    set(doc, 'f-dur', '48');
    set(doc, 'f-avg', '25');
    submit(doc);
    await b.tick(2);
    ok('average deeper than maximum is rejected',
       /cannot be deeper/i.test(doc.querySelector('#f-msg').textContent),
       doc.querySelector('#f-msg').textContent);

    set(doc, 'f-avg', '9');
    set(doc, 'f-start', '100');
    set(doc, 'f-end', '200');
    submit(doc);
    await b.tick(2);
    ok('end pressure above start is rejected',
       /higher than start/i.test(doc.querySelector('#f-msg').textContent),
       doc.querySelector('#f-msg').textContent);
  }

  // ── A successful create ──────────────────────────────────────────────────
  {
    const b = boot([]);
    await b.tick(6);
    const doc = b.window.document;
    doc.querySelector('#nav-log').click();
    await b.tick(2);

    set(doc, 'f-site', 'Ojamon kaivos');
    set(doc, 'f-date', '2026-08-12');
    set(doc, 'f-time', '09:15');
    set(doc, 'f-depth', '42.8');
    set(doc, 'f-dur', '48');
    set(doc, 'f-avg', '18.5');
    set(doc, 'f-water', '11');
    set(doc, 'f-air', '21');
    set(doc, 'f-o2', '32');
    set(doc, 'f-vol', '12');
    set(doc, 'f-start', '210');
    set(doc, 'f-end', '60');
    submit(doc);
    await b.tick(6);

    ok('one dive posted', b.posts.length === 1, String(b.posts.length));
    const p = b.posts[0] || {};
    ok('site name written', p.site_name === 'Ojamon kaivos', JSON.stringify(p.site_name));
    ok('date written', p.date === '2026-08-12', p.date);
    ok('time normalised to HH:MM:SS', p.time === '09:15:00', p.time);
    ok('depth written', p.maxdepth === 42.8, String(p.maxdepth));
    ok('duration converted to seconds', p.duration === 2880, String(p.duration));
    ok('water temperature written', p.temp_min === 11, String(p.temp_min));
    ok('gas stored as a mix', Array.isArray(p.gasmixes) && p.gasmixes[0].o2 === 32 &&
       p.gasmixes[0].n2 === 68, JSON.stringify(p.gasmixes));
    ok('cylinder stored', Array.isArray(p.tanks) && p.tanks[0].start === 210 &&
       p.tanks[0].end === 60, JSON.stringify(p.tanks));
    ok('user id attached', p.user_id === 'u1', String(p.user_id));
    ok('samples column kept well-formed', Array.isArray(p.samples), JSON.stringify(p.samples));

    // Only real columns; the table has no notes/buddy/conditions.
    const ALLOWED = new Set(['date','time','maxdepth','avgdepth','duration','divemode',
      'temp_surface','temp_min','temp_max','atmospheric','salinity_type','gasmixes','tanks',
      'samples','site_name','site_lat','site_lon','device_name','user_id']);
    const strays = Object.keys(p).filter((k) => !ALLOWED.has(k));
    ok('no columns the table does not have', strays.length === 0, strays.join(','));

    ok('the new dive appears in the log',
       /Ojamon kaivos/.test(doc.getElementById('view-dives').textContent));
    ok('a confirmation is shown',
       /Dive logged/.test(doc.getElementById('toast-host').textContent),
       doc.getElementById('toast-host').textContent);
    ok('the form closes on success', !doc.querySelector('#dive-form'));
    ok('no runtime errors', b.errors.length === 0, b.errors.join(' | '));
  }

  // ── Defaults are remembered ──────────────────────────────────────────────
  {
    const b = boot([]);
    await b.tick(6);
    const doc = b.window.document;
    b.window.localStorage.setItem('deeplog.diveDefaults',
      JSON.stringify({ divemode: 'CCR', o2: 32, volume: 15, salinity_type: '1' }));
    doc.querySelector('#nav-log').click();
    await b.tick(2);
    ok('oxygen defaults to what you last used', doc.querySelector('#f-o2').value === '32',
       doc.querySelector('#f-o2').value);
    ok('cylinder size remembered', doc.querySelector('#f-vol').value === '15',
       doc.querySelector('#f-vol').value);
    ok('dive mode remembered', doc.querySelector('#f-mode').value === 'CCR',
       doc.querySelector('#f-mode').value);
  }

  // ── Editing an existing dive ─────────────────────────────────────────────
  {
    const b = boot([existing]);
    await b.tick(6);
    const doc = b.window.document;

    doc.querySelector('.dive-card').click();
    await b.tick(3);
    doc.querySelector('#view-dive [data-act="edit"]').click();
    await b.tick(3);

    const form = doc.querySelector('#dive-form');
    ok('edit opens the form pre-filled', !!form && form.querySelector('#f-site').value === 'Vetokannas',
       form ? form.querySelector('#f-site').value : 'no form');
    ok('duration shown in minutes', doc.querySelector('#f-dur').value === '48',
       doc.querySelector('#f-dur').value);
    ok('existing gas shown', doc.querySelector('#f-o2').value === '32',
       doc.querySelector('#f-o2').value);
    ok('the button says save, not log',
       /Save changes/.test(doc.querySelector('#f-save').textContent));

    set(doc, 'f-depth', '19.9');
    submit(doc);
    await b.tick(6);

    ok('edit issues a PATCH, not a POST', b.patches.length === 1 && b.posts.length === 0,
       `patches=${b.patches.length} posts=${b.posts.length}`);
    ok('the change is written', b.patches[0].maxdepth === 19.9, String(b.patches[0].maxdepth));
    ok('the detail view reflects the change',
       /19.9/.test(doc.getElementById('view-dive').textContent));
  }

  // ── A refused insert is explained ────────────────────────────────────────
  {
    const b = boot([], { postStatus: 403 });
    await b.tick(6);
    const doc = b.window.document;
    doc.querySelector('#nav-log').click();
    await b.tick(2);
    set(doc, 'f-site', 'Vetokannas');
    set(doc, 'f-depth', '18');
    set(doc, 'f-dur', '40');
    submit(doc);
    await b.tick(5);

    ok('a refused insert stays on the form', !!doc.querySelector('#dive-form'));
    ok('and says the policy is missing',
       /policy/i.test(doc.querySelector('#f-msg').textContent),
       doc.querySelector('#f-msg').textContent);
    ok('the save button is usable again', !doc.querySelector('#f-save').disabled);
  }

  console.log(`\n${failed ? failed + ' failed' : 'all passed'}`);
  process.exit(failed ? 1 : 0);
})();
