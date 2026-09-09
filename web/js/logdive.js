/* Logging a dive by hand, and editing one already logged.
   A sequence of short sections rather than one long form, so it is workable
   one-handed on a phone. Only columns the dives table actually has are
   written; see DL.pickColumns. */
'use strict';

(() => {
const DL = window.DL;

const PREFS_KEY = 'deeplog.diveDefaults';

/** Remembers the values a diver repeats, so the next entry starts closer. */
function loadDefaults() {
  try { return JSON.parse(localStorage.getItem(PREFS_KEY) || '{}'); }
  catch { return {}; }
}
function saveDefaults(v) {
  try { localStorage.setItem(PREFS_KEY, JSON.stringify(v)); } catch { /* ignore */ }
}

const todayISO = () => new Date().toISOString().slice(0, 10);

/**
 * @param dive    an existing dive to edit, or null to create one
 * @param preset  values to start from, e.g. a site chosen on the map
 */
DL.openLogDive = (dive = null, preset = {}) => {
  const editing = !!dive;
  const defs = loadDefaults();
  const gas0 = DL.asArray(dive && dive.gasmixes)[0] || {};
  const tank0 = DL.asArray(dive && dive.tanks)[0] || {};

  // Duration is stored in seconds but entered in minutes.
  const minutes = dive && dive.duration ? Math.round(dive.duration / 60) : '';

  const v = {
    site_name: (dive && dive.site_name) || preset.site_name || '',
    site_lat:  dive ? dive.site_lat : (preset.site_lat ?? null),
    site_lon:  dive ? dive.site_lon : (preset.site_lon ?? null),
    date:      (dive && dive.date) || todayISO(),
    time:      DL.hhmm(dive && dive.time) || '',
    maxdepth:  dive ? dive.maxdepth : '',
    avgdepth:  dive ? dive.avgdepth : '',
    minutes,
    divemode:  (dive && dive.divemode) || defs.divemode || 'OC',
    temp_min:  dive ? dive.temp_min : '',
    temp_surface: dive ? dive.temp_surface : '',
    salinity_type: dive && dive.salinity_type != null ? String(dive.salinity_type)
                  : (defs.salinity_type ?? ''),
    o2: gas0.o2 != null ? gas0.o2 : (defs.o2 ?? 21),
    he: gas0.he != null ? gas0.he : 0,
    volume: tank0.volume != null ? tank0.volume : (defs.volume ?? ''),
    start: tank0.start != null ? tank0.start : '',
    end:   tank0.end != null ? tank0.end : '',
  };

  const num = (x) => (x === null || x === undefined || x === '' ? '' : x);

  const close = DL.openSheet(`
    <h3 class="section-title">${editing ? 'Edit dive' : 'Log a dive'}</h3>

    <form id="dive-form" class="stack" style="gap:18px;margin-top:16px" novalidate>

      <div class="stack" style="gap:10px">
        <div class="micro">Dive</div>
        <div class="field required">
          <label for="f-site">Site</label>
          <div class="row" style="gap:8px">
            <input class="input grow" id="f-site" value="${DL.esc(v.site_name)}"
                   placeholder="Where did you dive?">
            <button type="button" class="btn btn-ghost" id="f-pick">Browse</button>
          </div>
          <span class="hint" id="f-pos">${v.site_lat != null && v.site_lon != null
            ? `${DL.num(v.site_lat, 4)}, ${DL.num(v.site_lon, 4)}`
            : 'No position yet — pick from the catalogue to place it on the map'}</span>
        </div>
        <div class="field-row">
          <div class="field required">
            <label for="f-date">Date</label>
            <input class="input" id="f-date" type="date" value="${DL.esc(v.date)}">
          </div>
          <div class="field">
            <label for="f-time">Start time</label>
            <input class="input" id="f-time" type="time" value="${DL.esc(v.time)}">
          </div>
        </div>
      </div>

      <div class="stack" style="gap:10px">
        <div class="micro">Profile</div>
        <div class="field-row">
          <div class="field required">
            <label for="f-depth">Max depth (m)</label>
            <input class="input" id="f-depth" type="number" step="0.1" min="0" max="350"
                   inputmode="decimal" value="${num(v.maxdepth)}">
          </div>
          <div class="field required">
            <label for="f-dur">Duration (min)</label>
            <input class="input" id="f-dur" type="number" step="1" min="1" max="1440"
                   inputmode="numeric" value="${num(v.minutes)}">
          </div>
        </div>
        <div class="field-row">
          <div class="field">
            <label for="f-avg">Average depth (m)</label>
            <input class="input" id="f-avg" type="number" step="0.1" min="0" max="350"
                   inputmode="decimal" value="${num(v.avgdepth)}">
          </div>
          <div class="field">
            <label for="f-mode">Dive mode</label>
            <select class="input" id="f-mode">
              ${['OC', 'CCR', 'SCR', 'Freedive', 'Gauge'].map((m) =>
                `<option value="${m}"${v.divemode === m ? ' selected' : ''}>${m}</option>`).join('')}
            </select>
          </div>
        </div>
      </div>

      <div class="stack" style="gap:10px">
        <div class="micro">Conditions</div>
        <div class="field-row">
          <div class="field">
            <label for="f-water">Water temperature (°C)</label>
            <input class="input" id="f-water" type="number" step="0.1" min="-2" max="40"
                   inputmode="decimal" value="${num(v.temp_min)}">
          </div>
          <div class="field">
            <label for="f-air">Air temperature (°C)</label>
            <input class="input" id="f-air" type="number" step="0.1" min="-40" max="55"
                   inputmode="decimal" value="${num(v.temp_surface)}">
          </div>
        </div>
        <div class="field">
          <label for="f-salt">Water</label>
          <select class="input" id="f-salt">
            <option value=""${v.salinity_type === '' ? ' selected' : ''}>Not recorded</option>
            <option value="1"${v.salinity_type === '1' ? ' selected' : ''}>Salt</option>
            <option value="0"${v.salinity_type === '0' ? ' selected' : ''}>Fresh</option>
          </select>
        </div>
      </div>

      <div class="stack" style="gap:10px">
        <div class="micro">Gas &amp; cylinder</div>
        <div class="field-row">
          <div class="field">
            <label for="f-o2">Oxygen (%)</label>
            <input class="input" id="f-o2" type="number" step="1" min="4" max="100"
                   inputmode="numeric" value="${num(v.o2)}">
          </div>
          <div class="field">
            <label for="f-he">Helium (%)</label>
            <input class="input" id="f-he" type="number" step="1" min="0" max="90"
                   inputmode="numeric" value="${num(v.he)}">
          </div>
        </div>
        <div class="field-row">
          <div class="field">
            <label for="f-vol">Cylinder (L)</label>
            <input class="input" id="f-vol" type="number" step="0.1" min="0" max="60"
                   inputmode="decimal" value="${num(v.volume)}">
          </div>
          <div class="field">
            <label for="f-start">Start / end (bar)</label>
            <div class="row" style="gap:8px">
              <input class="input" id="f-start" type="number" step="1" min="0" max="400"
                     inputmode="numeric" placeholder="200" value="${num(v.start)}">
              <input class="input" id="f-end" type="number" step="1" min="0" max="400"
                     inputmode="numeric" placeholder="50" value="${num(v.end)}">
            </div>
          </div>
        </div>
      </div>

      <div class="msg" id="f-msg" style="min-height:18px;font-size:13px;color:var(--danger)"></div>

      <div class="row" style="gap:10px">
        <button type="button" class="btn btn-ghost grow" id="f-cancel">Cancel</button>
        <button type="submit" class="btn btn-primary grow" id="f-save">
          ${editing ? 'Save changes' : 'Log dive'}</button>
      </div>
    </form>`);

  const host = DL.el('sheet-host');
  const $ = (id) => host.querySelector('#' + id);
  const val = (id) => $(id).value.trim();
  const numOf = (id) => { const t = val(id); return t === '' ? null : Number(t); };

  // Chosen position rides alongside the free-text name.
  let lat = v.site_lat, lon = v.site_lon;

  $('f-pick').addEventListener('click', () => {
    // The picker replaces this sheet, so reopen the form with what was chosen.
    const current = collect();
    DL.pickSite(1, (site) => {
      DL.openLogDive(dive, { ...current, site_name: site.name,
                             site_lat: site.lat, site_lon: site.lon });
    });
  });

  function collect() {
    return {
      site_name: val('f-site'), site_lat: lat, site_lon: lon,
      date: val('f-date'), time: val('f-time'),
      maxdepth: numOf('f-depth'), avgdepth: numOf('f-avg'),
      minutes: numOf('f-dur'), divemode: val('f-mode'),
      temp_min: numOf('f-water'), temp_surface: numOf('f-air'),
      salinity_type: val('f-salt'),
      o2: numOf('f-o2'), he: numOf('f-he'),
      volume: numOf('f-vol'), start: numOf('f-start'), end: numOf('f-end'),
    };
  }

  $('f-cancel').addEventListener('click', () => close());

  $('dive-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const f = collect();
    const msg = $('f-msg');

    if (!f.site_name) return fail('Give the dive a site name.');
    if (!f.date) return fail('Pick a date.');
    if (f.maxdepth == null || !(f.maxdepth > 0)) return fail('Enter the maximum depth.');
    if (f.minutes == null || !(f.minutes > 0)) return fail('Enter how long the dive lasted.');
    if (f.avgdepth != null && f.avgdepth > f.maxdepth) {
      return fail('Average depth cannot be deeper than the maximum.');
    }
    if (f.start != null && f.end != null && f.end > f.start) {
      return fail('End pressure is higher than start pressure.');
    }

    function fail(text) { msg.textContent = text; return false; }

    const o2 = f.o2 == null ? 21 : f.o2;
    const he = f.he == null ? 0 : f.he;

    const row = {
      site_name: f.site_name,
      site_lat: f.site_lat, site_lon: f.site_lon,
      date: f.date,
      time: f.time ? `${f.time}:00` : '00:00:00',
      maxdepth: f.maxdepth,
      avgdepth: f.avgdepth == null ? 0 : f.avgdepth,
      duration: Math.round(f.minutes * 60),
      divemode: f.divemode || 'OC',
      temp_min: f.temp_min,
      temp_max: f.temp_min,          // one reading: same value both ways
      temp_surface: f.temp_surface,
      salinity_type: f.salinity_type === '' ? null : Number(f.salinity_type),
      gasmixes: [{ o2, he, n2: Math.max(0, 100 - o2 - he), usage: 0 }],
      tanks: (f.volume != null || f.start != null || f.end != null)
        ? [{ gasmix: 0, volume: f.volume || 0, workpressure: 0,
             start: f.start || 0, end: f.end || 0, usage: 0 }]
        : [],
      // A hand-logged dive has no sample data; keep the column well-formed.
      samples: dive ? DL.asArray(dive.samples) : [],
      device_name: dive ? dive.device_name : 'Logged by hand',
    };

    const save = $('f-save');
    save.disabled = true;
    save.textContent = editing ? 'Saving…' : 'Logging…';
    msg.textContent = '';

    try {
      if (editing) {
        await DL.updateDive(dive.id, row);
        Object.assign(dive, row);
        DL.toast('Dive updated', 'ok');
      } else {
        const created = await DL.createDive(row);
        // Fall back to the local row if the insert returned nothing.
        DL.state.dives.unshift(created || { ...row, id: null });
        DL.state.dives.sort((a, b) =>
          (b.date + (b.time || '')).localeCompare(a.date + (a.time || '')));
        DL.toast('Dive logged', 'ok');
      }

      saveDefaults({ divemode: row.divemode, o2, volume: f.volume,
                     salinity_type: f.salinity_type });
      close();

      if (editing) DL.showDive(dive.id);
      else await DL.navigate('dives');
    } catch (err) {
      save.disabled = false;
      save.textContent = editing ? 'Save changes' : 'Log dive';
      msg.textContent = err.message;
    }
  });
};
})();
