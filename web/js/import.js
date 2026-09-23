/* Reading dive logs exported from other services.
 *
 * Shearwater Cloud, the Suunto app and the rest have no public API a web page
 * may sign in to, so the way in is the file they export. These parsers take
 * what those exports actually contain and turn it into the same dive rows
 * DeepLog stores itself — one format per function, no dependencies. */
'use strict';

(() => {
const DL = window.DL;

// ── Small helpers ──────────────────────────────────────────────────────────

/** Kelvin, Fahrenheit and Celsius all appear in the wild; normalise to °C. */
const toCelsius = (v, unit = '') => {
  const n = Number(v);
  if (!isFinite(n) || n === 0) return null;
  if (/f/i.test(unit)) return Math.round(((n - 32) * 5 / 9) * 10) / 10;
  if (/k/i.test(unit) || (!unit && n > 150)) return Math.round((n - 273.15) * 10) / 10;
  return Math.round(n * 10) / 10;
};

const toMetres = (v, unit = '') => {
  const n = Number(v);
  if (!isFinite(n)) return null;
  return /ft|feet/i.test(unit) ? Math.round(n * 0.3048 * 100) / 100 : Math.round(n * 100) / 100;
};

/** "45:00", "45:00 min", "2700 s" and "45" all mean a dive length. */
const toSeconds = (raw) => {
  const s = String(raw == null ? '' : raw).trim();
  if (!s) return null;
  const clock = s.match(/^(\d+):(\d{1,2})(?::(\d{1,2}))?/);
  if (clock) {
    const [, a, b, c] = clock;
    return c ? (+a * 3600 + +b * 60 + +c) : (+a * 60 + +b);
  }
  const n = parseFloat(s.replace(',', '.'));
  if (!isFinite(n)) return null;
  if (/\bs(ec)?\b/i.test(s)) return Math.round(n);
  if (/\bh(our)?/i.test(s)) return Math.round(n * 3600);
  return Math.round(n * 60);                       // a bare number means minutes
};

/** Local date and time as the log recorded them, never shifted by a timezone. */
const splitDateTime = (raw) => {
  const s = String(raw == null ? '' : raw).trim();
  let m = s.match(/(\d{4})-(\d{2})-(\d{2})[T ]?(\d{2})?:?(\d{2})?:?(\d{2})?/);
  if (!m) {
    // 31/08/2026 or 08/31/2026 — day first unless that cannot be a day.
    const d = s.match(/(\d{1,2})[./-](\d{1,2})[./-](\d{4})\D*(\d{1,2})?:?(\d{2})?:?(\d{2})?/);
    if (!d) return null;
    const dayFirst = +d[1] > 12 || +d[2] <= 12;
    const day = dayFirst ? d[1] : d[2], mon = dayFirst ? d[2] : d[1];
    m = [null, d[3], mon, day, d[4], d[5], d[6]];
  }
  const p = (v, n = 2) => String(v == null ? 0 : +v).padStart(n, '0');
  return {
    date: `${p(m[1], 4)}-${p(m[2])}-${p(m[3])}`,
    time: `${p(m[4])}:${p(m[5])}:${p(m[6])}`,
  };
};

/** A dive row with every column the table expects, so inserts stay uniform. */
const diveRow = (v) => ({
  date: v.date,
  time: v.time || '00:00:00',
  maxdepth: v.maxdepth == null ? 0 : v.maxdepth,
  avgdepth: v.avgdepth == null ? 0 : v.avgdepth,
  duration: v.duration == null ? 0 : v.duration,
  divemode: v.divemode || 'OC',
  temp_min: v.temp_min == null ? null : v.temp_min,
  temp_max: v.temp_max == null ? v.temp_min : v.temp_max,
  temp_surface: v.temp_surface == null ? null : v.temp_surface,
  atmospheric: v.atmospheric == null ? null : v.atmospheric,
  salinity_type: v.salinity_type == null ? null : v.salinity_type,
  gasmixes: v.gasmixes || [],
  tanks: v.tanks || [],
  samples: v.samples || [],
  site_name: v.site_name || '',
  site_lat: v.site_lat == null ? null : v.site_lat,
  site_lon: v.site_lon == null ? null : v.site_lon,
  device_name: v.device_name || 'Imported',
});

/** Depth and temperature summarised from the profile when the file omits them. */
const fromSamples = (row) => {
  const depths = row.samples.map((s) => s[1]).filter((d) => isFinite(d));
  const temps = row.samples.map((s) => s[2]).filter((c) => isFinite(c) && c !== 0);
  if (!row.maxdepth && depths.length) row.maxdepth = Math.round(Math.max(...depths) * 100) / 100;
  if (!row.avgdepth && depths.length) {
    row.avgdepth = Math.round((depths.reduce((a, d) => a + d, 0) / depths.length) * 100) / 100;
  }
  if (!row.duration && row.samples.length) {
    row.duration = Math.round(Math.max(...row.samples.map((s) => s[0])) / 1000);
  }
  if (row.temp_min == null && temps.length) row.temp_min = Math.min(...temps);
  if (row.temp_max == null && temps.length) row.temp_max = Math.max(...temps);
  return row;
};

// XML is easier to read by local name: these files come with and without
// namespaces, and Suunto nests the same names at different depths.
const kids = (node, name) => [...node.children].filter((c) => c.localName === name);
const kid = (node, name) => kids(node, name)[0] || null;
const deep = (node, name) => [...node.getElementsByTagName('*')].filter((c) => c.localName === name);
const textOf = (node, name) => {
  const el = node && (kid(node, name) || deep(node, name)[0]);
  return el ? el.textContent.trim() : '';
};

/** "25.1 m", "8.2 C" — Subsurface writes the unit alongside the number. */
const valueUnit = (raw) => {
  const s = String(raw == null ? '' : raw).trim();
  const m = s.match(/(-?[\d.]+)\s*([^\d\s]*)/);
  return m ? { n: parseFloat(m[1]), unit: m[2] || '' } : { n: NaN, unit: '' };
};

// ── UDDF: Shearwater Cloud, Suunto DM5, Subsurface, MacDive ────────────────

function parseUddf(doc) {
  const root = doc.documentElement;

  const sites = new Map();
  for (const site of deep(root, 'site')) {
    const geo = kid(site, 'geography');
    sites.set(site.getAttribute('id'), {
      name: textOf(site, 'name'),
      lat: geo ? parseFloat(textOf(geo, 'latitude')) : NaN,
      lon: geo ? parseFloat(textOf(geo, 'longitude')) : NaN,
    });
  }

  const mixes = new Map();
  for (const mix of deep(root, 'mix')) {
    const frac = (name) => {
      const v = parseFloat(textOf(mix, name));
      if (!isFinite(v)) return 0;
      return Math.round(v <= 1 ? v * 100 : v);     // fraction or per cent
    };
    mixes.set(mix.getAttribute('id'), { o2: frac('o2') || 21, he: frac('he') });
  }

  const rows = [];
  for (const dive of deep(root, 'dive')) {
    const before = kid(dive, 'informationbeforedive');
    const after = kid(dive, 'informationafterdive');
    const when = splitDateTime(textOf(before, 'datetime') || dive.getAttribute('datetime'));
    if (!when) continue;

    const samples = [];
    for (const wp of deep(dive, 'waypoint')) {
      const t = parseFloat(textOf(wp, 'divetime'));
      const d = parseFloat(textOf(wp, 'depth'));
      if (!isFinite(t) || !isFinite(d)) continue;
      samples.push([Math.round(t * 1000), Math.round(d * 100) / 100,
                    toCelsius(textOf(wp, 'temperature'))]);
    }

    let site = null;
    for (const link of [...(before ? deep(before, 'link') : [])]) {
      const ref = link.getAttribute('ref');
      if (sites.has(ref)) { site = sites.get(ref); break; }
    }

    const gas = [...mixes.values()].slice(0, 1).map((m) => ({
      o2: m.o2, he: m.he, n2: Math.max(0, 100 - m.o2 - m.he), usage: 0,
    }));

    rows.push(fromSamples(diveRow({
      ...when,
      maxdepth: parseFloat(textOf(after, 'greatestdepth')) || null,
      avgdepth: parseFloat(textOf(after, 'averagedepth')) || null,
      duration: Math.round(parseFloat(textOf(after, 'diveduration'))) || null,
      temp_min: toCelsius(textOf(after, 'lowesttemperature')),
      temp_surface: toCelsius(textOf(before, 'airtemperature')),
      gasmixes: gas,
      samples,
      site_name: site ? site.name : '',
      site_lat: site && isFinite(site.lat) ? site.lat : null,
      site_lon: site && isFinite(site.lon) ? site.lon : null,
      device_name: 'Imported from UDDF',
    })));
  }
  return rows;
}

// ── Subsurface XML (.ssrf, .xml) ───────────────────────────────────────────

function parseSubsurface(doc) {
  const root = doc.documentElement;

  const sites = new Map();
  for (const site of deep(root, 'site')) {
    const gps = (site.getAttribute('gps') || '').split(/\s+/).map(Number);
    sites.set(site.getAttribute('uuid'), {
      name: site.getAttribute('name') || '',
      lat: gps.length === 2 && isFinite(gps[0]) ? gps[0] : null,
      lon: gps.length === 2 && isFinite(gps[1]) ? gps[1] : null,
    });
  }

  const rows = [];
  for (const dive of deep(root, 'dive')) {
    const when = splitDateTime(`${dive.getAttribute('date') || ''} ${dive.getAttribute('time') || ''}`);
    if (!when) continue;
    const dc = kid(dive, 'divecomputer');
    const depth = dc ? kid(dc, 'depth') : null;
    const water = dc ? kid(dc, 'temperature') : kid(dive, 'temperature');

    const samples = [];
    for (const s of dive.getElementsByTagName('sample')) {
      const t = toSeconds(s.getAttribute('time'));
      const d = valueUnit(s.getAttribute('depth'));
      if (t == null || !isFinite(d.n)) continue;
      const c = s.getAttribute('temp');
      const cv = c ? valueUnit(c) : null;
      samples.push([t * 1000, toMetres(d.n, d.unit), cv ? toCelsius(cv.n, cv.unit) : null]);
    }
    // Only some samples carry a temperature; carry the last one forward so the
    // profile is coloured all the way down rather than in stripes.
    let carried = null;
    for (const s of samples) { if (s[2] == null) s[2] = carried; else carried = s[2]; }

    const maxd = depth ? valueUnit(depth.getAttribute('max')) : { n: NaN, unit: '' };
    const meand = depth ? valueUnit(depth.getAttribute('mean')) : { n: NaN, unit: '' };
    const wt = water ? valueUnit(water.getAttribute('water')) : { n: NaN, unit: '' };
    const at = water ? valueUnit(water.getAttribute('air')) : { n: NaN, unit: '' };
    const site = sites.get(dive.getAttribute('divesiteid'));

    const gases = kids(dive, 'cylinder').map((c) => {
      const o2 = parseFloat(c.getAttribute('o2')) || 21;
      const he = parseFloat(c.getAttribute('he')) || 0;
      return { o2: Math.round(o2), he: Math.round(he), n2: Math.max(0, 100 - o2 - he), usage: 0 };
    });

    rows.push(fromSamples(diveRow({
      ...when,
      maxdepth: isFinite(maxd.n) ? toMetres(maxd.n, maxd.unit) : null,
      avgdepth: isFinite(meand.n) ? toMetres(meand.n, meand.unit) : null,
      duration: toSeconds(dive.getAttribute('duration')),
      temp_min: isFinite(wt.n) ? toCelsius(wt.n, wt.unit) : null,
      temp_surface: isFinite(at.n) ? toCelsius(at.n, at.unit) : null,
      gasmixes: gases,
      samples,
      site_name: site ? site.name : '',
      site_lat: site ? site.lat : null,
      site_lon: site ? site.lon : null,
      device_name: (dc && dc.getAttribute('model')) || 'Imported from Subsurface',
    })));
  }
  return rows;
}

// ── Suunto SML (.sml, and the .sde archive of them) ────────────────────────

function parseSuuntoSml(doc) {
  const rows = [];
  for (const log of deep(doc.documentElement, 'DeviceLog')) {
    const header = kid(log, 'Header');
    if (!header) continue;
    const when = splitDateTime(textOf(header, 'DateTime'));
    if (!when) continue;

    const depth = kid(header, 'Depth');
    const samples = [];
    for (const s of deep(log, 'Sample')) {
      const t = parseFloat(textOf(s, 'Time'));
      const d = parseFloat(textOf(s, 'Depth'));
      if (!isFinite(t) || !isFinite(d)) continue;
      samples.push([Math.round(t * 1000), Math.round(d * 100) / 100,
                    toCelsius(textOf(s, 'Temperature'))]);
    }

    const device = kid(log, 'Device');
    rows.push(fromSamples(diveRow({
      ...when,
      maxdepth: depth ? parseFloat(textOf(depth, 'Max')) : null,
      avgdepth: depth ? parseFloat(textOf(depth, 'Avg')) : null,
      duration: Math.round(parseFloat(textOf(header, 'Duration'))) || null,
      temp_min: toCelsius(textOf(header, 'MinTemperature') || textOf(header, 'Temperature')),
      temp_surface: toCelsius(textOf(header, 'SurfaceTemperature')),
      samples,
      device_name: device ? (textOf(device, 'Name') || 'Suunto') : 'Suunto',
    })));
  }
  return rows;
}

// ── CSV: Shearwater Cloud's dive-list export, and most other logs ──────────

/** A CSV row splitter that respects quotes and doubled quotes inside them. */
function csvRows(text) {
  const rows = [];
  let row = [], field = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; } else quoted = false;
      } else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ',' || c === ';' || c === '\t') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field || row.length) { row.push(field); rows.push(row); }
  return rows.filter((r) => r.some((f) => f.trim() !== ''));
}

function parseCsv(text) {
  const rows = csvRows(text);
  if (rows.length < 2) return [];

  // The header is the first row that names a date column; Shearwater Cloud
  // and others put a title line above it.
  const headerAt = rows.findIndex((r) => r.some((c) => /date/i.test(c)));
  if (headerAt < 0) return [];
  const head = rows[headerAt].map((h) => h.trim());
  const find = (re) => head.findIndex((h) => re.test(h));

  const col = {
    date: find(/^(dive\s*)?date|date\/time|start\s*time/i),
    time: find(/^(start\s*)?time\b(?!.*zone)/i),
    maxdepth: find(/max.*depth|depth.*max|^depth/i),
    avgdepth: find(/av(g|erage).*depth/i),
    duration: find(/duration|dive\s*time|bottom\s*time|runtime/i),
    tempMin: find(/min.*temp|temp.*min|water.*temp|bottom.*temp/i),
    tempMax: find(/max.*temp|temp.*max|surface.*temp/i),
    site: find(/site|location|place|dive\s*spot/i),
    device: find(/computer|device|model/i),
  };
  if (col.date < 0) return [];

  const unitOf = (i) => (i >= 0 ? (head[i].match(/\(([^)]+)\)|\b(ft|feet|m|°?[CF])\b/i) || [])[0] || '' : '');
  const cell = (r, i) => (i >= 0 && i < r.length ? r[i].trim() : '');

  const out = [];
  for (const r of rows.slice(headerAt + 1)) {
    const dateCell = cell(r, col.date);
    const timeCell = cell(r, col.time);
    const when = splitDateTime(timeCell && !/[a-z]{3}/i.test(dateCell)
      ? `${dateCell} ${timeCell}` : dateCell);
    if (!when || !/^\d{4}-\d{2}-\d{2}$/.test(when.date)) continue;

    const depth = cell(r, col.maxdepth);
    const tmin = cell(r, col.tempMin);
    const tmax = cell(r, col.tempMax);
    out.push(diveRow({
      ...when,
      maxdepth: depth ? toMetres(parseFloat(depth), unitOf(col.maxdepth)) : null,
      avgdepth: cell(r, col.avgdepth)
        ? toMetres(parseFloat(cell(r, col.avgdepth)), unitOf(col.avgdepth)) : null,
      duration: toSeconds(cell(r, col.duration)),
      temp_min: tmin ? toCelsius(parseFloat(tmin), unitOf(col.tempMin)) : null,
      temp_max: tmax ? toCelsius(parseFloat(tmax), unitOf(col.tempMax)) : null,
      site_name: cell(r, col.site),
      device_name: cell(r, col.device) || 'Imported from CSV',
    }));
  }
  return out;
}

// ── Zip (.sde from Suunto DM5, and any zipped export) ──────────────────────

/**
 * Reads a zip's central directory and inflates what it needs. Only the two
 * methods these exports use are supported: stored, and deflate through the
 * browser's own DecompressionStream.
 */
async function unzip(buffer) {
  const dv = new DataView(buffer);
  const bytes = new Uint8Array(buffer);
  let end = -1;
  for (let i = dv.byteLength - 22; i >= 0 && i > dv.byteLength - 66000; i--) {
    if (dv.getUint32(i, true) === 0x06054b50) { end = i; break; }
  }
  if (end < 0) throw new Error('That zip file could not be read.');

  const count = dv.getUint16(end + 10, true);
  let p = dv.getUint32(end + 16, true);
  const files = [];
  for (let i = 0; i < count; i++) {
    if (dv.getUint32(p, true) !== 0x02014b50) break;
    const method = dv.getUint16(p + 10, true);
    const compressed = dv.getUint32(p + 20, true);
    const nameLen = dv.getUint16(p + 28, true);
    const extraLen = dv.getUint16(p + 30, true);
    const commentLen = dv.getUint16(p + 32, true);
    const offset = dv.getUint32(p + 42, true);
    const name = new TextDecoder().decode(bytes.subarray(p + 46, p + 46 + nameLen));
    files.push({ name, method, compressed, offset });
    p += 46 + nameLen + extraLen + commentLen;
  }

  const out = [];
  for (const f of files) {
    if (f.name.endsWith('/')) continue;
    const nameLen = dv.getUint16(f.offset + 26, true);
    const extraLen = dv.getUint16(f.offset + 28, true);
    const start = f.offset + 30 + nameLen + extraLen;
    const raw = bytes.subarray(start, start + f.compressed);
    if (f.method === 0) { out.push({ name: f.name, text: new TextDecoder().decode(raw) }); continue; }
    if (f.method !== 8) continue;
    if (typeof DecompressionStream !== 'function') {
      throw new Error('This browser cannot open zipped exports; unzip the file first.');
    }
    const stream = new Blob([raw]).stream().pipeThrough(new DecompressionStream('deflate-raw'));
    out.push({ name: f.name, text: await new Response(stream).text() });
  }
  return out;
}

// ── One file in, dives out ─────────────────────────────────────────────────

function parseText(name, text) {
  const trimmed = text.replace(/^﻿/, '').trimStart();
  if (trimmed.startsWith('<')) {
    const doc = new DOMParser().parseFromString(trimmed, 'application/xml');
    if (doc.querySelector('parsererror')) throw new Error('That XML file could not be read.');
    const root = doc.documentElement.localName.toLowerCase();
    if (root === 'uddf') return { format: 'UDDF', dives: parseUddf(doc) };
    if (root === 'divelog' || root === 'subsurface') {
      return { format: 'Subsurface', dives: parseSubsurface(doc) };
    }
    if (root === 'sml' || deep(doc.documentElement, 'DeviceLog').length) {
      return { format: 'Suunto SML', dives: parseSuuntoSml(doc) };
    }
    throw new Error(`Unsupported XML (<${root}>) — export as UDDF instead.`);
  }
  if (/\.csv$|\.txt$/i.test(name) || /[,;\t].*[,;\t]/.test(trimmed.split('\n')[0] || '')) {
    return { format: 'CSV', dives: parseCsv(trimmed) };
  }
  throw new Error('That file is not a dive log DeepLog can read.');
}

// Blob.text() and Blob.arrayBuffer() are missing in some browsers still in
// use, and in the test runner; FileReader is there everywhere.
const readAs = (file, how) => new Promise((resolve, reject) => {
  if (typeof file[how === 'buffer' ? 'arrayBuffer' : 'text'] === 'function') {
    return resolve(file[how === 'buffer' ? 'arrayBuffer' : 'text']());
  }
  const fr = new FileReader();
  fr.onload = () => resolve(fr.result);
  fr.onerror = () => reject(fr.error || new Error(`Could not read ${file.name}`));
  if (how === 'buffer') fr.readAsArrayBuffer(file); else fr.readAsText(file);
});

/** Reads one dropped or chosen file. Zips are opened and their parts merged. */
DL.parseDiveFile = async (file) => {
  const name = file.name || 'file';
  if (/\.(sde|zip)$/i.test(name)) {
    const parts = await unzip(await readAs(file, 'buffer'));
    const dives = [];
    const formats = new Set();
    for (const part of parts) {
      if (!/\.(sml|xml|uddf|ssrf|csv)$/i.test(part.name)) continue;
      try {
        const r = parseText(part.name, part.text);
        formats.add(r.format);
        dives.push(...r.dives);
      } catch { /* a stray file in the archive is not an error */ }
    }
    if (!dives.length) throw new Error('No dives found inside that archive.');
    return { format: [...formats].join(' + ') || 'archive', dives };
  }
  if (/\.(db|sqlite|slg|fit)$/i.test(name)) {
    throw new Error(`DeepLog cannot read ${name.split('.').pop().toUpperCase()} files. ` +
                    'Export your dives as UDDF or CSV instead.');
  }
  return parseText(name, await readAs(file, 'text'));
};

DL.importHelp = [
  ['Shearwater Cloud',
   'Open Shearwater Cloud on a computer, select the dives, then File → Export → UDDF ' +
   '(or CSV). Import the saved file here.'],
  ['Suunto',
   'In Suunto DM5, File → Export and choose UDDF, or the .sde archive. ' +
   'A single dive exported from the Suunto app as .sml works too.'],
  ['Subsurface',
   'File → Export → Subsurface XML for everything, or UDDF for a selection.'],
  ['Anything else',
   'Most logs export UDDF or CSV. A CSV needs at least a date column; depth, ' +
   'duration, temperature and site are used when present.'],
];
})();

// ── The import dialog ──────────────────────────────────────────────────────

(() => {
const DL = window.DL;

const keyOf = (d) => `${d.date}_${String(d.time || '').slice(0, 5)}`;

/**
 * Pick files (or drop them), see what was found, then write the new dives to
 * the database. Dives already in the log are listed but not imported again,
 * so importing the same export twice changes nothing.
 */
DL.openImport = () => {
  let parsed = [];          // {row, key, duplicate}
  let busy = false;

  const close = DL.openSheet(`
    <h3 class="section-title">Import dives</h3>
    <p class="small" style="margin:8px 0 14px">
      Shearwater Cloud, Suunto and the others have no sign-in for other apps,
      so bring your dives across as a file they export.
    </p>

    <label class="drop" id="im-drop">
      <input type="file" id="im-file" multiple
             accept=".uddf,.xml,.ssrf,.sml,.sde,.zip,.csv,.txt" hidden>
      <span class="drop-main">Choose files</span>
      <span class="small">or drag them here — UDDF, Subsurface XML, Suunto SML/SDE, CSV</span>
    </label>

    <div id="im-status" class="small" style="margin:12px 0" role="status"></div>
    <div id="im-preview"></div>

    <details style="margin-top:14px">
      <summary class="small">How to export from each service</summary>
      <div style="margin-top:8px">
        ${DL.importHelp.map(([name, how]) => `
          <p class="small" style="margin:6px 0"><strong>${DL.esc(name)}:</strong> ${DL.esc(how)}</p>`).join('')}
      </div>
    </details>

    <div class="row" style="justify-content:flex-end;margin-top:18px">
      <button class="btn btn-ghost" data-sheet-close>Close</button>
      <button class="btn btn-primary" id="im-go" disabled>Import</button>
    </div>`);

  const host = DL.el('sheet-host');
  const status = host.querySelector('#im-status');
  const preview = host.querySelector('#im-preview');
  const go = host.querySelector('#im-go');
  const drop = host.querySelector('#im-drop');
  const input = host.querySelector('#im-file');

  const show = (text, kind = '') => {
    status.textContent = text;
    status.className = `small ${kind}`;
  };

  const review = async (files) => {
    if (busy || !files.length) return;
    parsed = []; preview.innerHTML = ''; go.disabled = true;
    show(`Reading ${files.length} file${files.length === 1 ? '' : 's'}…`);

    const seen = new Set(DL.state.dives.map(keyOf));
    const problems = [];
    const formats = new Set();
    for (const file of files) {
      try {
        const { format, dives } = await DL.parseDiveFile(file);
        formats.add(format);
        for (const row of dives) {
          const key = keyOf(row);
          // A file can hold the same dive twice, and so can two files.
          const duplicate = seen.has(key);
          seen.add(key);
          parsed.push({ row, key, duplicate });
        }
      } catch (e) {
        problems.push(`${file.name}: ${e.message}`);
      }
    }

    const fresh = parsed.filter((p) => !p.duplicate);
    const dupes = parsed.length - fresh.length;
    if (!parsed.length) {
      show(problems.join(' · ') || 'No dives found in those files.', 'err');
      return;
    }
    show([
      `${parsed.length} dive${parsed.length === 1 ? '' : 's'} found`,
      formats.size ? `(${[...formats].join(', ')})` : '',
      dupes ? `· ${dupes} already in your log` : '',
      problems.length ? `· ${problems.join(' · ')}` : '',
    ].filter(Boolean).join(' '));

    preview.innerHTML = `<div class="scroll-x"><table class="table">
      <thead><tr><th>Date</th><th>Depth</th><th>Duration</th><th>Water</th><th></th></tr></thead>
      <tbody>${parsed.slice(0, 12).map((p) => `
        <tr${p.duplicate ? ' class="muted"' : ''}>
          <td>${DL.prettyDate(p.row.date)} ${DL.esc(String(p.row.time).slice(0, 5))}</td>
          <td>${DL.num(p.row.maxdepth)} m</td>
          <td>${DL.duration(p.row.duration)}</td>
          <td>${p.row.temp_min != null ? DL.num(p.row.temp_min) + ' °C' : '–'}</td>
          <td class="small">${p.duplicate ? 'already in log' : (p.row.samples.length ? 'with profile' : '')}</td>
        </tr>`).join('')}
      </tbody></table></div>
      ${parsed.length > 12 ? `<p class="small">…and ${parsed.length - 12} more</p>` : ''}`;

    go.disabled = fresh.length === 0;
    go.textContent = fresh.length ? `Import ${fresh.length} dive${fresh.length === 1 ? '' : 's'}`
                                  : 'Nothing new to import';
  };

  input.addEventListener('change', () => review([...input.files]));
  drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('over'); });
  drop.addEventListener('dragleave', () => drop.classList.remove('over'));
  drop.addEventListener('drop', (e) => {
    e.preventDefault(); drop.classList.remove('over');
    review([...e.dataTransfer.files]);
  });

  go.addEventListener('click', async () => {
    const todo = parsed.filter((p) => !p.duplicate);
    if (!todo.length || busy) return;
    busy = true; go.disabled = true;

    let done = 0;
    const failed = [];
    for (const p of todo) {
      show(`Importing ${done + 1} of ${todo.length}…`);
      try { await DL.createDive(p.row); done++; }
      catch (e) { failed.push(`${p.row.date}: ${e.message}`); }
    }

    busy = false;
    DL.state.dives = await DL.fetchDives().catch(() => DL.state.dives);
    if (failed.length) {
      go.disabled = false;
      show(`Imported ${done} of ${todo.length}. ${failed[0]}`, 'err');
      return;
    }
    close();
    DL.toast(`Imported ${done} dive${done === 1 ? '' : 's'}`, 'ok');
    await DL.navigate('dives');
  });
};
})();
