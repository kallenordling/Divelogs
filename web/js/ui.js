/* Reusable pieces of interface. Each returns an HTML string; views compose
   them and wire behaviour afterwards. */
'use strict';

(() => {
const DL = window.DL;

// ── Small parts ────────────────────────────────────────────────────────────

/** A labelled measurement. `icon` is optional and used sparingly. */
DL.metric = ({ label, value, unit = '', icon = null, big = false }) => `
  <div class="metric${big ? ' metric-lg' : ''}">
    <div class="k">${icon ? DL.icon(icon) : ''}${DL.esc(label)}</div>
    <div class="v tnum">${value}${unit ? `<small> ${DL.esc(unit)}</small>` : ''}</div>
  </div>`;

DL.sectionHead = (title, right = '') => `
  <div class="section-head">
    <h2 class="section-title">${DL.esc(title)}</h2>
    <span class="grow"></span>
    ${right}
  </div>`;

DL.emptyState = ({ icon = 'wave', title, detail = '', action = '' }) => `
  <div class="empty">
    <div class="glyph">${DL.icon(icon)}</div>
    <div class="t">${DL.esc(title)}</div>
    ${detail ? `<div class="d">${DL.esc(detail)}</div>` : ''}
    ${action}
  </div>`;

DL.backButton = (label, target) =>
  `<button class="btn btn-quiet" data-back="${DL.esc(target)}" style="margin-bottom:14px">
     ${DL.icon('back')} ${DL.esc(label)}</button>`;

/** The four-up summary under a page title. */
DL.summaryStrip = (cells) => `
  <div class="summary">
    ${cells.map((c) => `
      <div class="cell${c.hero ? ' hero' : ''}">
        <div class="k">${DL.esc(c.label)}</div>
        <div class="v tnum">${c.value}${c.unit ? `<small>${DL.esc(c.unit)}</small>` : ''}</div>
      </div>`).join('')}
  </div>`;

// ── Dive card ──────────────────────────────────────────────────────────────

/**
 * One dive in the log. Ordered site → date → depth → duration → temperature →
 * gas, so the eye lands on where and how deep before anything else.
 */
DL.diveCard = (d, index) => {
  const site = DL.siteOf(d);
  const place = DL.placeOf(d);
  const gas = DL.gasLabel(d.gasmixes);
  const temp = d.temp_min != null ? d.temp_min : d.temp_max;

  return `
  <article class="card tappable dive-card" data-dive="${DL.esc(String(d.id ?? index))}"
           tabindex="0" role="button">
    <div class="head">
      <div class="grow">
        <div class="site">${site ? DL.esc(site) : 'Unnamed site'}</div>
        ${place ? `<div class="place">${DL.esc(place)}</div>` : ''}
        <div class="when">${DL.prettyDate(d.date)}${
          d.time ? ` · ${DL.hhmm(d.time)}` : ''}${
          d.divemode && d.divemode !== 'OC' ? ` · ${DL.esc(d.divemode)}` : ''}</div>
      </div>
      ${index != null ? `<span class="badge-n tnum">#${index}</span>` : ''}
    </div>

    <div class="stats">
      ${DL.metric({ label: 'Depth', value: DL.num(d.maxdepth), unit: 'm', icon: 'depth' })}
      ${DL.metric({ label: 'Time', value: DL.duration(d.duration), icon: 'clock' })}
      ${DL.metric({ label: 'Water', value: temp != null ? DL.num(temp) : '–',
                    unit: temp != null ? '°C' : '', icon: 'temp' })}
      ${DL.metric({ label: 'Gas', value: gas ? DL.esc(gas) : '–', icon: 'gas' })}
    </div>

    ${DL.sparkProfile(d.samples)}
  </article>`;
};

// ── Site card ──────────────────────────────────────────────────────────────

/**
 * A site in the catalogue or the log. Sites already dived carry a green
 * "Dived N times" chip, which is what turns the list into a sense of progress.
 */
DL.siteCard = (s) => {
  const type = DL.siteType(s.entry);
  const where = [s.entry && s.entry.region, s.entry && s.entry.country]
    .filter(Boolean).join(', ');

  return `
  <article class="card tappable site-card" data-site="${DL.esc(s.name)}" tabindex="0" role="button">
    <div class="grow">
      <div class="name">${DL.esc(s.name)}</div>
      ${where ? `<div class="where">${DL.esc(where)}</div>` : ''}
      <div class="row wrap" style="gap:6px;margin-top:6px">
        ${type ? `<span class="chip type">${DL.esc(type)}</span>` : ''}
        ${s.count
          ? `<span class="chip dived">${DL.icon('check')} Dived ${s.count} time${s.count === 1 ? '' : 's'}</span>`
          : '<span class="chip">Not dived</span>'}
      </div>
    </div>
    ${s.count ? `<div class="right">
      <div class="metric"><div class="k">Deepest</div>
        <div class="v tnum">${DL.num(s.deepest)}<small> m</small></div></div>
    </div>` : ''}
  </article>`;
};

// ── Sheet ──────────────────────────────────────────────────────────────────

/**
 * A bottom sheet on a phone, a centred card on a wider screen. Used for the
 * map's site preview, the site picker and confirmations.
 */
DL.openSheet = (html, { onClose = null } = {}) => {
  const host = DL.el('sheet-host');
  host.innerHTML = `
    <div class="sheet-backdrop" data-sheet-close></div>
    <div class="sheet" role="dialog" aria-modal="true">
      <div class="grabber"></div>
      ${html}
    </div>`;

  const close = () => {
    host.innerHTML = '';
    document.removeEventListener('keydown', onKey);
    if (onClose) onClose();
  };
  function onKey(e) { if (e.key === 'Escape') close(); }

  host.querySelectorAll('[data-sheet-close]').forEach((n) =>
    n.addEventListener('click', close));
  document.addEventListener('keydown', onKey);

  // Move focus in, so keyboard and screen-reader users land inside the sheet.
  const focusable = host.querySelector('input, button, select, textarea, a[href]');
  if (focusable) focusable.focus({ preventScroll: true });

  DL.closeSheet = close;
  return close;
};

DL.confirmSheet = ({ title, detail, confirmLabel = 'Confirm', danger = false }) =>
  new Promise((resolve) => {
    let decided = false;
    const close = DL.openSheet(`
      <h3 class="section-title">${DL.esc(title)}</h3>
      <p class="small" style="margin:8px 0 20px">${DL.esc(detail)}</p>
      <div class="row" style="justify-content:flex-end">
        <button class="btn btn-ghost" data-act="no">Cancel</button>
        <button class="btn ${danger ? 'btn-danger' : 'btn-primary'}" data-act="yes">
          ${DL.esc(confirmLabel)}</button>
      </div>`, { onClose: () => { if (!decided) resolve(false); } });

    const host = DL.el('sheet-host');
    host.querySelector('[data-act="no"]').addEventListener('click', () => {
      decided = true; close(); resolve(false);
    });
    host.querySelector('[data-act="yes"]').addEventListener('click', () => {
      decided = true; close(); resolve(true);
    });
  });

// ── Site picker ────────────────────────────────────────────────────────────

/**
 * Search the bundled catalogue, or name a site that is not in it. Used both
 * for assigning sites to existing dives and for choosing one while logging.
 */
DL.pickSite = async (count, onChoose) => {
  await DL.loadCatalogue();
  const entries = DL.catalogueSites;

  const close = DL.openSheet(`
    <h3 class="section-title">${count > 1 ? `Site for ${count} dives` : 'Choose a dive site'}</h3>
    <div class="search" style="margin:14px 0 4px">
      ${DL.icon('search')}
      <input class="input" id="pick-q" type="search"
             placeholder="Search ${entries.length} known sites…">
    </div>
    <div id="pick-list" style="max-height:44vh;overflow-y:auto;margin:0 -6px"></div>
    <details style="margin-top:14px">
      <summary class="small" style="cursor:pointer">Not listed? Enter it yourself</summary>
      <div class="stack" style="gap:10px;margin-top:12px">
        <input class="input" id="pick-name" placeholder="Site name">
        <div class="field-row">
          <input class="input" id="pick-lat" placeholder="Latitude (optional)" inputmode="decimal">
          <input class="input" id="pick-lon" placeholder="Longitude (optional)" inputmode="decimal">
        </div>
        <button class="btn btn-primary" id="pick-custom">Use this site</button>
      </div>
    </details>`);

  const list = DL.el('pick-list');
  let shown = [];

  const draw = (q) => {
    const f = DL.fold(q);
    shown = (f
      ? entries.filter((e) => DL.fold(e.name).includes(f) ||
                              DL.fold(e.country || '').includes(f) ||
                              DL.fold(e.region || '').includes(f))
      : entries).slice(0, 150);

    list.innerHTML = shown.length ? shown.map((e, i) => `
      <button class="btn btn-quiet" data-i="${i}"
              style="width:100%;justify-content:flex-start;text-align:left;padding:10px 12px">
        <span style="display:block">
          <span style="display:block;font-weight:620;color:var(--text)">${DL.esc(e.name)}</span>
          <span class="small" style="display:block">${DL.esc(
            [e.country, e.region, e.desc].filter(Boolean).join(' · ') ||
            `${e.lat.toFixed(4)}, ${e.lon.toFixed(4)}`)}</span>
        </span>
      </button>`).join('')
      : `<div class="small" style="padding:18px 12px">No match — use “Not listed?” below.</div>`;

    list.querySelectorAll('[data-i]').forEach((b) => {
      b.addEventListener('click', () => {
        const e = shown[+b.dataset.i];
        close();
        onChoose({ name: e.name, lat: e.lat, lon: e.lon });
      });
    });
  };
  draw('');

  DL.el('pick-q').addEventListener('input', (e) => draw(e.target.value));
  DL.el('pick-custom').addEventListener('click', () => {
    const name = DL.el('pick-name').value.trim();
    if (!name) return;
    const lat = parseFloat(DL.el('pick-lat').value);
    const lon = parseFloat(DL.el('pick-lon').value);
    close();
    onChoose({ name, lat: isFinite(lat) ? lat : null, lon: isFinite(lon) ? lon : null });
  });
};
})();
