/* The screens: dive log, one dive, site discovery, one site, statistics. */
'use strict';

(() => {
const DL = window.DL;

// Dive numbers count up from the oldest, matching the phone app.
function diveNumbers() {
  const order = new Map();
  [...DL.state.dives]
    .sort((a, b) => (a.date + (a.time || '')).localeCompare(b.date + (b.time || '')))
    .forEach((d, i) => order.set(d, i + 1));
  return order;
}
DL.diveNumbers = diveNumbers;

const byId = (id) => DL.state.dives.find((d) => String(d.id) === String(id));

// ── Dive log (home) ────────────────────────────────────────────────────────

DL.filters = { dives: '' };

DL.renderDives = async () => {
  await DL.loadCatalogue();
  const view = DL.el('view-dives');
  const dives = DL.state.dives;

  if (!dives.length) {
    view.innerHTML = `
      <h1 class="page-title">Your diving</h1>
      ${DL.emptyState({
        icon: 'wave',
        title: 'No dives yet',
        detail: 'Your next adventure starts here. Log a dive by hand, or download one from your computer in the phone app.',
        action: '<button class="btn btn-primary" data-act="log">Log your first dive</button>',
      })}`;
    view.querySelector('[data-act="log"]').addEventListener('click', () => DL.openLogDive());
    return;
  }

  const totalTime = dives.reduce((a, d) => a + (Number(d.duration) || 0), 0);
  const deepest = Math.max(...dives.map((d) => Number(d.maxdepth) || 0));
  const last = dives.map((d) => d.date).filter(Boolean).sort().slice(-1)[0];

  const q = DL.fold(DL.filters.dives);
  const shown = q
    ? dives.filter((d) => [d.site_name, d.date, d.device_name, DL.placeOf(d)]
        .some((f) => DL.fold(f || '').includes(q)))
    : dives;

  const order = diveNumbers();

  view.innerHTML = `
    <h1 class="page-title">Your diving</h1>
    <div style="margin:16px 0 4px">
      ${DL.summaryStrip([
        { label: 'Total dives', value: dives.length, hero: true },
        { label: 'Total time',  value: DL.longDuration(totalTime) },
        { label: 'Max depth',   value: DL.num(deepest), unit: ' m' },
        { label: 'Last dive',   value: last ? DL.prettyDate(last) : '–' },
      ])}
    </div>

    ${DL.sectionHead('Recent dives',
      `<span class="count tnum">${shown.length}${shown.length !== dives.length ? ` of ${dives.length}` : ''}</span>`)}

    <div class="search" style="margin-bottom:14px">
      ${DL.icon('search')}
      <input class="input" id="dive-search" type="search"
             placeholder="Search site, place or date…" value="${DL.esc(DL.filters.dives)}">
    </div>

    <div class="stack" id="dive-list">
      ${shown.length
        ? shown.map((d) => DL.diveCard(d, order.get(d))).join('')
        : DL.emptyState({ icon: 'search', title: 'Nothing matches',
                          detail: `No dive matches “${DL.filters.dives}”.` })}
    </div>`;

  const search = DL.el('dive-search');
  search.addEventListener('input', async (e) => {
    DL.filters.dives = e.target.value;
    // The render must finish before the caret is restored: it replaces the
    // input, and focusing the old node would silently drop the keyboard.
    await DL.renderDives();
    const next = DL.el('dive-search');
    if (next) {
      next.focus();
      next.setSelectionRange(next.value.length, next.value.length);
    }
  });

  view.querySelectorAll('[data-dive]').forEach((card) => {
    const open = () => DL.showDive(card.dataset.dive);
    card.addEventListener('click', open);
    card.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open(); }
    });
  });
};

// ── One dive ───────────────────────────────────────────────────────────────

DL.showDive = (id) => {
  const d = byId(id);
  if (!d) return DL.go('dives');

  const order = diveNumbers();
  const site = DL.siteOf(d);
  const place = DL.placeOf(d);
  const gases = DL.asArray(d.gasmixes);
  const tanks = DL.asArray(d.tanks);
  const temp = d.temp_min != null ? d.temp_min : d.temp_max;
  const pos = DL.coordsFor(d);

  const gasName = (g) => {
    const o2 = Math.round(Number(g.o2) || 0), he = Math.round(Number(g.he) || 0);
    if (he > 0) return `Tx ${o2}/${he}`;
    return (!o2 || o2 === 21) ? 'Air' : `EAN${o2}`;
  };

  DL.el('view-dive').innerHTML = `
    ${DL.backButton('Back to dives', 'dives')}

    <div class="row wrap" style="align-items:flex-start;gap:12px">
      <div class="grow">
        <h1 class="page-title">${site ? DL.esc(site) : 'Unnamed site'}</h1>
        ${place ? `<div class="small" style="margin-top:4px">${DL.esc(place)}</div>` : ''}
        <div class="small">${DL.longDate(d.date)}${d.time ? ` · ${DL.hhmm(d.time)}` : ''}${
          order.get(d) ? ` · Dive #${order.get(d)}` : ''}</div>
      </div>
      <div class="row" style="gap:8px">
        <button class="btn btn-ghost btn-sm" data-act="edit">${DL.icon('edit')} Edit</button>
        <button class="btn btn-ghost btn-sm btn-danger" data-act="delete">${DL.icon('trash')} Delete</button>
      </div>
    </div>

    <div class="metrics-grid" style="margin:24px 0 26px">
      ${DL.metric({ label: 'Max depth', value: DL.num(d.maxdepth), unit: 'm', big: true })}
      ${DL.metric({ label: 'Duration', value: DL.duration(d.duration), big: true })}
      ${DL.metric({ label: 'Water', value: temp != null ? DL.num(temp) : '–',
                    unit: temp != null ? '°C' : '', big: true })}
      ${DL.metric({ label: 'Gas', value: DL.gasLabel(d.gasmixes) || '–', big: true })}
    </div>

    ${DL.sectionHead('Profile')}
    ${DL.diveProfile(d.samples, d.maxdepth)}

    ${DL.sectionHead('Details')}
    <div class="metrics-grid">
      ${DL.metric({ label: 'Average depth', value: DL.num(d.avgdepth), unit: 'm' })}
      ${DL.metric({ label: 'Dive mode', value: DL.esc(d.divemode || 'OC') })}
      ${d.temp_surface != null ? DL.metric({ label: 'Air', value: DL.num(d.temp_surface), unit: '°C' }) : ''}
      ${d.temp_max != null ? DL.metric({ label: 'Warmest', value: DL.num(d.temp_max), unit: '°C' }) : ''}
      ${d.atmospheric ? DL.metric({ label: 'Atmospheric', value: DL.num(d.atmospheric, 3), unit: 'bar' }) : ''}
      ${d.salinity_type != null ? DL.metric({ label: 'Water type',
          value: Number(d.salinity_type) === 1 ? 'Salt' : 'Fresh' }) : ''}
      ${d.device_name ? DL.metric({ label: 'Computer', value: DL.esc(d.device_name) }) : ''}
    </div>

    ${gases.length ? `${DL.sectionHead('Gas')}
      <div class="scroll-x"><table class="table">
        <thead><tr><th>Mix</th><th>O₂</th><th>He</th><th>N₂</th></tr></thead>
        <tbody>${gases.map((g) => `<tr>
          <td>${gasName(g)}</td><td>${DL.num(g.o2)} %</td>
          <td>${DL.num(g.he)} %</td><td>${DL.num(g.n2)} %</td></tr>`).join('')}
        </tbody></table></div>` : ''}

    ${tanks.length ? `${DL.sectionHead('Cylinders')}
      <div class="scroll-x"><table class="table">
        <thead><tr><th>Size</th><th>Working</th><th>Start</th><th>End</th><th>Used</th></tr></thead>
        <tbody>${tanks.map((t) => {
          const used = (Number(t.start) || 0) - (Number(t.end) || 0);
          return `<tr><td>${DL.num(t.volume)} L</td><td>${DL.num(t.workpressure, 0)} bar</td>
            <td>${DL.num(t.start, 0)} bar</td><td>${DL.num(t.end, 0)} bar</td>
            <td>${used > 0 ? DL.num(used, 0) + ' bar' : '–'}</td></tr>`;
        }).join('')}</tbody></table></div>` : ''}

    ${DL.sectionHead('Dive site')}
    <div class="card" style="padding:16px">
      <div class="row wrap">
        <div class="grow">
          <div class="card-title">${site ? DL.esc(site) : 'No site set'}</div>
          <div class="small">${pos
            ? `${DL.num(pos.lat, 5)}, ${DL.num(pos.lon, 5)}${pos.fromCatalogue ? ' · from the site catalogue' : ''}`
            : 'No position, so this dive is not on the map'}</div>
        </div>
        <div class="row" style="gap:8px">
          ${site ? `<button class="btn btn-ghost btn-sm" data-act="site">View site</button>` : ''}
          <button class="btn btn-ghost btn-sm" data-act="setsite">${site ? 'Change' : 'Set site'}</button>
        </div>
      </div>
    </div>`;

  const view = DL.el('view-dive');
  view.querySelector('[data-act="edit"]').addEventListener('click', () => DL.openLogDive(d));
  view.querySelector('[data-act="delete"]').addEventListener('click', async () => {
    const ok = await DL.confirmSheet({
      title: 'Delete this dive?',
      detail: `${site || 'Unnamed site'} on ${DL.prettyDate(d.date)} will be removed permanently.`,
      confirmLabel: 'Delete', danger: true,
    });
    if (!ok) return;
    try {
      await DL.deleteDive(d.id);
      DL.state.dives = DL.state.dives.filter((x) => x !== d);
      DL.toast('Dive deleted', 'ok');
      await DL.navigate('dives');     // re-render, or the deleted card lingers
    } catch (e) { DL.toast(e.message, 'err'); }
  });
  const siteBtn = view.querySelector('[data-act="site"]');
  if (siteBtn) siteBtn.addEventListener('click', () => DL.showSite(site));
  view.querySelector('[data-act="setsite"]').addEventListener('click', () => {
    DL.pickSite(1, async (chosen) => {
      try {
        await DL.applySiteTo([d], chosen);
        DL.toast(`Site set to ${chosen.name}`, 'ok');
        DL.showDive(d.id);
      } catch (e) { DL.toast(e.message, 'err'); }
    });
  });

  DL.go('dive');
};

// ── Sites ──────────────────────────────────────────────────────────────────

DL.filters.sites = { q: '', kind: 'all' };

/**
 * Filters are built from the data rather than hard-coded: the Finnish
 * catalogue has Quarry, Mine and Lake types that a fixed list would leave
 * unreachable, and a worldwide-only log should not show empty Finnish types.
 */
function siteFilters(rows) {
  const counts = new Map();
  for (const r of rows) {
    const t = DL.siteType(r.entry);
    if (t) counts.set(t, (counts.get(t) || 0) + 1);
  }
  const types = [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 8)
    .map(([t]) => [t, t]);

  return [['all', 'All'], ['near', 'Nearby'], ['dived', 'Dived'], ['not', 'Not dived'], ...types];
}

/** Set by the Nearby filter once the browser gives up a position. */
let here = null;

function distanceKm(a, b) {
  const dLat = (a.lat - b.lat) * 111.32;
  const dLon = (a.lon - b.lon) * 111.32 * Math.cos(a.lat * Math.PI / 180);
  return Math.hypot(dLat, dLon);
}

DL.renderSites = async () => {
  await DL.loadCatalogue();
  const view = DL.el('view-sites');
  const groups = DL.groupBySite();

  // Everything you have dived, plus the whole catalogue, as one list.
  const rows = new Map();
  for (const [name, ds] of groups) {
    const r = DL.siteRow(name, ds);
    rows.set(DL.fold(name), {
      name, count: ds.length, deepest: r.deepest, last: r.last,
      entry: DL.catalogueEntry(name),
    });
  }
  for (const e of DL.catalogueSites) {
    if ((e.kind || 'site') !== 'site') continue;      // clubs live on the map
    const k = DL.fold(e.name);
    if (!rows.has(k)) rows.set(k, { name: e.name, count: 0, deepest: 0, last: null, entry: e });
  }

  const { q, kind } = DL.filters.sites;
  const f = DL.fold(q);
  let list = [...rows.values()].filter((s) => {
    if (kind === 'dived' && !s.count) return false;
    if (kind === 'not' && s.count) return false;
    if (kind === 'near') {
      if (!here || !s.entry) return false;
      if (distanceKm(here, s.entry) > 150) return false;
    }
    if (!['all', 'near', 'dived', 'not'].includes(kind) && DL.siteType(s.entry) !== kind) return false;
    if (!f) return true;
    return DL.fold(s.name).includes(f) ||
           DL.fold((s.entry && s.entry.country) || '').includes(f) ||
           DL.fold((s.entry && s.entry.region) || '').includes(f);
  });

  if (kind === 'near' && here) {
    list.sort((a, b) => distanceKm(here, a.entry) - distanceKm(here, b.entry));
  } else {
    // Your own sites first, then the catalogue alphabetically.
    list.sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  }
  const capped = list.slice(0, 300);
  const dived = [...rows.values()].filter((s) => s.count).length;

  view.innerHTML = `
    <h1 class="page-title">Explore dive sites</h1>
    <div class="small" style="margin-top:6px">
      ${rows.size.toLocaleString()} sites known · ${dived} dived by you
    </div>

    <div class="search" style="margin:18px 0 12px">
      ${DL.icon('search')}
      <input class="input" id="site-search" type="search"
             placeholder="Search sites, countries or regions…" value="${DL.esc(q)}">
    </div>

    <div class="filters" role="tablist">
      ${siteFilters([...rows.values()]).map(([v, label]) =>
        `<button class="filter${kind === v ? ' active' : ''}" data-filter="${v}">${label}</button>`).join('')}
    </div>

    <div class="stack" style="margin-top:16px">
      ${capped.length
        ? capped.map(DL.siteCard).join('')
        : DL.emptyState({ icon: 'search', title: 'No sites match',
                          detail: kind === 'near'
                            ? 'No known dive sites within 150 km of you.'
                            : 'Try a different search or filter.' })}
    </div>
    ${list.length > capped.length
      ? `<div class="small" style="text-align:center;margin-top:16px">
           Showing ${capped.length} of ${list.length.toLocaleString()} — narrow the search to see more.</div>`
      : ''}`;

  const search = DL.el('site-search');
  search.addEventListener('input', async (e) => {
    DL.filters.sites.q = e.target.value;
    await DL.renderSites();
    const next = DL.el('site-search');
    if (next) {
      next.focus();
      next.setSelectionRange(next.value.length, next.value.length);
    }
  });

  view.querySelectorAll('[data-filter]').forEach((b) => {
    b.addEventListener('click', async () => {
      if (b.dataset.filter === 'near' && !here) {
        if (!navigator.geolocation) return DL.toast('This browser cannot share a location', 'err');
        DL.toast('Finding your location…');
        try {
          const pos = await new Promise((res, rej) =>
            navigator.geolocation.getCurrentPosition(res, rej, { timeout: 10000 }));
          here = { lat: pos.coords.latitude, lon: pos.coords.longitude };
        } catch {
          return DL.toast('Location unavailable — allow it to use Nearby', 'err');
        }
      }
      DL.filters.sites.kind = b.dataset.filter;
      await DL.renderSites();
    });
  });
  view.querySelectorAll('[data-site]').forEach((card) => {
    const open = () => DL.showSite(card.dataset.site);
    card.addEventListener('click', open);
    card.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open(); }
    });
  });
};

// ── One site ───────────────────────────────────────────────────────────────

DL.showSite = (name) => {
  const ds = DL.divesAtSite(name);
  const row = DL.siteRow(name, ds);
  const dived = ds.length > 0;
  const entry = DL.catalogueEntry(name);

  const temps = ds.flatMap((d) => [d.temp_min, d.temp_max])
    .filter((t) => t != null && isFinite(t)).map(Number);
  const totalTime = ds.reduce((a, d) => a + (Number(d.duration) || 0), 0);
  const pos = dived ? DL.coordsFor(ds[0])
            : entry ? { lat: entry.lat, lon: entry.lon, fromCatalogue: true } : null;
  const where = entry ? [entry.region, entry.country].filter(Boolean).join(', ') : '';
  const type = DL.siteType(entry);

  DL.el('view-site').innerHTML = `
    ${DL.backButton('Back to sites', 'sites')}

    <h1 class="page-title">${DL.esc(name)}</h1>
    <div class="row wrap" style="gap:8px;margin-top:8px">
      ${where ? `<span class="small">${DL.esc(where)}</span>` : ''}
      ${type ? `<span class="chip type">${DL.esc(type)}</span>` : ''}
      ${dived
        ? `<span class="chip dived">${DL.icon('check')} Dived ${ds.length} time${ds.length === 1 ? '' : 's'}</span>`
        : '<span class="chip">Not dived yet</span>'}
    </div>

    ${dived ? `
      <div class="summary" style="margin:20px 0 4px">
        <div class="cell hero"><div class="k">Dives here</div><div class="v tnum">${ds.length}</div></div>
        <div class="cell"><div class="k">Deepest</div><div class="v tnum">${DL.num(row.deepest)}<small> m</small></div></div>
        <div class="cell"><div class="k">Total time</div><div class="v tnum">${DL.longDuration(totalTime)}</div></div>
        <div class="cell"><div class="k">Last dived</div><div class="v tnum">${DL.prettyDate(row.last)}</div></div>
      </div>

      ${DL.sectionHead('Temperature profile')}
      ${DL.siteProfile(ds)}

      <div class="metrics-grid" style="margin-top:20px">
        ${temps.length ? DL.metric({ label: 'Coldest', value: DL.num(Math.min(...temps)), unit: '°C' }) : ''}
        ${temps.length ? DL.metric({ label: 'Warmest', value: DL.num(Math.max(...temps)), unit: '°C' }) : ''}
        ${DL.metric({ label: 'Average depth',
          value: DL.num(ds.reduce((a, d) => a + (Number(d.maxdepth) || 0), 0) / ds.length), unit: 'm' })}
      </div>

      ${DL.sectionHead('Dives here')}
      <div class="scroll-x"><table class="table">
        <thead><tr><th>Date</th><th>Depth</th><th>Duration</th><th>Water</th></tr></thead>
        <tbody>${[...ds].sort((a, b) => (b.date || '').localeCompare(a.date || '')).map((d) => `
          <tr class="tappable" data-dive="${DL.esc(String(d.id))}">
            <td>${DL.prettyDate(d.date)}</td><td>${DL.num(d.maxdepth)} m</td>
            <td>${DL.duration(d.duration)}</td>
            <td>${d.temp_min != null ? DL.num(d.temp_min) + ' °C' : '–'}</td></tr>`).join('')}
        </tbody></table></div>
    ` : `
      <div style="margin-top:24px">
        ${DL.emptyState({
          icon: 'pin', title: 'No dives at this site yet',
          detail: 'When you log a dive here it will appear with its profile and temperatures.',
          action: '<button class="btn btn-primary" data-act="log">Log a dive here</button>',
        })}
      </div>`}

    ${entry && entry.notes ? `${DL.sectionHead('About this site')}
      <div class="journal">${DL.esc(entry.notes)}</div>` : ''}

    ${pos ? `<div class="small" style="margin-top:18px">
      ${DL.num(pos.lat, 5)}, ${DL.num(pos.lon, 5)} ·
      <a href="https://www.openstreetmap.org/?mlat=${pos.lat}&mlon=${pos.lon}#map=14/${pos.lat}/${pos.lon}"
         target="_blank" rel="noopener">open in OpenStreetMap</a>
      ${entry && entry.source ? ` · <a href="${DL.esc(entry.source)}" target="_blank" rel="noopener">source</a>` : ''}
    </div>` : ''}`;

  const view = DL.el('view-site');
  const logBtn = view.querySelector('[data-act="log"]');
  if (logBtn) {
    logBtn.addEventListener('click', () => DL.openLogDive(null, {
      site_name: name,
      site_lat: pos ? pos.lat : null,
      site_lon: pos ? pos.lon : null,
    }));
  }
  view.querySelectorAll('tr[data-dive]').forEach((tr) =>
    tr.addEventListener('click', () => DL.showDive(tr.dataset.dive)));

  DL.go('site');
};

// ── Statistics ─────────────────────────────────────────────────────────────

DL.filters.stats = 'overview';

const STAT_TABS = [
  ['overview', 'Overview'], ['depth', 'Depth'],
  ['temp', 'Temperature'], ['gas', 'Gas'], ['places', 'Locations'],
];

DL.renderStats = async () => {
  await DL.loadCatalogue();
  const view = DL.el('view-stats');
  const dives = DL.state.dives;

  if (!dives.length) {
    view.innerHTML = `<h1 class="page-title">Statistics</h1>
      ${DL.emptyState({ icon: 'chart', title: 'Nothing to measure yet',
        detail: 'Log a few dives and your progress will show up here.',
        action: '<button class="btn btn-primary" data-act="log">Log your first dive</button>' })}`;
    view.querySelector('[data-act="log"]').addEventListener('click', () => DL.openLogDive());
    return;
  }

  // Oldest first, so every trend reads left to right in time.
  const chrono = [...dives].sort((a, b) =>
    (a.date + (a.time || '')).localeCompare(b.date + (b.time || '')));

  const totalTime = dives.reduce((a, d) => a + (Number(d.duration) || 0), 0);
  const deepest = Math.max(...dives.map((d) => Number(d.maxdepth) || 0));
  const avgDepth = dives.reduce((a, d) => a + (Number(d.maxdepth) || 0), 0) / dives.length;
  const sites = new Set(dives.map(DL.siteOf).filter(Boolean));
  const countries = new Set(dives.map((d) => {
    const e = DL.catalogueEntry(DL.siteOf(d) || '');
    return e && e.country;
  }).filter(Boolean));

  const label = (d) => DL.prettyDate(d.date);
  const tab = DL.filters.stats;

  const counted = (keyFn) => {
    const m = new Map();
    for (const d of dives) {
      const k = keyFn(d);
      if (!k) continue;
      m.set(k, (m.get(k) || 0) + 1);
    }
    return [...m.entries()].map(([k, v]) => ({ label: k, value: v }))
      .sort((a, b) => b.value - a.value);
  };

  let body = '';
  if (tab === 'overview') {
    const perMonth = new Map();
    for (const d of chrono) {
      const k = (d.date || '').slice(0, 7);
      if (k) perMonth.set(k, (perMonth.get(k) || 0) + 1);
    }
    const months = [...perMonth.entries()].slice(-18).map(([k, v]) => ({
      label: new Date(k + '-01T00:00:00').toLocaleDateString('en-GB',
        { month: 'short', year: '2-digit' }),
      value: v,
    }));
    body = `
      ${DL.sectionHead('Dives per month')}
      ${DL.barList(months)}
      ${DL.sectionHead('Depth over time')}
      ${DL.trendChart(chrono.map((d) => ({ v: Number(d.maxdepth), label: label(d) })),
                      { unit: 'm', label: 'Maximum depth' })}`;
  } else if (tab === 'depth') {
    body = `
      ${DL.sectionHead('Maximum depth over time')}
      ${DL.trendChart(chrono.map((d) => ({ v: Number(d.maxdepth), label: label(d) })),
                      { unit: 'm', label: 'Maximum depth' })}
      ${DL.sectionHead('Dive duration over time')}
      ${DL.trendChart(chrono.map((d) => ({ v: (Number(d.duration) || 0) / 60, label: label(d) })),
                      { unit: 'min', label: 'Duration' })}`;
  } else if (tab === 'temp') {
    const withTemp = chrono.filter((d) => d.temp_min != null);
    body = withTemp.length > 1 ? `
      ${DL.sectionHead('Water temperature over time')}
      ${DL.trendChart(withTemp.map((d) => ({ v: Number(d.temp_min), label: label(d) })),
                      { unit: '°C', colour: '#F2B866', label: 'Water temperature' })}`
      : DL.emptyState({ icon: 'temp', title: 'No temperatures recorded',
                        detail: 'Dives downloaded from a computer usually carry water temperature.' });
  } else if (tab === 'gas') {
    const rows = counted((d) => DL.gasLabel(d.gasmixes));
    body = rows.length
      ? `${DL.sectionHead('Dives by gas')}${DL.barList(rows)}`
      : DL.emptyState({ icon: 'gas', title: 'No gas recorded',
                        detail: 'Gas mixes come from the dive computer download.' });
  } else {
    const byCountry = counted((d) => {
      const e = DL.catalogueEntry(DL.siteOf(d) || '');
      return e && e.country;
    });
    const bySite = counted((d) => DL.siteOf(d));
    body = `
      ${byCountry.length ? `${DL.sectionHead('Dives by country')}${DL.barList(byCountry)}` : ''}
      ${bySite.length ? `${DL.sectionHead('Dives by site')}${DL.barList(bySite.slice(0, 15))}`
        : DL.emptyState({ icon: 'pin', title: 'No sites set',
                          detail: 'Set sites on your dives to see where you dive most.' })}`;
  }

  view.innerHTML = `
    <h1 class="page-title">Statistics</h1>

    <div style="margin:16px 0 20px">
      ${DL.summaryStrip([
        { label: 'Dives', value: dives.length, hero: true },
        { label: 'Total time', value: DL.longDuration(totalTime) },
        { label: 'Max depth', value: DL.num(deepest), unit: ' m' },
        { label: 'Avg depth', value: DL.num(avgDepth), unit: ' m' },
        { label: 'Sites', value: sites.size },
        { label: 'Countries', value: countries.size },
      ])}
    </div>

    <div class="filters">
      ${STAT_TABS.map(([v, l]) =>
        `<button class="filter${tab === v ? ' active' : ''}" data-tab="${v}">${l}</button>`).join('')}
    </div>

    <div style="margin-top:4px">${body}</div>`;

  view.querySelectorAll('[data-tab]').forEach((b) =>
    b.addEventListener('click', () => { DL.filters.stats = b.dataset.tab; DL.renderStats(); }));
};
})();
