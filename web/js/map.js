/* The map: the whole known catalogue plus your own dives, with a floating
   search, layer chips and a site sheet instead of a cramped popup. */
'use strict';

(() => {
const DL = window.DL;

let map = null;
let catalogueLayer = null;    // every known site from the bundled catalogues
let divedLayer = null;        // the places you have actually logged dives
let fitted = false;

const layers = { mine: true, sites: true, services: false };

const STYLE = {
  site:    { radius: 4.5, color: '#7FD4A8', fillColor: '#3E8C6B', weight: 1 },
  club:    { radius: 3.5, color: '#9FB6CC', fillColor: '#4E6C86', weight: 1 },
  lodging: { radius: 3.5, color: '#E0C07A', fillColor: '#8E7233', weight: 1 },
};
const MINE = { radius: 8, color: '#07141F', weight: 2.5, fillColor: '#39C6E8', fillOpacity: 1 };

DL.renderMap = async () => {
  await DL.loadCatalogue();
  const view = DL.el('view-map');

  // Build the shell once; re-entering the tab must not throw the map away.
  if (!view.dataset.ready) {
    view.innerHTML = `
      <div class="map-wrap">
        <div class="map-overlay">
          <div class="search">
            ${DL.icon('search')}
            <input class="input" id="map-search" type="search" placeholder="Search dive sites…">
          </div>
          <div class="map-layers">
            <button class="layer-chip mine on" data-layer="mine"><span class="dot"></span>My dives</button>
            <button class="layer-chip on" data-layer="sites"><span class="dot" style="background:#3E8C6B"></span>Dive sites</button>
            <button class="layer-chip" data-layer="services"><span class="dot" style="background:#4E6C86"></span>Clubs &amp; stays</button>
          </div>
        </div>
        <div id="map"></div>
      </div>
      <div class="map-note" id="map-note"></div>`;
    view.dataset.ready = '1';

    view.querySelectorAll('[data-layer]').forEach((chip) => {
      chip.addEventListener('click', () => {
        const k = chip.dataset.layer;
        layers[k] = !layers[k];
        chip.classList.toggle('on', layers[k]);
        if (k === 'mine') drawMine(); else drawCatalogue();
      });
    });

    view.querySelector('#map-search').addEventListener('keydown', (e) => {
      if (e.key !== 'Enter') return;
      jumpTo(e.target.value);
    });
  }

  if (!map) {
    // The default zoom control sits top-left, underneath the floating search
    // box; move it out of the way rather than crowding the overlay.
    map = L.map('map', { scrollWheelZoom: true, preferCanvas: true, zoomControl: false })
      .setView([30, 12], 2);
    L.control.zoom({ position: 'bottomright' }).addTo(map);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    }).addTo(map);
    catalogueLayer = L.layerGroup().addTo(map);
    divedLayer = L.layerGroup().addTo(map);
    map.on('moveend', drawCatalogue);
  }

  drawMine();
  drawCatalogue();

  // Leaflet mis-measures a container that was hidden when it was created.
  setTimeout(() => map.invalidateSize(), 60);
};

/** Where each dive sits, grouped into one marker per site. */
function placedSites() {
  const placed = [];
  let named = 0, unnamed = 0;
  for (const d of DL.state.dives) {
    const c = DL.coordsFor(d);
    if (c) placed.push({ dive: d, ...c });
    else if (DL.siteOf(d)) named++;
    else unnamed++;
  }

  const bySite = new Map();
  for (const p of placed) {
    const key = `${DL.siteOf(p.dive) || ''}@${p.lat.toFixed(5)},${p.lon.toFixed(5)}`;
    if (!bySite.has(key)) {
      bySite.set(key, { name: DL.siteOf(p.dive), lat: p.lat, lon: p.lon, dives: [] });
    }
    bySite.get(key).dives.push(p.dive);
  }
  return { bySite, placed, named, unnamed };
}

function drawMine() {
  if (!divedLayer) return;
  divedLayer.clearLayers();
  const { bySite, placed, named, unnamed } = placedSites();

  const points = [];
  if (layers.mine) {
    for (const site of bySite.values()) {
      L.circleMarker([site.lat, site.lon], MINE)
        .addTo(divedLayer)
        .on('click', () => openSiteSheet(site.name, site));
      points.push([site.lat, site.lon]);
    }
  }

  // Open on your own diving the first time, then leave the view alone.
  if (points.length && !fitted) {
    map.fitBounds(points, { padding: [44, 44], maxZoom: 9 });
    fitted = true;
  }
  note(bySite.size, placed.length, named, unnamed);
}

function drawCatalogue() {
  if (!catalogueLayer) return;
  catalogueLayer.clearLayers();

  // Canvas circles are cheap, so the whole catalogue can draw at world zoom;
  // the cap guards against the file growing, rather than a limit we hit.
  const CAP = 4000;
  const b = map.getBounds();
  const tally = { site: 0, club: 0, lodging: 0 };
  let inView = 0, drawn = 0;

  for (const s of DL.catalogueSites) {
    const kind = s.kind || 'site';
    if (kind === 'site' ? !layers.sites : !layers.services) continue;
    if (!b.contains([s.lat, s.lon])) continue;
    inView++;
    if (drawn >= CAP) continue;
    drawn++;
    tally[kind] = (tally[kind] || 0) + 1;

    L.circleMarker([s.lat, s.lon], { ...(STYLE[kind] || STYLE.site), fillOpacity: .82 })
      .addTo(catalogueLayer)
      .on('click', () => openSiteSheet(s.name, null, s));
  }
  count(drawn, inView, tally);
}

function count(drawn, inView, tally) {
  const el = DL.el('map-note');
  if (!el) return;
  const bits = [];
  if (tally.site) bits.push(`${tally.site} dive site${tally.site === 1 ? '' : 's'}`);
  if (tally.club) bits.push(`${tally.club} club${tally.club === 1 ? '' : 's'}`);
  if (tally.lodging) bits.push(`${tally.lodging} stay${tally.lodging === 1 ? '' : 's'}`);
  el.dataset.inView = bits.length
    ? `${bits.join(', ')} in view${inView > drawn ? ` (of ${inView} — zoom in for the rest)` : ''}`
    : 'nothing in view';
  paintNote();
}

function note(siteCount, placedCount, named, unnamed) {
  const el = DL.el('map-note');
  if (!el) return;
  const nSites = DL.catalogueSites.filter((s) => (s.kind || 'site') === 'site').length;
  const nFin = DL.catalogueSites.filter((s) => s.src === 'mymaps').length;

  const parts = [`${nSites.toLocaleString()} known dive sites — ${nFin} Finnish ones from the ` +
                 `DeepLog Google My Maps map, the rest from OpenStreetMap (ODbL).`];
  if (siteCount) parts.push(`You have dived ${siteCount} of them across ${placedCount} dives.`);
  else parts.push('None of your own dives are placed yet.');
  if (named) parts.push(`${named} dive(s) name a site that is not in the catalogue and have no saved position.`);
  if (unnamed) parts.push(`${unnamed} dive(s) have no site — open one and use “Set site”.`);
  el.dataset.summary = parts.join(' ');
  paintNote();
}

function paintNote() {
  const el = DL.el('map-note');
  if (!el) return;
  el.textContent = [el.dataset.inView, el.dataset.summary].filter(Boolean).join(' · ');
}

/** Compact preview: a bottom sheet on a phone, a centred card on desktop. */
function openSiteSheet(name, mine = null, entry = null) {
  const ds = name ? DL.divesAtSite(name) : [];
  const e = entry || (name ? DL.catalogueEntry(name) : null);
  const where = e ? [e.region, e.country].filter(Boolean).join(', ') : '';
  const type = DL.siteType(e);
  const deepest = ds.length ? Math.max(...ds.map((d) => Number(d.maxdepth) || 0)) : 0;
  const pos = mine || (e ? { lat: e.lat, lon: e.lon } : null);

  DL.openSheet(`
    <h3 class="section-title">${DL.esc(name || 'Unnamed site')}</h3>
    <div class="row wrap" style="gap:8px;margin-top:8px">
      ${where ? `<span class="small">${DL.esc(where)}</span>` : ''}
      ${type ? `<span class="chip type">${DL.esc(type)}</span>` : ''}
    </div>

    ${ds.length ? `
      <div class="metrics-grid" style="margin:18px 0 20px">
        ${DL.metric({ label: 'Your dives', value: ds.length })}
        ${DL.metric({ label: 'Deepest', value: DL.num(deepest), unit: 'm' })}
        ${DL.metric({ label: 'Last dived', value: DL.prettyDate(ds.map((d) => d.date).sort().slice(-1)[0]) })}
      </div>
      <button class="btn btn-primary" style="width:100%" data-act="view">View site</button>
    ` : `
      <p class="small" style="margin:16px 0 20px">Not dived yet.${
        pos ? ` ${DL.num(pos.lat, 4)}, ${DL.num(pos.lon, 4)}` : ''}</p>
      <div class="row" style="gap:10px">
        <button class="btn btn-ghost grow" data-act="view">View site</button>
        <button class="btn btn-primary grow" data-act="log">Log a dive</button>
      </div>`}`);

  const host = DL.el('sheet-host');
  const view = host.querySelector('[data-act="view"]');
  if (view) view.addEventListener('click', () => { DL.closeSheet(); DL.showSite(name); });
  const log = host.querySelector('[data-act="log"]');
  if (log) {
    log.addEventListener('click', () => {
      DL.closeSheet();
      DL.openLogDive(null, {
        site_name: name,
        site_lat: pos ? pos.lat : null,
        site_lon: pos ? pos.lon : null,
      });
    });
  }
}

/** Jump the map to a named site, or fit it to a whole country. */
function jumpTo(query) {
  const q = DL.fold(query);
  if (!q || !map) return;

  const byName = DL.catalogueSites.filter((s) => DL.fold(s.name).includes(q));
  const byPlace = DL.catalogueSites.filter((s) =>
    DL.fold(s.country || '').includes(q) || DL.fold(s.region || '').includes(q));
  const hits = byName.length ? byName : byPlace;

  if (!hits.length) return DL.toast(`Nothing in the catalogue matches “${query}”`, 'err');
  if (hits.length === 1) map.setView([hits[0].lat, hits[0].lon], 13);
  else map.fitBounds(hits.map((h) => [h.lat, h.lon]), { padding: [40, 40] });
  drawCatalogue();
}

/** Forgets the fitted-once flag, so a fresh sign-in re-centres. */
DL.resetMap = () => { fitted = false; };
})();
