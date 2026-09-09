/* The bundled dive-site catalogues, and how a dive is matched to a place.
   Logic unchanged from before the redesign; only the packaging moved. */
'use strict';

(() => {
const DL = window.DL;

DL.catalogue = null;        // folded name -> { lat, lon }
DL.catalogueSites = [];     // full entries, for the map, search and the picker

/**
 * Two catalogues are merged:
 *
 *  - dive_sites.json — OpenStreetMap, worldwide, nearly empty over Finland
 *  - finnish_sites.json — the DeepLog Google My Maps map, which is where the
 *    Finnish lakes, quarries, mines and Baltic wrecks actually are
 *
 * Where both describe the same place the My Maps entry wins: it is curated and
 * carries a description and a source link.
 */
DL.loadCatalogue = async function loadCatalogue() {
  if (DL.catalogue) return DL.catalogue;
  DL.catalogue = new Map();

  const grab = async (file, src) => {
    try {
      const res = await fetch(file);
      if (!res.ok) throw new Error(String(res.status));
      return ((await res.json()).sites || []).map((s) => ({ ...s, src }));
    } catch {
      return [];      // a missing catalogue must not break the page
    }
  };

  const [osm, fin] = await Promise.all([
    grab('dive_sites.json', 'osm'),
    grab('finnish_sites.json', 'mymaps'),
  ]);

  // Position alone cannot decide a duplicate: the sources pin Ojamon 200 m
  // apart, while distinct reef sites can be closer than that. So a duplicate
  // needs proximity *and* a shared word in the name.
  const metres = (a, b) => {
    const dLat = (a.lat - b.lat) * 111320;
    const dLon = (a.lon - b.lon) * 111320 * Math.cos(a.lat * Math.PI / 180);
    return Math.hypot(dLat, dLon);
  };
  const words = (n) => new Set(DL.fold(n).split(/[^a-z0-9]+/).filter((w) => w.length >= 4));
  const sameName = (a, b) => {
    const wa = words(a.name), wb = words(b.name);
    for (const w of wa) if (wb.has(w)) return true;
    return false;
  };
  const duplicate = (o, f) =>
    metres(o, f) < 150 || (metres(o, f) < 500 && sameName(o, f));

  DL.catalogueSites = fin.concat(osm.filter((o) => !fin.some((f) => duplicate(o, f))));
  for (const s of DL.catalogueSites) {
    const k = DL.fold(s.name);
    if (k && !DL.catalogue.has(k)) DL.catalogue.set(k, { lat: s.lat, lon: s.lon });
  }
  return DL.catalogue;
};

/**
 * Where a dive happened: its own coordinates when it has them, otherwise the
 * catalogue's for that site name. The phone app only stores coordinates when
 * the site was already saved, so most dives rely on the lookup.
 */
DL.coordsFor = (d) => {
  if (d.site_lat != null && d.site_lon != null) {
    return { lat: +d.site_lat, lon: +d.site_lon, fromCatalogue: false };
  }
  const hit = DL.catalogue && d.site_name && DL.catalogue.get(DL.fold(d.site_name));
  return hit ? { lat: hit.lat, lon: hit.lon, fromCatalogue: true } : null;
};

/** The catalogue entry for a site name, if there is one. */
DL.catalogueEntry = (name) =>
  DL.catalogueSites.find((c) => DL.fold(c.name) === DL.fold(name)) || null;

/** Country / region for a dive, from the catalogue where the row is silent. */
DL.placeOf = (d) => {
  const name = DL.siteOf(d);
  if (!name) return null;
  const e = DL.catalogueEntry(name);
  if (!e) return null;
  return [e.region, e.country].filter(Boolean).join(', ') || null;
};

/** Dives grouped by site name, in the order they arrived (newest first). */
DL.groupBySite = () => {
  const groups = new Map();
  for (const d of DL.state.dives) {
    const name = DL.siteOf(d);
    if (!name) continue;
    if (!groups.has(name)) groups.set(name, []);
    groups.get(name).push(d);
  }
  return groups;
};

/** The shape the site view expects; `ds` may be empty for a site never dived. */
DL.siteRow = (name, ds) => ({
  name, ds,
  deepest: ds.length ? Math.max(...ds.map((d) => Number(d.maxdepth) || 0)) : 0,
  last: ds.length ? ds.map((d) => d.date).sort().slice(-1)[0] : null,
  located: ds.length ? !!DL.coordsFor(ds[0]) : false,
});

/** Every dive a site has, matched loosely so spelling variants still land. */
DL.divesAtSite = (name) => {
  const groups = DL.groupBySite();
  const exact = groups.get(name);
  if (exact) return exact;
  const target = DL.fold(name);
  for (const [k, v] of groups) if (DL.fold(k) === target) return v;
  return [];
};

/**
 * The site "type" chip: Wreck, Quarry, Reef, Cave, Shore, Boat, Lake, Mine.
 * The two sources describe type differently, so both are normalised here.
 */
DL.siteType = (entry) => {
  if (!entry) return null;
  const d = DL.fold([entry.desc, entry.name].filter(Boolean).join(' '));
  if (/wreck|hylky/.test(d)) return 'Wreck';
  if (/cave|luola/.test(d)) return 'Cave';
  if (/quarry|louhos/.test(d)) return 'Quarry';
  if (/mine|kaivos/.test(d)) return 'Mine';
  if (/reef/.test(d)) return 'Reef';
  if (/boat|vene/.test(d)) return 'Boat';
  if (/shore|beach|rant/.test(d)) return 'Shore';
  if (/lake|jarvi/.test(d)) return 'Lake';
  if (/club|seura/.test(d)) return 'Club';
  if (/accommodation|majoitus/.test(d)) return 'Stay';
  return entry.desc || null;
};
})();
