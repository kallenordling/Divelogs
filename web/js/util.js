/* Formatting, escaping and small shared helpers. Loaded first; everything
   else assumes these exist. */
'use strict';

const DL = window.DL || (window.DL = {});

/** Escapes text destined for innerHTML. Used on every value from the database. */
DL.esc = (s) => String(s ?? '').replace(/[&<>"']/g,
  (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

/** A number to `d` places, or an en dash when there is nothing to show. */
DL.num = (v, d = 1) =>
  (v === null || v === undefined || v === '' || isNaN(v)) ? '–' : Number(v).toFixed(d);

/** Seconds as "48 min" / "1h 12m"; the unit travels with the value. */
DL.duration = (seconds) => {
  if (!seconds) return '–';
  const m = Math.round(seconds / 60);
  if (m < 60) return `${m} min`;
  return `${Math.floor(m / 60)}h ${String(m % 60).padStart(2, '0')}m`;
};

/** Seconds as "86h 42m", for totals that run to days. */
DL.longDuration = (seconds) => {
  if (!seconds) return '0h 00m';
  const m = Math.floor(seconds / 60);
  return `${Math.floor(m / 60)}h ${String(m % 60).padStart(2, '0')}m`;
};

DL.prettyDate = (iso) => {
  if (!iso) return '–';
  const d = new Date(iso + 'T00:00:00');
  if (isNaN(d)) return iso;
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
};

DL.longDate = (iso) => {
  if (!iso) return '–';
  const d = new Date(iso + 'T00:00:00');
  if (isNaN(d)) return iso;
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
};

DL.hhmm = (t) => (t || '').slice(0, 5);

/**
 * JSON columns come back as objects from PostgREST but as strings from some
 * clients; accept either.
 */
DL.asArray = (v) => {
  if (Array.isArray(v)) return v;
  if (typeof v === 'string') { try { return JSON.parse(v) || []; } catch { return []; } }
  return [];
};

/** Case- and accent-insensitive key, so "Ärjänsaaren" matches "arjansaaren". */
DL.fold = (s) => String(s ?? '')
  .toLowerCase().normalize('NFD').replace(/\p{Mn}+/gu, '').trim();

/** `<svg class="ico"><use href="#i-depth"></use></svg>` in one call. */
DL.icon = (name, cls = 'ico') => `<svg class="${cls}" aria-hidden="true"><use href="#i-${name}"></use></svg>`;

/**
 * The gas a dive breathed, named the way divers say it: Air, EAN32, Tx 18/45.
 * Reads the first mix; multi-gas dives are shown in full on the detail page.
 */
DL.gasLabel = (gasmixes) => {
  const gases = DL.asArray(gasmixes);
  if (!gases.length) return null;
  const g = gases[0];
  const o2 = Math.round(Number(g.o2) || 0);
  const he = Math.round(Number(g.he) || 0);
  let name;
  if (he > 0) name = `Tx ${o2}/${he}`;
  else if (!o2 || o2 === 21) name = 'Air';
  else name = `EAN${o2}`;
  return gases.length > 1 ? `${name} +${gases.length - 1}` : name;
};

/** A dive's own site name, trimmed, or null. */
DL.siteOf = (d) => {
  const n = (d.site_name || '').trim();
  return n || null;
};

DL.el = (id) => document.getElementById(id);

/** Replaces a container's children with HTML, then wires listeners. */
DL.fill = (node, html) => { if (node) node.innerHTML = html; return node; };

/** Brief message at the bottom of the screen. */
DL.toast = (message, kind = '') => {
  const host = DL.el('toast-host');
  if (!host) return;
  host.innerHTML = `<div class="toast ${kind}">${DL.esc(message)}</div>`;
  clearTimeout(DL._toastTimer);
  DL._toastTimer = setTimeout(() => { host.innerHTML = ''; }, 3200);
};
