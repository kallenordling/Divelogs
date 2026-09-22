/* Every chart the app draws, as inline SVG. No charting library: these are all
   simple shapes, and a dive profile has conventions a generic library fights. */
'use strict';

(() => {
const DL = window.DL;

/** [{t: seconds, d: metres, c: °C}] from the stored [ms, depth, temp] rows. */
function points(samples) {
  return DL.asArray(samples)
    .map((s) => ({ t: s[0] / 1000, d: s[1], c: s[2] }))
    .filter((p) => isFinite(p.t) && isFinite(p.d));
}
DL.profilePoints = points;

/**
 * Cold blue through green to warm red, across the range actually recorded.
 * A fixed absolute scale renders a Finnish quarry as one flat shade.
 */
DL.tempColour = (c, lo, hi) => {
  const stops = [
    [0.00, [ 49,  84, 196]],
    [0.35, [ 38, 166, 214]],
    [0.60, [ 94, 190, 106]],
    [0.80, [242, 190,  70]],
    [1.00, [225,  74,  56]],
  ];
  const x = Math.max(0, Math.min(1, hi > lo ? (c - lo) / (hi - lo) : 0.5));
  for (let i = 1; i < stops.length; i++) {
    if (x <= stops[i][0]) {
      const [a, ca] = stops[i - 1], [b, cb] = stops[i];
      const f = (x - a) / (b - a || 1);
      const ch = ca.map((v, j) => Math.round(v + (cb[j] - v) * f));
      return `rgb(${ch[0]},${ch[1]},${ch[2]})`;
    }
  }
  return 'rgb(225,74,56)';
};

/**
 * The small profile on a dive card. Depth only, no axes: it is a glance at the
 * shape of the dive, and detail would only add noise at this size.
 */
DL.sparkProfile = (samples) => {
  const pts = points(samples);
  if (pts.length < 2) return '';

  const W = 320, H = 44;
  const maxT = Math.max(...pts.map((p) => p.t)) || 1;
  const maxD = Math.max(...pts.map((p) => p.d)) || 1;
  const x = (t) => (t / maxT) * W;
  const y = (d) => 3 + (d / (maxD * 1.12)) * (H - 6);

  const line = pts.map((p, i) => `${i ? 'L' : 'M'}${x(p.t).toFixed(1)},${y(p.d).toFixed(1)}`).join('');
  // Fill the water column *above* the trace, between the surface and the
  // diver — the same way round as the full profile. Filling below it reads
  // as a hill rather than a dive.
  const fill = `${line}L${W},${y(0).toFixed(1)}L0,${y(0).toFixed(1)}Z`;

  return `<div class="spark"><svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"
       aria-hidden="true">
      <path d="${fill}" fill="rgba(57,198,232,.13)"/>
      <path d="${line}" fill="none" stroke="var(--accent)" stroke-width="1.6"
            stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/>
    </svg></div>`;
};

/**
 * The full profile on the dive detail page, drawn the way a dive computer
 * draws one: depth increasing downward, time along the bottom, the water
 * column shaded, and temperature as a second trace when it was recorded.
 */
DL.diveProfile = (samples, recordedMax = null) => {
  const pts = points(samples);
  if (pts.length < 2) {
    return DL.emptyState({
      icon: 'wave', title: 'No profile recorded',
      detail: 'This dive was logged without sample data.',
    });
  }

  const W = 900, H = 320, L = 46, R = 50, T = 14, B = 30;
  const gw = W - L - R, gh = H - T - B;

  const maxT = Math.max(...pts.map((p) => p.t)) || 1;
  // Samples are often subsampled, so the recorded maximum can be deeper than
  // anything in them; scale to whichever is greater.
  const sampleMax = Math.max(...pts.map((p) => p.d)) || 1;
  const maxD = Math.max(sampleMax, Number(recordedMax) || 0) || 1;
  const depthTop = maxD * 1.1;

  const temps = pts.map((p) => p.c).filter((c) => isFinite(c) && c !== 0);
  const hasTemp = temps.length > 1 && Math.max(...temps) > Math.min(...temps);
  const tLo = hasTemp ? Math.min(...temps) : 0;
  const tHi = hasTemp ? Math.max(...temps) : 1;

  const x = (t) => L + (t / maxT) * gw;
  const yD = (d) => T + (d / depthTop) * gh;
  const yT = (c) => T + gh - ((c - tLo) / ((tHi - tLo) || 1)) * gh * 0.7 - gh * 0.12;

  const line = pts.map((p, i) => `${i ? 'L' : 'M'}${x(p.t).toFixed(1)},${yD(p.d).toFixed(1)}`).join('');
  const fill = `${line}L${x(maxT).toFixed(1)},${yD(0)}L${x(0).toFixed(1)},${yD(0)}Z`;

  let tempPath = '', started = false;
  if (hasTemp) {
    for (const p of pts) {
      if (!isFinite(p.c) || p.c === 0) continue;
      tempPath += `${started ? 'L' : 'M'}${x(p.t).toFixed(1)},${yT(p.c).toFixed(1)}`;
      started = true;
    }
  }

  const step = maxD <= 12 ? 3 : maxD <= 30 ? 5 : maxD <= 60 ? 10 : 20;
  let grid = '';
  for (let d = 0; d <= depthTop; d += step) {
    const py = yD(d).toFixed(1);
    grid += `<line x1="${L}" y1="${py}" x2="${W - R}" y2="${py}" stroke="#15303F"/>`
         +  `<text x="${L - 8}" y="${+py + 4}" fill="#668595" font-size="11"
                   text-anchor="end">${d}</text>`;
  }
  const minutes = maxT / 60;
  const mStep = Math.max(1, Math.ceil(minutes / 7));
  for (let m = 0; m <= minutes; m += mStep) {
    const px = x(m * 60).toFixed(1);
    grid += `<line x1="${px}" y1="${T}" x2="${px}" y2="${T + gh}" stroke="#122B39"/>`
         +  `<text x="${px}" y="${H - 9}" fill="#668595" font-size="11"
                   text-anchor="middle">${m}</text>`;
  }
  if (hasTemp) {
    for (const c of [tLo, tHi]) {
      grid += `<text x="${W - R + 8}" y="${(yT(c) + 4).toFixed(1)}" fill="#F2B866"
                     font-size="11">${c.toFixed(1)}°</text>`;
    }
  }

  // Mark the deepest point, which is the number divers look for first.
  const deepest = pts.reduce((a, p) => (p.d > a.d ? p : a), pts[0]);

  return `<div class="chart">
    <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet" role="img"
         aria-label="Dive profile reaching ${maxD.toFixed(1)} metres over ${Math.round(minutes)} minutes">
      ${grid}
      <path d="${fill}" fill="rgba(57,198,232,.12)"/>
      <path d="${line}" fill="none" stroke="var(--accent)" stroke-width="2.4"
            stroke-linejoin="round" stroke-linecap="round"/>
      ${tempPath ? `<path d="${tempPath}" fill="none" stroke="#F2B866" stroke-width="1.8"
            stroke-linejoin="round" stroke-linecap="round" opacity=".85"/>` : ''}
      <circle cx="${x(deepest.t).toFixed(1)}" cy="${yD(deepest.d).toFixed(1)}" r="3.5"
              fill="var(--accent)" stroke="#07141F" stroke-width="1.6"/>
      <text x="${L}" y="${T - 1}" fill="#668595" font-size="10.5">metres</text>
      <text x="${W - R}" y="${H - 9}" fill="#668595" font-size="10.5" text-anchor="end">minutes</text>
    </svg>
    <div class="chart-legend">
      <span><i class="sw" style="background:var(--accent)"></i>Depth</span>
      ${hasTemp ? '<span><i class="sw" style="background:#F2B866"></i>Temperature</span>' : ''}
      <span>Deepest ${(recordedMax != null && isFinite(recordedMax)
                        ? Number(recordedMax) : maxD).toFixed(1)} m</span>
    </div>
  </div>`;
};

/**
 * Every dive at one site on one chart: the date along the bottom, depth down
 * the side, and each dive a column coloured by the water temperature it
 * recorded at each depth. Read across, it shows how the water column changes
 * through the seasons — when the thermocline forms, how deep it sits.
 */
DL.siteProfile = (siteDives) => {
  const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  const dives = siteDives.map((d) => {
    const when = Date.parse(`${d.date}T${(d.time || '12:00').slice(0, 5)}:00`);
    const pts = points(d.samples);
    const maxD = Math.max(Number(d.maxdepth) || 0, ...pts.map((p) => p.d));
    return { d, when, pts, maxD };
  }).filter((v) => isFinite(v.when) && v.maxD > 0)
    .sort((a, z) => a.when - z.when);

  if (!dives.length) {
    return DL.emptyState({
      icon: 'wave', title: 'No profiles yet',
      detail: 'Dives logged here have no depth recorded to plot.',
    });
  }

  const validTemp = (c) => c != null && isFinite(c) && c !== 0;
  const temps = dives.flatMap((v) => [
    ...v.pts.map((p) => p.c), Number(v.d.temp_min), Number(v.d.temp_max),
  ]).filter(validTemp);
  const hasTemp = temps.length > 0;
  const lo = hasTemp ? Math.min(...temps) : 0;
  const hi = hasTemp ? Math.max(...temps) : 1;

  const maxD = Math.max(...dives.map((v) => v.maxD));
  const depthTop = maxD * 1.1;

  const W = 900, H = 340, L = 46, R = 66, T = 14, B = 30;
  const gw = W - L - R, gh = H - T - B;
  const y = (d) => T + (d / depthTop) * gh;

  // Time axis, padded so the first and last columns sit inside the frame.
  const DAY = 86400000;
  const first = dives[0].when, last = dives[dives.length - 1].when;
  const pad = Math.max((last - first) * 0.05, 3 * DAY);
  const t0 = first - pad, t1 = last + pad;
  const x = (t) => L + ((t - t0) / (t1 - t0)) * gw;

  // Every dive sits on its true date, so the daily interpolation lines up
  // with it. Columns are at most three days wide — over a span of years that
  // is the 3 px minimum, while a single summer gets broad columns.
  const cw = Math.max(3, Math.min(22, gw / (dives.length * 1.8), x(t0 + 3 * DAY) - x(t0)));
  for (const v of dives) v.cx = x(v.when);

  // Depth bins: fine enough to show a thermocline, coarse enough that the
  // chart stays a few thousand rectangles.
  const bin = Math.max(0.5, depthTop / 90);
  const colourOf = (c) => validTemp(c) ? DL.tempColour(c, lo, hi) : '#3d5a75';

  // One temperature per depth bin for each dive: the mean of its samples in
  // that band, with bands it has no reading for filled from the readings
  // above and below. A dive with no readings at all takes its recorded water
  // temperature throughout, so it still appears.
  for (const v of dives) {
    const n = Math.floor(v.maxD / bin) + 1;
    const sum = new Array(n).fill(0), cnt = new Array(n).fill(0);
    for (const p of v.pts) {
      const k = Math.min(n - 1, Math.floor(p.d / bin));
      if (validTemp(p.c)) { sum[k] += p.c; cnt[k]++; }
    }
    const prof = sum.map((s, k) => (cnt[k] ? s / cnt[k] : NaN));
    const known = prof.map((c, k) => (isFinite(c) ? k : -1)).filter((k) => k >= 0);
    if (!known.length) {
      prof.fill(validTemp(Number(v.d.temp_min)) ? Number(v.d.temp_min) : NaN);
    } else {
      for (let k = 0; k < n; k++) {
        if (isFinite(prof[k])) continue;
        const up = known.filter((j) => j < k).pop();
        const down = known.find((j) => j > k);
        prof[k] = up == null ? prof[down] : down == null ? prof[up]
          : prof[up] + (prof[down] - prof[up]) * (k - up) / (down - up);
      }
    }
    v.prof = prof;
  }
  const tempAt = (v, k) => v.prof[Math.min(k, v.prof.length - 1)];

  // One vertical strip of colour: bins of the same shade are merged into a
  // single rectangle, which keeps a smooth gradient to a handful of shapes.
  const SHADES = 48;
  const shade = (c) => validTemp(c)
    ? lo + (Math.round((c - lo) / ((hi - lo) || 1) * SHADES) / SHADES) * (hi - lo) : NaN;
  const strip = (x0, width, depth, tempOfBin) => {
    const last = Math.floor(depth / bin);
    let out = '', start = 0, cur = shade(tempOfBin(0));
    const flush = (end) => {
      const top = y(start * bin), bottom = y(Math.min(end * bin, depth));
      if (bottom > top) out += `<rect x="${x0.toFixed(1)}" y="${top.toFixed(1)}" width="${width.toFixed(1)}"
                     height="${(bottom - top + 0.3).toFixed(1)}" fill="${colourOf(cur)}"/>`;
    };
    for (let k = 1; k <= last; k++) {
      const c = shade(tempOfBin(k));
      if (c === cur || (!isFinite(c) && !isFinite(cur))) continue;
      flush(k); start = k; cur = c;
    }
    flush(last + 1);
    return out;
  };

  // Daily values for every month with two or more dives. Each day takes
  // the nearest dive before it and the nearest after it, and blends their
  // depth and their temperature at each depth by how far through the gap it
  // falls. A month with a single dive shows only that dive: one reading is
  // not enough to say how the month went.
  const monthOf = (t) => { const d = new Date(t); return d.getFullYear() * 12 + d.getMonth(); };
  const perMonth = new Map();
  for (const v of dives) perMonth.set(monthOf(v.when), (perMonth.get(monthOf(v.when)) || 0) + 1);

  let fill = '';
  const startDay = new Date(first); startDay.setHours(0, 0, 0, 0);
  let i = 0;
  for (let day = startDay.getTime(); day < last; day += DAY) {
    const noon = day + DAY / 2;
    if ((perMonth.get(monthOf(noon)) || 0) < 2) continue;
    while (i < dives.length - 2 && dives[i + 1].when <= noon) i++;
    const a = dives[i], z = dives[i + 1];
    if (!(a.when <= noon && noon <= z.when)) continue;
    const f = (noon - a.when) / ((z.when - a.when) || 1);
    const depth = a.maxD + (z.maxD - a.maxD) * f;
    const x0 = x(day), x1 = x(day + DAY);
    fill += strip(x0, x1 - x0 + 0.6, depth, (k) => {
      const ca = tempAt(a, k), cz = tempAt(z, k);
      return !isFinite(ca) ? cz : !isFinite(cz) ? ca : ca + (cz - ca) * f;
    });
  }

  // The dives themselves, drawn over the blend and marked at the surface so
  // measured water stays distinguishable from interpolated water.
  let cols = '';
  for (const v of dives) {
    const dt = [Number(v.d.temp_min), Number(v.d.temp_max)].filter(validTemp);
    const label = `${DL.prettyDate(v.d.date)} ${DL.hhmm(v.d.time)} · ${DL.num(v.maxD)} m`
      + (dt.length ? ` · ${DL.num(Math.min(...dt))}–${DL.num(Math.max(...dt))} °C` : '');
    cols += `<g data-dive="${DL.esc(String(v.d.id))}" style="cursor:pointer">
      <title>${DL.esc(label)}</title>
      ${strip(v.cx - cw / 2, cw, v.maxD, (k) => tempAt(v, k))}
      <path d="M${(v.cx - 4).toFixed(1)},${T - 7}L${(v.cx + 4).toFixed(1)},${T - 7}L${v.cx.toFixed(1)},${T - 1}Z"
            fill="#E6F1F6"/></g>`;
  }

  const step = maxD <= 12 ? 3 : maxD <= 30 ? 5 : maxD <= 60 ? 10 : 20;
  let grid = '';
  for (let d = 0; d <= depthTop; d += step) {
    const py = y(d).toFixed(1);
    grid += `<line x1="${L}" y1="${py}" x2="${W - R}" y2="${py}" stroke="#15303F"/>`
         +  `<text x="${L - 8}" y="${+py + 4}" fill="#668595" font-size="11"
                   text-anchor="end">${d}</text>`;
  }

  // Date ticks on calendar boundaries: days for a short span, otherwise the
  // first of a month, stepping far enough apart to leave room for labels.
  const span = t1 - t0;
  const ticks = [];
  if (span < 75 * DAY) {
    const days = [1, 2, 7, 14].find((n) => span / (n * DAY) <= 7) || 14;
    const s = new Date(t0); s.setHours(0, 0, 0, 0);
    for (let t = s.getTime() + DAY; t < t1; t += days * DAY) {
      const d = new Date(t);
      ticks.push([t, `${d.getDate()} ${MONTHS[d.getMonth()]}`]);
    }
  } else {
    const months = [1, 2, 3, 6, 12, 24, 60].find((n) => span / (n * 30.4 * DAY) <= 7) || 60;
    const s = new Date(t0);
    let d = new Date(s.getFullYear(), months >= 12 ? 0 : s.getMonth() + 1, 1);
    if (months >= 12 && d.getTime() < t0) d = new Date(d.getFullYear() + 1, 0, 1);
    if (months < 12) d.setMonth(Math.ceil(d.getMonth() / months) * months);
    for (; d.getTime() < t1; d = new Date(d.getFullYear(), d.getMonth() + months, 1)) {
      ticks.push([d.getTime(), months >= 12 ? String(d.getFullYear())
        : `${MONTHS[d.getMonth()]} ${d.getFullYear()}`]);
    }
  }
  for (const [t, text] of ticks) {
    const px = x(t).toFixed(1);
    grid += `<line x1="${px}" y1="${T}" x2="${px}" y2="${T + gh}" stroke="#122B39"/>`
         +  `<text x="${px}" y="${H - 9}" fill="#668595" font-size="11"
                   text-anchor="middle">${text}</text>`;
  }

  let bar = '';
  if (hasTemp) {
    const bx = W - R + 14, bw = 12, bh = gh * 0.72, by = T + gh * 0.1;
    for (let i = 0; i < 40; i++) {
      const c = lo + (i / 39) * (hi - lo);
      bar += `<rect x="${bx}" y="${(by + bh - (i + 1) / 40 * bh).toFixed(1)}" width="${bw}"
                    height="${(bh / 40 + 0.6).toFixed(2)}" fill="${DL.tempColour(c, lo, hi)}"/>`;
    }
    bar += `<text x="${bx + bw + 4}" y="${(by + 9).toFixed(1)}" fill="#9DB6C4" font-size="11">${hi.toFixed(1)}°</text>`
        +  `<text x="${bx + bw + 4}" y="${(by + bh).toFixed(1)}" fill="#9DB6C4" font-size="11">${lo.toFixed(1)}°</text>`;
  }

  return `<div class="chart">
    <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet" role="img"
         aria-label="Depth of ${dives.length} dive(s) by date, coloured by water temperature from ${lo.toFixed(1)} to ${hi.toFixed(1)} degrees Celsius">
      ${grid}<g opacity=".78">${fill}</g>${cols}${bar}
      <text x="${L - 8}" y="${H - 9}" fill="#668595" font-size="10.5" text-anchor="end">m</text>
    </svg>
    <div class="chart-legend">
      ${hasTemp ? '<span>▼ marks each dive · colour is water temperature · months with several dives are interpolated day by day</span>'
                : '<span>No temperature recorded here</span>'}
      <span>${dives.length} dive${dives.length === 1 ? '' : 's'} · tap one to open it</span>
    </div>
  </div>`;
};

/** A value against time — depth, duration or temperature across the log. */
DL.trendChart = (series, { unit = '', colour = 'var(--accent)', label = '' } = {}) => {
  const pts = series.filter((p) => isFinite(p.v));
  if (pts.length < 2) {
    return DL.emptyState({ icon: 'chart', title: 'Not enough dives yet',
                           detail: 'Two or more dives are needed to show a trend.' });
  }

  const W = 900, H = 240, L = 46, R = 16, T = 16, B = 28;
  const gw = W - L - R, gh = H - T - B;
  const lo = Math.min(...pts.map((p) => p.v));
  const hi = Math.max(...pts.map((p) => p.v));
  const pad = (hi - lo) * 0.12 || 1;
  const y0 = lo - pad, y1 = hi + pad;

  const x = (i) => L + (pts.length === 1 ? gw / 2 : (i / (pts.length - 1)) * gw);
  const y = (v) => T + gh - ((v - y0) / (y1 - y0)) * gh;

  const line = pts.map((p, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(p.v).toFixed(1)}`).join('');
  const area = `${line}L${x(pts.length - 1).toFixed(1)},${T + gh}L${x(0).toFixed(1)},${T + gh}Z`;

  let grid = '';
  for (let i = 0; i <= 3; i++) {
    const v = y0 + (i / 3) * (y1 - y0);
    const py = y(v).toFixed(1);
    grid += `<line x1="${L}" y1="${py}" x2="${W - R}" y2="${py}" stroke="#15303F"/>`
         +  `<text x="${L - 8}" y="${+py + 4}" fill="#668595" font-size="11"
                   text-anchor="end">${v.toFixed(v < 10 ? 1 : 0)}</text>`;
  }

  const first = pts[0].label, last = pts[pts.length - 1].label;
  return `<div class="chart">
    <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet" role="img"
         aria-label="${DL.esc(label)} across ${pts.length} dives, from ${DL.num(lo)} to ${DL.num(hi)} ${DL.esc(unit)}">
      ${grid}
      <path d="${area}" fill="rgba(57,198,232,.10)"/>
      <path d="${line}" fill="none" stroke="${colour}" stroke-width="2.2"
            stroke-linejoin="round" stroke-linecap="round"/>
      <text x="${L}" y="${H - 8}" fill="#668595" font-size="11">${DL.esc(first || '')}</text>
      <text x="${W - R}" y="${H - 8}" fill="#668595" font-size="11" text-anchor="end">${DL.esc(last || '')}</text>
      <text x="${L}" y="${T - 3}" fill="#668595" font-size="10.5">${DL.esc(unit)}</text>
    </svg>
  </div>`;
};

/** Labelled horizontal bars: dives per month, per country, per site. */
DL.barList = (rows, { max = null } = {}) => {
  if (!rows.length) return '';
  const peak = max || Math.max(...rows.map((r) => r.value), 1);
  return `<div class="bars">${rows.map((r) => `
    <div class="bar-row">
      <span class="lbl" title="${DL.esc(r.label)}">${DL.esc(r.label)}</span>
      <span class="track"><span class="fill" style="width:${(r.value / peak * 100).toFixed(1)}%"></span></span>
      <span class="n tnum">${r.value}</span>
    </div>`).join('')}</div>`;
};
})();
