// The site chart's yearly-cycle fit: can it say what a month nobody dived
// was like, and does it keep quiet where the readings cannot support it?
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const YEAR = 365.2425 * 86400000;
const DAY = 86400000;

/** A lake: warm at the surface in August, cold and steady deep down, and
    never below freezing — the fit is not allowed to invent water that could
    not exist, so the fixture must not ask it to. */
const truth = (t, k) => {
  const season = Math.cos(2 * Math.PI * (t - Date.parse('2024-08-05')) / YEAR);
  const surfaceSwing = 7.5 * Math.exp(-k / 6);
  return 8.5 + surfaceSwing * season;
};

function boot() {
  const errors = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));
  const dom = new JSDOM(buildHtml(), {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/', virtualConsole: vc,
    beforeParse(win) {
      win.L = undefined;
      win.scrollTo = () => {};
      win.fetch = async () => ({ ok: false, status: 404, json: async () => ({}) });
    },
  });
  return { window: dom.window, errors };
}

(() => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  const { window, errors } = boot();
  const DL = window.DL;

  // Dives from April to October across three years — the diving season, with
  // every winter missing, which is exactly what a fit has to bridge.
  const points = [];
  const diveDays = [];
  for (let year = 2023; year <= 2025; year++) {
    for (const [month, day] of [[4, 18], [5, 30], [6, 21], [7, 14], [8, 9], [9, 27], [10, 6]]) {
      const t = Date.parse(`${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}T12:00:00`);
      diveDays.push(t);
      for (let k = 0; k <= 20; k++) points.push({ t, k, c: truth(t, k) });
    }
  }
  // One band only two dives ever reached: too little to model.
  for (const t of diveDays.slice(0, 2)) points.push({ t, k: 40, c: 4.2 });

  const fit = DL.annualCycleFit(points);

  ok('the fit follows the readings it was given',
     fit.rmse < 0.35, `rmse ${fit.rmse.toFixed(2)} °C`);
  ok('every reading is counted', fit.readings === 21 * diveDays.length,
     `${fit.readings} of ${21 * diveDays.length}`);

  // February: never dived in this log, on either side of a year boundary.
  const feb = Date.parse('2024-02-12T12:00:00');
  const surface = fit.predict(feb, 0);
  ok('a month nobody dived is predicted from the cycle',
     Math.abs(surface - truth(feb, 0)) < 1.5,
     `predicted ${surface.toFixed(1)}, actually ${truth(feb, 0).toFixed(1)}`);
  ok('the prediction is a winter, not a summer', surface < 5,
     `${surface.toFixed(1)} °C in February`);

  // The readings run April to October, so the true February water is colder
  // than anything measured — the fit must reach it, but only by carrying the
  // swing it can see one swing further, and never below freezing point.
  const band0 = points.filter((p) => p.k === 0).map((p) => p.c);
  const seen0 = Math.min(...band0), seen1 = Math.max(...band0);
  const swing = seen1 - seen0;
  ok('the fit reaches past the readings, but only by one more swing',
     surface >= seen0 - swing && surface < seen0,
     `${surface.toFixed(1)}, readings ${seen0.toFixed(1)}–${seen1.toFixed(1)}`);

  const deep = fit.predict(feb, 18);
  ok('deep water is predicted as the steadier water it is',
     Math.abs(deep - truth(feb, 18)) < 1.5,
     `predicted ${deep.toFixed(1)}, actually ${truth(feb, 18).toFixed(1)}`);

  // August, which the dives do cover, should be close to right.
  const aug = Date.parse('2024-08-20T12:00:00');
  ok('a dived month is predicted closely',
     Math.abs(fit.predict(aug, 0) - truth(aug, 0)) < 0.6,
     `predicted ${fit.predict(aug, 0).toFixed(1)}, actually ${truth(aug, 0).toFixed(1)}`);

  ok('a band with two readings is not modelled',
     !isFinite(fit.predict(feb, 40)), String(fit.predict(feb, 40)));
  ok('the fit reaches only as deep as the readings do', fit.deepest === 20,
     String(fit.deepest));

  // The model must not invent water colder or warmer than was ever measured.
  let outside = 0;
  for (let t = Date.parse('2023-01-01'); t < Date.parse('2026-01-01'); t += 5 * DAY) {
    const v = fit.predict(t, 0);
    if (v < Math.max(-2, seen0 - swing) - 1e-9 || v > seen1 + swing + 1e-9) outside++;
  }
  ok('predictions never run away from the readings', outside === 0,
     `${outside} wild values`);

  // And the chart itself draws the fitted field.
  const dives = diveDays.map((t, i) => ({
    id: `d${i}`, date: new Date(t).toISOString().slice(0, 10), time: '12:00:00',
    maxdepth: 20, duration: 2400, temp_min: truth(t, 20), temp_max: truth(t, 0),
    samples: Array.from({ length: 21 }, (_, k) => [k * 60000, k, truth(t, k)]),
  }));
  const plain = DL.siteProfile(dives);
  const fitted = DL.siteProfile(dives, { fit: true });
  ok('the fitted chart says so', /fitted yearly cycle/.test(fitted),
     fitted.slice(fitted.indexOf('chart-legend'), fitted.indexOf('chart-legend') + 160));
  ok('the fitted chart fills the whole range',
     (fitted.match(/<rect/g) || []).length > (plain.match(/<rect/g) || []).length * 2,
     `${(fitted.match(/<rect/g) || []).length} vs ${(plain.match(/<rect/g) || []).length} rectangles`);
  ok('a chart without temperatures does not pretend to fit',
     /No temperature recorded here/.test(DL.siteProfile(
       [{ id: 'x', date: '2025-05-05', time: '10:00:00', maxdepth: 10, samples: [] }], { fit: true })));

  ok('no runtime errors', errors.length === 0, errors.join(' | '));

  console.log(failed ? `\n${failed} failed` : '\nall passed');
  process.exit(failed ? 1 : 0);
})();
