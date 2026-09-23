// Importing dives exported from other services: the parsers, and the dialog
// that writes what they find to the database.
const { JSDOM, VirtualConsole } = require('jsdom');
const { buildHtml } = require('./harness.js');

const HTML = buildHtml();

// ── Fixtures, hand-written to match what each service exports ──────────────

const UDDF = `<?xml version="1.0" encoding="UTF-8"?>
<uddf xmlns="http://www.streit.cc/uddf/3.2/" version="3.2.0">
  <divesite>
    <site id="s1">
      <name>Pikkusilta</name>
      <geography><latitude>61.1721</latitude><longitude>25.5462</longitude></geography>
    </site>
  </divesite>
  <gasdefinitions><mix id="m1"><o2>0.32</o2><he>0.0</he></mix></gasdefinitions>
  <profiledata><repetitiongroup id="g1">
    <dive id="d1">
      <informationbeforedive>
        <datetime>2026-07-01T11:40:00</datetime>
        <airtemperature>295.15</airtemperature>
        <link ref="s1"/>
      </informationbeforedive>
      <samples>
        <waypoint><depth>0.0</depth><divetime>0</divetime><temperature>291.15</temperature></waypoint>
        <waypoint><depth>12.4</depth><divetime>300</divetime><temperature>283.15</temperature></waypoint>
        <waypoint><depth>24.8</depth><divetime>600</divetime><temperature>278.15</temperature></waypoint>
      </samples>
      <informationafterdive>
        <greatestdepth>24.8</greatestdepth>
        <averagedepth>12.2</averagedepth>
        <diveduration>2700</diveduration>
        <lowesttemperature>278.15</lowesttemperature>
      </informationafterdive>
    </dive>
    <dive id="d2">
      <informationbeforedive><datetime>2026-07-23T17:12:00</datetime></informationbeforedive>
      <informationafterdive><greatestdepth>18.0</greatestdepth><diveduration>1800</diveduration></informationafterdive>
    </dive>
  </repetitiongroup></profiledata>
</uddf>`;

const SSRF = `<divelog program='subsurface' version='3'>
<divesites>
  <site uuid='abc' name='Likolampi' gps='61.9241 25.7482'/>
</divesites>
<dives>
<dive number='7' date='2026-02-20' time='12:34:00' duration='7:26 min' divesiteid='abc'>
  <cylinder size='12.0 l' o2='32.0%'/>
  <divecomputer model='Shearwater Perdix'>
    <depth max='5.4 m' mean='2.8 m'/>
    <temperature air='1.0 C' water='2.1 C'/>
    <sample time='0:00 min' depth='0.0 m' temp='2.6 C'/>
    <sample time='2:00 min' depth='5.4 m'/>
    <sample time='7:00 min' depth='1.0 m' temp='2.1 C'/>
  </divecomputer>
</dive>
</dives>
</divelog>`;

const SML = `<sml xmlns="http://www.suunto.com/schemas/sml">
  <DeviceLog>
    <Device><Name>Suunto D5</Name></Device>
    <Header>
      <DateTime>2025-08-09T12:54:00</DateTime>
      <Depth><Max>25.0</Max><Avg>11.3</Avg></Depth>
      <Duration>1921</Duration>
      <MinTemperature>277.65</MinTemperature>
    </Header>
    <Samples>
      <Sample><Time>0</Time><Depth>0.0</Depth><Temperature>288.15</Temperature></Sample>
      <Sample><Time>420</Time><Depth>25.0</Depth><Temperature>277.65</Temperature></Sample>
    </Samples>
  </DeviceLog>
</sml>`;

// Shearwater Cloud writes a title line above the header, and can be set to
// imperial units.
const CSV = `Shearwater Cloud dive list
Number,Dive Date,Start Time,Max Depth (ft),Dive Duration (min),Min Temp (F),Location,Computer
41,08/31/2026,16:37,82.3,42.8,46.4,Pikkusilta,Perdix
42,09/21/2026,16:33,119.1,40,45,Pikkusilta,Perdix`;

// Suunto's own export, as found inside a DM3/DM4 .sde: comma decimals,
// day-first dates, and 0 where the computer recorded no temperature.
const SDM = `<?xml version="1.0" encoding="ISO-8859-15" ?>
<SUUNTO><HEADER><MSGNAME>SDM001A</MSGNAME></HEADER>
<MSG><SAMPLECNT>3</SAMPLECNT>
<DATE>17.05.2011</DATE><TIME>11:01:00</TIME>
<MAXDEPTH>24,69</MAXDEPTH><MEANDEPTH>11,89</MEANDEPTH>
<SITE>Sund Rock</SITE><LOCATION>Hoodsport, WA</LOCATION>
<AIRTEMP>14</AIRTEMP><WATERTEMPMAXDEPTH>10</WATERTEMPMAXDEPTH>
<DEVICEMODEL>Vyper</DEVICEMODEL><DIVETIMESEC>1890</DIVETIMESEC><O2PCT>32</O2PCT>
<SAMPLE><SAMPLETIME>0</SAMPLETIME><DEPTH>0</DEPTH><TEMPERATURE>14</TEMPERATURE></SAMPLE>
<SAMPLE><SAMPLETIME>30</SAMPLETIME><DEPTH>2,74</DEPTH><TEMPERATURE>0</TEMPERATURE></SAMPLE>
<SAMPLE><SAMPLETIME>60</SAMPLETIME><DEPTH>24,69</DEPTH><TEMPERATURE>10</TEMPERATURE></SAMPLE>
</MSG></SUUNTO>`;

// What a modern Suunto (EON, D5, Ocean) writes: absolute stamps per sample,
// Kelvin temperatures, and event-only samples with no depth.
const SUUNTO_JSON = JSON.stringify({ DeviceLog: {
  Header: {
    DateTime: '2024-10-06T02:33:51.530+02:00',
    Depth: { Avg: 13.26, Max: 22.65 },
    Duration: 3970, SampleInterval: 10,
    Device: { Name: 'EON Core', SerialNumber: '2804271178' },
  },
  Samples: [
    { Events: [{ GasSwitch: { GasNumber: 1 } }], TimeISO8601: '2024-10-06T02:33:51.530+02:00' },
    { Depth: 1.55, Temperature: 302.85, TimeISO8601: '2024-10-06T02:33:52.196+02:00' },
    { Depth: 22.65, Temperature: 302.25, TimeISO8601: '2024-10-06T02:34:02.196+02:00' },
  ],
} });

/** A zip holding one stored (uncompressed) file, as a .sde of SML. */
function storedZip(name, content) {
  const enc = new TextEncoder();
  const nameB = enc.encode(name), body = enc.encode(content);
  // CRC-32 of the body, which the reader does not check but the format needs.
  let crc = ~0;
  for (const b of body) {
    crc ^= b;
    for (let i = 0; i < 8; i++) crc = (crc >>> 1) ^ (0xEDB88320 & -(crc & 1));
  }
  crc = ~crc >>> 0;

  const local = 30 + nameB.length + body.length;
  const central = 46 + nameB.length;
  const buf = new Uint8Array(local + central + 22);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0x04034b50, true); dv.setUint16(4, 20, true);
  dv.setUint16(8, 0, true);                                  // stored
  dv.setUint32(14, crc, true);
  dv.setUint32(18, body.length, true); dv.setUint32(22, body.length, true);
  dv.setUint16(26, nameB.length, true);
  buf.set(nameB, 30); buf.set(body, 30 + nameB.length);

  let p = local;
  dv.setUint32(p, 0x02014b50, true); dv.setUint16(p + 10, 0, true);
  dv.setUint32(p + 16, crc, true);
  dv.setUint32(p + 20, body.length, true); dv.setUint32(p + 24, body.length, true);
  dv.setUint16(p + 28, nameB.length, true);
  dv.setUint32(p + 42, 0, true);
  buf.set(nameB, p + 46);

  p = local + central;
  dv.setUint32(p, 0x06054b50, true);
  dv.setUint16(p + 8, 1, true); dv.setUint16(p + 10, 1, true);
  dv.setUint32(p + 12, central, true); dv.setUint32(p + 16, local, true);
  return buf;
}

function boot(dives) {
  const errors = [];
  const posted = [];
  const vc = new VirtualConsole();
  vc.on('jsdomError', (e) => errors.push(e.message));
  const dom = new JSDOM(HTML, {
    runScripts: 'dangerously', pretendToBeVisual: true,
    url: 'https://example.github.io/Divelogs/',
    virtualConsole: vc,
    beforeParse(win) {
      win.L = undefined;
      win.scrollTo = () => {};
      // jsdom omits these two; every browser the app runs in has them.
      win.TextDecoder = TextDecoder;
      win.TextEncoder = TextEncoder;
      win.localStorage.setItem('deeplog.session', JSON.stringify({
        access_token: 'AT1', refresh_token: 'RT1', email: 'd@e.com', user_id: 'u1' }));
      win.fetch = async (url, opts = {}) => {
        const u = String(url);
        if (u.includes('_sites.json')) return { ok: true, status: 200, json: async () => ({ sites: [] }) };
        if (u.includes('/rest/v1/dives')) {
          if ((opts.method || 'GET') === 'POST') {
            const row = JSON.parse(opts.body);
            posted.push(row);
            return { ok: true, status: 201, json: async () => [{ ...row, id: `new${posted.length}` }] };
          }
          return { ok: true, status: 200, json: async () => dives };
        }
        return { ok: false, status: 404, json: async () => ({}) };
      };
    },
  });
  const tick = (n = 5) => new Promise((r) => setTimeout(r, 30 * n));
  return { window: dom.window, errors, posted, tick };
}

(async () => {
  let failed = 0;
  const ok = (n, c, extra = '') => {
    if (!c) failed++;
    console.log(`${c ? 'ok  ' : 'FAIL'}  ${n}${c ? '' : '   << ' + extra}`);
  };

  const existing = [{
    id: 'x1', date: '2026-07-23', time: '17:12:00', maxdepth: 18, duration: 1800,
    divemode: 'OC', gasmixes: [], tanks: [], samples: [], site_name: '',
  }];
  const { window, errors, posted, tick } = boot(existing);
  await tick(6);
  const doc = window.document;
  const DL = window.DL;
  const file = (name, text) => new window.File([text], name);

  // ── UDDF ────────────────────────────────────────────────────────────────
  const uddf = await DL.parseDiveFile(file('log.uddf', UDDF));
  ok('UDDF recognised', uddf.format === 'UDDF', uddf.format);
  ok('every dive in the file is read', uddf.dives.length === 2, String(uddf.dives.length));
  const u0 = uddf.dives[0];
  ok('date and time kept as recorded', u0.date === '2026-07-01' && u0.time === '11:40:00',
     `${u0.date} ${u0.time}`);
  ok('depth and duration read', u0.maxdepth === 24.8 && u0.duration === 2700,
     `${u0.maxdepth}m ${u0.duration}s`);
  ok('Kelvin converted to Celsius', u0.temp_min === 5 && u0.temp_surface === 22,
     `${u0.temp_min} / ${u0.temp_surface}`);
  ok('site name and position carried over',
     u0.site_name === 'Pikkusilta' && Math.abs(u0.site_lat - 61.1721) < 1e-6,
     `${u0.site_name} ${u0.site_lat},${u0.site_lon}`);
  ok('profile samples are [ms, depth, °C]',
     u0.samples.length === 3 && u0.samples[1][0] === 300000 &&
     u0.samples[1][1] === 12.4 && u0.samples[1][2] === 10,
     JSON.stringify(u0.samples[1]));
  ok('gas mix read as percentages',
     u0.gasmixes[0].o2 === 32 && u0.gasmixes[0].n2 === 68, JSON.stringify(u0.gasmixes));
  ok('a dive without a profile still imports',
     uddf.dives[1].maxdepth === 18 && uddf.dives[1].samples.length === 0);

  // ── Subsurface ──────────────────────────────────────────────────────────
  const ssrf = await DL.parseDiveFile(file('log.ssrf', SSRF));
  ok('Subsurface recognised', ssrf.format === 'Subsurface', ssrf.format);
  const s0 = ssrf.dives[0];
  ok('units stripped from values', s0.maxdepth === 5.4 && s0.avgdepth === 2.8,
     `${s0.maxdepth} ${s0.avgdepth}`);
  ok('mm:ss duration understood', s0.duration === 446, String(s0.duration));
  ok('water and air temperature read', s0.temp_min === 2.1 && s0.temp_surface === 1,
     `${s0.temp_min} ${s0.temp_surface}`);
  ok('site and position from the site list',
     s0.site_name === 'Likolampi' && s0.site_lon === 25.7482, s0.site_name);
  ok('samples without a temperature keep the last one',
     s0.samples[1][2] === 2.6, JSON.stringify(s0.samples));
  ok('the dive computer name is kept', s0.device_name === 'Shearwater Perdix', s0.device_name);

  // ── Suunto ──────────────────────────────────────────────────────────────
  const sml = await DL.parseDiveFile(file('dive.sml', SML));
  ok('Suunto SML recognised', sml.format === 'Suunto SML', sml.format);
  const m0 = sml.dives[0];
  ok('SML header read', m0.date === '2025-08-09' && m0.maxdepth === 25 && m0.duration === 1921,
     `${m0.date} ${m0.maxdepth} ${m0.duration}`);
  ok('SML temperatures in Celsius', m0.temp_min === 4.5, String(m0.temp_min));
  ok('device named from the file', m0.device_name === 'Suunto D5', m0.device_name);

  const sdm = await DL.parseDiveFile(file('0.xml', SDM));
  ok('Suunto DM3/DM4 XML recognised', sdm.format === 'Suunto DM3/DM4', sdm.format);
  const q0 = sdm.dives[0];
  ok('day-first dates and comma decimals read',
     q0.date === '2011-05-17' && q0.maxdepth === 24.69, `${q0.date} ${q0.maxdepth}`);
  ok('site and location joined', q0.site_name === 'Sund Rock, Hoodsport, WA', q0.site_name);
  ok('"no reading" temperatures carry the last one forward',
     q0.samples[1][2] === 14, JSON.stringify(q0.samples));
  ok('the Suunto model is kept', q0.device_name === 'Vyper', q0.device_name);

  const sjson = await DL.parseDiveFile(file('dive.json', SUUNTO_JSON));
  ok('Suunto JSON recognised', sjson.format === 'Suunto JSON', sjson.format);
  const j0 = sjson.dives[0];
  ok('JSON header read', j0.date === '2024-10-06' && j0.time === '02:33:51' &&
     j0.maxdepth === 22.65 && j0.duration === 3970,
     `${j0.date} ${j0.time} ${j0.maxdepth} ${j0.duration}`);
  ok('event-only samples are not readings', j0.samples.length === 2,
     String(j0.samples.length));
  ok('sample times are measured from the start of the dive',
     j0.samples[1][0] - j0.samples[0][0] === 10000, JSON.stringify(j0.samples));
  ok('JSON temperatures in Celsius', j0.samples[0][2] === 29.7, String(j0.samples[0][2]));
  ok('the computer is named', j0.device_name === 'Suunto EON Core', j0.device_name);

  const sde = await DL.parseDiveFile(file('dives.sde', storedZip('dive1.sml', SML)));
  ok('a .sde archive is opened', sde.dives.length === 1 && sde.dives[0].maxdepth === 25,
     JSON.stringify(sde.dives.map((d) => d.date)));

  // ── CSV ─────────────────────────────────────────────────────────────────
  const csv = await DL.parseDiveFile(file('dives.csv', CSV));
  ok('CSV recognised below its title line', csv.format === 'CSV' && csv.dives.length === 2,
     `${csv.format} ${csv.dives.length}`);
  const c0 = csv.dives[0];
  ok('American dates read correctly', c0.date === '2026-08-31' && c0.time === '16:37:00',
     `${c0.date} ${c0.time}`);
  ok('feet converted to metres', Math.abs(c0.maxdepth - 25.08) < 0.02, String(c0.maxdepth));
  ok('Fahrenheit converted to Celsius', c0.temp_min === 8, String(c0.temp_min));
  ok('minutes converted to seconds', c0.duration === 2568, String(c0.duration));
  ok('site name taken from the location column', c0.site_name === 'Pikkusilta', c0.site_name);

  // ── Unsupported files say what to do instead ────────────────────────────
  let told = '';
  try { await DL.parseDiveFile(file('cloud.db', 'SQLite format 3')); }
  catch (e) { told = e.message; }
  ok('a Shearwater .db says what to export instead',
     /DB files/.test(told) && /UDDF or CSV/.test(told), told);

  // ── The dialog ──────────────────────────────────────────────────────────
  doc.querySelector('[data-nav="dives"]').click();
  await tick(4);
  const importBtn = doc.querySelector('#view-dives [data-act="import"]');
  ok('the dives screen offers an import', !!importBtn, 'no import button');
  importBtn.click();
  await tick(2);

  const input = doc.querySelector('#im-file');
  ok('the dialog explains where files come from',
     /Shearwater Cloud/.test(doc.querySelector('#sheet-host').textContent));

  Object.defineProperty(input, 'files', {
    value: [file('log.uddf', UDDF), file('dives.csv', CSV)], configurable: true });
  input.dispatchEvent(new window.Event('change'));
  await tick(4);

  const status = doc.querySelector('#im-status').textContent;
  ok('the count found is shown', /4 dives found/.test(status), status);
  ok('dives already in the log are named as such', /1 already in your log/.test(status), status);
  const go = doc.querySelector('#im-go');
  ok('the button offers only the new dives', /Import 3 dives/.test(go.textContent), go.textContent);
  ok('the preview marks the duplicate',
     doc.querySelectorAll('#im-preview tr.muted').length === 1,
     String(doc.querySelectorAll('#im-preview tr.muted').length));

  go.click();
  await tick(8);
  ok('exactly the new dives were written', posted.length === 3,
     posted.map((p) => p.date).join(','));
  ok('the duplicate was not written again',
     !posted.some((p) => p.date === '2026-07-23'), posted.map((p) => p.date).join(','));
  ok('rows carry the signed-in user', posted.every((p) => p.user_id === 'u1'));
  ok('the profile is stored with the dive',
     posted.some((p) => Array.isArray(p.samples) && p.samples.length === 3),
     JSON.stringify(posted.map((p) => (p.samples || []).length)));
  ok('the dialog closes when it is done',
     !doc.querySelector('#im-file'), 'dialog still open');
  ok('no runtime errors', errors.length === 0, errors.join(' | '));

  console.log(failed ? `\n${failed} failed` : '\nall passed');
  process.exit(failed ? 1 : 0);
})();
