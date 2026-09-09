/* Supabase: authentication, session persistence and the dives table.
   The wire behaviour here is unchanged from before the redesign — same
   endpoints, same headers, same refresh-on-401 retry. */
'use strict';

(() => {
const DL = window.DL;

// The anon key is a publishable identifier, not a secret: it is already in the
// Android app. Row-level security is what protects the data.
const SUPABASE_URL = 'https://bdquivweiecffyopsevs.supabase.co';
const ANON_KEY =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9' +
  '.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJkcXVpdndlaWVjZmZ5b3BzZXZzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIyMTM2MjYsImV4cCI6MjA4Nzc4OTYyNn0' +
  '.wXLAcj5NeyVnO2nTB5ZzNWwe_hFtPkZYHBKkMT2FmAo';

const STORE_KEY = 'deeplog.session';

DL.state = { session: null, dives: [] };

// ── Session ────────────────────────────────────────────────────────────────

function loadSession() {
  try { return JSON.parse(localStorage.getItem(STORE_KEY) || 'null'); }
  catch { return null; }
}

function saveSession(s) {
  DL.state.session = s;
  try {
    if (s) localStorage.setItem(STORE_KEY, JSON.stringify(s));
    else localStorage.removeItem(STORE_KEY);
  } catch { /* private browsing: this tab only */ }
}

DL.loadStoredSession = () => { DL.state.session = loadSession(); return DL.state.session; };
DL.clearSession = () => saveSession(null);

async function authRequest(path, body) {
  const res = await fetch(`${SUPABASE_URL}/auth/v1/${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', apikey: ANON_KEY },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error_description || data.msg || data.message ||
                    `Sign-in failed (${res.status})`);
  }
  return data;
}

function storeAuth(data) {
  saveSession({
    access_token:  data.access_token,
    refresh_token: data.refresh_token,
    email:         data.user && data.user.email,
    user_id:       data.user && data.user.id,
  });
}

DL.signIn = async (email, password) => {
  storeAuth(await authRequest('token?grant_type=password', { email, password }));
};

/** @returns true when a session came back; false when the email needs confirming. */
DL.signUp = async (email, password) => {
  const data = await authRequest('signup', { email, password });
  if (data.access_token) storeAuth(data);
  return !!data.access_token;
};

async function refreshSession() {
  const s = DL.state.session;
  if (!s || !s.refresh_token) return false;
  try {
    storeAuth(await authRequest('token?grant_type=refresh_token',
                                { refresh_token: s.refresh_token }));
    return true;
  } catch { return false; }
}

// ── REST ───────────────────────────────────────────────────────────────────

DL.rest = async function rest(path, opts = {}) {
  const { retry = true, method = 'GET', body = null, prefer = null } = opts;
  const headers = {
    apikey: ANON_KEY,
    Authorization: `Bearer ${DL.state.session.access_token}`,
    Accept: 'application/json',
  };
  if (body) headers['Content-Type'] = 'application/json';
  if (prefer) headers.Prefer = prefer;

  const res = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    method, headers, body: body ? JSON.stringify(body) : undefined,
  });

  // Access tokens are short-lived; swap in a fresh one and try once more.
  if (res.status === 401 && retry) {
    if (await refreshSession()) return rest(path, { ...opts, retry: false });
    DL.onSessionLost && DL.onSessionLost();
    throw new Error('Session expired — please sign in again');
  }
  if (res.status === 403) {
    throw new Error('The database refused that change (403). The dives table ' +
                    'likely has no policy allowing it for your account.');
  }
  if (!res.ok) {
    let detail = '';
    try { detail = (await res.json()).message || ''; } catch { /* no body */ }
    throw new Error(`Request failed (${res.status})${detail ? ': ' + detail : ''}`);
  }
  if (res.status === 204) return null;
  return res.json().catch(() => null);
};

// ── Dives ──────────────────────────────────────────────────────────────────

DL.fetchDives = () => DL.rest('dives?select=*&order=date.desc,time.desc');

/** Columns the table actually has. Anything else is silently dropped. */
const DIVE_COLUMNS = [
  'date', 'time', 'maxdepth', 'avgdepth', 'duration', 'divemode',
  'temp_surface', 'temp_min', 'temp_max', 'atmospheric', 'salinity_type',
  'gasmixes', 'tanks', 'samples', 'site_name', 'site_lat', 'site_lon',
  'device_name',
];

DL.pickColumns = (fields) => {
  const out = {};
  for (const k of DIVE_COLUMNS) if (k in fields) out[k] = fields[k];
  return out;
};

DL.createDive = async (fields) => {
  const row = DL.pickColumns(fields);
  row.user_id = DL.state.session.user_id;
  const created = await DL.rest('dives', {
    method: 'POST', body: row, prefer: 'return=representation',
  });
  return Array.isArray(created) ? created[0] : created;
};

DL.updateDive = async (id, fields) => {
  if (id == null) throw new Error('That dive has no id, so it cannot be changed');
  await DL.rest(`dives?id=eq.${encodeURIComponent(id)}`, {
    method: 'PATCH', body: DL.pickColumns(fields), prefer: 'return=minimal',
  });
};

DL.deleteDive = async (id) => {
  if (id == null) throw new Error('That dive has no id, so it cannot be deleted');
  await DL.rest(`dives?id=eq.${encodeURIComponent(id)}`, {
    method: 'DELETE', prefer: 'return=minimal',
  });
};

/** Sets site name and position on a batch of dives. */
DL.applySiteTo = async (targets, site) => {
  const fields = {
    site_name: site.name,
    site_lat: site.lat == null ? null : Number(site.lat),
    site_lon: site.lon == null ? null : Number(site.lon),
  };
  for (const d of targets) {
    await DL.updateDive(d.id, fields);
    Object.assign(d, fields);       // keep the local copy in step
  }
  return targets.length;
};
})();
