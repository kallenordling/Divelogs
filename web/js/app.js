/* Boot, sign-in and navigation between the views. */
'use strict';

(() => {
const DL = window.DL;

const VIEWS = ['dives', 'dive', 'sites', 'site', 'map', 'stats'];

/** Detail views keep their parent tab highlighted. */
const PARENT = { dive: 'dives', site: 'sites' };

DL.go = (name) => {
  for (const v of VIEWS) DL.el('view-' + v).classList.toggle('hidden', v !== name);
  const top = PARENT[name] || name;
  document.querySelectorAll('[data-nav]').forEach((b) =>
    b.classList.toggle('active', b.dataset.nav === top));
  window.scrollTo({ top: 0, behavior: 'instant' in window ? 'instant' : 'auto' });
  DL.current = name;
};

/** Navigates and renders. Detail views are opened by their own callers. */
DL.navigate = async (name) => {
  DL.go(name);
  if (name === 'dives') await DL.renderDives();
  if (name === 'sites') await DL.renderSites();
  if (name === 'stats') await DL.renderStats();
  if (name === 'map')   await DL.renderMap();
};

// ── Sign in ────────────────────────────────────────────────────────────────

function showLogin(message = '', kind = '') {
  DL.el('login-view').classList.remove('hidden');
  DL.el('app-view').classList.add('hidden');
  const el = DL.el('login-msg');
  el.textContent = message;
  el.className = 'msg' + (kind ? ' ' + kind : '');
}

function signOut() {
  DL.clearSession();
  DL.state.dives = [];
  DL.resetMap();
  showLogin();
}
DL.onSessionLost = signOut;

async function enterApp() {
  DL.el('login-view').classList.add('hidden');
  DL.el('app-view').classList.remove('hidden');

  const email = DL.state.session.email || '';
  const acct = DL.el('btn-account');
  acct.textContent = (email[0] || '?').toUpperCase();
  acct.title = email;

  await DL.navigate('dives');
}

async function reload() {
  DL.state.dives = await DL.fetchDives();
}

async function handleSignIn() {
  const email = DL.el('email').value.trim();
  const password = DL.el('password').value;
  if (!email || !password) return showLogin('Enter your email and password.', 'err');

  showLogin('Signing in…');
  try {
    await DL.signIn(email, password);
    await reload();
    await enterApp();
  } catch (e) {
    showLogin(e.message, 'err');
  }
}

async function handleSignUp() {
  const email = DL.el('email').value.trim();
  const password = DL.el('password').value;
  if (!email || !password) return showLogin('Enter an email and a password.', 'err');

  showLogin('Creating your account…');
  try {
    if (await DL.signUp(email, password)) { await reload(); await enterApp(); }
    else showLogin('Account created. Confirm your email, then sign in.', 'ok');
  } catch (e) {
    showLogin(e.message, 'err');
  }
}

// ── Wiring ─────────────────────────────────────────────────────────────────

DL.el('btn-signin').addEventListener('click', handleSignIn);
DL.el('btn-signup').addEventListener('click', handleSignUp);
DL.el('password').addEventListener('keydown', (e) => { if (e.key === 'Enter') handleSignIn(); });
DL.el('email').addEventListener('keydown', (e) => { if (e.key === 'Enter') handleSignIn(); });

document.querySelectorAll('[data-nav]').forEach((b) =>
  b.addEventListener('click', () => DL.navigate(b.dataset.nav)));

DL.el('nav-log').addEventListener('click', () => DL.openLogDive());
DL.el('tab-log').addEventListener('click', () => DL.openLogDive());

DL.el('btn-account').addEventListener('click', async () => {
  const email = DL.state.session ? DL.state.session.email : '';
  const ok = await DL.confirmSheet({
    title: 'Sign out?',
    detail: `You are signed in as ${email}. Your dives stay in the cloud.`,
    confirmLabel: 'Sign out',
  });
  if (ok) signOut();
});

// Back buttons inside rendered views.
document.addEventListener('click', (e) => {
  const b = e.target.closest && e.target.closest('[data-back]');
  if (b) DL.navigate(b.dataset.back);
});

// ── Boot ───────────────────────────────────────────────────────────────────

(async () => {
  // A stored session is only a hint; the first request proves whether it works.
  const s = DL.loadStoredSession();
  if (s && s.access_token) {
    try {
      await reload();
      await enterApp();
      return;
    } catch {
      DL.clearSession();
    }
  }
  showLogin();
})();
})();
