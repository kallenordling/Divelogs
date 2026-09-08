# DeepLog web

A single-file web front end for the dive log. Sign in with the **same email and
password as the phone app** — both talk to the same Supabase project, so dives
uploaded from the phone show up here straight away.

- `index.html` — the whole app: no build step, no dependencies to install
- `test/` — jsdom tests that drive the real UI against a stubbed backend

## What it does

- Email/password sign-in and sign-up against Supabase Auth, with automatic
  access-token refresh; the session is kept in `localStorage`
- Dive list with search over site, date and dive computer
- Per-dive detail: depth and temperature profile drawn as inline SVG, plus gas
  mixes and cylinder pressures
- Stats: dive count, time underwater, deepest, coldest, sites, dives per year
- Map of every dive that has site coordinates (Leaflet + OpenStreetMap tiles)

## Deploying

It is a static file. Any host will do:

```bash
# GitHub Pages: serve the web/ directory from your default branch,
# then open https://<user>.github.io/<repo>/

# Or locally:
cd web && python3 -m http.server 8080   # http://localhost:8080
```

Open it over `http(s)://`, not `file://` — browsers block `fetch` from
`file://` origins.

## Running the tests

```bash
cd web/test
npm install      # jsdom
npm test
```

The tests boot `index.html` in jsdom with a fake Supabase and check sign-in,
bad-password handling, the dive list and its ordering, the profile chart,
cylinder maths, stats, sign-out, token refresh, and that a site name containing
markup is escaped rather than rendered.

## A note on the anon key

`index.html` contains the Supabase **anon** key. That is a publishable
identifier, not a secret — the Android app already ships it, and it grants
nothing by itself.

What actually protects the data is **row-level security** in Postgres. Before
putting this on a public URL, confirm the `dives` table has RLS enabled with
policies keyed on the signed-in user, for example:

```sql
alter table dives enable row level security;

create policy "read own dives"   on dives for select using (auth.uid() = user_id);
create policy "insert own dives" on dives for insert with check (auth.uid() = user_id);
create policy "update own dives" on dives for update using (auth.uid() = user_id);
create policy "delete own dives" on dives for delete using (auth.uid() = user_id);
```

An unauthenticated read of the table currently returns `[]` rather than rows,
which is consistent with RLS being on — but that is worth confirming in the
Supabase dashboard rather than inferring, since an empty table looks identical.
