# DeepLog web

A single-file web front end for the dive log. Sign in with the **same email and
password as the phone app** — both talk to the same Supabase project, so dives
uploaded from the phone show up here straight away.

- `index.html` — the whole app: no build step, no dependencies to install
- `dive_sites.json` — a copy of `app/src/main/assets/dive_sites.json`, used to
  turn a dive's site *name* into coordinates. A unit test fails if the two
  copies drift, so re-copy it whenever the app asset changes.
- `test/` — jsdom tests that drive the real UI against a stubbed backend

## What it does

- Email/password sign-in and sign-up against Supabase Auth, with automatic
  access-token refresh; the session is kept in `localStorage`
- Dive list with search over site, date and dive computer
- Per-dive detail: depth and temperature profile drawn as inline SVG, plus gas
  mixes and cylinder pressures
- Stats: dive count, time underwater, deepest, coldest, sites, dives per year
- Sites: every site you have dived, with the number of recorded dives, and a
  combined profile of all dives there — time across, depth down, each segment
  coloured by the water temperature recorded at that point
- Map of your dive sites (Leaflet + OpenStreetMap tiles)

### Setting dive sites

The phone app only records a site when you pick one by hand, so most dives
arrive with `site_name` null and nothing to place them by. You can set sites
here instead:

- **Dive log → Set sites…** → *Without a site* selects every dive missing one,
  then *Set site…* applies one site to all of them at once
- or open a single dive and use **Set dive site**

Either way you search the bundled catalogue, or type a name and optional
coordinates for somewhere that is not in it. The change is written straight
back to the dive rows, so the phone app sees it too. This needs an `update`
row-level-security policy on `dives`; without one the write is refused with a
403 and the page says so.

### Why the map may look empty

The phone app only stores coordinates on a dive when its site name matched a
site you had already saved (`MainActivity.kt`, `uploadNewDives`). Dives logged
against an unsaved site arrive with a name and no position.

The web app works around this by looking the name up in `dive_sites.json`, so
anything in the bundled catalogue is placed automatically. Names that are in
neither place cannot be placed — the Map tab says how many dives that affects,
and the fix is to save the site in the phone app under **Sites**.

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
