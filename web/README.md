# DeepLog web

A single-file web front end for the dive log. Sign in with the **same email and
password as the phone app** — both talk to the same Supabase project, so dives
uploaded from the phone show up here straight away.

- `index.html` — the whole app: no build step, no dependencies to install
- `dive_sites.json` — a copy of `app/src/main/assets/dive_sites.json`
  (OpenStreetMap, ODbL). A unit test fails if the two copies drift, so re-copy
  it whenever the app asset changes.
- `finnish_sites.json` — 354 Finnish entries snapshotted from the DeepLog
  [Google My Maps map](https://www.google.com/maps/d/viewer?mid=1GoyVpKrxdGMYhXkX5B6fr5ShrnphJhU).
  OpenStreetMap has almost nothing over Finland — 13 sites nationwide — so
  without this the Baltic wrecks, lakes and quarries are simply missing.
  Regenerate with `python3 scripts/fetch_finnish_sites.py`.
- `test/` — jsdom tests that drive the real UI against a stubbed backend

## What it does

- **Dives** — the home screen: total dives, time underwater, max depth and last
  dive, then a card per dive leading with site, place and date, with depth,
  duration, water temperature and gas, and a small profile sparkline
- **Dive detail** — headline metrics, a full dive-computer style profile,
  gas and cylinder tables, and edit / delete
- **Log dive** — a sectioned form (dive, profile, conditions, gas), reachable
  from the top bar, the mobile centre button, a site page or a map marker.
  It remembers the values you repeat
- **Sites** — every site you have dived plus the whole catalogue, searchable
  and filtered by dived / not dived / type / nearby, with "Dived N times"
- **Site detail** — dive count, deepest, total time, temperature range, and a
  combined profile of all dives there coloured by water temperature
- **Map** — the whole catalogue with your own dives picked out, floating
  search, layer chips and a bottom sheet per site
- **Statistics** — summary plus Overview / Depth / Temperature / Gas /
  Locations tabs

## Layout

    index.html      markup shell and the icon sprite
    css/app.css     the design system: tokens, type scale, components
    js/util.js      formatting, escaping, icons
    js/api.js       Supabase auth, session, dive create/read/update/delete
    js/catalogue.js the bundled site catalogues and site matching
    js/charts.js    profiles, trends and bars, all inline SVG
    js/ui.js        reusable components: metric, cards, sheet, site picker
    js/views.js     the screens
    js/map.js       the map
    js/logdive.js   the log-dive form
    js/app.js       boot, sign-in, navigation

No framework and no build step: plain scripts in load order.

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

`npm run live` is different: it fetches the **deployed** page and data files
and checks what the map actually draws over Finland. Use it when the site
looks wrong in a browser, to tell a code problem from a stale deploy or a
cached tab.

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
