-- Public, aggregated water data for DeepLog's site pages.
--
-- Dives themselves stay private: row-level security on `dives` keeps every
-- row to its owner, and nothing here changes that. What these two views
-- publish is the water, not the diver — temperature averaged across everyone
-- who logged a dive at a site, by day and by depth band, with no owner, no
-- dive identity, no depth or duration of anyone's dive.
--
-- Run this once in the Supabase SQL editor (Dashboard → SQL → New query).
-- Re-running it is safe: both views are replaced.
--
-- Note on how this works: a view executes with its owner's rights, so it can
-- read across all rows while `dives` itself stays locked down. That is the
-- point here, and the reason each view must expose only aggregates. Do not
-- add columns from `dives` to them without thinking about who may read it.

-- ── Temperature by site, day and depth band ────────────────────────────────
--
-- One row per site, date and whole metre of depth, averaging every sample
-- any diver recorded in that band that day. `readings` says how many samples
-- went into the average, so the app can tell a well-measured band from a
-- single passing glance.

create or replace view public.site_water as
select
  d.site_name,
  d.date,
  floor((s->>1)::numeric)::int              as depth_m,
  round(avg((s->>2)::numeric), 2)::float8   as temp_c,
  count(*)                                  as readings
from public.dives d
cross join lateral jsonb_array_elements(d.samples) as s
where coalesce(d.site_name, '') <> ''
  and jsonb_typeof(d.samples) = 'array'
  -- Guard against junk: depths within a diveable range, temperatures within
  -- what liquid water can be, and the zeroes computers write for "no reading".
  and (s->>1) ~ '^-?[0-9.]+$'
  and (s->>2) ~ '^-?[0-9.]+$'
  and (s->>1)::numeric between 0 and 350
  and (s->>2)::numeric between -2 and 40
  and (s->>2)::numeric <> 0
group by d.site_name, d.date, floor((s->>1)::numeric)::int;

-- ── Where those sites are ──────────────────────────────────────────────────
--
-- Sites people have logged that are not in the bundled catalogue, so a
-- visitor can find them on the map. Position only; no counts, no dates.

create or replace view public.site_places as
select
  d.site_name,
  round(avg(d.site_lat)::numeric, 5)::float8 as lat,
  round(avg(d.site_lon)::numeric, 5)::float8 as lon
from public.dives d
where coalesce(d.site_name, '') <> ''
  and d.site_lat is not null and d.site_lon is not null
group by d.site_name;

-- Readable by anyone, including visitors who are not signed in. Nothing else
-- is granted: `dives` remains private to its owner.
grant select on public.site_water  to anon, authenticated;
grant select on public.site_places to anon, authenticated;

-- A site page asks for one site at a time, so an index on the filter pays off
-- as the log grows. (Harmless if it already exists.)
create index if not exists dives_site_name_idx on public.dives (site_name);
