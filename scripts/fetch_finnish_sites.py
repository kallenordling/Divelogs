#!/usr/bin/env python3
"""Rebuild web/finnish_sites.json from the DeepLog Google My Maps map.

The map at

    https://www.google.com/maps/d/viewer?mid=1GoyVpKrxdGMYhXkX5B6fr5ShrnphJhU

is the good source for Finnish diving: lakes, quarries, mines, shore entries
and boat sites including the Baltic wrecks. OpenStreetMap has almost none of
these, which is why the bundled OSM catalogue looks so thin over Finland.

Google serves it as KMZ (a zip around doc.kml) with a permissive CORS header,
so the browser could in principle fetch it live — but it would then need a zip
decoder and a network round trip on every visit. Snapshotting it here instead
keeps the page fast and working offline. Re-run this whenever the map changes:

    python3 scripts/fetch_finnish_sites.py
"""

import html
import io
import json
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

MID = "1GoyVpKrxdGMYhXkX5B6fr5ShrnphJhU"
KML_URL = f"https://www.google.com/maps/d/kml?mid={MID}"
VIEWER_URL = f"https://www.google.com/maps/d/viewer?mid={MID}"
OUT = Path(__file__).resolve().parent.parent / "web" / "finnish_sites.json"

KML_NS = {"k": "http://www.opengis.net/kml/2.2"}

# The map's own Finnish folder names, and what they mean for the map legend.
# "kind" drives how the web app draws them: dive sites are what you plan a dive
# around, the others are context.
CATEGORIES = {
    "Järvikohde":          ("Lake site",       "site"),
    "Merenrantakohde":     ("Sea shore site",  "site"),
    "Louhokset ja montut": ("Quarry",          "site"),
    "Kaivos":              ("Mine",            "site"),
    "Venekohde":           ("Boat site",       "site"),
    "Sukellusseurat":      ("Dive club",       "club"),
    "Majoitus":            ("Accommodation",   "lodging"),
}


def fetch_kml() -> str:
    req = urllib.request.Request(KML_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read()
    # Google returns KMZ; older exports were bare KML, so handle both.
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            name = next((n for n in z.namelist() if n.endswith(".kml")), None)
            if not name:
                sys.exit("No .kml inside the KMZ")
            return z.read(name).decode("utf-8")
    return raw.decode("utf-8")


def clean_description(raw: str | None) -> tuple[str | None, str | None]:
    """Return (text, source_url) from a My Maps description blob.

    The descriptions are hand-written HTML mixing an image, a "Lähde:" source
    link and a "kuvaus:" prose section, so pull out the prose and the first
    link and drop the markup.
    """
    if not raw:
        return None, None

    source = None
    m = re.search(r"(?:Lähde|Sivu|Source)\s*:\s*(https?://\S+?)(?:<|\s|$)", raw, re.I)
    if m:
        source = m.group(1).rstrip(".,;")

    # Prefer the prose after "kuvaus:" (Finnish for "description").
    body = raw
    m = re.search(r"kuvaus\s*:\s*(.*)", raw, re.I | re.S)
    if m:
        body = m.group(1)

    body = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
    body = re.sub(r"<[^>]+>", " ", body)          # drop remaining tags
    body = html.unescape(body)
    body = re.sub(r"https?://\S+", "", body)      # links live in `source`
    body = re.sub(r"[ \t]+", " ", body)
    body = re.sub(r"\n\s*\n+", "\n", body).strip()

    if len(body) > 400:
        body = body[:397].rstrip() + "…"
    return (body or None), source


def main() -> None:
    root = ET.fromstring(fetch_kml())
    sites, skipped = [], []

    for folder in root.iter("{http://www.opengis.net/kml/2.2}Folder"):
        fname_el = folder.find("k:name", KML_NS)
        folder_name = (fname_el.text or "").strip() if fname_el is not None else ""
        label, kind = CATEGORIES.get(folder_name, (folder_name or "Dive site", "site"))

        for pm in folder.findall("k:Placemark", KML_NS):
            name_el = pm.find("k:name", KML_NS)
            name = (name_el.text or "").strip() if name_el is not None else ""

            coord_el = pm.find(".//k:Point/k:coordinates", KML_NS)
            if not name or coord_el is None or not (coord_el.text or "").strip():
                skipped.append(name or "(unnamed)")
                continue

            # KML orders coordinates lon,lat[,alt].
            parts = coord_el.text.strip().split(",")
            try:
                lon, lat = float(parts[0]), float(parts[1])
            except (ValueError, IndexError):
                skipped.append(name)
                continue

            desc_el = pm.find("k:description", KML_NS)
            text, source = clean_description(desc_el.text if desc_el is not None else None)

            entry = {
                "name": name,
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "country": "Finland",
                "desc": label,
                "kind": kind,
            }
            if text:
                entry["notes"] = text
            if source:
                entry["source"] = source
            sites.append(entry)

    sites.sort(key=lambda s: (s["desc"], s["name"]))

    doc = {
        "metadata": {
            "source": "DeepLog dive site map on Google My Maps",
            "source_url": VIEWER_URL,
            "note": "Compiled by the map's author from Finnish diving sources; "
                    "each entry keeps its own source link where one was given.",
            "count": len(sites),
        },
        "sites": sites,
    }
    OUT.write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    by_kind: dict[str, int] = {}
    for s in sites:
        by_kind[s["desc"]] = by_kind.get(s["desc"], 0) + 1
    print(f"Wrote {OUT.relative_to(Path.cwd())}: {len(sites)} entries")
    for k, n in sorted(by_kind.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4d}  {k}")
    if skipped:
        print(f"  skipped {len(skipped)} placemark(s) with no name or point")


if __name__ == "__main__":
    main()
