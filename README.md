# DeepLog

A dive log in two halves that share one account and one database:

- **`app/`** — a native Android app that downloads dives straight off a dive
  computer over Bluetooth, using libdivecomputer, and uploads them
- **`web/`** — a dive journal in the browser: log, sites, map and statistics,
  installable as an app on Android and iPhone

Both sign in to the same Supabase project, so a dive downloaded on the phone
appears in the browser and a site set in the browser appears on the phone.

---

## The Android app

Native Kotlin. Device support is **libdivecomputer**, vendored as a git
submodule and built with the NDK, reached through a JNI bridge in
`app/src/main/cpp/bridge.cpp`. That is where the supported computers come
from — Shearwater, Suunto, Aqualung/Oceanic, Cressi, Mares, Scubapro and the
rest — rather than from any hand-written protocol code.

Besides downloading, it imports dives from other logbooks: UDDF, Subsurface
XML, MacDive, DAN DL7, CSV, Shearwater Cloud databases and Garmin FIT files.

### Building

Needs JDK 17, the Android SDK, and NDK 27.0.12077973.

```bash
git clone --recurse-submodules git@github.com:kallenordling/Divelogs.git
cd Divelogs
./gradlew assembleDebug        # app/build/outputs/apk/debug/app-debug.apk
./gradlew testDebugUnitTest
```

The submodule matters: the NDK build compiles libdivecomputer's sources
directly, so a checkout without it fails at the first missing `.c` file. If you
already cloned without `--recurse-submodules`, run `git submodule update
--init --recursive`.

CI also generates `libdivecomputer/include/libdivecomputer/version.h` and
`src/revision.h`, which autotools would normally produce and the submodule
gitignores. Locally they are already present.

## The web app

Plain HTML, CSS and JavaScript — no framework and no build step. Deployed
free on GitHub Pages at
**<https://kallenordling.github.io/Divelogs/>**, and installable to the
home screen on both Android and iPhone. See [`web/README.md`](web/README.md)
for the layout, the tests, and the row-level-security policies worth
confirming before sharing the URL.

## Mobile shells

[`mobile/`](mobile/README.md) wraps the web app with Capacitor for iOS. The
Xcode project is generated during the build rather than committed. An iOS
build needs macOS, so it runs on a GitHub `macos` runner — free, because this
repository is public.

Android does not have a shell here on purpose: `app/` is already a native
Android app, and a second one would only be confusing.

## Site catalogues

The map and site browser are backed by two bundled files:

- `dive_sites.json` — worldwide, from OpenStreetMap under ODbL
- `finnish_sites.json` — 354 Finnish entries from the DeepLog
  [Google My Maps map](https://www.google.com/maps/d/viewer?mid=1GoyVpKrxdGMYhXkX5B6fr5ShrnphJhU),
  regenerated with `python3 scripts/fetch_finnish_sites.py`

Finland needs its own source: OpenStreetMap has thirteen dive sites in the
whole country, so without it the Baltic wrecks, quarries and lakes are simply
missing.

## Continuous integration

| Workflow | Runs | Produces |
|---|---|---|
| `build.yml` — Android APK | every push | `deeplog-apk` |
| `build.yml` — Web tests | every push | the web suites and the installable-app check |
| `ios.yml` — iOS app | on demand, and on `mobile/**` | a Simulator build and an unsigned `.ipa` |
| `pages.yml` — Deploy site | on `web/**` | publishes `web/` as the site root |

Run the iOS build with `gh workflow run ios.yml`. Neither iOS artifact installs
on a device as-is; see [`mobile/README.md`](mobile/README.md) for why, and what
signing would take.
