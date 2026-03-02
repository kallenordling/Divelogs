# DeepLog Bridge — Android BLE Companion

Minimal Android app (~300 lines Kotlin) that:
- Runs a local HTTP server on `localhost:8765`
- Exposes the same REST API as `suunto_bridge.py`
- Handles BLE download for EON Steel (passkey via Android OS) and Perdix 2
- Uploads dives directly to Supabase
- Opens DeepLog in Chrome

**The web app needs zero changes** — it already has the Bridge tab.

## Build

### Prerequisites
- Android Studio Hedgehog or later (or just the Android SDK + Gradle)
- Android phone running Android 8.0+ (API 26)
- JDK 17

### Steps

```bash
# Clone / open project
cd deeplog_bridge

# Before building: set your DeepLog URL in MainActivity.kt
# Find: private const val DEEPLOG_URL = "https://YOUR_NAME.github.io/deeplog"
# Replace with your actual GitHub Pages / Netlify URL

# Build debug APK
./gradlew assembleDebug

# APK at:
# app/build/outputs/apk/debug/app-debug.apk

# Build release APK (smaller, needs signing key)
./gradlew assembleRelease
```

### Install directly (no Play Store)

```bash
# Via ADB
adb install app/build/outputs/apk/debug/app-debug.apk

# Or: copy APK to phone, enable "Install unknown apps" for your file manager, tap APK
```

## Usage

1. Open the **DeepLog Bridge** app
2. Enter your username (e.g. `kalle`)
3. Tap **Start Bridge** → green dot appears, notification shows in status bar
4. Tap **Open DeepLog in Chrome**
5. In DeepLog → **Bridge tab** → tap **↓ Download via Bridge**
6. App sends request to `localhost:8765/download`
7. Android scans for EON Steel / Perdix 2
8. **EON Steel only:** device shows passkey → Android system dialog pops up → type passkey → tap Pair
9. Download proceeds, dives uploaded to Supabase automatically
10. DeepLog shows dives with ☁ synced badge

## API (same as suunto_bridge.py)

```
GET  /health       → {"ok":true, "dctool":"android-native"}
GET  /status       → {"status":"...", "progress":80, "message":"...", "dives":[...]}
POST /download     → {"username":"kalle", "address":"AA:BB:CC:DD:EE:FF"}
POST /reset        → {"ok":true}
```

## Architecture

```
Chrome on Android
  └── http://localhost:8765 ──► BridgeService (Kotlin foreground service)
                                    ├── NanoHTTPD (HTTP server)
                                    ├── BluetoothGattCallback (BLE)
                                    │     └── Android OS handles passkey dialog
                                    └── HttpURLConnection (Supabase upload)
```

## File structure

```
app/src/main/
  java/fi/deeplog/bridge/
    BridgeService.kt    ← All BLE logic + HTTP server (~280 lines)
    MainActivity.kt     ← Minimal UI: start/stop + open Chrome (~100 lines)
  res/
    layout/activity_main.xml
    drawable/           ← Status dot, button backgrounds
    values/             ← Strings, theme
  AndroidManifest.xml   ← BLE + foreground service permissions
```

## Why NanoHTTPD?

Single dependency, 60KB jar, no transitive deps, runs on the main thread fine for
our low-traffic use case (1 request per download). No OkHttp, no Ktor, no framework.

## Troubleshooting

**Bridge not reachable from Chrome**
- Make sure the Bridge app is running (green dot + notification visible)
- Chrome must be on the same device — this uses `localhost`, not your local network IP

**EON Steel not found in scan**
- Go to EON Steel: Settings → Connectivity → Airplane Mode: **Off**
- BLE scanning needs Location permission on Android 11 and below

**Passkey dialog doesn't appear**
- If previously paired: Android Settings → Bluetooth → forget the EON Steel, try again
- The dialog appears only on first pairing or after forgetting

**Upload fails but download works**
- Check internet connection
- Tap retry in DeepLog's dive list

## Comparison with Python bridge

| | Python bridge | Android bridge |
|---|---|---|
| Setup | pip + dctool install | APK install |
| Requires laptop | Yes | No |
| BLE passkey | Via OS (dctool) | Via OS (Android native) |
| Post-dive UX | Laptop present | Phone in pocket |
| Lines of code | ~350 Python | ~300 Kotlin |
| Web app changes | None | None |
